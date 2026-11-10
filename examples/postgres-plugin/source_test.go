package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

func openSource(t *testing.T, options map[string]any) (*Source, error) {
	t.Helper()
	s, err := offline.newSource(context.Background(), plugin.Spec{Name: "outbox", Kind: "postgres", Options: options})
	if err != nil {
		return nil, err
	}
	src := s.(*Source)
	t.Cleanup(func() { _ = src.Close(context.Background()) })
	return src, nil
}

func TestSourceOptions(t *testing.T) {
	s, err := openSource(t, map[string]any{"table": "outbox"})
	require.NoError(t, err)
	assert.Equal(t, time.Second, s.interval)
	assert.Equal(t, 5*time.Minute, s.claimTTL)
	assert.Equal(t, 100, s.batch)
	assert.Equal(t, "delivered", s.ack)
	assert.Equal(t, `UPDATE "outbox" SET delivered_at = now() WHERE id = $1`, s.final)
	assert.Equal(t, `UPDATE "outbox" SET claimed_at = NULL WHERE id = $1`, s.release)
	assert.Contains(t, s.claim, `FROM "outbox" WHERE delivered_at IS NULL AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => $1)) ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED`)
	assert.Empty(t, s.create)

	s, err = openSource(t, map[string]any{"table": "app.outbox", "interval": "250ms", "claim_ttl": "30s", "batch": 7, "on_delivered": "delete", "create_table": true})
	require.NoError(t, err)
	assert.Equal(t, 250*time.Millisecond, s.interval)
	assert.Equal(t, 30*time.Second, s.claimTTL)
	assert.Equal(t, 7, s.batch)
	assert.Equal(t, `DELETE FROM "app"."outbox" WHERE id = $1`, s.final)
	require.Len(t, s.create, 2)
	assert.Contains(t, s.create[0], `CREATE TABLE IF NOT EXISTS "app"."outbox" (id bigserial PRIMARY KEY`)
	assert.Contains(t, s.create[1], `CREATE INDEX IF NOT EXISTS "outbox_pending" ON "app"."outbox" (id) WHERE delivered_at IS NULL`)

	for _, bad := range []struct {
		options map[string]any
		want    string
	}{
		{map[string]any{"table": "outbox", "intervall": "1s"}, "intervall"},
		{map[string]any{"table": "outbox", "interval": "soon"}, "options.interval"},
		{map[string]any{"table": "outbox", "interval": "-1s"}, "options.interval"},
		{map[string]any{"table": "outbox", "claim_ttl": "0s"}, "options.claim_ttl"},
		{map[string]any{"table": "outbox", "batch": -1}, "options.batch"},
		{map[string]any{"table": "outbox", "on_delivered": "archive"}, "mark or delete"},
		{map[string]any{"table": "1outbox"}, "options.table"},
	} {
		_, err := openSource(t, bad.options)
		require.ErrorContains(t, err, bad.want, "%v", bad.options)
	}
}

func TestEnvelopeOfARow(t *testing.T) {
	s, err := openSource(t, map[string]any{"table": "outbox"})
	require.NoError(t, err)
	subject := "order-42"
	env := s.envelope(row{ID: 7, Subject: &subject, Payload: []byte(`{"n": 1}`), Headers: []byte(`{"X-Event":"order.paid","X-Tags":["a","b"],"X-Odd":3}`)})
	assert.Equal(t, "outbox-7", env.ID, "stable across redeliveries")
	assert.Equal(t, "outbox", env.Source)
	assert.Equal(t, "postgres", env.Protocol)
	assert.Equal(t, "order-42", env.Subject)
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, `{"n":1}`, string(env.Body), "jsonb's spacing is dropped")
	assert.Equal(t, "order.paid", env.Header("X-Event"))
	assert.Equal(t, []string{"a", "b"}, env.Headers["X-Tags"])
	assert.Empty(t, env.Headers["X-Odd"], "a header is a string or strings")
	key, _ := env.Attribute("ordering_key")
	assert.Equal(t, "order-42", key, "the subject orders the rows")
	id, _ := env.Attribute("postgres.id")
	assert.Equal(t, int64(7), id)
	table, _ := env.Attribute("postgres.table")
	assert.Equal(t, "outbox", table)

	env = s.envelope(row{ID: 8, Payload: []byte("plain text")})
	assert.Equal(t, "outbox-8", env.ID)
	assert.Equal(t, "plain text", string(env.Body))
	assert.Empty(t, env.Subject)
	assert.Empty(t, env.ContentType)
	_, ok := env.Attribute("ordering_key")
	assert.False(t, ok, "no subject, no ordering key")
}
