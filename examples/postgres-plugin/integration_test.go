//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// startPostgres runs a server container and returns the plugin's database
// and an admin connection to it.
func startPostgres(t *testing.T) (*database, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("relay"), tcpostgres.WithUsername("hikyaku"), tcpostgres.WithPassword("secret"),
		tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	return &database{url: url, maxConns: 4}, admin
}

func TestSinkInsertsRows(t *testing.T) {
	db, admin := startPostgres(t)
	ctx := context.Background()
	s, err := openSink(t, db, map[string]any{"table": "events", "create_table": true})
	require.NoError(t, err)

	env := envelope.New("webhooks", "http", []byte(`{"type":"invoice.paid","amount":12}`))
	res, err := s.Deliver(ctx, env)
	require.NoError(t, err)
	assert.Equal(t, `{"inserted":1}`, string(res.Body))
	assert.Equal(t, `"events"`, res.Endpoint)

	res, err = s.Deliver(ctx, env)
	require.NoError(t, err, "the same message again: the retry of one that reached the table")
	assert.Equal(t, `{"inserted":0}`, string(res.Body))

	var n int
	var typ string
	require.NoError(t, admin.QueryRow(ctx, `SELECT count(*), min(body->>'type') FROM events WHERE id = $1`, env.ID).Scan(&n, &typ))
	assert.Equal(t, 1, n, "one row for one message")
	assert.Equal(t, "invoice.paid", typ, "the body is jsonb")

	_, err = s.Deliver(ctx, envelope.New("webhooks", "http", []byte("not json")))
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe, "a jsonb column refuses text")
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"postgres:22P02"}, pe.Tags)
	assert.NoError(t, s.Health(ctx))

	// Columns of the message, into a table of the user's own.
	_, err = admin.Exec(ctx, `CREATE TABLE leads (msg text PRIMARY KEY, email text NOT NULL, amount int, raw jsonb)`)
	require.NoError(t, err)
	custom, err := openSink(t, db, map[string]any{"table": "leads", "on_conflict": "error",
		"columns": map[string]any{"msg": "$id", "email": "customer.email", "amount": "amount", "raw": "$body"}})
	require.NoError(t, err)
	lead := envelope.New("forms", "http", []byte(`{"customer":{"email":"ada@example.com"},"amount":12}`))
	_, err = custom.Deliver(ctx, lead)
	require.NoError(t, err)
	var email string
	var amount int
	require.NoError(t, admin.QueryRow(ctx, `SELECT email, amount FROM leads WHERE msg = $1`, lead.ID).Scan(&email, &amount))
	assert.Equal(t, "ada@example.com", email)
	assert.Equal(t, 12, amount)
	_, err = custom.Deliver(ctx, lead)
	require.ErrorAs(t, err, &pe, "on_conflict: error")
	assert.Equal(t, []string{"postgres:23505"}, pe.Tags)
	_, err = custom.Deliver(ctx, envelope.New("forms", "http", []byte(`{"amount":1}`)))
	require.ErrorAs(t, err, &pe, "NULL into a NOT NULL column")
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"postgres:23502"}, pe.Tags)
}

// settle returns the settle hook of a message, for the test to play the
// relay's outcome.
func settle(t *testing.T, env *envelope.Envelope, final bool) {
	t.Helper()
	outcome := envelope.Outcome{Status: envelope.Delivered}
	if !final {
		outcome.Status = envelope.Failed
	}
	env.Ack(context.Background(), outcome)
}

func TestSourceDeliversTheOutbox(t *testing.T) {
	db, admin := startPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := db.newSource(ctx, plugin.Spec{Name: "outbox", Kind: "postgres",
		Options: map[string]any{"table": "outbox", "interval": "100ms", "claim_ttl": "1s", "batch": 2, "create_table": true}})
	require.NoError(t, err)
	src := s.(*Source)
	defer func() { _ = src.Close(context.Background()) }()

	out := make(chan *envelope.Envelope, 16)
	done := make(chan error, 1)
	go func() { done <- src.Start(ctx, out) }()

	// The table is created by the first poll.
	require.Eventually(t, func() bool {
		var ok bool
		return admin.QueryRow(context.Background(), `SELECT to_regclass('outbox') IS NOT NULL`).Scan(&ok) == nil && ok
	}, 10*time.Second, 50*time.Millisecond)
	for i := 1; i <= 3; i++ {
		_, err := admin.Exec(context.Background(), `INSERT INTO outbox (subject, payload, headers) VALUES ($1, $2, $3)`,
			"order-1", `{"n":`+string(rune('0'+i))+`}`, `{"X-Event":"order.updated"}`)
		require.NoError(t, err)
	}

	next := func() *envelope.Envelope {
		t.Helper()
		select {
		case env := <-out:
			return env
		case <-time.After(10 * time.Second):
			t.Fatal("no message")
			return nil
		}
	}
	first, second, third := next(), next(), next()
	assert.Equal(t, "outbox-1", first.ID)
	assert.Equal(t, "outbox-2", second.ID)
	assert.Equal(t, "outbox-3", third.ID, "a full batch is followed by another poll at once")
	assert.JSONEq(t, `{"n":1}`, string(first.Body), "jsonb, as PostgreSQL formats it")
	assert.Equal(t, "order-1", first.Subject)
	assert.Equal(t, "order.updated", first.Header("X-Event"))

	claimed := func() int {
		var n int
		require.NoError(t, admin.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE claimed_at IS NOT NULL AND delivered_at IS NULL`).Scan(&n))
		return n
	}
	assert.Equal(t, 3, claimed(), "claimed rows are not emitted again")

	settle(t, first, true)
	settle(t, second, false)
	var delivered int
	require.Eventually(t, func() bool {
		return admin.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE delivered_at IS NOT NULL`).Scan(&delivered) == nil && delivered == 1
	}, 5*time.Second, 50*time.Millisecond, "a final outcome marks the row delivered")
	again := next()
	assert.Equal(t, "outbox-2", again.ID, "a failed one is released and emitted again, with the same ID")
	assert.JSONEq(t, `{"n":2}`, string(again.Body))

	// The third was never settled: its claim expires after claim_ttl.
	expired := next()
	assert.Equal(t, "outbox-3", expired.ID, "an expired claim is taken again")
	settle(t, again, true)
	settle(t, expired, true)
	require.Eventually(t, func() bool { return claimed() == 0 }, 5*time.Second, 50*time.Millisecond)
	assert.NoError(t, src.Health(context.Background()))

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}

	// on_delivered: delete, on a table that exists already.
	del, err := db.newSource(context.Background(), plugin.Spec{Name: "outbox", Kind: "postgres",
		Options: map[string]any{"table": "outbox", "interval": "100ms", "on_delivered": "delete"}})
	require.NoError(t, err)
	defer func() { _ = del.(*Source).Close(context.Background()) }()
	_, err = admin.Exec(context.Background(), `INSERT INTO outbox (payload) VALUES ('{"n":4}')`)
	require.NoError(t, err)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = del.Start(ctx2, out) }()
	fourth := next()
	assert.JSONEq(t, `{"n":4}`, string(fourth.Body))
	settle(t, fourth, true)
	require.Eventually(t, func() bool {
		var n int
		return admin.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE id = 4`).Scan(&n) == nil && n == 0
	}, 5*time.Second, 50*time.Millisecond, "deleted once delivered")
}
