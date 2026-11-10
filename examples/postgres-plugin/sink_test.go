package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// offline is a database nothing connects to: pgx opens a pool lazily, so
// options and SQL can be checked without a server.
var offline = &database{url: "postgres://hikyaku:secret@127.0.0.1:1/relay", maxConns: 1}

func openSink(t *testing.T, db *database, options map[string]any) (*Sink, error) {
	t.Helper()
	s, err := db.newSink(context.Background(), plugin.Spec{Name: "events", Kind: "postgres", Options: options})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s.(*Sink), nil
}

func TestSinkOptions(t *testing.T) {
	_, err := openSink(t, &database{}, map[string]any{"table": "events"})
	require.ErrorContains(t, err, "DATABASE_URL is not set")

	_, err = openSink(t, &database{url: "not a url"}, map[string]any{"table": "events"})
	require.ErrorContains(t, err, "DATABASE_URL")
	assert.NotContains(t, err.Error(), "secret")

	for _, bad := range []struct {
		options map[string]any
		want    string
	}{
		{map[string]any{"table": "events", "tabel": "x"}, "tabel"},
		{map[string]any{"table": "a.b.c"}, "name or schema.name"},
		{map[string]any{"table": "drop table;"}, "letters, digits and _"},
		{map[string]any{"table": "events", "columns": map[string]any{"bad name": "$id"}}, "not a column name"},
		{map[string]any{"table": "events", "columns": map[string]any{"id": "$nope"}}, "neither one of"},
		{map[string]any{"table": "events", "columns": map[string]any{"id": "a..b"}}, "neither one of"},
		{map[string]any{"table": "events", "columns": map[string]any{"id": "$id"}, "create_table": true}, "create_table"},
		{map[string]any{"table": "events", "on_conflict": "update"}, "nothing or error"},
	} {
		_, err := openSink(t, offline, bad.options)
		require.ErrorContains(t, err, bad.want, "%v", bad.options)
	}
}

func TestInsertSQL(t *testing.T) {
	s, err := openSink(t, offline, map[string]any{"table": "events"})
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "events" ("id", "source", "received_at", "body") VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, s.insert)
	assert.Equal(t, []string{"$id", "$source", "$received_at", "$body"}, s.values)
	assert.Empty(t, s.create)

	s, err = openSink(t, offline, map[string]any{"table": "events", "create_table": true})
	require.NoError(t, err)
	assert.Contains(t, s.create, `CREATE TABLE IF NOT EXISTS "events" (id text PRIMARY KEY`)

	s, err = openSink(t, offline, map[string]any{
		"table":       "app.leads",
		"on_conflict": "error",
		"columns":     map[string]any{"email": "customer.email", "msg": "$id", "raw": "$body"},
	})
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "app"."leads" ("email", "msg", "raw") VALUES ($1, $2, $3)`, s.insert, "columns in name order, no conflict clause")
	assert.Equal(t, []string{"customer.email", "$id", "$body"}, s.values)
}

func TestRowValues(t *testing.T) {
	env := envelope.New("webhooks", "http", []byte(`{"customer":{"email":"ada@example.com","tags":["a","b"]},"amount":12,"ratio":0.5,"paid":true,"note":null}`))
	env.ID = "01J"
	env.Subject = "invoice.paid"
	env.ContentType = "application/json"
	env.ReceivedAt = time.Date(2026, 11, 10, 12, 0, 0, 0, time.FixedZone("x", 3600))
	env.SetHeader("X-Event", "invoice.paid")
	env.SetAttribute("http.path", "/ingest")

	got, err := rowValues(env, []string{"$id", "$source", "$subject", "$content_type", "$received_at", "$body", "$headers", "$attributes",
		"customer.email", "customer.tags.1", "customer", "amount", "ratio", "paid", "note", "missing.deep"})
	require.NoError(t, err)
	assert.Equal(t, "01J", got[0])
	assert.Equal(t, "webhooks", got[1])
	assert.Equal(t, "invoice.paid", got[2])
	assert.Equal(t, "application/json", got[3])
	assert.Equal(t, time.Date(2026, 11, 10, 11, 0, 0, 0, time.UTC), got[4], "received_at in UTC")
	assert.JSONEq(t, string(env.Body), got[5].(string))
	assert.JSONEq(t, `{"X-Event":["invoice.paid"]}`, got[6].(string))
	assert.JSONEq(t, `{"http.path":"/ingest"}`, got[7].(string))
	assert.Equal(t, "ada@example.com", got[8])
	assert.Equal(t, "b", got[9], "an array index is a path segment")
	assert.JSONEq(t, `{"email":"ada@example.com","tags":["a","b"]}`, got[10].(string), "an object is JSON text")
	assert.Equal(t, int64(12), got[11])
	assert.Equal(t, 0.5, got[12])
	assert.Equal(t, true, got[13])
	assert.Nil(t, got[14])
	assert.Nil(t, got[15], "a missing path is NULL")

	plain := envelope.New("webhooks", "http", []byte("hello"))
	got, err = rowValues(plain, []string{"$subject", "$body"})
	require.NoError(t, err)
	assert.Nil(t, got[0], "an empty subject is NULL")
	assert.Equal(t, "hello", got[1], "$body needs no JSON")

	_, err = rowValues(plain, []string{"customer.email"})
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"postgres:invalid"}, pe.Tags)
}

func TestClassify(t *testing.T) {
	for code, class := range map[string]plugin.Class{
		"08006": plugin.Transient, // connection failure
		"53300": plugin.Transient, // too many connections
		"57P01": plugin.Transient, // admin shutdown
		"40001": plugin.Transient, // serialization failure
		"40P01": plugin.Transient, // deadlock
		"23505": plugin.Permanent, // unique violation
		"22P02": plugin.Permanent, // invalid text representation (not JSON for jsonb)
		"42P01": plugin.Permanent, // undefined table
	} {
		err := classify(&pgconn.PgError{Code: code, Message: "m"})
		var pe *plugin.Error
		require.ErrorAs(t, err, &pe, code)
		assert.Equal(t, class, pe.Class, code)
		assert.Equal(t, []string{"postgres:" + code}, pe.Tags, code)
	}
	plain := errors.New("dial tcp: connection refused")
	assert.Same(t, plain, classify(plain), "hikyaku classifies the rest")
	assert.NoError(t, classify(nil))
}

func TestSinkHealthWithoutAServer(t *testing.T) {
	s, err := openSink(t, offline, map[string]any{"table": "events"})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.Error(t, s.Health(ctx), "nothing listens on port 1")
}
