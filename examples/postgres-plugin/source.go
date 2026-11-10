package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin"
	"github.com/xavidop/hikyaku/plugin"
)

// SourceOptions are the settings of a postgres source: a transactional
// outbox table hikyaku polls.
type SourceOptions struct {
	Table       string `yaml:"table" jsonschema:"required,pattern=^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?$" jsonschema_description:"The outbox table: name, or schema.name. Its columns are id, subject, payload, headers, created_at, claimed_at and delivered_at."`
	Interval    string `yaml:"interval,omitempty" jsonschema:"pattern=^[0-9.]+(ns|us|µs|ms|s|m|h)$" jsonschema_description:"Time between polls once the table has nothing to deliver, a Go duration (default 1s)."`
	Batch       int    `yaml:"batch,omitempty" jsonschema:"minimum=1" jsonschema_description:"Rows claimed per poll (default 100)."`
	ClaimTTL    string `yaml:"claim_ttl,omitempty" jsonschema:"pattern=^[0-9.]+(ns|us|µs|ms|s|m|h)$" jsonschema_description:"A row claimed this long ago without an outcome is claimed again (default 5m), so a relay that died mid-delivery does not keep its rows."`
	OnDelivered string `yaml:"on_delivered,omitempty" jsonschema:"enum=mark,enum=delete" jsonschema_description:"mark (default): set delivered_at on a row hikyaku is done with; delete: remove the row."`
	CreateTable bool   `yaml:"create_table,omitempty" jsonschema_description:"Create the outbox table when it does not exist."`
}

// Source polls an outbox table. A poll claims up to batch rows nobody
// delivered or claimed (FOR UPDATE SKIP LOCKED, so several relays can share
// the table) and emits each as a message. hikyaku's outcome settles the
// row: final marks it delivered (or deletes it), failed releases it, so the
// next poll emits it again.
type Source struct {
	name     string
	table    string // the option, for logs and attributes
	ack      string
	interval time.Duration
	claimTTL time.Duration
	batch    int
	pool     *pgxpool.Pool
	log      *slog.Logger

	create  []string // empty when the table is not created
	claim   string
	final   string
	release string

	mu      sync.Mutex
	created bool
}

// row is a claimed outbox row.
type row struct {
	ID      int64
	Subject *string
	Payload []byte
	Headers []byte
}

// newSource is the postgres source type's factory.
func (d *database) newSource(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
	who := "postgres " + spec.Name
	var o SourceOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	table, err := tableName(who, o.Table)
	if err != nil {
		return nil, err
	}
	interval, err := durationOption(who, "interval", o.Interval, time.Second)
	if err != nil {
		return nil, err
	}
	claimTTL, err := durationOption(who, "claim_ttl", o.ClaimTTL, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	batch := o.Batch
	switch {
	case batch == 0:
		batch = 100
	case batch < 0:
		return nil, fmt.Errorf("%s: options.batch must be positive", who)
	}
	var final string
	switch o.OnDelivered {
	case "", "mark":
		final = "UPDATE " + table + " SET delivered_at = now() WHERE id = $1"
	case "delete":
		final = "DELETE FROM " + table + " WHERE id = $1"
	default:
		return nil, fmt.Errorf("%s: options.on_delivered %q: mark or delete", who, o.OnDelivered)
	}
	pool, err := d.open(ctx, who)
	if err != nil {
		return nil, err
	}
	s := &Source{
		name:     spec.Name,
		table:    o.Table,
		ack:      grpcplugin.AckMode(spec),
		interval: interval,
		claimTTL: claimTTL,
		batch:    batch,
		pool:     pool,
		log:      spec.Env.Log(),
		claim: "UPDATE " + table + " SET claimed_at = now() WHERE id IN (" +
			"SELECT id FROM " + table + " WHERE delivered_at IS NULL AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => $1)) " +
			"ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED) RETURNING id, subject, payload, headers",
		final:   final,
		release: "UPDATE " + table + " SET claimed_at = NULL WHERE id = $1",
	}
	if o.CreateTable {
		s.create = createOutboxSQL(table, o.Table)
	}
	s.log.Info("postgres source opened", "table", o.Table, "interval", interval, "batch", batch)
	return s, nil
}

// createOutboxSQL makes the outbox table and an index on its pending rows.
func createOutboxSQL(quoted, name string) []string {
	return []string{
		"CREATE TABLE IF NOT EXISTS " + quoted + " (" +
			"id bigserial PRIMARY KEY, subject text, payload jsonb NOT NULL, headers jsonb, " +
			"created_at timestamptz NOT NULL DEFAULT now(), claimed_at timestamptz, delivered_at timestamptz)",
		"CREATE INDEX IF NOT EXISTS " + pgx.Identifier{indexName(name)}.Sanitize() + " ON " + quoted + " (id) WHERE delivered_at IS NULL",
	}
}

func indexName(table string) string {
	for i := range table {
		if table[i] == '.' {
			table = table[i+1:]
			break
		}
	}
	return table + "_pending"
}

func durationOption(who, key, v string, def time.Duration) (time.Duration, error) {
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s: options.%s %q is not a positive duration", who, key, v)
	}
	return d, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Start polls until ctx ends. A full batch is followed by another poll at
// once; an empty or partial one by a wait of interval.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	for {
		rows, err := s.poll(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.Warn("postgres outbox poll failed", "table", s.table, "error", err)
		}
		for i, r := range rows {
			select {
			case out <- s.envelope(r):
			case <-ctx.Done():
				s.releaseAll(rows[i:]) // never handed over: the next relay claims them
				return nil
			}
		}
		if len(rows) == s.batch {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.interval):
		}
	}
}

// poll creates the table once when asked, then claims a batch.
func (s *Source) poll(ctx context.Context) ([]row, error) {
	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}
	rs, err := s.pool.Query(ctx, s.claim, s.claimTTL.Seconds(), s.batch)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rs, pgx.RowToStructByPos[row])
}

func (s *Source) ensureTable(ctx context.Context) error {
	if len(s.create) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.created {
		return nil
	}
	for _, stmt := range s.create {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	s.created = true
	return nil
}

// envelope is the message of a row. Its ID is stable (<source>-<row id>), so
// a destination that dedupes by ID sees a redelivery as the same message.
// The subject is the ordering key: rows that share one stay in order. A JSON
// payload is compacted: jsonb kept no formatting of its own.
func (s *Source) envelope(r row) *envelope.Envelope {
	payload := r.Payload
	isJSON := json.Valid(payload)
	if isJSON {
		var buf bytes.Buffer
		if json.Compact(&buf, payload) == nil {
			payload = buf.Bytes()
		}
	}
	env := envelope.New(s.name, "postgres", payload)
	env.ID = s.name + "-" + strconv.FormatInt(r.ID, 10)
	if isJSON {
		env.ContentType = "application/json"
	}
	if r.Subject != nil {
		env.Subject = *r.Subject
		env.SetAttribute("ordering_key", *r.Subject)
	}
	var headers map[string]any
	if len(r.Headers) > 0 && json.Unmarshal(r.Headers, &headers) == nil {
		for k, v := range headers {
			switch x := v.(type) {
			case string:
				env.SetHeader(k, x)
			case []any:
				for _, e := range x {
					if str, ok := e.(string); ok {
						env.Headers[k] = append(env.Headers[k], str)
					}
				}
			}
		}
	}
	env.SetAttribute("postgres.table", s.table)
	env.SetAttribute("postgres.id", r.ID)
	plugin.OnSettle(env, s.ack, func(ctx context.Context, final bool) { s.settle(ctx, r.ID, final) })
	return env
}

// settle marks a finished row delivered (or deletes it), and releases a
// failed one for the next poll. When the database refuses, the claim
// expires after claim_ttl and the row is delivered again: at least once.
func (s *Source) settle(ctx context.Context, id int64, final bool) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	stmt := s.release
	if final {
		stmt = s.final
	}
	if _, err := s.pool.Exec(ctx, stmt, id); err != nil {
		s.log.Warn("postgres outbox settle failed", "table", s.table, "id", id, "final", final, "error", err)
		return
	}
	s.log.Debug("postgres outbox row settled", "table", s.table, "id", id, "final", final)
}

func (s *Source) releaseAll(rows []row) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, r := range rows {
		if _, err := s.pool.Exec(ctx, s.release, r.ID); err != nil {
			s.log.Warn("postgres outbox release failed", "table", s.table, "id", r.ID, "error", err)
		}
	}
}

// Health asks the database for a round trip.
func (s *Source) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return classify(s.pool.Ping(ctx))
}

// Close returns the connections, once hikyaku has settled what was in
// flight.
func (s *Source) Close(context.Context) error {
	s.pool.Close()
	return nil
}
