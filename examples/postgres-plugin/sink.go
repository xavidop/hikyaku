package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// SinkOptions are the settings of a postgres destination, under options: in
// the spec. The yaml tags name the keys; the jsonschema tags become the JSON
// Schema hikyaku validates options: with, before it runs anything.
type SinkOptions struct {
	Table       string            `yaml:"table" jsonschema:"required,pattern=^[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?$" jsonschema_description:"Table the rows go into: name, or schema.name."`
	Columns     map[string]string `yaml:"columns,omitempty" jsonschema_description:"Column to value. A value is $id, $source, $subject, $content_type, $received_at, $body, $headers or $attributes, or a path into the JSON body (customer.email). Default: id, source, received_at and body."`
	OnConflict  string            `yaml:"on_conflict,omitempty" jsonschema:"enum=nothing,enum=error" jsonschema_description:"nothing (default): a row that exists already is left alone, so a retry of a message inserts no second row; error: the insert fails."`
	CreateTable bool              `yaml:"create_table,omitempty" jsonschema_description:"Create the default table (id text primary key, source text, received_at timestamptz, body jsonb) when it does not exist. Only without columns."`
}

// defaultColumns is the table a postgres destination writes without
// columns:, in order.
var defaultColumns = [][2]string{{"id", "$id"}, {"source", "$source"}, {"received_at", "$received_at"}, {"body", "$body"}}

// specials are the values that are not a path into the body.
var specials = []string{"$id", "$source", "$subject", "$content_type", "$received_at", "$body", "$headers", "$attributes"}

var pathSyntax = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

// Sink inserts one row per message.
type Sink struct {
	name   string
	table  string // quoted
	insert string
	values []string // one per column, in the insert's order
	create string   // "" when the table is not created
	pool   *pgxpool.Pool
	log    *slog.Logger

	mu      sync.Mutex
	created bool
}

// newSink is the postgres destination type's factory.
func (d *database) newSink(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
	who := "postgres " + spec.Name
	var o SinkOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	table, err := tableName(who, o.Table)
	if err != nil {
		return nil, err
	}
	columns := defaultColumns
	if len(o.Columns) > 0 {
		if o.CreateTable {
			return nil, fmt.Errorf("%s: options.create_table makes the default table: drop it, or drop options.columns", who)
		}
		columns = nil
		for _, name := range slices.Sorted(maps.Keys(o.Columns)) {
			value := o.Columns[name]
			if !identifier.MatchString(name) {
				return nil, fmt.Errorf("%s: options.columns: %q is not a column name", who, name)
			}
			if !slices.Contains(specials, value) && (strings.HasPrefix(value, "$") || !pathSyntax.MatchString(value)) {
				return nil, fmt.Errorf("%s: options.columns.%s: %q is neither one of %s nor a path into the body (customer.email)", who, name, value, strings.Join(specials, ", "))
			}
			columns = append(columns, [2]string{name, value})
		}
	}
	switch o.OnConflict {
	case "", "nothing", "error":
	default:
		return nil, fmt.Errorf("%s: options.on_conflict %q: nothing or error", who, o.OnConflict)
	}
	pool, err := d.open(ctx, who)
	if err != nil {
		return nil, err
	}
	s := &Sink{name: spec.Name, table: table, pool: pool, log: spec.Env.Log()}
	s.insert, s.values = insertSQL(table, columns, o.OnConflict != "error")
	if o.CreateTable {
		s.create = "CREATE TABLE IF NOT EXISTS " + table + " (id text PRIMARY KEY, source text, received_at timestamptz, body jsonb)"
	}
	s.log.Info("postgres destination opened", "table", o.Table)
	return s, nil
}

// insertSQL is the statement for a table and its columns, and the value of
// each parameter in order.
func insertSQL(table string, columns [][2]string, ignoreConflict bool) (string, []string) {
	names := make([]string, len(columns))
	params := make([]string, len(columns))
	values := make([]string, len(columns))
	for i, c := range columns {
		names[i] = pgx.Identifier{c[0]}.Sanitize()
		params[i] = "$" + strconv.Itoa(i+1)
		values[i] = c[1]
	}
	sql := "INSERT INTO " + table + " (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(params, ", ") + ")"
	if ignoreConflict {
		sql += " ON CONFLICT DO NOTHING"
	}
	return sql, values
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver inserts the row. A duplicate (on_conflict: nothing) is a success
// with "inserted":0 in the reply, so a retry of a message that reached the
// table before the relay heard back does not make a second row.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	args, err := rowValues(env, s.values)
	if err != nil {
		return nil, err
	}
	if err := s.ensureTable(ctx); err != nil {
		return nil, classify(err)
	}
	tag, err := s.pool.Exec(ctx, s.insert, args...)
	if err != nil {
		return nil, classify(err)
	}
	return &plugin.Result{
		Status:      200,
		Body:        []byte(`{"inserted":` + strconv.FormatInt(tag.RowsAffected(), 10) + `}`),
		ContentType: "application/json",
		Endpoint:    s.table,
	}, nil
}

// ensureTable creates the table once, on the first delivery, so a database
// that is down at start-up costs a transient failure, not the relay.
func (s *Sink) ensureTable(ctx context.Context) error {
	if s.create == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.created {
		return nil
	}
	if _, err := s.pool.Exec(ctx, s.create); err != nil {
		return err
	}
	s.created = true
	return nil
}

// rowValues resolves each column's value from the message.
func rowValues(env *envelope.Envelope, values []string) ([]any, error) {
	args := make([]any, len(values))
	var body any
	parsed := false
	for i, v := range values {
		switch v {
		case "$id":
			args[i] = env.ID
		case "$source":
			args[i] = env.Source
		case "$subject":
			args[i] = nullable(env.Subject)
		case "$content_type":
			args[i] = nullable(env.ContentType)
		case "$received_at":
			args[i] = env.ReceivedAt.UTC()
		case "$body":
			args[i] = string(env.Body)
		case "$headers":
			args[i] = jsonText(env.Headers)
		case "$attributes":
			args[i] = jsonText(env.Attributes)
		default:
			if !parsed {
				dec := json.NewDecoder(strings.NewReader(string(env.Body)))
				dec.UseNumber()
				if err := dec.Decode(&body); err != nil {
					return nil, plugin.Errorf(plugin.Permanent, []string{"postgres:invalid"}, "column value %s: the body is not JSON", v)
				}
				parsed = true
			}
			args[i] = sqlValue(lookup(body, v))
		}
	}
	return args, nil
}

// lookup walks a dotted path through objects and arrays; nil when absent.
func lookup(v any, path string) any {
	for _, seg := range strings.Split(path, ".") {
		switch cur := v.(type) {
		case map[string]any:
			v = cur[seg]
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(cur) {
				return nil
			}
			v = cur[i]
		default:
			return nil
		}
	}
	return v
}

// sqlValue is a JSON value as a parameter: numbers as int64 or float64,
// objects and arrays as JSON text (for a jsonb or text column).
func sqlValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	case map[string]any, []any:
		return jsonText(x)
	default:
		return v
	}
}

func jsonText(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// nullable is NULL for an empty string.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Health asks the database for a round trip.
func (s *Sink) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return classify(s.pool.Ping(ctx))
}

// Close returns the connections.
func (s *Sink) Close(context.Context) error {
	s.pool.Close()
	return nil
}
