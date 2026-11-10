package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xavidop/hikyaku/plugin"
)

// database is what every source and destination of this plugin shares: the
// connection string from DATABASE_URL and the size of each pool.
type database struct {
	url      string
	maxConns int32
}

// open makes a pool for one source or destination. Nothing connects yet:
// the first query does, so a database that is down at start-up fails that
// delivery (transient, retried) instead of the whole relay.
func (d *database) open(ctx context.Context, who string) (*pgxpool.Pool, error) {
	if d.url == "" {
		return nil, fmt.Errorf("%s: DATABASE_URL is not set: pass it in the plugin's env", who)
	}
	cfg, err := pgxpool.ParseConfig(d.url)
	if err != nil {
		// pgx redacts the password in this error.
		return nil, fmt.Errorf("%s: DATABASE_URL: %w", who, err)
	}
	if d.maxConns > 0 {
		cfg.MaxConns = d.maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", who, err)
	}
	return pool, nil
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// tableName checks a table option (name, or schema.name) and returns it
// quoted for SQL.
func tableName(who, table string) (string, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("%s: options.table %q: use name or schema.name", who, table)
	}
	for _, p := range parts {
		if !identifier.MatchString(p) {
			return "", fmt.Errorf("%s: options.table %q: a name is letters, digits and _, and starts with a letter or _", who, table)
		}
	}
	return pgx.Identifier(parts).Sanitize(), nil
}

// classify turns a PostgreSQL error into a classified one, so hikyaku
// retries what a retry can fix and dead-letters the rest. The tag is
// postgres:<SQLSTATE>. Any other error (a refused connection, the deadline)
// is returned as it is: hikyaku classifies those itself.
func classify(err error) error {
	var pg *pgconn.PgError
	if !errors.As(err, &pg) {
		return err
	}
	class := plugin.Permanent
	switch {
	case strings.HasPrefix(pg.Code, "08"), // connection exception
		strings.HasPrefix(pg.Code, "53"), // insufficient resources
		strings.HasPrefix(pg.Code, "57"), // operator intervention (shutdown, cancel)
		strings.HasPrefix(pg.Code, "58"), // system error (I/O)
		strings.HasPrefix(pg.Code, "XX"), // internal error
		pg.Code == "40001",               // serialization failure
		pg.Code == "40P01":               // deadlock detected
		class = plugin.Transient
	}
	return plugin.Wrap(class, []string{"postgres:" + pg.Code}, err)
}
