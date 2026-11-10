# postgres-plugin

This example is an out-of-process plugin written in Go. The `hikyaku-postgres` binary adds PostgreSQL to hikyaku, as a destination and as a source:

- The **`postgres` destination** inserts one row per message into a table.
- The **`postgres` source** is a transactional outbox. Your application writes a row to an outbox table in the same transaction as its own change. hikyaku polls the table, delivers each row, and marks it delivered once it is. No message is lost between your database and the partner's API.

hikyaku starts the binary, talks gRPC to it, and treats both types like built-in ones: `validate` checks their `options:`, and the engine retries, falls back and dead-letters around them. Nothing in hikyaku is rebuilt. [crm-plugin](../crm-plugin) is a smaller plugin with one destination; to compile your types into hikyaku itself instead, see [custom-destination](../custom-destination).

The steps are the same as for any plugin:

1. **Implement `plugin.Sink`** ([sink.go](sink.go)) **and `plugin.Source`** ([source.go](source.go)), exactly as for an in-process plugin. `Deliver` makes one attempt and returns a classified error when it fails: [db.go](db.go) maps PostgreSQL's SQLSTATE codes onto hikyaku's classes, so a lost connection is retried and a constraint violation is permanent. `Start` emits messages until its context ends, and `plugin.OnSettle` hands the source hikyaku's outcome for each one.
2. **Read your settings from `options:`.** `spec.Decode(&opts)` decodes them strictly: an unknown key is an error. The `jsonschema` tags of `SinkOptions` and `SourceOptions` become the JSON Schema the plugin declares, so `hikyaku validate` catches a typo before anything runs.
3. **Serve both** ([main.go](main.go)):

   ```go
   grpcplugin.Serve(grpcplugin.Plugins{
   	Sinks:   map[string]grpcplugin.SinkDef{"postgres": {Factory: db.newSink, Options: SinkOptions{}, Description: "..."}},
   	Sources: map[string]grpcplugin.SourceDef{"postgres": {Factory: db.newSource, Options: SourceOptions{}, Description: "..."}},
   })
   ```

4. **List the binary under `plugins:`** ([hikyaku.yaml](hikyaku.yaml)). The database comes from `DATABASE_URL` in the plugin's `env`, a secret the spec never spells out. `sha256` pins the binary you built.

```yaml
plugins:
  - name: postgres
    command: ./bin/hikyaku-postgres
    env: { DATABASE_URL: "{{secrets.DATABASE_URL}}" }
sources:
  - { name: outbox, type: postgres, options: { table: outbox, create_table: true } }
destinations:
  - { name: events, type: postgres, options: { table: events, create_table: true } }
```

## The destination

| Option | Meaning |
|---|---|
| `table` | The table, `name` or `schema.name`. Required. |
| `columns` | Column to value. A value is `$id`, `$source`, `$subject`, `$content_type`, `$received_at`, `$body`, `$headers`, `$attributes`, or a path into the JSON body, such as `customer.email`. Without it, the row is `id`, `source`, `received_at` and `body`. |
| `on_conflict` | `nothing` (default): a row that exists already is left alone. `error`: the insert fails. |
| `create_table` | Create the default table when it is missing: `id text primary key, source text, received_at timestamptz, body jsonb`. Only without `columns`. |

The message ID is the row's `id`, so a retry of a message that reached the table before the relay heard back writes no second row. The reply says what happened: `{"inserted":1}`, or `{"inserted":0}` for a duplicate. A path that is missing from the body is `NULL`. An object or an array becomes JSON text, for a `jsonb` or `text` column.

Failures are classified by SQLSTATE, with the tag `postgres:<code>`:

- A lost connection (`08xxx`), too many connections (`53xxx`), a shutdown (`57xxx`), a serialization failure or a deadlock is transient: retried.
- A constraint violation (`23xxx`), a value the column refuses (`22xxx`, such as text into `jsonb`) or an unknown table (`42xxx`) is permanent: straight to the fallback or the DLQ (the dead-letter queue).
- A body that is not JSON, when a column needs a path into it, is permanent with the tag `postgres:invalid`.

## The source

| Option | Meaning |
|---|---|
| `table` | The outbox table. Required. |
| `interval` | Time between polls once the table has nothing to deliver (default `1s`). |
| `batch` | Rows claimed per poll (default 100). A full batch is followed by another poll at once. |
| `claim_ttl` | A row claimed this long ago without an outcome is claimed again (default `5m`). |
| `on_delivered` | `mark` (default): set `delivered_at`. `delete`: remove the row. |
| `create_table` | Create the table when it is missing. |

The table has these columns. Your application fills `subject`, `payload` and `headers`; hikyaku owns the rest.

```sql
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  subject      text,                         -- the message subject, and its ordering key
  payload      jsonb NOT NULL,               -- the message body
  headers      jsonb,                        -- {"X-Event": "order.paid"} or {"X-Tags": ["a", "b"]}
  created_at   timestamptz NOT NULL DEFAULT now(),
  claimed_at   timestamptz,                  -- set by hikyaku while a row is in flight
  delivered_at timestamptz                   -- set by hikyaku once the row is delivered
);
```

A poll claims up to `batch` rows that are not delivered and not claimed, with `FOR UPDATE SKIP LOCKED`, so several relays can share one table. Each row is a message with the ID `<source>-<row id>`, which stays the same on a redelivery. Rows that share a `subject` stay in order. hikyaku's outcome settles the row:

- **Final** (delivered, dead-lettered, filtered): the row is marked delivered, or deleted.
- **Failed** (the DLQ failed too, or the relay is stopping): the row is released and the next poll emits it again.
- **No outcome** (the relay died mid-delivery): the claim expires after `claim_ttl` and another poll takes the row. Delivery is at least once.

## Try it

Run this from this directory, with a hikyaku binary on your `PATH` and a PostgreSQL to talk to. You need the full build: `hikyaku-lite` has no out-of-process plugins.

```sh
docker run --rm -d --name relay-db -p 5432:5432 \
  -e POSTGRES_USER=hikyaku -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=relay postgres:17-alpine
go build -o bin/hikyaku-postgres .
export DATABASE_URL=postgres://hikyaku:secret@localhost:5432/relay
hikyaku run -f hikyaku.llmfake.yaml
```

Both tables are created on first use. Post a webhook, then put a row in the outbox:

```sh
curl -s localhost:8080/ingest -d '{"type":"invoice.paid","amount":12}'     # 200: the row is in events
docker exec relay-db psql -U hikyaku relay -c "SELECT id, body FROM events"
docker exec relay-db psql -U hikyaku relay \
  -c "INSERT INTO outbox (subject, payload) VALUES ('order-1', '{\"n\":1}')"
cat out/outbox.ndjson                                                       # the row, delivered
docker exec relay-db psql -U hikyaku relay -c "SELECT id, delivered_at FROM outbox"
```

`hikyaku.yaml` is the same relay with the outbox delivered to a partner's API. Pin the binary you built with `sha256:` (`shasum -a 256 bin/hikyaku-postgres`), so hikyaku refuses to start any other.

Outside a clone of this repository, drop the `replace` lines of [go.mod](go.mod) and run `go get github.com/xavidop/hikyaku/grpcplugin@latest`.

## Tests

`go test ./...` checks the options, the SQL and the error classes without a database. `go test -tags integration ./...` runs both types against a PostgreSQL container (testcontainers). `TestPostgresPluginExample`, in `cmd/hikyaku/app`, builds this binary and validates the example through the CLI; its `-tags integration` twin runs the relay against a container, posts a webhook and delivers an outbox row.
