# protocols/sse

The server-sent events protocol for hikyaku: the `sse` source and destination type. Server-sent events (SSE) are a one-way stream of events from a server to a client, sent as one long HTTP response. The event-stream parser is hand-written to follow the WHATWG rules ("Interpreting an event stream"), and it is exported as `sse.NewParser`.

```go
import "github.com/xavidop/hikyaku/protocols/sse"

sse.Register() // registers sse as a source and a destination
```

## Sources

A source brings the events of a stream into the relay. It runs in one of two modes:

- **subscribe**: hikyaku is the client. It GETs `url` with `Accept: text/event-stream` and `Cache-Control: no-cache`, and reads the stream. A non-2xx answer, or a content type other than `text/event-stream`, is a failed connect. When the stream ends or fails, hikyaku reconnects with backoff and jitter. It sends `Last-Event-ID`, the id of the last event it saw, so the server can resume the stream from there. A server's `retry:` replaces the base delay. Each failed connect moves on to the next URL of `url` + `failover_urls`. A stream that delivered an event, or stayed up for 30s, resets the failure count. Each reconnect is logged at warn. After `max_attempts` failed connects in a row, the source fails, and the relay stops.
- **listen**: hikyaku is the server. Something pushes an event stream to it as the body of a `POST` to `listen` + `path`. The body is parsed as it streams in, one envelope per event. hikyaku answers `200` when the body ends. The listener is shared through hikyaku's HTTP listeners.

The mode defaults to `subscribe` when the source has a `url` and no `listen`, and to `listen` otherwise.

Every event becomes one envelope:

| Field | Value |
|---|---|
| `Subject` | the event's `event:` name when it has one, else the request path (listen) or the URL's path (subscribe) |
| `ContentType` | `application/json` when the data is valid JSON, else `text/plain; charset=utf-8` |
| `Headers` | listen: the POST request's headers (header names in their standard form, such as `Content-Type`), without the caller's credentials (`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`, `Api-Key` and the `auth.api_key` header). subscribe: none |
| `Attributes` | `sse.id` (the last event ID, `""` when the stream set none); `sse.event` (the event name, `message` when it had none, as browsers name it); `remote_addr` (listen) or `url` (subscribe, without its password) |

The parser follows the standard:

- `data` lines join with `\n`.
- An `id` persists across events and across reconnects. An `id` containing NUL is ignored.
- `retry` takes integer milliseconds only.
- Lines starting with `:` are comments.
- A line without a colon is a field with an empty value. One space after the colon is stripped.
- LF, CR and CRLF all end a line.
- A blank line dispatches the event. An event with no `data` field is not dispatched. `data:` with an empty value is dispatched, with empty data, as browsers do.
- A leading byte order mark is skipped. An event cut off by the end of the stream is dropped.

| Key | Mode | Meaning |
|---|---|---|
| `listen` | listen | Address, such as `:8080`. |
| `path` | listen | Path served (default `/`). |
| `tls` | both | listen: `cert_file` + `key_file`, and `client_ca_file` for mTLS (mutual TLS: the client must show a certificate signed by that CA). subscribe: `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |
| `auth` | listen | `bearer`, `jwt`, `api_key`, `ip_allow`, checked before the body is read (401 on failure). `hmac` is refused: the body is a stream, not a signed payload. |
| `max_body_bytes` | both | Largest event data (default 10MiB). A larger event ends the stream (`413` in listen mode). |
| `url` | subscribe | `http://` or `https://` URL. |
| `failover_urls` | subscribe | URLs rotated through when a connect fails. |
| `headers` | subscribe | Static request headers (`Host` overrides the host). |
| `reconnect` | subscribe | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

In listen mode, a request that is not a `POST` gets `405`. A content type other than `text/event-stream` gets `415`; leaving the content type out is fine. At shutdown, a stream still arriving gets `503`.

## Destination

A destination serves an event stream to clients, such as dashboards. hikyaku serves it at `listen` + `path` once the relay runs, so clients can connect before messages flow. Clients `GET` it and receive `text/event-stream`, flushed per event, with a `: ping` comment every 15s. One-shot commands (`hikyaku send`, `route`, `ai test`, `dlq replay`) never serve it, so they do not take a running relay's port. Their deliveries only reach the replay buffer.

Each delivery is broadcast as one event to the clients connected at that moment:

```
id: <the message ID>
event: <the rendered event template, else the message subject; left out when empty>
data: <one line per line of the body>
```

The last `replay` events are kept, so a client that reconnects can catch up. A client that connects with `Last-Event-ID` first gets the kept events after that id, then goes live. If the id is unknown or no longer kept, the client gets nothing replayed and goes live at once. Delivering with no client connected succeeds, because fan-out to dashboards is best-effort. A delivery never waits for a client: a client that falls 256 events behind is dropped, and its stream is closed.

| Key | Meaning |
|---|---|
| `listen` | Address the stream is served on (required). |
| `path` | Stream path (default `/`). |
| `event` | Per-message template of the event name, such as `{{ .json.type }}`. Without it the name is the message subject. A template that fails to render is a permanent failure. |
| `replay` | Events kept for `Last-Event-ID` resume (default 1000). |
| `tls` | Serve HTTPS: `cert_file` + `key_file` (`client_ca_file` for mTLS). |
| `auth` | Who may read the stream: `bearer`, `api_key`, `jwt`, `ip_allow`, as for listen sources, checked before the stream starts (401 on failure). A browser's `EventSource` cannot set headers: give `api_key` a `query` parameter. `hmac` is refused: a client reading the stream sends no body to sign. |

Without `auth` (or mTLS), anyone who reaches the address can read the stream.

## Example

```yaml
secrets:
  FEED_TOKEN: env:FEED_TOKEN
  PUSH_KEY: env:PUSH_KEY
  DASHBOARD_KEY: env:DASHBOARD_KEY

sources:
  - name: market
    type: sse
    mode: subscribe
    url: https://feed.example.com/v1/stream
    headers: { Authorization: "Bearer {{secrets.FEED_TOKEN}}" }
    reconnect: { backoff: 1s, max_backoff: 30s, max_attempts: 0 }
    failover_urls: [https://feed-eu.example.com/v1/stream]

  - name: pushed
    type: sse
    mode: listen
    listen: ":8080"
    path: /push
    auth: { api_key: { header: X-API-Key, keys: ["{{secrets.PUSH_KEY}}"] } }

destinations:
  - name: live-dashboard
    type: sse
    listen: ":8090"
    path: /events
    event: "{{ .json.type }}"
    replay: 500
    required: false
    auth:                                # a browser's EventSource sends ?access_token=...
      api_key:
        query: access_token
        keys: ["{{secrets.DASHBOARD_KEY}}"]
```
