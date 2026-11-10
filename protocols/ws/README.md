# protocols/ws

The WebSocket protocol for hikyaku: the `ws` and `wss` source and destination types. A WebSocket is a long-lived, two-way connection that starts as an HTTP request. Built on [`github.com/coder/websocket`](https://github.com/coder/websocket).

```go
import "github.com/xavidop/hikyaku/protocols/ws"

ws.Register() // registers ws and wss as sources and destinations
```

## Sources

A source brings WebSocket messages into the relay. It runs in one of two modes:

- **listen**: hikyaku is the server. Clients connect to `listen` + `path` and send messages. The listener is shared through hikyaku's HTTP listeners, so other listen sources and `sse` destinations can serve other paths on the same address.
- **subscribe**: hikyaku is the client. It dials `url` and reads what the server sends. When the connection fails or ends, it reconnects with exponential backoff (a wait that doubles after each failure) and jitter (a random part added to the wait). Each failed connect moves on to the next URL of `url` + `failover_urls`. A connection that delivered a message, or stayed up for 30s, resets the failure count. A connection that ends sooner without a message counts as a failed connect. Each reconnect is logged at warn. After `max_attempts` failed connects in a row, the source fails, and the relay stops.

The mode defaults to `subscribe` when the source has a `url` and no `listen`, and to `listen` otherwise.

Every text or binary message becomes one envelope:

| Field | Value |
|---|---|
| `Subject` | the request path (listen) or the URL's path (subscribe) |
| `ContentType` | `application/json` for a text message that is valid JSON, `text/plain; charset=utf-8` for other text, `application/octet-stream` for binary |
| `Headers` | listen: the handshake request's headers (header names in their standard form, such as `Content-Type`), without the caller's credentials (`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`, `Api-Key` and the `auth.api_key` header). subscribe: none |
| `Attributes` | `ws.frame` (`text` or `binary`); `remote_addr` (listen) or `url` (subscribe, without its password) |

Both modes ping the peer every 30s. A peer that does not answer within 10s is dropped. While the relay applies backpressure (it stops taking messages until it catches up), the source stops reading, so the connection's flow control holds the peer back. A peer is not dropped for pongs it could not deliver during that time. At shutdown, every connection is closed with `1001 Going Away`.

| Key | Mode | Meaning |
|---|---|---|
| `listen` | listen | Address, such as `:8080`. |
| `path` | listen | Path served (default `/`; a path ending in `/` serves everything below it). |
| `tls` | both | listen: `cert_file` + `key_file`, and `client_ca_file` for mTLS (mutual TLS: the client must show a certificate signed by that CA). A `wss` listener needs a certificate. subscribe: `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |
| `auth` | listen | `bearer`, `jwt`, `api_key`, `ip_allow`, checked on the upgrade request (401 on failure). A browser's `WebSocket` cannot set headers: give `api_key` a `query` parameter. `hmac` is refused: a WebSocket request carries no signed body. |
| `max_body_bytes` | both | Largest message (default 10MiB). A larger one closes the connection with `1009`. |
| `url` | subscribe | `ws://` or `wss://` URL. |
| `failover_urls` | subscribe | URLs rotated through when a connect fails. |
| `headers` | subscribe | Static handshake headers. `Host` overrides the host. `Sec-WebSocket-Protocol` lists the subprotocols offered. |
| `reconnect` | subscribe | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

Browsers must connect from the listener's own origin, because the WebSocket library checks `Origin`. Non-browser clients send no `Origin` and are not affected.

## Destination

A destination sends each message to a WebSocket server as one frame. It keeps one persistent connection to `url`. It dials on the first delivery, then writes each message body as one frame (`frame: text`, the default, or `binary`). The destination reads and discards what the peer sends, so it answers pings and sees close frames. It also pings the peer every 30s.

| Key | Meaning |
|---|---|
| `url` | `ws://` or `wss://` URL. It cannot be a per-message template: the connection is dialed before any message. |
| `headers` | Static handshake headers (a `{{ }}` template is refused). `Host` and `Sec-WebSocket-Protocol` work as for sources. |
| `tls` | Client TLS: `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |
| `frame` | `text` (default) or `binary`. |

Each delivery is one attempt, because the relay owns retries. A failed attempt is an error with a class and tags. The class says whether hikyaku retries it by default. The tags are what `retry_on` and `never_on` match:

| Failure | Class | Tags |
|---|---|---|
| The dial failed (refused, DNS, TLS) | transient | `connect` |
| The handshake was answered with an HTTP status | as HTTP: 5xx transient, 429 rate limited (with `Retry-After`), other 4xx permanent | `503` + `5xx`, ... |
| The attempt's timeout ran out | timeout | `timeout` |
| A write failed on a broken connection | transient | `connect` |
| The peer closed the connection with an error status | transient | `ws:close_<code>`, such as `ws:close_1011` |
| The peer closed normally (1000, 1001, or 1005 with no status) during a write | transient | `ws:close_1000`, `ws:close_1001` or `ws:close_1005`, plus `connect` |

A failure whose tags match neither `retry_on` nor `never_on` falls back to its class: it is retried unless it is permanent. Only one failure is permanent: a handshake refused with a 4xx other than 408 and 429. So the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries every `ws:close_<code>`. List a code in `never_on` to stop retrying it. Two good candidates are `ws:close_1008` (policy violation) and `ws:close_1009` (message too big), which the peer would send again.

A connection that ends between deliveries is redialed by the next delivery. There is one exception: a peer that closed it with an error status. Then the next delivery fails with that `ws:close_<code>` and does not dial, and the delivery after it redials.

## Example

```yaml
secrets:
  INGEST_TOKEN: env:INGEST_TOKEN
  STREAM_TOKEN: env:STREAM_TOKEN

sources:
  - name: browser-events
    type: wss
    mode: listen
    listen: ":8443"
    path: /events
    tls: { cert_file: /etc/hikyaku/tls.crt, key_file: /etc/hikyaku/tls.key }
    auth: { bearer: { tokens: ["{{secrets.INGEST_TOKEN}}"] } }
    max_body_bytes: 1MiB

  - name: live
    type: wss
    mode: subscribe
    url: wss://stream.example.com/events
    headers: { Authorization: "Bearer {{secrets.STREAM_TOKEN}}" }
    reconnect: { backoff: 500ms, max_backoff: 30s, jitter: full, max_attempts: 0 }
    failover_urls: [wss://stream-eu.example.com/events]

destinations:
  - name: realtime-dashboard
    type: wss
    url: wss://dash.internal/ingest
    frame: text
    required: false
    retry: { attempts: 3, never_on: [4xx, ws:close_1008, ws:close_1009] }
```
