# protocols/nats

The NATS protocol for hikyaku: the `nats` and `jetstream` source and destination types. `nats` is core NATS, which has no acknowledgements, so delivery is at most once. `jetstream` adds streams that store messages and acknowledge each one, so delivery is at least once. Built on [`github.com/nats-io/nats.go`](https://github.com/nats-io/nats.go) and its [`jetstream`](https://pkg.go.dev/github.com/nats-io/nats.go/jetstream) package.

```go
import "github.com/xavidop/hikyaku/protocols/nats"

nats.Register() // registers nats and jetstream as sources and destinations
```

| Type | Source | Destination | Delivery |
|---|---|---|---|
| `nats` | subscribe to a subject, optionally in a queue group | publish, then flush | at most once (core NATS has no acks) |
| `jetstream` | durable pull consumer on a stream | publish, then wait for the stream's ack | at least once, deduplicated by `Nats-Msg-Id` |

## Connection

These keys set up the connection to the NATS servers. They apply to every `nats` and `jetstream` source and destination. Each one keeps one connection, named `hikyaku-<relay>-<name>`; that is the name `nats server` and its monitoring show.

| Key | Meaning |
|---|---|
| `url` | `nats://`, `tls://`, `ws://` or `wss://` URL. Several servers may be listed, comma separated. It cannot be a per-message template. |
| `failover_urls` | Sources: more servers, tried after `url`, in order. (Destinations have no `failover_urls` key: list the servers in `url`.) |
| `username`, `password` | User and password; use `{{secrets.X}}`. |
| `token` | Authentication token; use `{{secrets.X}}`. |
| `creds_file` | A `.creds` file (user JWT and NKey seed), checked when the relay starts. |
| `tls` | Client TLS: `ca_file`, `cert_file` + `key_file` (mTLS), `server_name`, `insecure_skip_verify`. A `tls://` or `wss://` URL uses TLS without it. |
| `reconnect` | Sources: `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). Destinations use the defaults. |

Use only one of `username`/`password`, `token` and `creds_file`. Credentials may also go in the URL (`nats://user:pass@host`, `nats://token@host`).

A server that is down when the relay starts does not stop the relay. The connection keeps trying in the background, and `Health` says why it is not connected. The connection tries `url`, then `failover_urls`, then starts over. After each round in which every server failed, it waits with exponential backoff (a wait that doubles each round) and jitter (a random part), from `reconnect.*`. Bad credentials are retried like any other failure, because they may be fixed on the server. With `reconnect.max_attempts` set, the connection gives up after that many reconnects to each server in a row. The source then fails, which stops the relay. Disconnects and failed connects are logged at warn, reconnects at info.

URLs are logged, and returned as `Result.Endpoint`, without their credentials: `nats://user:xxxxx@host:4222`, and `nats://xxxxx@host:4222` for a token.

## Sources

A source brings NATS messages into hikyaku. It runs in `subscribe` mode (the default). Every message becomes one envelope (the message as hikyaku carries it):

| Field | Value |
|---|---|
| `ID` | `nats`: the `Nats-Msg-Id` header, else `Idempotency-Key`, else a new ULID. `jetstream`: `Nats-Msg-Id`, else `<stream>-<stream sequence>`, which stays the same on every redelivery. |
| `Subject` | The message subject. |
| `Headers` | The NATS headers, as sent. NATS header names are case sensitive; templates and CEL see them with each word capitalized, as `X-Tenant`. |
| `ContentType` | The `Content-Type` header, else `application/json` for a JSON body, `text/plain; charset=utf-8` for other UTF-8, `application/octet-stream` for anything else. |
| `Trace` | The W3C `traceparent` header, so a trace started upstream continues. |
| `Attributes` | `ordering_key` (the subject). `nats`: `nats.subject`, and `nats.reply` when the message has a reply subject. `jetstream`: `jetstream.stream`, `jetstream.sequence` (stream sequence), `jetstream.consumer`, `jetstream.delivered` (delivery count, 1 for the first delivery), `jetstream.subject`. |

The numeric attributes are integers. A ULID is a unique ID that sorts by time.

### nats

Use a `nats` source for core NATS subjects, when at-most-once delivery is enough. The source subscribes to `subject` (wildcards `*` and `>` allowed). With `queue`, it joins that queue group: the members of a queue group share the messages, and each message goes to one of them.

Core NATS has no acknowledgements (no way to tell the server a message is done), so delivery is **at most once**. The source holds up to `prefetch` messages (default 65536) or 64MiB for the relay. When the relay falls further behind, the client drops what the server sends. NATS calls this a slow consumer. The source logs it at warn with the total dropped. The source still settles every message, but there is nothing to send back to the server. So a message the relay failed (the dead-letter queue failed too, or a shutdown cut its delivery) is lost and logged. At shutdown, messages not yet handed to the relay are dropped and logged. **Use `jetstream` when every message counts.**

| Key | Meaning |
|---|---|
| `subject` | **Required.** The subject subscribed to. |
| `queue` | Queue group. |
| `prefetch` | Messages held for the relay before the client drops them (default 65536, like nats.go; the byte limit is 64MiB). |
| `max_body_bytes` | Largest message (default 10MiB). A larger one is dropped and logged. |
| `ack` | `delivered` (default) or `accepted`. Without acks, it only decides when a message counts as settled. |

### jetstream

Use a `jetstream` source when every message must arrive. The source reads `stream` through a durable pull consumer. A consumer is the server's record of a reader's position in a stream. Durable means the server keeps that record when the reader disconnects. Pull means the source asks the server for messages, instead of the server pushing them. The consumer is named `durable` (default: the source name, with `.` and other characters a consumer name cannot hold replaced by `_`).

The source creates the consumer, or updates it, when it starts. A consumer that already exists keeps what it was made with: where it started (`start` applies to a new consumer only), and settings an operator changed, such as `max_deliver`. The source then sets explicit acks, `ack_wait`, `max_ack_pending` = `prefetch`, and the filter subject. Because the consumer is durable, a restarted relay resumes where the last one stopped. Relays that share a `durable` name share the messages.

A stream that does not exist yet, or a server that is down, does not stop the source. The source retries with the `reconnect` backoff and logs each failure, and `Health` reports it. With `reconnect.max_attempts`, the source fails after that many setups in a row fail. A consumer deleted while the source runs, or lost by the server, is created again.

JetStream expects an answer for each message: an **ack** (the message is done), a **nak** (a negative ack: redeliver it), or **in progress** (still working on it, do not redeliver yet). With `ack: delivered` (the default), the source answers like this:

- **Final outcome** (delivered, dead-lettered, unrouted, filtered): **ack**. The source waits for the server to confirm the ack (a double ack). If the ack is lost, the server redelivers the message after `ack_wait`.
- **Failed** (the dead-letter queue failed too, or a shutdown cut the delivery): **nak with a delay**. The delay grows with the message's delivery count (`reconnect.backoff` doubled per delivery, capped at `reconnect.max_backoff`, with `reconnect.jitter`). The server then redelivers the message, to this relay or another one.
- **In flight**: from receipt until the relay settles the message, the source tells the server every `ack_wait`/2 that it is still in progress. So a slow delivery (retries, fallbacks, a busy relay) is not redelivered while the relay still works on it.
- **Too large** (over `max_body_bytes`): logged and **terminated** (`Term`), so it is not redelivered forever.
- **Shutdown**: the source stops pulling. Messages it received but had not handed to the relay are nak'ed at once, so the server redelivers them right away. Close waits for the relay to settle what it holds, so those acks go out, and then drains the connection. Whatever is still unsettled when the shutdown grace runs out is nak'ed too.

With `ack: accepted`, a message is acked as soon as the relay holds it. Use it with `buffer.type: disk` only.

The source keeps the stream's order: it hands messages to the relay one at a time, in the order the server delivered them. With `prefetch` above 1, a nak'ed message comes back after messages delivered later. Use `ordering` on the source to keep a subject's messages in order through the relay.

| Key | Meaning |
|---|---|
| `stream` | **Required.** The stream consumed. |
| `subject` | Filter subject of the consumer (wildcards allowed; default: the whole stream). |
| `durable` | Durable consumer name (default: the source name). |
| `start` | Where a new consumer starts: `new` (default, messages published from now on), `all` (the whole stream) or `last` (the last message, then new ones). |
| `ack_wait` | How long the server waits for an ack before it redelivers (default 30s, at least 100ms). |
| `prefetch` | Unacknowledged messages in flight: the consumer's `max_ack_pending` and the pull batch (default 64). |
| `max_body_bytes` | Largest message (default 10MiB). A larger one is terminated. |
| `ack` | `delivered` (default) or `accepted`. |

## Destinations

A destination publishes each message to a NATS subject. It connects when the relay starts, or at the first delivery for one-shot commands. Each delivery is one attempt, because the relay owns retries, fallbacks and the dead-letter queue. A delivery made while the connection is down fails at once (`connect`). Nothing is buffered to go out after a reconnect, so a retry never sends the message a second time.

The published message carries:

- the subject: `subject`, a per-message template such as `orders.{{ .json.region }}`;
- the body: the message body, or the `body` template;
- the headers: the envelope's headers, minus three kinds: hop-by-hop headers; the caller's credentials (`Authorization`, `Cookie`, `Proxy-Authorization`); and NATS control headers (`Nats-*`, such as `Nats-Expected-Stream` or `Nats-Rollup`, which steer the hop they were set on). Then `Content-Type` (the envelope's content type) and `Idempotency-Key` (the envelope ID, unless the relay already set one). The destination's `headers` templates come last and may set any header, `Nats-*` included.

### nats

The `nats` destination publishes, then flushes. A successful delivery means the server has the message. Core NATS does not say whether anyone received it.

### jetstream

The `jetstream` destination publishes and waits for the stream's ack. The message's `Nats-Msg-Id` header is the envelope ID. It stays the same on every retry, every fallback and a `dlq replay`. Within the stream's duplicate window (`duplicate_window`, 2 minutes by default), the stream stores a message with the same ID once. To publish one message twice into the same stream, through two destinations, give one of them its own ID: `headers: {Nats-Msg-Id: "{{ .id }}-audit"}`.

The result's headers are `jetstream-stream`, `jetstream-sequence` and `jetstream-duplicate` (`true` when the stream already had the message). With `respond.from`, the HTTP caller sees them.

A destination with `batch:` publishes every message of the batch without waiting between them, then waits for every ack. The result's headers then hold one value per message, in order. The batch fails as a whole when one message fails. When the relay retries the batch, the stream drops the copies of the messages it already stored.

| Key | Meaning |
|---|---|
| `url` | Server URL (several, comma separated). |
| `subject` | **Required.** Per-message template of the subject. |
| `headers` | Per-message templates of headers. |
| `body` | Per-message template of the body (default: the message body). |
| `stream` | `jetstream`: the stream the subject must land in. The publish fails (`jetstream:10060`) when another stream holds it. |

### Errors

| Failure | Class | Tags |
|---|---|---|
| Not connected, connection closed or draining | transient | `connect` |
| The attempt's deadline passed (flush, stream ack) | timeout | `timeout` |
| The server refused the publish (permissions) | permanent | `nats:permissions` |
| The message is over the server's `max_payload` | permanent | `nats:max_payload` |
| The subject template failed, or rendered an invalid subject | permanent | `template` |
| `jetstream`: no stream takes the subject (no responders, after the client's 2 quick retries for a leader election) | permanent | `jetstream:no_responders` |
| `jetstream`: the stream answered with an error | by its code, like HTTP: 503 transient, 429 rate limited, 408 timeout, other 4xx permanent | `jetstream:<err_code>` plus the code, such as `jetstream:10060`, `400`, `4xx` |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries a stream that is temporarily unavailable (`503`). It sends a missing or wrong stream to the fallback or the dead-letter queue. Add `jetstream:no_responders` to `retry_on` to wait for a stream that is being created.

`Health` is nil while the destination is connected. For `jetstream`, the account must also answer (`AccountInfo`).

## Example

A relay that reads an orders stream and a telemetry subject, and publishes to a billing stream and a live subject:

```yaml
secrets:
  NATS_TOKEN: env:NATS_TOKEN

sources:
  - name: orders
    type: jetstream
    url: nats://nats-1:4222
    failover_urls: [nats://nats-2:4222, nats://nats-3:4222]
    creds_file: /etc/hikyaku/relay.creds
    stream: ORDERS
    subject: orders.>
    durable: hikyaku-orders
    start: all
    ack_wait: 1m
    prefetch: 128
    reconnect: { backoff: 500ms, max_backoff: 30s, jitter: full }

  - name: telemetry
    type: nats
    url: tls://nats.internal:4222
    token: "{{secrets.NATS_TOKEN}}"
    subject: telemetry.*.metrics
    queue: hikyaku

destinations:
  - name: billing
    type: jetstream
    url: nats://nats-1:4222,nats://nats-2:4222
    creds_file: /etc/hikyaku/relay.creds
    subject: billing.{{ .json.region }}
    stream: BILLING
    batch: { max_messages: 100, max_wait: 200ms }

  - name: live
    type: nats
    url: nats://nats-1:4222
    subject: live.orders.{{ .json.region }}
    headers: { X-Tenant: "{{ .json.tenant }}" }
    required: false
```
