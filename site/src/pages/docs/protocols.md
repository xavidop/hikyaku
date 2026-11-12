---
layout: ../../layouts/DocsLayout.astro
title: Protocols
---

# Protocols

A protocol is how hikyaku talks to the outside world: HTTP, WebSocket, Kafka, a file on disk, and
so on. Every protocol is a Go module of its own under `protocols/<name>`. Each one sits behind the
same `Source`/`Sink` interface, and `cmd/hikyaku` registers them all. A source of any protocol
produces the same [envelope](/docs/concepts/#the-envelope), and a destination of any protocol
accepts it. So any source can feed any destination.

A source runs in one of three modes:

- `listen`: hikyaku is the server, and clients send to it.
- `poll`: hikyaku fetches on a schedule.
- `subscribe`: hikyaku is a long-lived client that stays connected and reads what arrives.

`failover_urls` (other URLs to try when a connect fails) belongs to the subscribe sources that dial
a URL: `ws`, `wss`, `sse`, `grpc`, `nats`, `jetstream`, `mqtt`, `amqp` and `redis`. Validation
rejects it on any other source.

## The matrix

The table says what each protocol can do, and in which source modes.

| Protocol | As a source | As a destination | Notes |
|---|---|---|---|
| `http` / `https` | listen, poll | yes | TLS and mTLS; HMAC, bearer, JWT (JWKS), API key and IP allowlist; request/response passthrough |
| `ws` / `wss` | listen, subscribe | yes | text and binary frames, ping/pong, reconnect with failover URLs |
| `sse` | listen, subscribe | yes | `Last-Event-ID` resume, a replay buffer on the destination side |
| `stdin` / `file` | one message per line | | NDJSON; local development and piping |
| `stdout` / `stderr` / `file` | | yes | one envelope, or one body, per line |
| `grpc` | listen, subscribe | yes | the built-in `hikyaku.relay.v1.Relay`, or any service from a descriptor set or server reflection |
| `tcp` / `udp` | listen | yes | newline, length-prefix or raw framing; TLS and mTLS on tcp, DTLS on udp |
| `kafka` | subscribe | yes | consumer groups, commits past the settled prefix, SASL and TLS, the idempotent producer |
| `nats` / `jetstream` | subscribe | yes | queue groups; durable pull consumers with ack, nak and in-progress |
| `mqtt` | subscribe | yes | MQTT 5 and 3.1.1, persistent sessions, in-order acks, shared subscriptions |
| `amqp` | subscribe | yes | RabbitMQ: prefetch, manual acks, publisher confirms |
| `redis` | subscribe | yes | Streams through consumer groups, and Pub/Sub (the protocol; Redis as a store is separate) |
| `sqs` / `sns` / `eventbridge` | subscribe | yes | SNS and EventBridge through an SQS queue; FIFO groups and dedupe |
| `s3` / `gcs` | poll | yes | an object per message, or per batch as NDJSON |
| `pubsub` | subscribe | yes | Google Cloud Pub/Sub, ordering keys |
| `servicebus` / `eventhubs` | subscribe | yes | sessions and lock renewal; Event Hubs checkpoints and leases in a store |
| `openai` | listen | | the [LLM proxy](/docs/llm-proxy/)'s OpenAI-compatible endpoint |
| `model` | | yes | a Genkit model answering a chat request ([LLM proxy](/docs/llm-proxy/)) |
| `fault` | | yes | injects failures, latency and status codes, for tests |
| `group` | | yes | not a protocol: members of any type, picked by a strategy |

gRPC, the brokers and the clouds are in the full build only. `hikyaku-lite` leaves their SDKs
out, and [out-of-process plugins](/docs/plugins/) with them. It keeps http, ws, sse, tcp, udp,
the files, the fault sink and the LLM proxy. A lite binary still validates a spec that uses the
others, but does not run it. Each module's README, linked from its section below, lists every
key, the attributes it sets and its error tags.

## How sources acknowledge

To acknowledge a message is to tell the broker it came from that the message is done, so the
broker does not send it again. Every broker and cloud source follows one contract:

- **Final outcome, acked.** A message is acknowledged to its broker (committed, acked, deleted,
  completed) once its outcome is final: delivered, dead-lettered (put in the dead-letter queue,
  the DLQ, because every delivery attempt failed), unrouted or filtered.
- **Failed, redelivered.** A message that failed (its destination and the DLQ were down, or a
  shutdown cut its delivery) comes back. It is nacked (reported as not done) where the broker can
  (AMQP, JetStream, SQS, Pub/Sub, Service Bus), or left unacknowledged (Kafka, Redis Streams,
  MQTT, Event Hubs).
- **`ack: accepted`** acknowledges as soon as the relay holds the message. That is only safe
  with [a disk buffer](/docs/delivery/#the-disk-buffer): `hikyaku lint` warns otherwise.
- **Resume.** After a restart, a source carries on where the last run stopped. It uses what the
  broker kept: Kafka committed offsets, JetStream durable consumers, MQTT persistent sessions,
  Redis Streams pending entries (reclaimed after `ack_wait`), Event Hubs checkpoints.
- **Oversize** messages (over `max_body_bytes`) are logged and acknowledged, so they cannot loop.
- **Order.** Every source sets the `ordering_key` attribute to its natural unit (a Kafka
  partition, an MQTT topic, an SQS message group, and so on), which
  [`ordering`](/docs/delivery/#ordering) uses.

Destinations classify their errors for the [retry policy](/docs/resilience/) and tag them
`<protocol>:<code>` (`kafka:NOT_LEADER_FOR_PARTITION`, `grpc:UNAVAILABLE`, `amqp:404`, and so
on). A tag that neither `retry_on` nor `never_on` names falls back to its error's class, so
transient broker errors retry by default. List a tag in `never_on` to stop retrying it.
`retry_on` is needed only to retry a code whose class is permanent. Where the broker has a
message ID, it is the envelope ID, which dedupes retries.

## Dedupe

Dedupe drops a message the relay has already seen. Any source, of any type, can do it:

```yaml
sources:
  - name: orders
    type: kafka
    brokers: [kafka-1:9092, kafka-2:9092]
    topic: orders
    group: hikyaku
    dedupe:
      key: "{{ .json.order_id }}"            # a per-message template
      ttl: 24h                               # default 1h
      store: redis-main                      # default: the relay's default store
```

A message whose key was seen within the `ttl` is dropped before it is routed. It is acknowledged
to its source as filtered, so a broker forgets it, and counted by `hikyaku_deduped_total{source}`.
With a shared [store](/docs/spec/#stores), dedupe holds across replicas. Dedupe never drops by
accident: a key that renders empty, or a store that is down, lets the message through. The same
message coming back (a retry from the disk buffer, a replay) is not a duplicate of itself. The
key of a message that failed is forgotten, so its redelivery goes through.

The http sources check the key themselves, before the message is queued: a listen source answers
a repeat with `200` and the status `duplicate`, and a poll source skips it.

## HTTP and HTTPS

### Listen

In listen mode, hikyaku is the server, and each request is one message. Use it to receive
webhooks.

```yaml
sources:
  - name: webhooks
    type: https
    mode: listen
    listen: ":8443"
    path: /ingest
    methods: [POST]                          # default POST and PUT
    tls:
      cert_file: /etc/hikyaku/tls.crt
      key_file: /etc/hikyaku/tls.key
      client_ca_file: /etc/hikyaku/clients-ca.pem
    auth:
      hmac:
        header: X-Signature-256
        algo: sha256
        prefix: "sha256="
        secret: "{{secrets.WEBHOOK_SECRET}}"
      ip_allow: [203.0.113.0/24]
    max_body_bytes: 1MiB                     # default 10MiB
```

Sources can share an address with different paths. Inbound authentication goes under `auth:`.
Every check that is set must pass:

| Key | Checks |
|---|---|
| `hmac` | an HMAC of the body in `header` (`sha256`, `sha1` or `sha512`; `hex` or `base64`; an optional `prefix` such as `sha256=`) |
| `webhook` | the signature of GitHub, Stripe, Slack, Shopify or Twilio: `provider` and `secret` (http sources; see [Security](/docs/security/#webhooks-from-github-stripe-slack-shopify-and-twilio)) |
| `bearer` | `Authorization: Bearer <token>` against `tokens` |
| `jwt` | a JWT verified against `jwks_url`, with optional `issuer`, `audience` and `algorithms` |
| `api_key` | a key in `header` (default `X-API-Key`) against `keys` |
| `ip_allow` | the client address against IPs and CIDRs |

`tls.client_ca_file` turns on mTLS (mutual TLS: the client must show a certificate signed by that
CA). When the buffer is full, the source answers `503` with `Retry-After` instead of dropping.
This is backpressure: hikyaku takes no more until it catches up, and `Retry-After` tells the
caller when to try again.

The answer to a request depends on `ack`:

- `ack: delivered` (the default) answers once the message is delivered (`200`) or dead-lettered
  (`202`). A body that a route's `validate` step refused gets `422`, with what is wrong with it.
- `ack: accepted` answers `202` as soon as the message is queued.
- `respond.from: <destination>` returns that destination's reply to the caller instead: `502` if
  the message was dead-lettered, `504` when no reply came within `respond.timeout` (default 30s).

See [the acknowledgement modes](/docs/concepts/#ack-semantics).

Each request is one message. An `Idempotency-Key` header becomes its ID. (An idempotency key is a
value the caller picks to mark one request, so a retry with the same key is the same message.)
Its subject is the request path. It carries the request's headers and content type, plus the
attributes `http.method`, `http.path`, `http.query` (when the URL has a query), `remote_addr` and
`idempotency_key` (when the caller sent one). The caller's credentials never enter a message:
`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`, `Api-Key` and the configured
`auth.api_key.header` are dropped before the message is built. So no destination, file, DLQ
record or `hikyaku tail` sees them.

A request sent with `Content-Encoding: gzip` is decoded, with no setting: the message holds the
decoded body. `max_body_bytes` applies to the decoded body too, so a small request cannot expand
into a huge message (it gets a `413`). Signatures are checked on the bytes as they were sent.

### Poll

In poll mode, hikyaku is the client, and it fetches `url` on a schedule. Use it for an API that
you have to ask.

```yaml
sources:
  - name: open-tickets
    type: https
    mode: poll
    url: https://api.example.com/v1/tickets?status=open
    method: GET                              # default GET
    headers:
      Authorization: "Bearer {{secrets.API_TOKEN}}"
    interval: 30s                            # default 30s
    timeout: 10s                             # per request, default 10s
    retry:
      attempts: 3                            # default 3, then skip to the next interval
    split: .items[]                          # jq: one message per item
    dedupe:
      key: "{{ .json.id }}"
      ttl: 1h
```

Each poll sends `method` (default `GET`) to `url`, with `headers` and an optional static `body`
(for a search API that takes a `POST`). The first poll runs at once, the next ones every
`interval`. Each poll retries per `retry` (`retry_on`, `never_on` and `respect_retry_after` work
as for a destination), then waits for the next interval.

Polls are conditional, so an unchanged response makes no message: after a `2xx`, hikyaku sends
the response's `ETag` and `Last-Modified` back as `If-None-Match` and `If-Modified-Since`, and a
`304 Not Modified` makes no message. `split` is a jq expression that turns one JSON response into
several messages. A message's subject and its `url` attribute are the `url` without its
credentials and query. [`dedupe`](#dedupe) drops an item a later poll fetches again.

### Destination

A destination sends one request per message to `url`.

```yaml
destinations:
  - name: billing
    type: https
    url: https://billing.internal/api/events
    method: POST                             # default POST
    headers:
      Authorization: "Bearer {{secrets.BILLING_TOKEN}}"
      X-Event-Type: "{{ .json.type }}"
    body: '{"event": {{ json .json }}, "source": "{{ .source }}"}'   # default: the message body
    timeout: 3s
```

The reply, or the failure, decides what happens next:

| Reply or failure | Class | Tags |
|---|---|---|
| `2xx` | success | |
| `429` | rate limited: wait for `Retry-After`, capped by `max_backoff` | `4xx`, `429` |
| `408` | timeout | `4xx`, `408`, `timeout` |
| `504` | timeout | `5xx`, `504` |
| other `5xx` | transient: retry | `5xx`, the code |
| other `4xx` | permanent: straight to the fallback chain | `4xx`, the code |
| no connection | transient | `connect` |
| the attempt's `timeout` ran out | timeout | `timeout` |
| a host or address `egress` refuses | permanent | `egress` |

Every attempt carries the `Idempotency-Key` header (`idempotency_header` renames it), so the
receiver can tell a retry from a new message. A reply with an `X-Hikyaku-Reroute: <destination>`
header records a correction for [history-aware routing](/docs/history/#the-feedback-loop). A URL
whose host is a template is checked against [`egress`](/docs/security/#ssrf-guard), and so is
every redirect it follows.

`compression: gzip` compresses every request body and sets `Content-Encoding: gzip`. It pays off
for large bodies and [batches](/docs/delivery/#batching). The receiver must accept gzip requests.
With [`sign`](/docs/security/#signed-deliveries), the signature covers the compressed bytes.

[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/http).

## WebSocket (ws, wss)

A WebSocket is a long-lived, two-way connection that starts as an HTTP request.

```yaml
sources:
  - name: live-events
    type: wss
    mode: subscribe
    url: wss://stream.example.com/events
    headers:
      Authorization: "Bearer {{secrets.STREAM_TOKEN}}"
    reconnect:
      backoff: 500ms
      max_backoff: 30s
      jitter: full
      max_attempts: 0                        # 0 = forever
    failover_urls: [wss://stream-eu.example.com/events]

destinations:
  - name: realtime-dashboard
    type: wss
    url: wss://dash.internal/ingest
    frame: text                              # text (default) | binary
```

- **Subscribe**: hikyaku connects as a client, and every text or binary frame is a message. When
  the connection drops, it reconnects with backoff and jitter, rotating through `failover_urls`.
- **Listen**: hikyaku serves a WebSocket on `listen` and `path`, and every frame a client sends is
  a message.
- **Destination**: one persistent connection, kept alive with ping/pong, sending one frame per
  message. A close code becomes an error tag such as `ws:close_1011`. The error is transient, so
  the default policy retries it. List the tag in `never_on` to stop that.
  [Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/ws).

## Server-sent events (sse)

Server-sent events (SSE) are a one-way stream of events from a server to a client, sent as one
long HTTP response.

```yaml
sources:
  - name: model-stream
    type: sse
    mode: subscribe
    url: https://events.example.com/stream
    reconnect:
      backoff: 1s
      max_backoff: 30s

destinations:
  - name: live-feed
    type: sse
    listen: ":8090"
    path: /events
    event: "{{ .json.type }}"                # the event name, per message
    replay: 1000                             # events kept for Last-Event-ID resume
    auth:
      bearer:
        tokens: ["{{secrets.FEED_READ_TOKEN}}"]
```

- **Subscribe**: every event of the stream is a message. After a reconnect, hikyaku resumes
  with `Last-Event-ID` (the id of the last event it saw, so the server continues from there) and
  follows the server's `retry:` hint.
- **Listen**: a client POSTs an event stream to `listen` and `path`, and every event is a message.
- **Destination**: while the relay runs, hikyaku serves an event stream on `listen` and `path`,
  and every message is an event. One-shot commands such as `hikyaku send` do not serve it. The
  last `replay` events are kept, so a client that reconnects with `Last-Event-ID` catches up.
  `auth` checks the clients that read the stream (`bearer`, `api_key`, `jwt`, `ip_allow`), with
  a 401 for the others. Without it, anyone who reaches the address can read the stream.
  [Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/sse).

## stdin, stdout, stderr and file

These types read and write local streams: a pipe, the terminal, or a file on disk. Use them for
local runs and archives.

```yaml
sources:
  - name: input
    type: stdin                              # one message per line
    format: body                             # body (default) | envelope
  - name: inbox
    type: file
    path: ./in/events.ndjson
    follow: true                             # keep reading lines appended to the file

destinations:
  - name: console
    type: stdout
    format: body                             # envelope (default) | body
  - name: archive
    type: file
    path: ./out/archive.ndjson
```

Every line is a message. `format: envelope` reads and writes the NDJSON wire form of an
envelope (one JSON document per line), ID and headers included. So a file written by one relay
can be read by another, and `hikyaku dlq replay` reads the file DLQ the same way. A file
destination is safe to share between concurrent deliveries: each line is appended whole. Once
every source has ended (stdin at EOF, a file without `follow` at its end), `hikyaku run` finishes
the messages in flight and exits. So a relay works in a pipe:

```sh
cat events.ndjson | hikyaku run -f pipe.yaml
```

[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/file).

## fault

A destination that fails on purpose. Use it in tests, to watch retries, fallbacks and the DLQ at
work. The resilience suite is built on it:

```yaml
destinations:
  - name: flaky
    type: fault
    fault:
      fail: 50%
      latency: 200ms
      error: status
      status: 503
      retry_after: 1s
      seed: 42
```

| Key | Meaning |
|---|---|
| `fail` | share of attempts that fail, such as `50%` |
| `fail_first` | fail the first N attempts, then follow `fail` |
| `latency` | added to every attempt |
| `error` | how an attempt fails: `status` (default), `connect` or `timeout` |
| `status` | the status of a failed attempt (default `503`) |
| `retry_after` | the `Retry-After` hint of a failed attempt |
| `seed` | random seed, for repeatable runs |

[`hikyaku chaos`](/docs/cli/#hikyaku-chaos) wraps any declared destination with the same faults,
without editing the spec.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/fault).

## gRPC (grpc)

A **listen** source is a gRPC server. It always serves `hikyaku.relay.v1.Relay/Send` (subject,
headers, content type and body bytes). It also serves the methods of the services in
`descriptor_set`, of every kind. Requests become JSON envelopes. With `respond.from`, the reply
is mapped into the method's output type.

- **Streams:** each request of a client- or bidi-streaming call is one message. A bidi call
  answers each request in order, once its outcome is final, with at most 64 waiting at a time.
  The first error status ends the call. A server-streaming call streams a reply that is a JSON
  array (or NDJSON), one response per value.
- **Subscribe:** a subscribe source calls a server-streaming `method`, resolved through
  `descriptor_set` or server reflection, and relays every response.
- **Destination:** one call per message, to a method of any kind. The responses of a server- or
  bidi-streaming method are a JSON array. A client- or bidi-streaming method takes a whole
  `batch` in one call, one request per message.

Codes map to the retry classes (`UNAVAILABLE` retries, `INVALID_ARGUMENT` does not).
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/grpc).

## TCP and UDP (tcp, udp)

Raw sockets, for devices and other systems that send bytes with no HTTP in between. Listen
sources and destinations with `frame: newline` (default), `length` (a `length_bytes` big-endian
prefix) or `raw` (tcp: a whole connection; udp: a datagram). Framing is how the bytes are cut
into messages. TCP has no acknowledgement, so a connection reads its next frame only once the
relay took the last one (backpressure through TCP flow control). UDP drops what a full queue
cannot take, and counts it.

`tls` turns on TLS for tcp and DTLS (1.2 and 1.3) for udp, with the same keys: a certificate,
and `client_ca_file` to require client certificates. DTLS is TLS for datagrams. A DTLS source
reads each record the way it reads a datagram. A destination turns DTLS on with a
`dtls://host:port` URL, keeps one association, and writes each message as one record (at most
16,384 bytes). It redials after a failure, when the server closes the association, and when the
association has gone unused for 30s. DTLS acknowledges nothing after the handshake, so a record
the kernel accepted counts as delivered.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/tcp).

```yaml
sources:
  - { name: sensors, type: udp, listen: ":5684", tls: { cert_file: tls.crt, key_file: tls.key, client_ca_file: sensors-ca.crt } }
destinations:
  - { name: collector, type: udp, url: "dtls://collector.internal:5684", tls: { ca_file: internal-ca.crt } }
```

## Kafka (kafka)

A source is a consumer group member (`brokers`, `topic` or `topics`, `group`, `start`). It
commits an offset only past the contiguous prefix of records whose outcome is final. So records
can finish out of order, and a restart resumes without losing one. A failed record holds its
partition's commit until the next restart or rebalance. A destination produces with the
idempotent producer (`acks: all`), a per-message `key`, `compression`, and one produce per
batch. SASL (plain, SCRAM) and TLS.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/kafka).

## NATS and JetStream (nats, jetstream)

`nats` subscribes to a `subject` (with an optional `queue` group) at most once: core NATS keeps
nothing. `jetstream` reads a `stream` through a durable pull consumer. It acks final outcomes,
naks failures with a growing delay, and reports slow messages in progress. Its destination
publishes with `Nats-Msg-Id` set to the envelope ID, so the stream dedupes retries.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/nats).

## MQTT (mqtt)

MQTT 5 (default) and 3.1.1. A source subscribes to `topic` or `topics` at `qos`, with a
persistent session (`clean_session: false`), so the broker keeps what arrives while the relay is
away. Acknowledgements leave in arrival order, as MQTT requires. A failed message is redelivered
through the session. `group` makes an MQTT 5 shared subscription. A destination publishes at
`qos`, with `retain`, and sends headers as user properties (MQTT 5).
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/mqtt).

## RabbitMQ (amqp)

A source consumes a `queue` with `prefetch` and manual acks. Failures are nacked back after a
short delay. `declare: true` declares the queue and binds it to `exchange` with `routing_key`.
A destination publishes with publisher confirms, persistent by default. `mandatory: true` makes
an unroutable message a permanent error.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/amqp).

## Redis Streams and Pub/Sub (redis)

`stream` with `group` reads through a consumer group: XACK on final outcomes. Entries left
pending (a failure, a dead consumer) are reclaimed after `ack_wait`. `channel` subscribes to
Pub/Sub (a pattern with `*`), at most once. Destinations XADD (with `max_len`) or PUBLISH.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/redis).

## AWS: SQS, SNS, EventBridge, S3

`sqs` long-polls a `queue_url`. It deletes final messages in batches, makes failed ones visible
again after a backoff, and extends the visibility of slow ones. `sns` and `eventbridge` read the
SQS queue subscribed to a topic or targeted by a rule, and unwrap the notification or the event.
Destinations send, publish and put events, with FIFO groups and dedupe IDs. `s3` polls a
`bucket` (`split: lines` for NDJSON objects), honors `timeout` for its S3 requests, and deletes,
moves or keeps an object once its messages are final. Its destination writes an object per
message or per batch. Credentials come from the SDK chain, or the `aws:` block (LocalStack
through `aws.endpoint`).
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/aws).

## Google Cloud: Pub/Sub, GCS (pubsub, gcs)

`pubsub` pulls a `subscription` (acks final outcomes, nacks failures) and publishes with
ordering keys. `gcs` polls and writes objects like `s3`. It writes with a does-not-exist
precondition, so a retry never writes twice. A `gcs` source takes no `retry`: the storage client
retries within `timeout`.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/gcp).

## Azure: Service Bus, Event Hubs (servicebus, eventhubs)

`servicebus` receives a `queue` or a `topic` + `subscription` in peek-lock mode: complete on
final outcomes, abandon on failure, lock renewal for slow messages, and sessions when the entity
needs them. `eventhubs` reads the partitions it leases. It keeps checkpoints and leases in a
hikyaku store (`state_store`), so replicas share the partitions and a restart resumes.
[Module README](https://github.com/xavidop/hikyaku/tree/main/protocols/azure).

## OpenAI-compatible endpoint (openai)

The LLM proxy's source. See [LLM proxy](/docs/llm-proxy/).
