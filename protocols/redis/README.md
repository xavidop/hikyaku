# protocols/redis

This module adds the `redis` type to hikyaku, as a source and as a destination. It works over Redis Streams (a log of entries that readers acknowledge one by one) and Redis Pub/Sub (channels that deliver to whoever is listening right now). It is built on [`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis).

```go
import "github.com/xavidop/hikyaku/protocols/redis"

redis.Register() // registers redis as a source and a destination
```

## Connection

Sources and destinations dial one server, from `url`. These keys are shared by both:

| Key | Meaning |
|---|---|
| `url` | `redis://[user:password@]host:port/db` or `rediss://` (TLS). It is dialed before any message exists, so it cannot be a per-message template. go-redis URL options pass through (`?dial_timeout=3s&pool_size=20`), except `max_retries`: each call is one attempt. |
| `username`, `password` | Replace the URL's. Use `{{secrets.X}}`. |
| `tls` | `ca_file`, `cert_file` + `key_file` (mTLS), `server_name` (default: the URL's host), `insecure_skip_verify`. Any of them, or `enabled: true`, turns TLS on for a `redis://` URL. `rediss://` is always TLS, so `enabled: false` with it is an error. |
| `failover_urls` | Sources only. Servers to rotate through, such as a standby that takes over as primary. A failed attempt moves on to the next one. |

In the `tls` row, mTLS is mutual TLS: the client shows a certificate too.

Logs, errors and results show URLs with the password hidden (`redis://user:xxxxx@host`). Each connection names itself `hikyaku-<relay>-<entry>` (CLIENT SETNAME), so `CLIENT LIST` shows who is connected.

## Sources

A source reads either a stream, with `stream` and `group`, or a pub/sub channel, with `channel`. It runs in `subscribe` mode.

Some stream terms the keys use:

- A **consumer group** is a named group of readers that share one stream. Redis remembers which entries each reader took and has not acknowledged yet.
- A **pending entry** is an entry a consumer read but has not acknowledged yet.
- To **reclaim** an entry is to take over a pending entry from the consumer that held it, so it can be delivered again.
- **Prefetch** is how many entries the source may hold in flight at once.

| Key | Applies to | Meaning |
|---|---|---|
| `stream` | streams | The stream key read with XREADGROUP. |
| `group` | streams | The consumer group. It is required with `stream`. The source creates it, and the stream with it, when it is missing (`XGROUP CREATE ... MKSTREAM`). |
| `consumer` | streams | This consumer's name in the group. The default is `<host name>-<source name>`. A restart resumes the entries its name left pending, so keep the name stable. Two processes must not share one. |
| `start` | streams | Where a new group starts: `latest` (default, `$`) or `earliest` (`0`). It is ignored when the group exists. |
| `prefetch` | streams | Entries in flight at once (default 64): read and not yet acknowledged. A reclaim pass can briefly go past it. |
| `ack_wait` | streams | How long a pending entry stays idle before it is reclaimed and delivered again (default 30s). |
| `ack` | streams | `delivered` (default): acknowledge once the outcome is final. `accepted`: acknowledge once the relay queued the message, which is only safe with `buffer.type: disk`. |
| `channel` | pub/sub | The channel to SUBSCRIBE to, or a pattern for PSUBSCRIBE when it holds `*`, `?` or `[` (`events.*`). |
| `max_body_bytes` | both | The largest message (default 10MiB). A larger stream entry is logged at warn and acknowledged, because redelivering it would fail forever. A larger pub/sub message is logged and dropped. |
| `reconnect` | both | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

The stream settings (`group`, `consumer`, `start`, `prefetch`, `ack_wait`, `ack: accepted`) are refused with `channel`.

When a command fails, the source waits with backoff and jitter (a random part added to each wait). Then it tries again on the next server of `url` + `failover_urls`. Each retry is logged at warn. A failure right after a working read is a dropped connection: the reconnect after it is the attempt that counts. After `max_attempts` failed attempts in a row (such as bad credentials), `Start` returns an error and the relay stops. `Health` reports the last error while the source is failing, and otherwise PINGs the server.

### Streams: acknowledgement and resume

A stream source acknowledges each entry when it is done with it, and picks up where it left off after a restart. Delivery is at least once.

- **Acknowledgement.** An entry is acknowledged (XACK) once its outcome is final: delivered, dead-lettered, unrouted or filtered. With `ack: accepted`, that happens once the relay has queued it. Acks are sent in batches. While Redis is unreachable they are retried with the reconnect backoff, and the entry stays in flight until its XACK succeeds. At shutdown, `Close` waits for the queued acks, within the relay's close timeout. An ack it cannot send leaves its entry pending, and the entry is delivered again later.
- **Failure.** A failed entry stays pending. That is an entry whose dead-letter write failed too, or whose delivery the shutdown cut short. Every `ack_wait / 2` the source reclaims (XAUTOCLAIM) the group's entries that have been idle longer than `ack_wait`, whichever consumer held them. Those are the entries it failed, and the entries a dead consumer left behind. It delivers them again, and `redis.deliveries` counts the deliveries.
- **In flight.** On the same schedule, the source refreshes the idle time of its entries in flight (`XCLAIM ... JUSTID`, which leaves the delivery count alone). That covers entries read and still waiting for the relay. So no consumer, this one or another, reclaims an entry that is still being delivered, however long the retries and fallbacks take.
- **Resume.** At startup, the consumer first reads its own pending entries (`XREADGROUP ... 0`), then new ones (`>`). A restart therefore resumes where the previous run stopped. A pending entry deleted since (XDEL, or trimmed by `MAXLEN`) comes back without fields. It is acknowledged and skipped.
- **Lost group.** After any failure the source creates the group again if it is gone. So a Redis restart that lost its data does not stop the source.
- **Guarantee.** Delivery is at least once. An entry is delivered again after a crash, after a failure, or when its XACK is lost. Its envelope ID stays the same, so a destination that dedupes on the idempotency key can drop the copy.

### Pub/Sub: at most once

A pub/sub source gets each message at most once. A message it does not receive is gone.

Redis keeps nothing for a subscriber that is away. A message published while the source reconnects is lost. There is nothing to acknowledge: the source does not settle with Redis. Every message is its raw body; pub/sub has no headers and no envelope format. While the relay applies backpressure (it stops reading until it catches up), the source stops reading. Redis buffers what is published meanwhile, up to its `client-output-buffer-limit` for pub/sub clients (32MB, or 8MB for 60s, by default). Past that limit, Redis drops the connection and those messages are lost.

A subscription that has been quiet for 30s is pinged. One that does not answer within 10s is closed and made again, so a half-open connection cannot lose messages unnoticed. A source waiting for the relay is not pinged. After any reconnect the source subscribes again.

### Envelope

Each entry or message becomes one envelope (the message as hikyaku carries it). Streams and pub/sub fill it differently:

| Field | Streams | Pub/Sub |
|---|---|---|
| `ID` | the entry's `id` field, else `<stream>-<entry ID>` (stable across redeliveries) | a new ULID |
| `Subject` | the entry's `subject` field, else the stream key | the channel the message was published to |
| `ContentType` | the entry's `content_type` field, else sniffed: `application/json`, `text/plain; charset=utf-8` or `application/octet-stream` | sniffed |
| `Headers` | one per `h:<Name>` field | none |
| `Body` | the entry's `body` field | the message |
| `Trace` | from `h:Traceparent` / `h:Tracestate`, when present | none |

| Attribute | Value |
|---|---|
| `redis.stream` | the stream key |
| `redis.entry_id` | the entry's ID, such as `1700000000000-0` |
| `redis.group` | the consumer group |
| `redis.consumer` | this consumer's name |
| `redis.deliveries` | how many times the entry was delivered, this time included (1 for a new entry; read from XPENDING for a resumed or reclaimed one) |
| `redis.channel` | pub/sub: the channel the message was published to |
| `redis.pattern` | pub/sub: the pattern that matched, for a pattern subscription |
| `ordering_key` | the stream key, or the channel |

## Destination

A destination appends each message to a stream (`stream`, XADD) or publishes it to a channel (`channel`, PUBLISH).

| Key | Meaning |
|---|---|
| `stream` | The stream key appended to, a fixed name. |
| `max_len` | Streams: trim the stream to about this many entries (`XADD ... MAXLEN ~ n`). Redis trims whole nodes, so the stream can hold a little more. The default 0 never trims. |
| `channel` | The channel published to, as a per-message template (`alerts.{{ .json.level }}`). A channel that renders empty fails the message as permanent. |
| `body` | Per-message template of the body written (default: the message body). |
| `headers` | Streams: per-message templates written as `h:<Name>` fields. Each replaces the message's header of the same name, matched case-insensitively. They are refused with `channel`. |
| `url`, `username`, `password`, `tls` | See [Connection](#connection). |

Each delivery is one attempt, because the relay owns retries. The result carries `redis-entry-id` (the ID Redis gave the entry) for a stream, or `redis-receivers` (how many subscribers got it) for a channel. A publish that reached no subscriber still succeeds. `Health` PINGs the server. `Close` closes the connections. A delivery returns once Redis has answered, so there is nothing to flush.

With `batch:` on the destination, a batch is one round trip. For a stream it is one MULTI/EXEC of XADDs, so a connection lost before EXEC writes none of them. For a channel it is one pipeline of PUBLISHes. The result's headers then hold one value per message, in order. Any failure fails the whole batch. A retry may append entries that were already written. Their `id` field lets a reader dedupe them.

### Stream entry format

A stream destination writes these fields, and a stream source reads them back:

| Field | Value |
|---|---|
| `id` | the envelope ID |
| `body` | the body, binary-safe |
| `content_type` | the content type, when the message has one |
| `subject` | the subject, when the message has one |
| `h:<Header-Name>` | one per header, with repeated values joined by `, ` |

The message's own credentials and hop-by-hop headers are not written: `Authorization`, `Cookie`, `Proxy-Authorization`, `Connection`, `Host`, `Content-Length` and so on. `Content-Type` is not written as a header either, because `content_type` carries it.

An entry written by another producer, without a `body` field, becomes a message whose body is the JSON object of all its fields (string values), such as `{"n":"1","user":"ada"}`. Its content type is `application/json` and its ID is `<stream>-<entry ID>`. An `id` field among its fields is data, not the message ID.

### Errors

A failed delivery carries a class and tags. `retry_on` and `never_on` match the tags:

| Failure | Class | Tags |
|---|---|---|
| Refused, reset or broken connection; i/o or pool timeout; TLS failure | transient | `connect` |
| The attempt's timeout ran out | timeout | `timeout` |
| `LOADING`, `BUSY`, `TRYAGAIN`, `CLUSTERDOWN`, `MASTERDOWN`, `READONLY` | transient | `redis:<CODE>`, such as `redis:LOADING` |
| `OOM` | rate limited, retry after 1s | `redis:OOM`, `429` |
| `WRONGTYPE`, `NOPERM`, `NOAUTH`, `WRONGPASS`, `ERR` | permanent | `redis:<CODE>` |
| `ERR max number of clients reached` | transient | `connect`, `redis:ERR` |
| Any other error code (`MISCONF`, `NOREPLICAS`, ...) | transient | `redis:<CODE>` |
| A reply that is not an error code (free text, such as a proxy's) | transient | none, so it is retried |
| A template (`channel`, `body`, `headers`) failed to render | permanent | `template` |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`) retries connection failures, timeouts and OOM. A tag that neither `retry_on` nor `never_on` names falls back to its error's class. So the transient server states (`redis:LOADING`, `redis:READONLY`, `redis:TRYAGAIN`, `redis:BUSY` and so on) are retried too, and the permanent ones (`redis:WRONGTYPE`, `redis:NOPERM` and so on) are not. List a tag in `never_on` to send it to the fallback at once instead.

## Example

Two sources: one reads a stream through a consumer group, from the oldest entry, and one subscribes to a channel pattern over TLS. Two destinations: one appends to a trimmed audit stream, and one publishes to a per-tenant channel as best effort.

```yaml
secrets:
  REDIS_PASSWORD: env:REDIS_PASSWORD

sources:
  - name: orders
    type: redis
    url: redis://redis.internal:6379/0
    password: "{{secrets.REDIS_PASSWORD}}"
    stream: orders
    group: hikyaku
    start: earliest
    prefetch: 128
    ack_wait: 1m
    reconnect: { backoff: 500ms, max_backoff: 30s, max_attempts: 0 }

  - name: alerts
    type: redis
    url: rediss://cache.example.com:6380
    channel: "alerts.*"

destinations:
  - name: audit-log
    type: redis
    url: redis://redis.internal:6379/0
    password: "{{secrets.REDIS_PASSWORD}}"
    stream: audit
    max_len: 1000000
    retry: { attempts: 5, never_on: [redis:MISCONF] }

  - name: live-feed
    type: redis
    url: redis://redis.internal:6379
    channel: "feed.{{ .json.tenant }}"
    required: false
```
