# protocols/amqp

This module adds the `amqp` type to hikyaku, as a source and as a destination. It speaks AMQP 0-9-1, the protocol of RabbitMQ. Use it to consume a queue, or to publish to an exchange. It is built on [`github.com/rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go). The module does its own reconnects: the client's experimental automatic recovery is off.

```go
import "github.com/xavidop/hikyaku/protocols/amqp"

amqp.Register() // registers amqp as a source and a destination
```

## Connection

Every `amqp` source and destination dials one broker, named by `url`. This section covers the URL, the credentials, TLS and the connection name.

`url` is `amqp://` or `amqps://`, with the user, password and vhost in it: `amqp://user:pass@host:5672/vhost`. Percent-encode reserved characters in the user name and password. Write the default vhost `/` as `%2F`, or leave the path out. `username` and `password` replace the URL's (use `{{secrets.X}}` for the password). The URL's own query parameters work as the client documents them: `heartbeat`, `channel_max`, `auth_mechanism`, and `cacertfile`, `certfile`, `keyfile`, `server_name_indication` when there is no `tls` block.

An `amqps://` URL uses TLS. The `tls` block sets `ca_file`, `cert_file` + `key_file` (for mutual TLS, where the client shows a certificate too), `server_name` and `insecure_skip_verify`. A `tls` block next to an `amqp://` URL is refused, because it would be ignored. The URL cannot be a per-message template: the connection is dialed before any message exists.

Each connection is named `hikyaku-<relay>-<name>` (the `connection_name` client property). So the broker's management UI shows which relay, and which source or destination, holds it. Logs and errors show URLs with the password replaced by `xxxxx`.

## Source

A source consumes one queue and turns each delivery into one envelope. It subscribes (`mode: subscribe`, the default and the only mode) with manual acknowledgements: the source tells the broker when each message is done. The consumer tag is the connection name.

Prefetch is how many unacknowledged messages the broker sends ahead to this consumer.

| Key | Meaning |
|---|---|
| `url` | The broker (required). |
| `failover_urls` | More brokers (the other nodes of a cluster), rotated through when a connect fails. |
| `username`, `password` | Replace the URL's credentials. |
| `tls` | Client TLS for `amqps://`. |
| `queue` | The queue consumed (required). |
| `prefetch` | Unacknowledged messages the broker sends ahead (`basic.qos`, per consumer), 1 to 65535. Default 64. |
| `declare` | Declare the queue (durable, not exclusive, not auto-deleted) when it is missing. Leave it `false` for queues provisioned elsewhere, such as quorum queues, streams, or queues with arguments. |
| `exchange` | With `declare: true`: also declare this exchange (`topic`, durable) and bind the queue to it. Without `declare` it is refused, since the source would not bind anything. |
| `routing_key` | The binding key on `exchange` (default `#`, everything). Needs `exchange`. |
| `ack` | `delivered` (default) or `accepted`; see below. |
| `max_body_bytes` | Largest message (default 10MiB). A larger one is logged at warn and acknowledged, so it does not come back forever. |
| `reconnect` | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

### Acknowledgements

To acknowledge a message is to tell the broker the message is done. `ack` decides when that happens. A message the relay could not finish goes back to the queue.

- **`ack: delivered`** (default). A message is acknowledged (`basic.ack` of its own delivery tag) once the relay is done with it: delivered to every required destination, dead-lettered, unrouted or filtered. Messages finish in any order. Each tag is acknowledged as its message finishes, so one slow message does not hold the others back.
- **A failed message** (the dead-letter queue failed too, or shutdown cut its delivery) goes back to the queue with `basic.nack` and `requeue`. The nack waits first, so a message that keeps failing does not spin through the relay: 500ms on its first failed attempt, doubling per attempt up to 10s, with equal jitter. Attempts are counted by message ID (else the `Idempotency-Key` header, else the exchange, routing key and body), or taken from a quorum queue's `x-delivery-count` header. While the source stops, failed messages go back at once.
- **`ack: accepted`** acknowledges as soon as the relay holds the message. That is only safe with `buffer.type: disk`.
- **Oversize messages** are acknowledged and dropped, with a warning naming the delivery tag, message ID and size.

### Reconnects and redeliveries

This section covers what happens when the connection, the channel or the consumer goes away, and what happens at shutdown.

There is nothing to resume. The broker keeps every unacknowledged message. When the channel it was sent on closes, the broker redelivers the message, with `redelivered` set.

The source reconnects when the connection or the channel closes (the broker restarted, `CONNECTION_FORCED`, a channel exception), or when the broker cancels the consumer (its queue was deleted). It reconnects with backoff and jitter (a random part added to each wait), declares again when `declare` is set, and consumes again. Each failed connect moves on to the next URL of `url` + `failover_urls`. A connection that delivered a message, or stayed up for 30s, resets the failure count, and the source tries the same URL first again. Each reconnect is logged at warn. After `max_attempts` failed connects in a row (bad credentials, say), the source fails, and the relay stops.

Delivery tags only mean something on the channel that sent them. A message in flight when its channel closed can no longer be acknowledged. Its ack is dropped (logged at debug), and the broker redelivers the message, so the relay sees it twice. Destinations that dedupe on the message ID (the envelope ID) drop the copy.

At shutdown the consumer is canceled first. The deliveries the client received but never handed to the relay go back to the queue at once (`basic.nack` with `requeue`). The messages in flight still settle on the open channel. The relay closes the source (`plugin.Closer`) once they are done. Delayed nacks go out then, and the connection closes. Anything still unacknowledged at that point goes back to the queue.

### Envelope

Each delivery becomes one envelope (the message as hikyaku carries it). Its fields come from the AMQP properties and headers:

| Field | Value |
|---|---|
| `ID` | The `message_id` property; else the `Idempotency-Key` header (up to 256 bytes); else a new ULID |
| `Subject` | The routing key the message was published with |
| `ContentType` | The `content_type` property; else `application/json` for a body that is valid JSON, `text/plain; charset=utf-8` for other UTF-8, `application/octet-stream` for anything else |
| `Headers` | The headers table, under its own names. Strings, numbers and booleans as written, timestamps in RFC 3339, byte arrays as text (base64 unless UTF-8), nested tables as JSON. An array becomes one header value per element. Plus `Content-Type`, `Content-Encoding`, `Correlation-Id` and `Reply-To` from the properties, when set |
| `Attributes` | `amqp.queue`, `amqp.exchange` (`""` for the default exchange), `amqp.routing_key`, `amqp.delivery_tag` (int64), `amqp.redelivered` (bool), `amqp.message_id` (when set), `amqp.timestamp` (RFC 3339, when set), and `ordering_key`: the queue |

`ordering_key` is the queue. So a source with `ordering:` and no key delivers the queue's messages one at a time, in the order the broker sent them.

### Health

A source is healthy while it consumes and the broker still has its queue. The queue check is a passive declare on a short-lived channel of its own. So a missing queue closes that channel, not the consumer's.

## Destination

A destination publishes each message to an exchange, with a routing key rendered per message.

A destination keeps one connection and one channel. Deliveries take turns on it. Publisher confirms are always on: the broker confirms each message it took in. A delivery succeeds once the broker confirms the message. For a persistent message on a durable queue, that means it is on disk. The connection is dialed on the first delivery (or health check). A closed connection is redialed by the next delivery. A channel that a channel exception closed is reopened on the same connection.

| Key | Meaning |
|---|---|
| `url` | The broker (required). |
| `username`, `password` | Replace the URL's credentials. |
| `tls` | Client TLS for `amqps://`. |
| `exchange` | The exchange published to. Default the default exchange, which routes a message to the queue named by its routing key. |
| `routing_key` | Per-message template of the routing key, such as `{{ .subject }}` or `orders.{{ .json.region }}`. Required on the default exchange, where it is the queue name. At most 255 bytes once rendered. |
| `headers` | Per-message templates of headers, set in the headers table as strings (they replace the message's headers of the same name). |
| `body` | Per-message template of the body (default the message body). |
| `persistent` | Persistent delivery mode (2), the default; `false` publishes transient messages (1). |
| `mandatory` | Have the broker return a message no queue is bound to receive, which then fails as permanent (`amqp:312`), instead of dropping it silently. |
| `declare` | Declare `exchange` (`topic`, durable) when it is missing. Needs `exchange`. |

Each message is published with:

- **Message ID:** the envelope ID (its SHA-256 when the ID is over the 255-byte AMQP limit).
- **Timestamp:** when the relay received the message.
- **Content type:** the envelope's.
- **Content encoding, correlation ID, reply-to:** the `Content-Encoding`, `Correlation-Id` and `Reply-To` headers. A value over 255 bytes stays a header.
- **Headers table:** the envelope's headers, one value as a string and several as an array of strings. `Idempotency-Key` is always there (the envelope ID when the message has none).

Some headers are left out of the table:

- hop-by-hop and framing headers (`Connection`, `Host`, `Content-Length` and so on);
- the caller's credentials (`Authorization`, `Cookie`, `Proxy-Authorization`);
- the ones that became properties;
- the broker's bookkeeping of a message read from another queue (`x-death`, `x-first-death-*`, `x-last-death-*`, `x-delivery-count`), which would be wrong on a new message.

A batch (`batch:` on the destination) is published whole. Then the destination waits for every confirm. The batch fails as a whole, with its most severe failure, and the retry publishes every message again: consumers drop the copies by message ID.

Each delivery is one attempt, because the relay owns retries. A failed delivery carries a class and tags. `retry_on` and `never_on` match the tags:

| Failure | Class | Tags |
|---|---|---|
| The dial failed (refused, DNS, TLS) | transient | `connect` |
| The attempt's timeout ran out (dialing, or waiting for the confirm) | timeout | `timeout` |
| The broker nacked the message (an internal error, or a full queue with `x-overflow: reject-publish`) | transient | `amqp:nack`, `5xx` |
| A mandatory message was returned: no queue is bound for it | permanent | `amqp:312` |
| The broker blocks the connection (a memory or disk alarm) | rate limited, retry after 5s | `amqp:blocked`, `429` |
| An exception a retry cannot fix: 403 `ACCESS_REFUSED` (bad credentials or permissions), 404 `NOT_FOUND` (no such exchange), 406 `PRECONDITION_FAILED`, 530 `NOT_ALLOWED` (the vhost), and 311, 313, 402, 540 | permanent | `amqp:<code>`, such as `amqp:404` |
| Any other exception, which closed the channel or the connection: 320 `CONNECTION_FORCED`, 504 `CHANNEL_ERROR`, 506 `RESOURCE_ERROR`, 541 `INTERNAL_ERROR`, ... | transient | `amqp:<code>`, `connect` |
| The channel or the connection closed before the broker confirmed | transient | `connect` |
| A template failed to render | permanent | `template` |

So the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries dropped connections, timeouts, nacks and blocked connections (after the broker's hint). It sends permanent failures straight to the fallback or the dead-letter queue. A delivery while the connection is blocked fails at once, without publishing. If the attempt runs out while the broker's TCP pushback holds a publish, the connection is closed under it, and the next delivery reconnects.

The destination is healthy while it is connected (it dials when it is not) and not blocked. Closing it waits for the delivery in flight, as long as its context allows, then closes the connection.

## Example

One source consumes a queue on a three-node cluster over TLS, and declares the queue and its exchange binding. One destination publishes to an audit exchange, with `mandatory` so an unrouted message fails instead of vanishing.

```yaml
secrets:
  RABBIT_PASSWORD: env:RABBIT_PASSWORD

sources:
  - name: jobs
    type: amqp
    url: amqps://relay@rabbit-0.internal:5671/prod
    failover_urls: [amqps://relay@rabbit-1.internal:5671/prod, amqps://relay@rabbit-2.internal:5671/prod]
    password: "{{secrets.RABBIT_PASSWORD}}"
    tls: { ca_file: /etc/hikyaku/rabbit-ca.pem }
    queue: hikyaku.jobs
    declare: true
    exchange: events
    routing_key: "job.*"
    prefetch: 32
    max_body_bytes: 1MiB
    reconnect: { backoff: 500ms, max_backoff: 30s, max_attempts: 0 }

destinations:
  - name: audit
    type: amqp
    url: amqps://relay@rabbit-0.internal:5671/prod
    password: "{{secrets.RABBIT_PASSWORD}}"
    exchange: audit
    declare: true
    routing_key: "audit.{{ .subject }}"
    headers: { X-Relay: hikyaku }
    mandatory: true
    retry: { attempts: 5, retry_on: [connect, timeout, 5xx, 429] }
```
