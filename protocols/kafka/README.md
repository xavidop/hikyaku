# protocols/kafka

The Kafka protocol for hikyaku: the `kafka` source and destination types. Use the source to read records from Kafka topics, and the destination to write messages to them. It is built on [franz-go](https://github.com/twmb/franz-go) (`kgo`, with `kerr` and SASL `plain`/`scram`), and tested on franz-go's in-process cluster, `kfake`.

```go
import "github.com/xavidop/hikyaku/protocols/kafka"

kafka.Register() // registers kafka as a source and a destination
```

## Source

A `kafka` source reads records from one or more topics. It runs in `subscribe` mode, its only mode, as a member of a consumer group. A consumer group is a set of readers that share a topic: Kafka gives each partition of the topic to one member of the group. Members of the same `group` split the partitions between them, with the cooperative-sticky balancer (it keeps each member's partitions where it can when the group changes). So you scale out by running more replicas of the relay.

| Key | Meaning |
|---|---|
| `brokers` | Bootstrap brokers, `host:port` (a host alone uses port 9092). Required. |
| `topic` / `topics` | The topic consumed, or several. Exact names, no patterns. One of the two is required. |
| `group` | Consumer group. Required: its committed offsets are how a restart resumes. |
| `start` | Where a group with no committed offset begins: `earliest` or `latest` (default `latest`). A group that has committed always resumes from its commit. When the commit fell below the log start (retention removed those records), it resumes at the log start. |
| `ack` | `delivered` (default): a record is final once the relay is done with it. `accepted`: final as soon as the relay holds it. That is only safe with `buffer.type: disk`. |
| `prefetch` | The most records in flight at once: polled, but not final yet (default 64). |
| `max_body_bytes` | Largest record value (default 10MiB). A larger record is logged at warn and skipped. It counts as final, because a redelivery would fail the same way forever. |
| `sasl` | `mechanism` (`plain`, `scram-sha-256` or `scram-sha-512`), `username`, `password` (use `{{secrets.X}}`). |
| `tls` | Client TLS: `enabled`, `ca_file`, `cert_file` + `key_file` (mTLS), `server_name`, `insecure_skip_verify`. |
| `client_id` | The Kafka `client.id`, which brokers use in logs and quotas (default `hikyaku-<relay>-<source>`). |
| `reconnect` | `max_attempts`: give up after that many refused connections in a row (credentials or certificate), or that many polls in a row that the ACLs refuse. `Start` then fails and the relay stops. 0 (the default) keeps retrying, and logs the refusals at warn (the first, then every tenth). `backoff` (500ms), `max_backoff` (30s) and `jitter` (`full`) space the polls after an ACL refusal. franz-go retries connections with its own backoff, from 250ms to 2.5s. Other consumer errors are logged at warn, at most every 30s, and franz-go retries them. |

`url` and `failover_urls` are refused: list every bootstrap broker in `brokers`.

### Acknowledgement and resume

Kafka does not acknowledge records one by one. Instead, a consumer commits an offset: it tells Kafka the position in each partition up to which it is done. A restart resumes from that commit. The source commits offsets itself. It never commits per record. It commits every second, when partitions are revoked or lost in a rebalance (when Kafka moves partitions between the members of the group), and when the relay closes it.

- **Only final records are committed.** A record is final when its outcome is `delivered`, `dead_lettered`, `unrouted` or `filtered`, or when it is skipped for `max_body_bytes`. With `ack: accepted`, it is final once the relay holds it.
- **The commit only moves past an unbroken run of final records.** Records finish out of order, because the relay delivers them in parallel. A partition's committed offset stops at the first record that is not final yet. For example, with records 0, 1 and 3 done and 2 in flight, the source commits offset 2.
- **A failed record holds its partition.** A record fails when its delivery failed and the dead-letter queue failed too, or when shutdown cut it off. Nothing at or after it is committed, and a warning is logged. The partition resumes from that record on the next restart or rebalance. Every record after it is delivered again then. So delivery is at least once, and the envelope ID (below) lets a destination drop the copies. Meanwhile the partition keeps flowing, and a hold costs no memory however long it lasts.
- **Rebalances.** Before a partition moves to another member, the source commits what is final (a blocking commit, bounded at 5s) and forgets the partition. A record of that partition that finishes afterwards is ignored; its new owner delivers it again. A lost partition (the member was dropped from the group) gets the same treatment, although the commit then usually fails.
- **Shutdown.** `Start` returns once the relay stops, and the source stops fetching. The relay then finishes the messages in flight and calls `Close`. `Close` commits what is final (bounded at 5s), leaves the group (bounded at 3s) so the partitions move at once, and closes the client. What was polled but not handed over, or not finished, is delivered again after a restart.
- **Backpressure** (hikyaku stops reading until it catches up). The source hands records over one at a time and waits while the relay is full. It polls at most `prefetch` minus the records in flight. While it is not polling, franz-go stops fetching once it holds one fetch per broker, so memory stays bounded.

A failed commit is logged at warn once (repeats go to debug until one succeeds), and the next commit retries it.

### Envelope

Each record becomes one envelope (the message as hikyaku carries it):

| Field | Value |
|---|---|
| `ID` | The record's `Idempotency-Key` header (up to 256 bytes, also set as the `idempotency_key` attribute). Otherwise `<topic>-<partition>-<offset>`, which is stable across redeliveries. |
| `Subject` | The topic. |
| `Headers` | The record headers. A repeated key keeps every value, in order. |
| `ContentType` | The `content-type` header, any case. Otherwise `application/json` for a JSON value, `text/plain; charset=utf-8` for other UTF-8, `application/octet-stream` for anything else, and nothing for an empty value. |
| `Trace` | The W3C `traceparent` header, when there is one. |

| Attribute | Value |
|---|---|
| `kafka.topic` | The topic (string). |
| `kafka.partition` | The partition (int64). |
| `kafka.offset` | The offset (int64). |
| `kafka.key` | The record key as a string, `""` without one. |
| `kafka.timestamp` | The record timestamp, RFC 3339 with nanoseconds, UTC. |
| `ordering_key` | `<topic>/<partition>`: the order unit that `ordering:` keeps when it has no key template. |

## Destination

A `kafka` destination writes each message to a topic as one record. By default it uses Kafka's idempotent producer with `acks: all`. Idempotent means safe to send twice: the broker drops the duplicates that the producer's own internal retries write.

| Key | Meaning |
|---|---|
| `brokers` | Bootstrap brokers, `host:port`. Required. |
| `topic` | The topic, a per-message template (`orders-{{ .json.region }}`). Required. Topics are never created: a missing one fails permanently. |
| `key` | Template of the record key, which picks the partition (the same hash as the Java client). A key that renders empty means no key. |
| `body` | Template of the record value (default: the message body). |
| `headers` | Record headers, each a template. They are added to the message's own headers (below). |
| `acks` | How many brokers must have the record before it counts as written. `all` (default): every in-sync replica, with the idempotent producer. `leader`: the leader alone. `none`: no acknowledgement. `leader` and `none` turn the idempotent producer off, because it needs `acks: all`. |
| `compression` | `none` (default), `gzip`, `snappy`, `lz4` or `zstd`. A codec falls back to none for brokers that cannot read it. |
| `sasl`, `tls`, `client_id` | As for the source (default client ID `hikyaku-<relay>-<destination>`). |

Each record carries:

- the message's headers, without credentials and hop-by-hop headers (`Authorization`, `Cookie`, `Proxy-Authorization`, `Host`, `Content-Length`, `Connection`, ...);
- then the configured `headers`, each replacing any header of its name;
- `Idempotency-Key`: the message's idempotency key. That is the envelope ID, or the key its source took from the sender. It is the same on every attempt and every fallback, so a consumer can drop the copies that retries write;
- `content-type`: the message's content type, when it has one.

A configured header named `Idempotency-Key` or `content-type` replaces those.

`Deliver` is one produce call, and it waits for the acknowledgement. The result has status 0, the headers `kafka-partition` and `kafka-offset` (no offset with `acks: none`), and the endpoint `kafka://<brokers>/<topic>`.

The destination batches natively. A destination with `batch:` sends each batch as one produce call and waits for every record. If any record fails, the whole batch fails with that record's error, so the relay retries it whole. The result lists every partition and offset, in order.

A delivery waits for its record until the attempt's `timeout`. The producer keeps a record it has not written yet for up to 30s, and retries on its own. So a delivery that timed out may still be written, and a retry can write the message twice. The `Idempotency-Key` header tells the copies apart. A delivery that times out never cancels other deliveries' records. `Close` flushes what the producer still holds, for up to 3s, and closes the client. `Health` pings the brokers.

### Errors

Each delivery is one attempt; the relay owns retries. The producer retries what Kafka calls retriable (a leader moving, too few replicas) by itself, within the attempt's timeout. So those failures usually reach the relay as timeouts. Each error keeps the Kafka error, and carries a class and tags that `retry_on` and `never_on` match:

| Failure | Class | Tags |
|---|---|---|
| The attempt's timeout ran out | timeout | `timeout`, plus `kafka:<CODE>` when the producer knew the last error |
| The timeout ran out while the broker throttled the client (a quota) | rate limited, retry after the throttle | `kafka:THROTTLED`, `429` |
| `THROTTLING_QUOTA_EXCEEDED` | rate limited | `kafka:THROTTLING_QUOTA_EXCEEDED`, `429` |
| `MESSAGE_TOO_LARGE`, `RECORD_LIST_TOO_LARGE`, `INVALID_TOPIC_EXCEPTION`, `INVALID_RECORD`, `TOPIC_AUTHORIZATION_FAILED`, `CLUSTER_AUTHORIZATION_FAILED`, `SASL_AUTHENTICATION_FAILED`, any other non-retriable Kafka error | permanent | `kafka:<CODE>` |
| `UNKNOWN_TOPIC_OR_PARTITION`, once metadata says the topic does not exist (about a second) | permanent | `kafka:UNKNOWN_TOPIC_OR_PARTITION` |
| A retriable Kafka error that reached the relay, and `UNKNOWN_SERVER_ERROR` | transient | `kafka:<CODE>`, and `kafka:NOT_LEADER` for `NOT_LEADER_FOR_PARTITION` |
| The brokers refused the credentials or the certificate | permanent, without waiting for the timeout | `kafka:SASL_AUTHENTICATION_FAILED` for a SASL refusal, none for a certificate |
| A connection failed (dial, reset, a broker that hung up at once) | transient | `connect` |
| A topic, key, header or body template failed, or rendered an empty or invalid topic name | permanent | `template` |
| The destination is closed, or the relay is shutting down | transient | none |

A failure whose tags match neither `retry_on` nor `never_on` falls back to its class: it is retried unless it is permanent. So the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries timeouts, connection failures, throttling and every transient `kafka:<CODE>`, such as `kafka:NOT_LEADER`. It gives up at once on the permanent ones. To stop retrying a code, list it in `never_on`. You only need `retry_on` to retry a permanent code, such as `kafka:UNKNOWN_TOPIC_OR_PARTITION` while a topic is being created.

## Example

A relay that reads two topics with SASL and TLS, and writes to a per-region billing topic:

```yaml
secrets:
  KAFKA_PASSWORD: env:KAFKA_PASSWORD

sources:
  - name: orders
    type: kafka
    brokers: ["kafka-0:9092", "kafka-1:9092"]
    topics: [orders, refunds]
    group: hikyaku-orders
    start: earliest
    prefetch: 128
    sasl: { mechanism: scram-sha-512, username: hikyaku, password: "{{secrets.KAFKA_PASSWORD}}" }
    tls: { ca_file: /etc/kafka/ca.pem }
    reconnect: { max_attempts: 10 }

destinations:
  - name: billing-queue
    type: kafka
    brokers: ["kafka-0:9092"]
    topic: "billing.{{ .json.region }}"
    key: "{{ .json.customer_id }}"
    headers: { X-Origin: "{{ .source }}" }
    compression: zstd
    retry: { attempts: 5, retry_on: [connect, timeout, 429, kafka:UNKNOWN_TOPIC_OR_PARTITION] }
```
