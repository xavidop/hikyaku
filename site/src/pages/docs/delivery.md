---
layout: ../../layouts/DocsLayout.astro
title: Buffering, ordering and batching
---

# Buffering, ordering and batching

This page covers three settings that shape how messages move from sources to destinations. You
need it when a crash must not lose queued messages, when messages for one key must arrive in
order, or when a destination should get many messages at once.

- The **buffer** holds queued messages: in memory, or in a write-ahead log on disk (a file every
  message is written to before anything else happens to it).
- **Ordering** delivers messages that share a key one at a time, in order.
- **Batching** delivers many messages to a destination in one call.

## The disk buffer

```yaml
buffer: { type: disk, path: /var/lib/hikyaku/buffer.db, size: 10000 }
```

With `type: memory` (the default), queued messages live in memory. Sources that wait for the
outcome (`ack: delivered`, the default) never lose a message. A crash, though, drops what is
queued at that moment.

With `type: disk`, hikyaku keeps a write-ahead log in a [bbolt](https://github.com/etcd-io/bbolt)
file. It works like this:

- **Written first.** hikyaku writes a message to the log before its source hears that it is
  queued. Writes are grouped, with one fsync (a forced write to the physical disk) per group, so
  a busy relay does not fsync once per message. `sync: false` skips the fsync. That is faster,
  but a host crash (not a process crash) can lose the last writes.
- **Acknowledged early, safely.** With `ack: accepted`, the source answers, or acknowledges to
  its broker, as soon as the message is on disk. An HTTP source answers `202`, and a broker
  source acks right away. The caller or broker never waits on the destination.
- **Deleted when final.** A message leaves the log once its outcome is final: delivered,
  dead-lettered, unrouted or filtered.
- **Retried from the log.** A message can fail because its destination and the dead-letter
  queue (DLQ, where a message goes when every delivery attempt failed) were both down. hikyaku
  retries it from the log with backoff (a growing wait between tries) while the relay runs. A
  message with an [ordering](#ordering) key is retried in its place, and the messages behind it
  on that key wait. If the relay stops first, they all stay in the log, and the next start
  delivers them in order.
- **Delivered after a crash.** Whatever is in the log when the relay stops, or is killed, is
  delivered by the next start. Each message keeps its envelope ID, so a destination can spot a
  duplicate. `scripts/chaos-e2e.sh` checks this: it accepts 20 messages, runs `kill -9`, and the
  next start delivers all 20.

`size` caps the log. When the log is full, listen sources answer `503` and subscribe sources
stop reading.

One relay owns a buffer file. A second relay that opens the same file fails fast. A reload that
keeps the same buffer hands the file over: the old relay stops its sources, finishes its
in-flight messages within `server.shutdown_grace`, and closes the file. Then the new relay opens
it. Listen sources refuse connections in between. One-shot commands (`send`, `route`,
`dlq replay`) never open the file.

In Kubernetes, put `path` on the chart's persistent volume (`persistence.enabled: true`, which
deploys a StatefulSet).

## Ordering

```yaml
sources:
  - name: orders
    type: kafka
    ordering: {}                            # the source's natural unit: the partition
  - name: webhooks
    type: http
    listen: ":8080"
    ordering: { key: "{{ .json.customer_id }}" }
```

Use ordering when messages that belong together must arrive in the order they were sent.
Messages with the same ordering key are delivered **one at a time, in the order the source
produced them**. Messages with other keys, or with no key, run in parallel. A message that is
retrying holds back the messages behind it with the same key, and only those. That is the point
of a key.

An empty `key` uses the source's natural unit of order: the `ordering_key` attribute that every
broker source sets. That unit is a Kafka partition, an MQTT topic, an AMQP queue, a JetStream
subject, an SQS FIFO message group, a Pub/Sub ordering key, a Service Bus session or an Event
Hubs partition. A constant key (`key: all`) orders the whole source.

## Batching

```yaml
destinations:
  - name: lake
    type: s3
    bucket: events
    key: "raw/{{ .id }}.ndjson"
    batch: { max_messages: 500, max_bytes: 4MiB, max_wait: 2s }
```

Use batching when a destination should get many messages in one call, such as a data lake. A
destination with `batch:` collects messages and delivers them together. A batch is sent when the
first of these happens: it holds `max_messages` (default 100), it holds `max_bytes` (default
1MiB), or `max_wait` (default 1s) has passed since its first message.

- **Native batch calls.** A protocol that has a batch call delivers all the members in one call:
  one Kafka produce, one SQS `SendMessageBatch`, one S3 or GCS object, one Pub/Sub or Event Hubs
  batch, or one call of a gRPC client-streaming or bidi-streaming method.
- **A combined body otherwise.** Any other destination (http, ws, file, and so on) gets one
  message. Its body holds the members' bodies, as NDJSON (the default) or as a JSON array
  (`format: json_array`).
- **One outcome, per member.** A batch walks the destination's chain once: retries, circuit
  breaker, fallbacks. Every member shares the outcome. A dead-lettered batch writes one DLQ
  record per message, so `dlq replay` replays them one by one.
- **Transforms per member.** Each hop's transforms run on each member before the members are
  combined. A member that a filter drops is finished: it is not sent.
- **Workers are not held.** A member that waits for its batch gives its worker slot back. So a
  batch can hold more messages than `server.workers`.
- **One-shot deliveries.** `hikyaku send`, `hikyaku dlq replay` to a destination, and
  `hikyaku chaos --data` hand a batching destination a batch of one message, right away. So the
  destination still gets the format it expects.
- **Batches are per destination.** A destination's `batch` applies to the messages routed to it.
  When that destination is another destination's fallback, it delivers the other destination's
  messages as they come: one at a time, or in the other destination's batches. `hikyaku lint`
  warns about a fallback that batches.

Ordering and batching work against each other: an ordered key waits for its batch on every
message. `hikyaku lint` warns when you combine them.

## Delayed delivery

```yaml
buffer: { type: disk, path: /var/lib/hikyaku/buffer.db }

destinations:
  - name: reminder
    type: https
    url: https://mailer.internal/send
    deliver_after: 1h

  - name: scheduled
    type: https
    url: https://mailer.internal/send
    deliver_at: "{{ .json.send_at }}"
```

A destination can hold a route's messages and deliver them later. That covers "send the reminder
in an hour" and "send this on Monday at nine" without a scheduler next to the relay.

- **`deliver_after`** holds every message for that long, counted from when the relay received
  it.
- **`deliver_at`** is a per-message template of the time to deliver at, in RFC 3339, such as
  `2026-12-24T09:00:00Z`. A time in the past delivers at once. A message it renders empty for
  follows `deliver_after`, or goes at once when there is none. A value that is not a time is the
  message's fault: a permanent failure, so the message goes to the dead-letter queue.

When a message is due, it goes through the destination's normal path: its batch, its retries,
its rate limit and its fallbacks. A held message does not hold a worker, so held messages do not
slow the others. `hikyaku_delayed_messages{destination}` is how many a destination holds now.

**Use the disk buffer.** A held message is not acked until it is delivered:

- With `buffer.type: disk`, a held message stays on disk. At a shutdown it is not dead-lettered:
  it stays in the buffer, and the next start delivers it when it is due. The wait does not start
  over, because both keys give an absolute time. Held messages count against `buffer.size`, so
  size the buffer for what you hold.
- With `buffer.type: memory`, held messages live in memory. A restart or a reload nacks them:
  a broker source delivers them again, and an HTTP caller that already got its answer does not.
  `hikyaku lint` warns about a delayed destination without the disk buffer.

What to know:

- An HTTP source with `ack: delivered` waits for the delivery, so its caller gets `202` with
  status `pending` after `respond.timeout`. Use `ack: accepted` on sources that feed a delayed
  destination.
- Only messages a route sends are held. `hikyaku send`, `hikyaku dlq replay --to` and
  `hikyaku chaos` deliver at once. A fallback hop is not held either: only the destination the
  route named.
- With [ordering](#ordering), a held message holds its key: the messages behind it wait too.
- For a short delay on SQS, the queue's own `delay` (up to 15 minutes) does the same on the
  broker's side.

## Shadow traffic

```yaml
destinations:
  - name: crm
    type: https
    url: https://crm.internal/events

  - name: crm-v2
    type: https
    url: https://crm-v2.internal/events
    shadow: true
    sample: 0.05          # 5% of the messages; default 1, every message

routes:
  - name: orders
    from: [api]
    to: [crm, crm-v2]
```

A shadow destination gets a copy of what the routes send it, on the side. Use it to try a new
consumer, a new version of a service or a new model against real messages, before anything
depends on it.

- **Nothing waits for the copy.** The message is acknowledged, and the caller is answered, when
  the other destinations have it.
- **A failure changes nothing.** It is counted and logged. It is never dead-lettered, it does not
  fail the message, and it does not make the relay not ready. The destination still retries and
  falls back as its own settings say.
- **`sample`** is the share of messages copied, from above 0 to 1. The message's ID decides, not
  chance: a retried or replayed message gets the same answer.
- **The copy is marked.** It carries the `shadow` attribute, and an http receiver sees the
  header `X-Hikyaku-Shadow: true`. A receiver with side effects (an email, a charge) should check
  it.
- At most 256 copies are in flight at once. A shadow destination that cannot keep up loses
  copies (counted as `dropped`); it never slows the relay down.
- At shutdown, the copies in flight get five seconds to finish.

This is not the AI router's [shadow mode](/docs/routing/#shadow-mode), which lets a router
decide without delivering. A shadow destination does deliver, to a place where a failure costs
nothing.

`hikyaku_shadow_total{destination,outcome}` counts the copies: `delivered`, `failed` or `dropped`.

A shadow destination is never `required`. It cannot be a fallback, the answer of `respond.from`,
or the only destination of a route, and it does not go with `deliver_after`. `hikyaku send` and
`dlq replay` to a shadow destination deliver to it like to any other.
