---
layout: ../../layouts/DocsLayout.astro
title: Benchmarks
---

# Benchmarks

This page shows what hikyaku itself costs per message, as `make bench` measures it. Read it to
know the relay's own overhead before you plan capacity, and to see which settings change it.

`make bench` measures:

- **The relay end to end.** A source produces N messages. They go through routing, the workers
  and delivery, to a sink that answers at once. So the numbers are the relay's overhead, not a
  network's.
- **Each transform step**, the envelope's wire format, and the disk buffer.

Measured on an Apple M1 Pro (10 cores), macOS, Go 1.27.3, with `server.workers: 64`:

| Benchmark | Per message | Throughput | Allocations |
|---|---|---|---|
| Relay, memory buffer | 5.5 µs | 181,000 msgs/s | 64 |
| Relay, CEL filter + template transform | 3.9 µs | 259,000 msgs/s | 131 |
| Relay, CEL rules router | 6.4 µs | 156,000 msgs/s | 97 |
| Relay, ordered by key (64 keys) | 3.5 µs | 289,000 msgs/s | 98 |
| Relay, batched (500 per batch) | 3.2 µs | 317,000 msgs/s | 26 |
| Relay, disk buffer (fsync) | 17 µs | 57,000 msgs/s | 80 |
| Relay, disk buffer (`sync: false`) | 7.7 µs | 129,000 msgs/s | 90 |

| Step | Time | Allocations |
|---|---|---|
| CEL filter (`startsWith` and a comparison) | 0.8 µs | 17 |
| jq reshape (an object of 4 fields) | 1.3 µs | 22 |
| Go template (3 fields) | 1.4 µs | 34 |
| A chain: filter, jq, header template | 7.8 µs | 118 |
| Envelope to wire JSON and back | 4.6 µs | 27 |
| Envelope clone | 0.25 µs | 6 |

## Reading them

- **These are ceilings, not predictions.** A real destination is a network round trip, usually
  milliseconds. `server.workers` sets how many deliveries run at once. With 64 workers and 20ms
  deliveries, a relay moves about 3,200 messages a second, whatever its overhead. Raise `workers`
  for slow destinations, or batch.
- **The disk buffer pays for its fsync once per group.** An fsync is the call that forces a write
  onto the disk. Messages are written in groups of up to 1,024 per transaction, and removed the
  same way. On macOS an fsync is a full flush to the device; Linux's is usually cheaper.
  `sync: false` keeps the write-ahead log but leaves flushing to the OS. Then a host crash can
  lose the last writes; a process crash cannot.
- **Batching spreads every cost after routing over the whole batch.** One delivery attempt, one
  breaker update and one round trip carry a whole batch.
- **AI routing is the model's latency.** A System One decision (TypeSafe `jev`) answered the
  ai-triage cases in 140 to 190ms. The decision cache (`cache:`) and sticky routing skip the call.

Run `make bench` on your hardware, or `go test -run '^$' -bench Relay -benchtime 100000x ./engine/`
for longer runs.
