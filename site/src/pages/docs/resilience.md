---
layout: ../../layouts/DocsLayout.astro
title: Retries and fallbacks
---

# Retries and fallbacks

This page explains what hikyaku does when a delivery fails: how it retries, where it falls back
to, and where a message ends up when nothing works. Read it when you tune a destination for an
endpoint that fails now and then, or when you need to know where a failed message went.

Every layer of hikyaku has a retry policy and a defined fallback, and **nothing is dropped
silently**. The last resort is always the dead-letter queue (DLQ): the place a message goes when
every delivery attempt failed. The dead-letter queue has a fallback of its own.

| Layer | Retries | Falls back to |
|---|---|---|
| Destination | `retry` per endpoint, with backoff, jitter and `Retry-After` | its `fallback` chain, then the DLQ |
| Group | a failed member is retried on the next one | the group's own `fallback` chain |
| AI model | `models.retry` per model | the next model of the chain |
| Router | | the `router.fallback` chain (rules, sticky, static), then the DLQ (`no_route`) |
| History store | `stores[].retry` per operation | routing without history |
| Subscribe source | `reconnect` with backoff | the next of `failover_urls` |
| Poll source | `retry` per poll | the next interval |
| Dead-letter queue | `dead_letter.retry` | `dead_letter.fallback`, then a nack |

## Destination retries

These settings say how often a destination is retried, and how long each try may take:

| Setting | Default | Meaning |
|---|---|---|
| `retry.attempts` | `5` | tries per endpoint (`1` means no retry) |
| `retry.backoff` / `retry.max_backoff` | `200ms` / `10s` | exponential, doubling on every try |
| `retry.jitter` | `full` | `full`, `equal` or `none`; full jitter avoids thundering herds |
| `retry.retry_on` | `connect, timeout, 5xx, 429` | the error tags that are retried |
| `retry.never_on` | `4xx` (except `408` and `429`) | permanent failures, which go straight to the fallback chain or the DLQ |
| `retry.respect_retry_after` | `true` | wait what `Retry-After` says, capped by `max_backoff` |
| `timeout` | `5s` | per attempt |
| `deadline` | none | the overall budget across attempts **and** fallbacks |

Every destination inherits these settings from `defaults:`, field by field, and can override any
of them.

### Error tags

Error tags describe a failure. A failure carries every tag that fits it, from the broad to the
exact:

- an HTTP `503` is `5xx` and `503`;
- a `408` is `4xx`, `408` and `timeout`;
- a refused connection is `connect`.

Protocols add tags of their own, such as `ws:close_1011` for a WebSocket close code. `retry_on`
and `never_on` match against these tags, and **the more specific tag wins**: `429` in `retry_on`
beats `4xx` in `never_on`.

A failure whose tags match neither list falls back to its **class**: hikyaku retries it unless
the protocol classed it as permanent. So a transient protocol code that no list names is retried
by default. Examples: `kafka:NOT_LEADER_FOR_PARTITION`, `mqtt:0x88`, `ws:close_1011` and
`grpc:UNAVAILABLE`. List a tag in `never_on` to stop retrying it. You only need `retry_on` for a
code whose class is permanent.

```yaml
retry:
  attempts: 4
  retry_on: [connect, timeout, 5xx, 429]
  never_on: [4xx, 501, ws:close_1008]
```

Here, a `501` and a WebSocket closed for a policy violation (`1008`) are not retried. They go
straight to the fallback chain.

### Backoff

Backoff is the wait before a retry. The first retry waits `backoff`. Each retry after it waits
twice as long as the one before, up to `max_backoff`. Then jitter applies. Jitter is a random
change to the wait, so that many clients do not all retry at the same moment (a "thundering
herd"):

- `full` waits a random time between zero and the computed wait;
- `equal` waits half of it, plus a random half;
- `none` waits exactly the computed wait.

When the endpoint answers with `Retry-After` (or a protocol gives a similar hint), hikyaku waits
that long instead, still capped by `max_backoff`.

## Destination fallbacks

```yaml
destinations:
  - name: billing
    type: https
    url: https://billing.internal/api/events
    deadline: 30s
    fallback: [billing-dr, billing-spool]    # tried in order
```

A fallback is another destination that takes a message when the first one fails. hikyaku moves
to the next destination in `fallback` when the primary:

- has used up its retries;
- hit a permanent error;
- has its circuit breaker open;
- has spent its retry budget;
- or has reached its deadline.

Each fallback has its own retry policy. Fallbacks can use other protocols (HTTP, then a
WebSocket, then a file), and they can have fallbacks of their own. Validation rejects cycles
(`a → b → a`), across files too. `hikyaku_fallback_total{from,to,reason}` counts each hop.
`reason` is one of `exhausted`, `permanent`, `breaker_open`, `budget` and `deadline`.

## Groups

```yaml
destinations:
  - name: support
    type: group
    strategy: failover                       # failover | round_robin | weighted | least_latency
    health_check: { path: /healthz, interval: 10s, timeout: 2s, unhealthy_after: 3, healthy_after: 1 }
    members:
      - { type: https, url: https://support-a.internal/hook }
      - { type: https, url: https://support-b.internal/hook, weight: 2 }
    fallback: [support-email]
```

A group is one destination made of several members. Its `strategy` says which member gets each
message:

| Strategy | Picks |
|---|---|
| `failover` | the members in order, skipping unhealthy ones until they recover |
| `round_robin` | each member in turn, failing over to the next on error |
| `weighted` | members in proportion to their `weight` (smooth weighted round robin), failing over on error |
| `least_latency` | the member with the lowest latency (an EWMA), failing over on error |

`health_check` probes the members: `GET path` on an HTTP member's origin, or the sink's own
health check. After `unhealthy_after` failed probes in a row, a member is skipped. After
`healthy_after` passing probes in a row, it is used again. Without health checks, a
load-balancing group keeps sending to a dead member until that member's circuit breaker opens.
`hikyaku lint` warns about that.

## Hedging

```yaml
  - name: support
    type: group
    idempotent: true                         # required
    hedge: { after: 800ms, max: 1 }
    # ...
```

Hedging sends the same message to a second member when the first one is slow, and keeps the
first answer. When the first member has not answered after `hedge.after`, hikyaku sends the
message to another member too (at most `hedge.max` more), and the first success wins. A hedged
message can arrive twice. So hedging is only allowed on a destination marked `idempotent: true`
(safe to send twice): validation rejects it otherwise. `hikyaku_hedges_total` counts the races.

## Circuit breaker

```yaml
circuit_breaker: { failure_ratio: 0.5, min_requests: 20, window: 60s, open_for: 30s, half_open_max: 5 }
```

A circuit breaker stops sending to an endpoint that keeps failing, so the endpoint gets time to
recover and hikyaku does not waste attempts on it. Each endpoint has a breaker with three states:

- **Closed**: traffic flows. Once the `window` holds `min_requests` requests, the failure ratio
  starts to count.
- **Open**: when the ratio reaches `failure_ratio`, the breaker opens. For `open_for`, traffic
  goes straight to the fallback chain and never touches the endpoint.
- **Half-open**: after `open_for`, the breaker lets `half_open_max` trial requests through. If
  they pass, it closes. If not, it opens again.

`hikyaku_breaker_state` shows the state of each breaker.

## Retry budget

```yaml
retry_budget: { ratio: 0.2, min_per_second: 10, store: redis-main }
```

A retry budget caps how many retries hikyaku may make, as a share of the traffic. It allows
`ratio` retries per first attempt in the window, plus `min_per_second` retries that are always
allowed. During an outage, this keeps retries from multiplying the load: once the budget is
spent, messages go to the fallback chain instead. `hikyaku_retry_budget_remaining` shows what is
left. With a shared `store`, the budget is counted across replicas.

## Idempotency

Delivery is at-least-once: every message is delivered, and some may be delivered more than once.
So the same message can reach a destination twice: after a timeout on a request that did
succeed, after a hedge, or after a replay. An idempotent destination is one that is safe to send
the same message twice, because it dedupes (drops the copy). To help it, every attempt and every
fallback carries the same idempotency key: the `Idempotency-Key` header by default
(`idempotency_header` renames it), with the envelope ID as its value. A destination that dedupes
on that key sees each message once. An HTTP source that receives a message with an
`Idempotency-Key` header keeps it as the envelope ID, so a caller's own retries keep the same
key end to end.

## Models and routers

The AI router adds two more layers. [Routing](/docs/routing/) covers them:

- [model retries and model fallbacks](/docs/routing/#model-retries-and-model-fallbacks): per
  model, and across providers. The System One plugin's own two transport retries count inside
  each attempt.
- the [router fallback chain](/docs/routing/#router-fallback-chain): when the AI step errors,
  times out, is over budget or chooses nothing, deterministic rules take over. After the rules
  comes a static catch-all. When no router of the chain decides, the message is dead-lettered
  with reason `no_route`.

## Source-side resilience

Sources recover from failures too:

- **Subscribe sources** reconnect with backoff and jitter (`reconnect.backoff`, `max_backoff`,
  `jitter`; `max_attempts: 0` retries forever). They rotate through `failover_urls`, and they
  resume where the protocol allows: SSE resumes with `Last-Event-ID`.
- **Poll sources** retry each poll as `retry` says, then skip to the next interval. `dedupe`
  keeps a retry from emitting a message twice. Every source type takes `dedupe`: a message whose
  key was seen within its `ttl` is dropped before routing, and acknowledged to the source as
  filtered.
- **Listen sources** apply backpressure (they push back on callers until the relay catches up)
  when the buffer is full: they answer `503` with `Retry-After` instead of dropping the message.

Broker sources resume from the position the broker remembers: Kafka committed offsets, JetStream
durable consumers, MQTT persistent sessions, Redis Streams pending entries, and Event Hubs
checkpoints. See [how sources acknowledge](/docs/protocols/#how-sources-acknowledge).

## Dead letters and replay

```yaml
dead_letter:
  type: file
  path: ./out/dlq.ndjson
  retry: { attempts: 3 }
  fallback: [stdout]
```

A message goes to `dead_letter` for one of four reasons, recorded in its `dlq` attribute:

| Reason | When | One dead letter per |
|---|---|---|
| `delivery` | a required destination failed: its primary, all its fallbacks or its deadline are exhausted | failed required destination, with `dlq.destination` |
| `no_route` | every router of a route's chain failed or chose no destination | route, with `dlq.route` |
| `transform` | a route transform failed | route, with `dlq.route` |
| `schema` | a route's [`validate`](/docs/transforms/#validate-json-schema) step refused the body | route, with `dlq.route` |

The `dlq` attribute also holds the time, the relay, the attempts, the last error and every hop.
When an AI router decided, the envelope keeps `ai.decision` too. A best-effort destination
(`required: false`) that fails is counted (`hikyaku_delivery_dropped_total`) and logged, not
dead-lettered.

A write to the DLQ is retried (3 attempts by default). If it still fails, `dead_letter.fallback`
takes the message: destination names, or `stdout` / `stderr`. Only when every hop has failed is
the message nacked (reported to its source as not done), never acked. So a source that can
redeliver does. Without `dead_letter:`, dead letters go to stderr.

```sh
hikyaku dlq list --since 1h
hikyaku dlq inspect 01JC3Z8Q6ZK3V0B9D3T5V7X9YB
hikyaku dlq replay --since 1h --dry-run
hikyaku dlq replay --id 01JC3Z8Q6ZK3V0B9D3T5V7X9YB --to billing-dr
```

`hikyaku dlq` reads a file DLQ. `replay` sends messages through again, with their original
idempotency keys. So a destination that already has a message sees a duplicate it can drop. A
`delivery` dead letter goes back to the destination that failed, or to `--to`, through its full
retry and fallback path. A `no_route`, `transform` or `schema` dead letter runs through its route again:
its filter, its transforms and its router chain, then delivery. A message that fails again is
dead-lettered again. `hikyaku_dlq_total` counts dead letters.

## Visibility

These metrics cover every retry and fallback hop:

| Metric | What it counts |
|---|---|
| `hikyaku_delivery_attempts_total{destination,endpoint,outcome}` | every attempt, by endpoint and outcome |
| `hikyaku_fallback_total{from,to,reason}` | destination fallback hops |
| `hikyaku_breaker_state` | the state of each circuit breaker |
| `hikyaku_retry_budget_remaining` | the retries left in each budget |
| `hikyaku_hedges_total` | hedged requests |
| `hikyaku_ai_model_fallback_total{route,from,to,reason}` | moves to the next model |
| `hikyaku_router_fallback_total{route,to}` | moves to the next router |
| `hikyaku_dlq_total` | messages dead-lettered |
| `hikyaku_history_reads_total{route,outcome}`, `hikyaku_history_errors_total{op}`, `hikyaku_history_turns{route}`, `hikyaku_sticky_total{route,outcome}`, `hikyaku_feedback_total{route}` | [history-aware routing](/docs/history/#metrics) |

Traces have one span per attempt and per hop. See [Observability](/docs/observability/).

## Try it

```sh
hikyaku chaos --destination billing --fail 50% --latency 2s --data @msg.json --count 100
hikyaku send --to billing --data @msg.json
```

`hikyaku chaos` injects faults into a declared destination, on your machine: a share of
failures, added latency, or a status code. With `--data`, it pushes `--count` messages through
the destination, reports the attempts, the fallbacks and the dead letters, and exits `1` if a
message was lost. Without `--data`, it runs the relay with the destination wrapped in those
faults. `hikyaku send` pushes one message through the full retry and fallback path of a
destination, and exits `1` when the message is not delivered. The resilience test suite of the
repository does the same with the `fault` sink.
