---
layout: ../../layouts/DocsLayout.astro
title: Concepts
---

# Concepts

This page is the vocabulary of hikyaku. Read it before you write a spec, and come back to it when
a term on another page is unclear.

A hikyaku **relay** is one spec: sources, destinations, the routes between them, and the policy
that decides what happens when a delivery fails. [The spec](/docs/spec/) is the YAML that writes
it down.

## The pieces

Six words cover the whole relay. Two short names appear in the table and the diagram below. CEL
is the Common Expression Language, a small language for yes/no conditions such as filters and
rules. The DLQ is the dead-letter queue, where a message goes when every delivery attempt failed.

| Concept | What it is |
|---|---|
| **Source** | Where messages come from. It runs in one of three **modes**: `listen` (hikyaku is the server), `poll` (it fetches an endpoint on a schedule) or `subscribe` (it holds a long-lived client connection, such as a WebSocket or an SSE stream). |
| **Destination** | Where messages go. Either a single endpoint, or a **group** of endpoints with a strategy (`failover`, `round_robin`, `weighted`, `least_latency`). Each has its own timeout, retry policy, circuit breaker, retry budget and **fallback chain**. |
| **Route** | Connects sources to destinations through a **router**: `static`, `rules` (CEL), `sticky` or `ai`. A router can fall back to other routers. |
| **Transform** | A CEL filter, a jq program, a Go template, a header edit or a codec. Applied per route (before routing) or per destination (before delivering there). |
| **Envelope** | The message as hikyaku carries it, whatever the protocol: what every source produces and every destination receives. It is CloudEvents-compatible. |
| **Relay** | One spec, with a `name` used in metrics, logs and shared-state keys. The CLI runs one relay per process; the Go library can run several. |

## How a message flows

The diagram shows one message's path through the relay. The steps below walk through it.

```text
 sources: http · ws · sse · grpc · tcp · kafka · nats · mqtt · amqp · sqs · … · file
   │
   ▼
 ┌──────────────────────────────── hikyaku ─────────────────────────────────┐
 │                                                                          │
 │  Source ──▶ Envelope ──▶ filter + transforms ──▶ Router chain            │
 │  listen · poll · subscribe                       ai ▸ rules ▸ static     │
 │  ▲ reconnect, backoff,                                  │                │
 │  │ failover_urls                                        ▼                │
 │                                   Genkit Go: System One decision models  │
 │                                   or GenerateData[T] on any model, with  │
 │                                   model retries and model fallbacks      │
 │                                                                          │
 │  Delivery, per destination: transforms ▸ timeout ▸ retry (backoff,       │
 │  Retry-After) ▸ breaker ▸ retry budget ▸ fallback chain ▸ DLQ            │
 │                                                                          │
 │  Admin API /healthz /readyz /metrics /reload · OTel traces · Prometheus  │
 │                                                                          │
 └──────────────────────────────────────────────────────────────────────────┘
   │
   ├──▶ billing ──(fallback)──▶ billing-dr
   ├──▶ support     (group: failover a → b)
   └──▶ dashboard   (best-effort: never holds the ack)
```

1. A source turns what it received into an [envelope](#the-envelope) and puts it on the buffer.
   The buffer is in memory and holds 10000 messages by default. When the buffer is full, listen
   sources push back instead of dropping: they answer `503` with `Retry-After`.
2. Delivery workers (`server.workers`, 256 by default) take envelopes off the buffer. Every
   route whose `from:` lists the source runs its `filter` and its `transforms`, then its router.
3. The router chain returns the destinations. The AI router only ever picks from the
   destinations declared in the spec. See [Routing](/docs/routing/).
4. Each chosen destination runs its own transforms and delivers, with the
   [retry and fallback policy](/docs/resilience/) of that destination.
5. When the outcome is known, the source acks the message (tells the sender it is done) or nacks
   it (tells the sender to deliver it again). See [Ack semantics](#ack-semantics).

## Sources

A source is where messages come from. It has a `type` (the protocol) and a `mode`:

| Mode | hikyaku is | Examples |
|---|---|---|
| `listen` | the server: callers send it messages | an HTTP webhook endpoint, a WebSocket server, an SSE endpoint |
| `poll` | a client that fetches an endpoint every `interval` | an HTTPS status API, with `dedupe` so a retried poll does not emit twice |
| `subscribe` | a long-lived client | a WebSocket or SSE stream, with `reconnect` backoff and `failover_urls` |

`stdin` and `file` sources read one message per line. [Protocols](/docs/protocols/) lists which
modes each protocol has.

Any source can set `dedupe: { key, ttl, store }` to drop repeats. The key is a template, such as
`{{ .json.id }}`. A message whose key was already seen within `ttl` (1h by default) is dropped
before it is routed. The source gets it acknowledged as filtered: an http listen source answers
`200` with the status `duplicate`. `hikyaku_deduped_total{source}` counts it. With a shared
`store`, the check holds across replicas. See [Dedupe](/docs/protocols/#dedupe).

## Destinations

A destination is where messages go. It is an endpoint (`type: https`, `wss`, `sse`, `file`,
`stdout`, ...) or a `type: group` of member endpoints. Every destination inherits the `defaults:`
policy. It can override any part of it: `timeout`, `deadline`, `retry`, `circuit_breaker` and
`retry_budget`.

- **`fallback: [a, b]`** names the destinations tried, in order, once this one gives up. A
  destination gives up when it has exhausted its retries, failed permanently, has its circuit
  breaker open, has spent its retry budget, or has hit its deadline. Each fallback runs its own
  policy and can use another protocol.
- **`required: true`** (the default) means the message is not acked until this destination, or
  one of its fallbacks, has it. A best-effort destination (`required: false`) never holds the
  ack. Its failures are counted and logged, not dead-lettered.
- **`transforms:`** reshape the message for this destination only.

## Routes and routers

A route connects sources to destinations. It has a `name`, the sources it takes (`from:`), an
optional CEL `filter` and `transforms`, and a router. `to: [a, b]` is the shortcut for a static
router.

| Router | Picks |
|---|---|
| `static` | always the same destinations |
| `rules` | the destinations of the first CEL rule that holds (or of every rule, with `match: all`) |
| `sticky` | the destinations the last call of the same history key went to |
| `ai` | the candidates a decision model (or a generative model) chooses, above a confidence threshold |

A source can feed several routes, and each route decides on its own. An `archive-everything`
route with a static router can sit next to a `smart-triage` route with an AI router.

A message that no route takes is **unrouted**: it is counted (`hikyaku_unrouted_total`) and
logged, then acked. A message that a route takes, but whose routers all chose nothing, is
dead-lettered with reason `no_route`. When a route transform fails, the reason is `transform`.
`hikyaku dlq replay` runs such a message through that route again. [Routing](/docs/routing/)
covers every router and the fallback chain between them.

## Transforms

Transforms reshape a message. They run in order, and each one sets one operation: `filter`
(CEL), `jq`, `template`, `headers`, `codec` or `content_type`. On a route they run before the
router, so the router sees the transformed message. On a destination they run right before that
destination delivers. See [Transforms](/docs/transforms/).

## The envelope

The envelope is the message as hikyaku carries it. Every message, whatever its protocol, becomes
one:

```go
// envelope/envelope.go (root module, public)
type Envelope struct {
	ID          string              // a ULID unless the source took one from the message; the default idempotency key
	Source      string              // the source name from the spec
	Protocol    string              // "http", "wss", "stdin", ...
	Subject     string              // the path, topic, channel or routing key the message came on
	Headers     map[string][]string
	ContentType string
	Body        []byte
	Attributes  map[string]any      // protocol metadata and decisions: ai.decision, delivery.attempt, dlq, ...
	ReceivedAt  time.Time
	Trace       trace.SpanContext
}
```

- The JSON body is parsed lazily, once, and cached. CEL, jq and templates all read the same
  parsed view.
- `Attributes` collect what the relay learns about the message:
  - the AI router records `ai.decision`: the model, the answers and probabilities, the reason,
    and which fallback served it;
  - delivery records `delivery.attempt`: the number of the attempt under way, from 1;
  - a dead letter carries `dlq`: the route, the destination, the reason, the attempts, the last
    error and every hop.
- In files, the dead-letter queue, `hikyaku tail` and `hikyaku dlq replay`, an envelope is one
  JSON line (NDJSON). A compact JSON body is embedded as JSON. Any other UTF-8 body goes in
  `body_text`, and anything else in `body_base64`, so it comes back byte for byte.

## Ack semantics

An ack tells the sender that hikyaku is done with a message. Delivery is **at-least-once**: a
message may be delivered more than once, but it is not dropped silently. Every attempt carries an
idempotency key. A message ends in one of these states:

| Outcome | When | The source |
|---|---|---|
| `delivered` | every required destination succeeded, on its primary or a fallback | acks |
| `dead_lettered` | a required destination and all its fallbacks failed, or a route's routers all chose nothing (`no_route`) or its transform failed (`transform`), and the message is safely in the dead-letter queue | acks |
| `unrouted` | no route takes the source | acks (counted and logged) |
| `filtered` | every route that takes the source filtered the message out, or the source's `dedupe` dropped it as a repeat | acks |
| `failed` | not handled: the dead-letter queue failed too, or the relay shut down first | nacks, so the sender can redeliver |

What an ack is depends on the protocol: an HTTP response, a stream position. For HTTP listen
sources:

- **`ack: delivered`** (the default) answers the caller once the outcome is known: `200` when the
  message is delivered, `202` when it is safely dead-lettered, `503` with `Retry-After` when it
  failed. An outcome that takes longer than 30 seconds gets a `202` with status `pending`, and
  delivery continues. If the caller gives up first, delivery continues too. A caller that retries
  with the same `Idempotency-Key` gets the same message ID, so destinations that dedupe see one
  message.
- **`ack: accepted`** answers `202` as soon as the message is queued. It is faster. But with the
  memory buffer, a crash loses what was queued; `hikyaku lint` warns about that combination.
- **`respond: { from: <destination> }`** sends that destination's reply back to the caller, so
  hikyaku acts as a smart reverse proxy. The caller waits up to `respond.timeout` (30s by
  default), then gets a `504`. A message dead-lettered there gets a `502`.

Exactly-once delivery is a non-goal. hikyaku sends an `Idempotency-Key` header (by default the
envelope ID) on every attempt and every fallback, so retries are safe for destinations that
dedupe.

## Relays and processes

`name` identifies the relay in metrics, logs and the keys of [shared state](/docs/history/#the-redis-layout).
The CLI runs one relay per process. When a spec is split across several files (`-f a.yaml -f b.yaml`,
or a directory), every file must agree on `name`. A process that embeds hikyaku as a Go library
can run several relays.

## What hikyaku is not

- A workflow engine. Multi-step orchestration belongs to [senro](https://github.com/xavidop/senro);
  hikyaku can deliver *to* it.
- Exactly-once. v1 is at-least-once with idempotency keys.
- A GUI. Observability goes through OpenTelemetry, Prometheus and the Genkit Developer UI.
- An LLM that picks URLs. The AI only ever chooses among destinations declared in the spec.
