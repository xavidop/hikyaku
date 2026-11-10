---
layout: ../../layouts/DocsLayout.astro
title: Observability
---

# Observability

This page shows how to watch a running relay. hikyaku has no GUI on purpose. You watch it
through:

- its admin API;
- OpenTelemetry traces;
- Prometheus metrics;
- structured logs;
- and, for AI routes, the Genkit Developer UI.

## Admin API

```yaml
server:
  admin:
    listen: ":9090"                          # default
    token: "{{secrets.ADMIN_TOKEN}}"         # bearer token for the guarded endpoints
```

The admin API is an HTTP server on its own port. It answers health checks, serves metrics,
reloads the spec, and lets you look inside the relay:

| Endpoint | Does | Guarded |
|---|---|---|
| `GET /healthz` | liveness: the process is up | no |
| `GET /readyz` | readiness, with the state of each destination, source and store: see [Readiness](#readiness) | no |
| `GET /metrics` | Prometheus metrics, unless `observability.metrics.prometheus: false` | no |
| `POST /reload` | reload the spec: validate, build, swap | yes |
| `GET /debug/config` | the running spec, with every secret redacted | yes |
| `GET /debug/state` | the live state of the relay, such as breakers and group members | yes |
| `GET /v1/tail` | a live stream of envelopes as server-sent events (`hikyaku tail`) | yes |
| `POST /v1/feedback` | record a correction: `{"msg_id": "...", "correct": ["billing"], "note": "..."}` | yes |
| `GET`, `DELETE /v1/history/{route}/{key}` | read a key's routing history, or forget it | yes |
| `GET /v1/history` | export records as NDJSON: `?route=`, `?since=30d`, `?labeled=true` | yes |

The guarded endpoints (`/reload`, `/debug/config`, `/debug/state`, `/v1/tail`, `/v1/feedback` and
`/v1/history*`) need `server.admin.token` as a bearer token (`Authorization: Bearer <token>`).
When no token is set, they answer loopback clients only. So `hikyaku history` and
`curl localhost:9090/...` work on your machine, and nothing else does. `hikyaku lint` warns when
the token is unset. `/healthz`, `/readyz` and `/metrics` are open. `server.admin.disabled: true`
turns the admin API off. A reload applies a change to any of these settings: a new `listen`
moves the admin API to the new address (see [hot reload](/docs/spec/#hot-reload)).

The admin port serves message contents (`/v1/tail`) and the shape of your relay. Keep it off the
public network, for example with the Helm chart's `networkPolicy`.

### Readiness

Readiness means the relay can take traffic and deliver it. `/readyz` answers `200` when the relay
is ready and `503` when it is not. Its JSON body shows each destination (its members, their
health and their breaker state), each source and each store. The relay is not ready while:

- **a required destination can take nothing:** it and every destination of its fallback chain
  are down. Down means every member is unhealthy, or the breaker is open.
  `server.readiness.destinations` (on by default) controls this check.
- **a listen source is down:** it cannot take the traffic sent to it. Subscribe and poll sources
  are shown, but do not fail readiness.
- **with `server.readiness.models: true`, every model of an AI route's chain fails its probe.**
  One model that answers is enough, since that model routes the messages. A probe calls each
  model once. Only the first `/readyz` waits for a probe, and the first one after a reload. After
  that, `/readyz` answers at once with the last result, and a probe older than 30 seconds is
  refreshed in the background. So a kubelet that asks every few seconds does not call the models
  each time, and does not time out on a slow one.

A store that is down is shown too, but does not fail readiness: routing keeps working without it.

```yaml
server:
  readiness:
    destinations: true                       # default
    models: true                             # default false
```

## Traces

```yaml
observability:
  otel:
    endpoint: "{{env.OTEL_EXPORTER_OTLP_ENDPOINT}}"   # OTLP/HTTP; empty turns tracing off
    service_name: hikyaku                            # default
    sample_ratio: 1                                  # default: keep every trace
```

A trace follows one message through the relay, as a tree of spans (timed steps). Traces go out
over OTLP/HTTP. Set `insecure: true` for a plain-HTTP collector, and `headers` for its auth. The
envelope carries its trace context from the source to every destination. Delivery has **one span
per attempt and per hop**: a retry, a fallback, a hedge and a dead letter each show up as their
own span, with the destination, the endpoint, the attempt number and the error.

The spans are:

- `hikyaku.message` for each message, with `hikyaku.deliver`, `hikyaku.hop` and
  `hikyaku.attempt` spans under it (`hikyaku.deliver_batch` for a batch);
- `hikyaku.dead_letter` for writing a dead letter, with the message ID, the reason, the route,
  the destination and the DLQ hop that took it (`hikyaku.dlq.hop`). It is marked failed when
  every hop failed;
- for AI routes, the spans of their Genkit flow: each model call, each model fallback, and the
  decision.

## Metrics

Prometheus metrics are served at `/metrics` on the admin port. `observability.metrics.prometheus`
is on by default. Set it to `false` to leave `/metrics` out of the admin API. The other endpoints
stay, and the metrics are still counted:

```yaml
observability:
  metrics:
    prometheus: false
```

This is every metric hikyaku exports. They sit next to the Go runtime (`go_*`) and process
(`process_*`) metrics:

| Metric | Labels | What it measures |
|---|---|---|
| `hikyaku_messages_received_total` | `source` | messages received |
| `hikyaku_messages_acked_total` | `source`, `status` | messages finished, by outcome: `delivered`, `dead_lettered`, `unrouted`, `filtered` or `failed` |
| `hikyaku_unrouted_total` | `source` | messages no route took |
| `hikyaku_deduped_total` | `source` | messages a source's `dedupe` dropped: their key was seen within the `ttl` |
| `hikyaku_queue_depth` | | messages waiting in the buffer: the memory queue, or the disk buffer with `buffer.type: disk` (gauge) |
| `hikyaku_backpressure_total` | `source` | messages a source refused because the relay's buffer was full: http and openai answer 503, grpc `RESOURCE_EXHAUSTED`, udp drops the datagram |
| `hikyaku_route_decisions_total` | `route`, `router` | routing decisions, by the router that decided |
| `hikyaku_router_fallback_total` | `route`, `to` | moves to the next router of a chain |
| `hikyaku_router_skipped_total` | `route`, `router` | messages a router's `when` left out: they went to the next router with no call |
| `hikyaku_ai_decisions_total` | `route`, `outcome` | AI routing decisions: `chosen`, `low_confidence`, `shadow`, `cached`, `sticky`, `error` or `over_budget` |
| `hikyaku_ai_latency_seconds` | `route`, `model` | model call latency (histogram) |
| `hikyaku_ai_model_fallback_total` | `route`, `from`, `to`, `reason` | moves to the next model of a chain; `reason` is `rate_limited`, `unavailable`, `timeout`, `bad_output`, `not_found`, `auth`, another status in lower case, or `error` |
| `hikyaku_ai_tokens_total` | `route`, `model` | model tokens the AI router used |
| `hikyaku_delivery_attempts_total` | `destination`, `endpoint`, `outcome` | delivery attempts; `outcome` is `ok` or the error's class (`transient`, `rate_limited`, `timeout`, `permanent`) |
| `hikyaku_delivery_duration_seconds` | `destination`, `endpoint` | the duration of one delivery attempt (histogram) |
| `hikyaku_batch_size` | `destination` | messages per delivered batch, for destinations with `batch:` (histogram) |
| `hikyaku_delivery_dropped_total` | `destination` | messages a best-effort (`required: false`) destination failed to take |
| `hikyaku_fallback_total` | `from`, `to`, `reason` | destination fallback hops; `reason` is `exhausted`, `permanent`, `breaker_open`, `budget` or `deadline` |
| `hikyaku_breaker_state` | `endpoint` | each circuit breaker: `0` closed, `1` half-open, `2` open (gauge) |
| `hikyaku_retry_budget_remaining` | `destination` | the retries the retry budget still allows in its window (gauge) |
| `hikyaku_hedges_total` | `destination` | hedged requests started |
| `hikyaku_member_healthy` | `destination`, `member` | group member health: `1` healthy, `0` unhealthy (gauge) |
| `hikyaku_model_tokens_total` | `destination`, `type` | tokens used by `model` destinations (the LLM proxy); `type` is `input` or `output` |
| `hikyaku_dlq_total` | `destination`, `outcome` | dead letters: `stored`, or `failed` when every hop of the DLQ chain failed; `destination` is the one that failed (empty for a routing failure) |
| `hikyaku_history_reads_total` | `route`, `outcome` | history reads: `hit`, `empty` or `error` |
| `hikyaku_history_errors_total` | `op` | history store errors: `read`, `append` or `queue_full` |
| `hikyaku_history_turns` | `route` | past calls sent to the model with a message (histogram) |
| `hikyaku_sticky_total` | `route`, `outcome` | sticky routing: `follow_up`, `new` or `no_history` |
| `hikyaku_feedback_total` | `route` | corrections recorded |
| `hikyaku_reloads_total` | `outcome` | spec reloads: `ok`; `invalid` when the spec does not validate; `failed` when it does not build or start, such as a model provider without its key or an admin address that is taken; `needs_restart` when only a restart can load it (new Genkit plugins while `GENKIT_REFLECTION_PORT` pins the Developer UI's port) |

A useful first dashboard shows the rate of `hikyaku_fallback_total` by `reason`,
`hikyaku_dlq_total`, `hikyaku_breaker_state` and, for AI routes, `hikyaku_router_fallback_total`.
A rising `hikyaku_ai_model_fallback_total` usually means a provider is struggling, before
anything is lost.

## Logs

```yaml
observability:
  logs: { level: info, format: json }        # debug | info | warn | error; json | text
```

Structured logs go to stderr. The format is text on a terminal and JSON otherwise, unless
`format` says which. Every secret value is redacted before a line is written. `-v, --verbose`
turns on debug logs for one command.

## Watching messages

```sh
hikyaku tail --source webhooks --admin http://localhost:9090
```

`hikyaku tail` streams the messages of a running relay. It prints one line when a message
arrives (time, source, ID, subject and the start of the body), and one line when the message is
finished, with its status. `--json` prints each event in full instead: the envelope, with its
headers, body and attributes, and, once the message is finished, its report, with each route's
decision and deliveries. Secret values are redacted. To look at one message after the fact, find
it in the dead-letter queue with `hikyaku dlq inspect <id>`, or in a key's history with
`hikyaku history show`.

## The Genkit Developer UI

Every AI route is a Genkit flow named `route/<name>`. Run the relay under the Genkit CLI to see
the flows in the Genkit Developer UI, with a trace of every decision: the state that was sent,
the questions, the answers and their probabilities, and each model retry and fallback.

```sh
npm i -g genkit-cli
genkit start -- hikyaku dev
```

`hikyaku dev` is `hikyaku run` plus the Genkit reflection server. The Developer UI can also run a
route's flow on a message you type, without delivering it. From a clone of the repository,
`make genkit-ui` does all of this for the `ai-triage` example, on a fake decision model.

`hikyaku dev` reloads the spec when a file changes. A reload that changes `genkit:` or the model
providers starts a new Genkit instance, with its own reflection server, and the Developer UI
moves to it. Leave `GENKIT_REFLECTION_PORT` unset. Two instances cannot share a pinned port, so
with the port pinned such a reload would need a restart.
