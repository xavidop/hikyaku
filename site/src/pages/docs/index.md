---
layout: ../../layouts/DocsLayout.astro
title: Overview
description: hikyaku reads from one or more endpoints and delivers to one or more endpoints, configured with one YAML spec, with an AI router and retries and fallbacks at every layer.
---

# hikyaku 飛脚

> **飛脚 (hikyaku)**: the Edo-period relay couriers who ran letters between cities, station to
> station, and found another way when a road was closed.

This page is the overview. Read it first to learn what hikyaku does and where the rest of the
docs are.

`hikyaku` is a message relay. It **reads from one or more endpoints and delivers to one or more
endpoints**, over almost any protocol. One YAML spec, `hikyaku.yaml`, declares four things: where
messages come from, where they go, how they are reshaped and routed, and what happens when a
delivery fails.

Its **AI router** picks destinations with [Genkit Go](https://genkit.dev). By default it asks
**System One decision models**: models that answer typed questions (a choice, or yes/no) with
calibrated probabilities. A calibrated probability of 0.9 means the model is right about nine
times in ten, so you can set thresholds on it. Any generative model works as an alternative.

Retries and fallbacks are built in at every layer, so **nothing is dropped silently**. The last
resort is the dead-letter queue (DLQ), where a message goes when every delivery attempt failed.
The dead-letter queue has a fallback of its own.

## Quick start

Four commands take you from install to a running relay.

```sh
brew install xavidop/tap/hikyaku          # macOS; Windows: Scoop; Linux: a .deb, .rpm or .apk
hikyaku init --template webhook-fanout    # scaffold hikyaku.yaml
hikyaku validate                          # references, fallback cycles, CEL, templates
hikyaku run                               # start the relay
```

The [Quick start](/docs/quickstart/) builds a first relay. Then it runs an AI-routed relay on a
fake decision model, so you need no API key.

## What you can do

Find the page for your task.

| Task | Where to read |
|---|---|
| Understand sources, destinations, routes, transforms and the envelope | [Concepts](/docs/concepts/) |
| Write `hikyaku.yaml`, with secrets, templates and editor completion | [The spec](/docs/spec/), [Spec reference](/docs/spec-reference/) |
| Receive webhooks, stream from a WebSocket or an SSE endpoint, poll an API | [Protocols](/docs/protocols/) |
| Route with CEL rules, or let a decision model pick the team | [Routing](/docs/routing/) |
| Send follow-ups where the previous message went, and learn from corrections | [History-aware routing](/docs/history/) |
| Retry, fail over, hedge, break circuits, dead-letter and replay | [Retries and fallbacks](/docs/resilience/) |
| Reshape messages with CEL filters, jq, templates, headers and codecs | [Transforms](/docs/transforms/) |
| Look up a command or a flag | [CLI reference](/docs/cli/) |
| Run hikyaku in Docker or on Kubernetes with the Helm chart | [Deploy](/docs/deploy/) |
| Health checks, metrics, traces, the admin API and the Genkit Developer UI | [Observability](/docs/observability/) |
| Start from a complete, working relay | [Examples](/docs/examples/) |

## How a message travels

Five steps take a message from a source to its destinations.

1. A **source** receives a message. It listens for messages (it is the server), polls for them
   (it fetches an endpoint on a schedule), or subscribes (it reads a long-lived connection). The
   message becomes an **envelope**: the message as hikyaku carries it, whatever the protocol.
2. Every **route** that takes that source applies its `filter` and its **transforms**. The filter
   is written in CEL (Common Expression Language), a small language for yes/no conditions.
3. The route's **router** picks the destinations. A router is a static list, a set of CEL rules,
   the last destinations of a key's history, or the AI router. Routers chain: AI first, then
   rules, then a static catch-all.
4. Each **destination** delivers with its own timeout, retries, circuit breaker and retry
   budget. When those are spent, it tries its **fallback chain**. A group spreads the load over
   its members.
5. The source **acks** the message (tells the sender it is done) once every required destination
   has it, on its primary or on a fallback. It also acks once the message is safely in the
   **dead-letter queue**.

[Concepts](/docs/concepts/) draws the whole picture.

## What it does

The full list of features, with the page that covers each one.

- Sources and destinations over every protocol of [the matrix](/docs/protocols/): `http`/`https`,
  `ws`/`wss`, `sse`, `grpc`, `tcp`/`udp`, `kafka`, `nats`/`jetstream`, `mqtt`, `amqp`, `redis`,
  `sqs`/`sns`/`eventbridge`, `s3`, `pubsub`, `gcs`, `servicebus`, `eventhubs` and the files. Also
  `group` destinations, and the `fault` sink that injects failures for tests.
- A [disk buffer](/docs/delivery/#the-disk-buffer) that keeps accepted messages through a crash,
  per-key [ordering](/docs/delivery/#ordering) and destination [batching](/docs/delivery/#batching).
- An OpenAI-compatible [LLM proxy](/docs/llm-proxy/) with model routing and fallback.
- Your own types, in process or as [out-of-process plugins](/docs/plugins/) over gRPC.
- The `static`, `rules`, `sticky` and `ai` routers, with the System One decision engine and the
  generative engine, model retries and model fallback chains.
- Every destination policy of [Retries and fallbacks](/docs/resilience/): retries with jitter and
  `Retry-After`, timeouts and deadlines, fallback chains, groups, hedging, circuit breakers,
  retry budgets, idempotency keys, the dead-letter queue and replay.
- [History-aware routing](/docs/history/) on the memory and Redis stores, sticky routing and the
  feedback loop.
- The admin API, OpenTelemetry traces and Prometheus metrics, and hot reload.
- The CLI, the container image, the Helm chart, and the Go library ([Embed it](/docs/library/)).

## Next steps

- [Install](/docs/install/) hikyaku.
- Follow the [Quick start](/docs/quickstart/).
- Read the [Concepts](/docs/concepts/), then [the spec](/docs/spec/).
