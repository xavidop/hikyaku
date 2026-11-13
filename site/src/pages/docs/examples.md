---
layout: ../../layouts/DocsLayout.astro
title: Examples
---

# Examples

This page lists the complete example relays in the repository, and what each one shows. Read it
to find a working spec close to what you need, then start from it.

The examples live under [`examples/`](https://github.com/xavidop/hikyaku/tree/main/examples).
Each one has:

- `hikyaku.yaml`, the relay as you would run it, with real endpoints and models;
- `hikyaku.llmfake.yaml`, the same relay on the `llmfake` fake model provider (a model that
  answers without a network or an API key), so it runs with no API key, and in CI;
- a `README.md` with what it shows and how to run it.

The `llmfake` specs need a build with the fake provider. From a clone of the repository:

```sh
make build-llmfake                    # writes bin/hikyaku, built with -tags llmfake
./bin/hikyaku validate -f examples/ai-triage/hikyaku.llmfake.yaml
```

## webhook-fanout

[`examples/webhook-fanout`](https://github.com/xavidop/hikyaku/tree/main/examples/webhook-fanout)

An HTTP webhook source. Every message fans out to several destinations, each with its own policy:
a required one with retries and a fallback chain, a best-effort one that never holds the ack (the
answer to the caller), and a file archive. It shows the static router, the delivery policy,
`ack: delivered` and the dead-letter queue (DLQ), where a message goes when every delivery attempt
failed.

```sh
./bin/hikyaku run -f examples/webhook-fanout/hikyaku.llmfake.yaml
```

Then post a webhook to it, as its README shows. `hikyaku init --template webhook-fanout`
scaffolds a relay of the same shape; the [Quick start](/docs/quickstart/) walks through it.

## wss-to-http

[`examples/wss-to-http`](https://github.com/xavidop/hikyaku/tree/main/examples/wss-to-http)

A WebSocket subscription delivered to an HTTP endpoint. The source reconnects with backoff and
jitter (a growing, randomized wait) and rotates through `failover_urls`. The destination retries
with `Retry-After`, opens its circuit breaker under sustained failure, and falls back before
anything reaches the DLQ. It shows the subscribe side of
[source resilience](/docs/resilience/#source-side-resilience) and the destination side of
[retries and fallbacks](/docs/resilience/).

```sh
./bin/hikyaku run -f examples/wss-to-http/hikyaku.llmfake.yaml
```

`hikyaku init --template wss-to-http` scaffolds a relay of the same shape.

## ai-triage

[`examples/ai-triage`](https://github.com/xavidop/hikyaku/tree/main/examples/ai-triage)

Support events routed to teams by the AI router. It shows:

- a System One decision model with one yes/no question per candidate (`multi: true`);
- `min_confidence` and `on_low_confidence`;
- history-aware routing with sticky follow-ups;
- a router fallback chain of CEL rules and a static catch-all.

`hikyaku.yaml` uses a real decision model (`TYPESAFE_API_KEY`); `hikyaku.llmfake.yaml` uses the
fake one. The example's README shows how to run it on
[Gemini Decision 1](/docs/routing/#gemini-decision-1) instead.

```sh
./bin/hikyaku route --dry-run -f examples/ai-triage/hikyaku.llmfake.yaml \
  --source webhooks --data @examples/ai-triage/msg.json
```

The decision comes back with the model, each candidate's probability, the destinations chosen
and the fallback hops it would take. To see the same decision as a trace, run the example under
the Genkit Developer UI:

```sh
make genkit-ui                        # genkit start -- ./bin/hikyaku dev -f examples/ai-triage/hikyaku.llmfake.yaml
```

`hikyaku init --template ai-triage` scaffolds a relay of the same shape. [Routing](/docs/routing/#the-ai-router)
and [History-aware routing](/docs/history/) explain every key it uses.

## kafka-bridge

[`examples/kafka-bridge`](https://github.com/xavidop/hikyaku/tree/main/examples/kafka-bridge)

HTTP events into Kafka, and a Kafka topic out to a webhook.

- **In.** `POST /events` lands on the `events.raw` topic, keyed by customer and produced in
  batches, with a file spool while the brokers are down. The caller waits while the produce
  retries, then gets its answer once the spool has the message.
- **Out.** A consumer group reads `orders` and calls the `fulfilment` webhook in partition order.
  It commits an offset only once a record is delivered or dead-lettered.

`docker compose up -d` starts a Redpanda for it. `hikyaku.llmfake.yaml` runs the ingest route
offline (the spool takes everything). `hikyaku init --template kafka-bridge` scaffolds the same
relay.

## llm-proxy

[`examples/llm-proxy`](https://github.com/xavidop/hikyaku/tree/main/examples/llm-proxy)

An OpenAI-compatible LLM proxy. Clients ask for `fast`, `smart` or `auto`. Rules route the first
two. For `auto`, a System One decision model picks by how hard the request is. Each model
destination falls back to another provider, and a local Ollama model is the last resort. The
llmfake spec runs it offline, streaming included. See [LLM proxy](/docs/llm-proxy/).

## custom-destination

[`examples/custom-destination`](https://github.com/xavidop/hikyaku/tree/main/examples/custom-destination)

A custom build (your own hikyaku binary) that adds a destination type (a chat webhook) and a
source type (an inbox directory, one message per file) with the plugin SDK. See
[Plugins](/docs/plugins/).

## crm-plugin

[`examples/crm-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/crm-plugin)

Leads from a web form, posted to a CRM by an out-of-process plugin in Go (a separate program that
hikyaku starts and talks to). `hikyaku-crm` serves the `crm` destination type with
`grpcplugin.Serve`. hikyaku starts it, checks `options:` against the JSON Schema it declares, and
retries or dead-letters around it: a `429` or a `5xx` from the CRM is retried; a message with no
email is a permanent failure, so it goes straight to the DLQ. In `hikyaku.llmfake.yaml`, the CRM's
API is an HTTP source of the same relay.

```sh
cd examples/crm-plugin && go build -o bin/hikyaku-crm .
export CRM_TOKEN=dev-token
../../bin/hikyaku run -f hikyaku.llmfake.yaml
```

Then post a lead to `localhost:8080/leads`, as its README shows. See [Plugins](/docs/plugins/).

## postgres-plugin

[`examples/postgres-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/postgres-plugin)

PostgreSQL as a destination and as a source, served by one out-of-process plugin in Go.
`hikyaku-postgres` serves both types with `grpcplugin.Serve`. The `postgres` destination inserts
one row per message into a table: webhooks land in `events`, and a retry of a message that reached
the table writes no second row. The `postgres` source is a transactional outbox: your application
writes a row to the `outbox` table in the same transaction as its own change, hikyaku polls the
table, delivers each row, and marks it delivered once it is. A row whose delivery failed is
emitted again, and a row the relay died with is claimed again after `claim_ttl`. Both tables are
created on first use. The plugin maps PostgreSQL's error codes onto hikyaku's classes, so a lost
connection is retried and a constraint violation goes to the DLQ.

```sh
docker run --rm -d --name relay-db -p 5432:5432 \
  -e POSTGRES_USER=hikyaku -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=relay postgres:17-alpine
cd examples/postgres-plugin && go build -o bin/hikyaku-postgres .
export DATABASE_URL=postgres://hikyaku:secret@localhost:5432/relay
../../bin/hikyaku run -f hikyaku.llmfake.yaml
```

Then post a webhook to `localhost:8080/ingest` and insert a row into `outbox`, as its README
shows. In `hikyaku.llmfake.yaml` the outbox is delivered to a file; in `hikyaku.yaml`, to a
partner's API. See [Plugins](/docs/plugins/).
