<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-wordmark-dark.svg">
    <img src="assets/logo-wordmark-light.svg" alt="hikyaku 飛脚" height="96">
  </picture>
</p>

<p align="center">
  <b>Read from endpoints. Deliver to endpoints. Route with AI. Never drop a message.</b>
</p>

<p align="center">
  <a href="https://xavidop.github.io/hikyaku/">Docs</a> ·
  <a href="https://xavidop.github.io/hikyaku/docs/quickstart/">Quick start</a> ·
  <a href="https://xavidop.github.io/hikyaku/docs/spec-reference/">Spec reference</a> ·
  <a href="https://xavidop.github.io/hikyaku/docs/concepts/">Concepts</a>
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/xavidop/hikyaku"><img src="https://pkg.go.dev/badge/github.com/xavidop/hikyaku.svg" alt="Go Reference"></a>
  <a href="https://github.com/xavidop/hikyaku/actions/workflows/ci.yml"><img src="https://github.com/xavidop/hikyaku/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

---

> **飛脚 (hikyaku)**: the Edo-period relay couriers who carried letters between cities, station to station, and found another way when a road was closed.

hikyaku is a message relay written in Go. It reads messages from one or more endpoints (a webhook, a WebSocket, a Kafka topic, an SQS queue, ...) and delivers them to one or more others. One YAML file describes the whole relay. Every step retries and falls back, so a message is never dropped without a trace. An AI router can pick the destination of each message, with a decision model that says how sure it is.

```
 sources ──▶ message ──▶ transforms ──▶ router ───────┬─▶ billing ──(fallback)─▶ billing-dr
 http · ws · kafka · …   filter · jq    ai ▸ rules    ├─▶ support (group: a → b)
                                        ▸ static      └─▶ dashboard (best-effort)
 delivery: timeout ▸ retry ▸ circuit breaker ▸ fallback ▸ dead-letter queue
```

<p align="center">
  <img src="site/public/demo.gif" alt="hikyaku validate, then route --dry-run: the AI router picks billing with p=0.85 after a model fallback" width="900" />
</p>

- **One YAML spec.** Sources, destinations, routes, transforms and the retry policy live in one file. Your editor gets a JSON Schema, and `hikyaku validate` catches mistakes before anything runs.
- **Retries and fallbacks at every step.** Timeouts, retries with backoff, circuit breakers, fallback chains, groups with failover, and a dead-letter queue (DLQ) with a fallback of its own. Dead letters can be replayed.
- **AI routing with real confidence.** A decision model answers one yes/no question per candidate destination and gives a calibrated probability. Rules and a static route take over when the model is unsure. A `when` condition sends only the messages that need a model to one.
- **Every protocol, one message.** HTTP, WebSocket, SSE, gRPC, TCP/UDP, Kafka, NATS, MQTT, RabbitMQ, Redis, SQS/SNS/EventBridge, S3, Pub/Sub, GCS, Service Bus and Event Hubs. A source tells its broker a message is done only once it is delivered or dead-lettered.
- **Made for production.** A disk buffer survives `kill -9`. Messages stay in order per key. Batches, hot reload, an admin API with health checks and Prometheus metrics, OpenTelemetry traces, and secrets through [mamori](https://github.com/xavidop/mamori).
- **An LLM proxy.** An OpenAI-compatible endpoint routes chat requests to a model by rules or by a decision model, and fails over between providers.
- **Your own types.** Add a destination or a source in Go: inside your own build, or as a plugin that hikyaku starts.

## Install

```sh
brew install xavidop/tap/hikyaku                                                            # macOS
scoop bucket add xavidop https://github.com/xavidop/scoop-bucket && scoop install hikyaku   # Windows
docker run --rm ghcr.io/xavidop/hikyaku:latest version                                      # Docker
helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku                                   # Kubernetes
go install github.com/xavidop/hikyaku/cmd/hikyaku@latest                                    # Go 1.27 or newer
```

Linux gets `.deb`, `.rpm` and `.apk` packages, and every platform gets archives, on the [releases page](https://github.com/xavidop/hikyaku/releases). `hikyaku-lite` is a smaller build without the brokers, the clouds and out-of-process plugins. Then run `hikyaku doctor`. The [install page](https://xavidop.github.io/hikyaku/docs/install/) has the details.

## Quick start

```sh
hikyaku init         # writes hikyaku.yaml: a webhook fanned out to stdout and a file
hikyaku validate     # checks references, fallback cycles, CEL and templates
hikyaku run          # starts the relay
curl -i localhost:8080/ingest -H 'Content-Type: application/json' -d '{"type":"invoice.paid"}'
```

The webhook gets a `200` once every destination has the message, or a `202` once a failed message is safe in the dead-letter queue.

A spec reads like a description of the relay:

```yaml
version: 1
name: support-intake

secrets:
  WEBHOOK_SECRET: env:WEBHOOK_SECRET          # a mamori ref, never a value
  TYPESAFE_API_KEY: env:TYPESAFE_API_KEY

models:
  decision: typesafe/jev-1.13.0               # a decision model; pin a version in production

sources:
  - name: webhooks
    type: http
    listen: ":8080"
    path: /ingest
    auth: { hmac: { header: X-Signature-256, secret: "{{secrets.WEBHOOK_SECRET}}" } }

destinations:
  - name: billing
    type: https
    url: https://billing.internal/api/events
    retry: { attempts: 5, backoff: 200ms }
    fallback: [billing-dr]
  - { name: billing-dr, type: https, url: https://billing-dr.internal/api/events }
  - { name: support, type: https, url: https://support.internal/hook }

dead_letter: { type: file, path: ./out/dlq.ndjson, fallback: [stderr] }

routes:
  - name: triage
    from: [webhooks]
    router:
      type: ai
      instructions: You route customer events for a SaaS company.
      candidates:
        - { name: billing, description: "Payments, invoices, refunds" }
        - { name: support, description: "Bugs, outages, how-to questions" }
      min_confidence: 0.6
      fallback: [{ type: static, to: [support] }]
```

The [quick start](https://xavidop.github.io/hikyaku/docs/quickstart/) builds this up step by step. Every key is in the [spec reference](https://xavidop.github.io/hikyaku/docs/spec-reference/), and `hikyaku schema` prints the JSON Schema.

## Commands

```
hikyaku run [--watch]                         run the relay; SIGHUP, POST /reload or --watch reload it
hikyaku dev                                   run with the Genkit reflection server: genkit start -- hikyaku dev
hikyaku validate [--check-secrets]            check the spec: references, fallback cycles, CEL, templates
hikyaku lint [--strict]                       warn about unpinned models, missing fallbacks, open endpoints
hikyaku graph [--format svg|png|dot|json]     draw the spec: sources, routes, destinations and fallbacks
hikyaku test [files...]                       check that test messages are routed and transformed as expected
hikyaku init [--template T]                   scaffold webhook-fanout, ai-triage, wss-to-http or kafka-bridge
hikyaku schema                                print the JSON Schema
hikyaku doctor                                check secrets, plugins, destinations, the DLQ, stores and models
hikyaku send --to <dest> --data @msg.json     push one message through the full retry and fallback path
hikyaku route --dry-run --source <s> --data @msg.json   show the decision, probabilities and fallback plans
hikyaku ai test --route <r> --cases cases.yaml          accuracy, confusion matrix, calibration, latency
hikyaku chaos --destination <d> --fail 50% --latency 2s inject faults to exercise retries and fallbacks
hikyaku tail [--source x]                     stream live messages from a running relay
hikyaku forward --to <local url>              send what a running relay receives to a local URL too
hikyaku dlq list|inspect|replay               work with dead letters
hikyaku history show|correct|export|forget    routing history and the feedback loop
hikyaku version                               version, commit and Genkit version
```

Exit codes: `0` ok, `1` a failed check (doctor, evals, `lint --strict`, a send or route that did not deliver, a lost chaos message, a failed replay), `2` a config or usage error, `3` an infrastructure error. The [CLI reference](https://xavidop.github.io/hikyaku/docs/cli/) has every flag.

## Protocols

| Protocol | Source | Destination |
|---|---|---|
| `http` / `https` | listen, poll | ✓ |
| `ws` / `wss` | listen, subscribe | ✓ |
| `sse` | listen, subscribe | ✓ |
| `grpc` | listen, subscribe | ✓ |
| `tcp` / `udp` | listen | ✓ |
| `kafka` | subscribe | ✓ |
| `nats` / `jetstream` | subscribe | ✓ |
| `mqtt` | subscribe | ✓ |
| `amqp` (RabbitMQ) | subscribe | ✓ |
| `redis` (Streams, Pub/Sub) | subscribe | ✓ |
| `sqs` / `sns` / `eventbridge` | subscribe | ✓ |
| `s3` / `gcs` | poll | ✓ |
| `pubsub` | subscribe | ✓ |
| `servicebus` / `eventhubs` | subscribe | ✓ |
| `stdin` / `stdout` / `stderr` / `file` | ✓ | ✓ |
| `openai` (the LLM proxy) | listen | `model` destinations |
| `fault` (fault injection) | | ✓ |

The [protocols page](https://xavidop.github.io/hikyaku/docs/protocols/) explains each one, and every protocol module has a README with its keys, acknowledgements and error tags. gRPC, the brokers and the clouds are in the full build only; `hikyaku-lite` keeps the rest.

## Documentation

Everything is on the [docs site](https://xavidop.github.io/hikyaku/):

- [Quick start](https://xavidop.github.io/hikyaku/docs/quickstart/), [Concepts](https://xavidop.github.io/hikyaku/docs/concepts/) and [Install](https://xavidop.github.io/hikyaku/docs/install/)
- [The spec](https://xavidop.github.io/hikyaku/docs/spec/) and the [Spec reference](https://xavidop.github.io/hikyaku/docs/spec-reference/)
- [Protocols](https://xavidop.github.io/hikyaku/docs/protocols/), [Transforms](https://xavidop.github.io/hikyaku/docs/transforms/) and [Delivery](https://xavidop.github.io/hikyaku/docs/delivery/)
- [Routing](https://xavidop.github.io/hikyaku/docs/routing/) and [History-aware routing](https://xavidop.github.io/hikyaku/docs/history/)
- [Retries and fallbacks](https://xavidop.github.io/hikyaku/docs/resilience/) and [Security](https://xavidop.github.io/hikyaku/docs/security/)
- [Observability](https://xavidop.github.io/hikyaku/docs/observability/) and [Deploy](https://xavidop.github.io/hikyaku/docs/deploy/) (Docker and the Helm chart)
- [LLM proxy](https://xavidop.github.io/hikyaku/docs/llm-proxy/), [Plugins](https://xavidop.github.io/hikyaku/docs/plugins/) and the [Go library](https://xavidop.github.io/hikyaku/docs/library/)
- [CLI reference](https://xavidop.github.io/hikyaku/docs/cli/) and [Examples](https://xavidop.github.io/hikyaku/docs/examples/)

The [examples](examples/) run as they are: a signed webhook fanned out with a fallback and a DLQ, a WebSocket subscription delivered over HTTP, AI triage with history, an HTTP-to-Kafka bridge, an LLM proxy, a custom build with its own types, and two out-of-process plugins (a CRM destination; PostgreSQL as a destination and an outbox source). Each has a `hikyaku.llmfake.yaml` that runs offline on fake models (`make build-llmfake`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). `make all` runs what CI runs. [Concepts](https://xavidop.github.io/hikyaku/docs/concepts/) explains how the relay is put together.

Sibling projects: [tamesu](https://github.com/xavidop/tamesu), [tobira](https://github.com/xavidop/tobira), [senro](https://github.com/xavidop/senro), [mamori](https://github.com/xavidop/mamori).

## License

[MIT](LICENSE) © Xavier Portilla Edo
