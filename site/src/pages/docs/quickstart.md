---
layout: ../../layouts/DocsLayout.astro
title: Quick start
---

# Quick start

This page gets a relay running in a few minutes. You build two relays: a webhook that fans out to
two destinations, then an AI-routed relay that runs on a fake decision model, so it needs no API
key.

## Before you start

You need:

- hikyaku on your `PATH` ([Install](/docs/install/));
- `curl`;
- for the AI part: a clone of the repository and Go 1.27 or newer.

## 1. Scaffold a spec

The spec is the YAML file that declares a relay. `hikyaku init` writes one for you.

```sh
mkdir relay && cd relay
hikyaku init --template webhook-fanout
```

`hikyaku init` writes `hikyaku.yaml` into the current directory. `--dir` picks another directory,
and `--force` overwrites an existing spec. The `webhook-fanout` template is a spec of this shape:

```yaml
# yaml-language-server: $schema=https://xavidop.github.io/hikyaku/schema/hikyaku.schema.json
version: 1
name: webhook-fanout

sources:
  - name: webhooks
    type: http
    mode: listen
    listen: ":8080"
    path: /ingest

destinations:
  - name: console
    type: stdout
    format: body
  - name: archive
    type: file
    path: ./out/archive.ndjson

routes:
  - name: fanout
    from: [webhooks]
    to: [console, archive]
```

Read it top to bottom: one source, two destinations, one route. `to:` is the static router, so
every message from `webhooks` goes to both destinations. Both destinations are `required` (the
default). So the webhook gets its answer once both have the message. The first line points your
editor at the JSON Schema, which gives you completion and inline errors. See
[The spec](/docs/spec/#editor-support).

## 2. Validate it

Check the spec before you run it.

```sh
hikyaku validate
```

`validate` loads the spec and checks it without starting anything. It reports:

- unknown keys, with their line;
- references to sources and destinations that do not exist;
- fallback cycles;
- every CEL expression, jq program and template that does not compile.

It exits `0` when the spec is valid and `2` with every error otherwise. Some protocol keys are
checked only by the protocol's own module, when the relay builds the source or destination.
`hikyaku doctor` builds the destinations too, so it catches their errors before `run` does.
`hikyaku lint` adds warnings, such as a listen source without `auth`.

## 3. Run it

Start the relay.

```sh
hikyaku run
```

The relay listens on `:8080` for webhooks. It serves the [admin API](/docs/observability/#admin-api)
on `:9090`. Logs go to stderr. Stop it with Ctrl-C: in-flight messages get `server.shutdown_grace`
(20s by default) to finish.

## 4. Send a webhook

Send one message and watch it reach both destinations. In a second terminal:

```sh
curl -i http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -d '{"type":"invoice.paid","customer_id":"c-42","amount":129}'
```

The message fans out:

- the `run` terminal prints the body, from the `console` destination;
- `out/archive.ndjson` gets one line: the whole envelope (ID, source, headers, body) as JSON;
- `curl` gets a `2xx` once both deliveries succeeded. An HTTP listen source answers when the
  message is delivered (`ack: delivered`, the default).

```sh
tail -n 1 out/archive.ndjson
curl -s localhost:9090/metrics | grep hikyaku_delivery_attempts_total
```

## 5. Watch it retry

`hikyaku chaos` wraps a destination with the fault injector, a sink that fails on purpose. With
`--data`, it pushes `--count` copies of a message through the destination's full retry and
fallback path. It does not start the sources:

```sh
hikyaku chaos --destination archive --fail 50% --count 20 --data '{"type":"invoice.paid"}'
```

It reports:

- the attempts per message;
- how many messages the destination and each fallback delivered;
- what reached the dead-letter queue (DLQ), where a message goes when every delivery attempt
  failed;
- what was lost.

It exits `1` if a message was lost. Without `--data`, it runs the relay instead, with the
destination wrapped, so the faults meet the messages your sources receive.
[Retries and fallbacks](/docs/resilience/) explains the policy it exercises.

## Route with AI, on a fake model

The [`ai-triage` example](/docs/examples/#ai-triage) routes support events with a System One
decision model: a model that answers typed questions with calibrated probabilities, instead of
generating text. Its `hikyaku.llmfake.yaml` uses the `llmfake` provider instead. That is a
deterministic fake that answers like a System One server, so the same router runs with no key and
no network. It needs a build with `-tags llmfake`, from a clone of the repository:

```sh
git clone https://github.com/xavidop/hikyaku && cd hikyaku
make build-llmfake
./bin/hikyaku route --dry-run -f examples/ai-triage/hikyaku.llmfake.yaml \
  --source webhooks --data @examples/ai-triage/msg.json
```

`make build-llmfake` writes `bin/hikyaku`. `hikyaku route` runs the message through every route
of the `webhooks` source. `--dry-run` stops before delivery and prints:

- which router answered, and which model;
- the probability of each candidate;
- the destinations it picked;
- the fallback hops it would take.

`--json` prints the same as JSON.

Try your own messages with `--data @file.json`. Score the router on labeled cases with
[`hikyaku ai test`](/docs/cli/#hikyaku-ai-test). To use a real decision model, run the example's
`hikyaku.yaml` with `TYPESAFE_API_KEY` set. `hikyaku doctor` checks that the model answers.

## Next steps

- [Concepts](/docs/concepts/): sources, destinations, routes and how a message is acknowledged.
- [The spec](/docs/spec/): a guided tour of `hikyaku.yaml`.
- [Routing](/docs/routing/): the AI router, its thresholds and its fallbacks.
- [Examples](/docs/examples/): the six example relays.
