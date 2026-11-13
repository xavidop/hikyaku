---
layout: ../../layouts/DocsLayout.astro
title: Routing and the AI router
---

# Routing and the AI router

This page explains how a route picks the destinations of a message. Read it when you write a
route, and when you set up the AI router, its thresholds and its fallbacks.

Every route has a router, and the router returns the destinations of each message. There are
four routers: `static`, `rules` (CEL), `sticky` and `ai`. Routers chain, so an AI router can fall
back to deterministic rules, and the rules to a static catch-all. Whatever router answers, the
destinations are always ones the spec declares.

## Routes

A route names the sources it takes, what to keep and reshape, and the router that decides.

```yaml
routes:
  - name: smart-triage
    from: [webhooks, live-events]            # the sources it takes
    filter: 'msg.json.type != "ping"'        # CEL; false skips this route
    transforms:                              # run before the router
      - jq: '{type, customer_id, subject, body}'
    router:
      type: ai
      # ...
```

`to: [a, b]` instead of `router:` is the static router in short. The `filter` is written in CEL
(Common Expression Language), a small language for yes/no conditions over the message. The filter
and the [transforms](/docs/transforms/) run first, so the router sees the transformed message.

## static

A `static` router always picks the same destinations.

```yaml
router: { type: static, to: [archive] }
```

It is the last link of most fallback chains, because it never fails.

## rules

A `rules` router picks destinations with CEL conditions, checked in order.

```yaml
router:
  type: rules
  match: first                               # first (default) | all
  rules:
    - { when: 'msg.json.type.startsWith("invoice.")', to: [billing] }
    - { when: 'msg.headers["X-Priority"] == "high"', to: [support, realtime-dashboard] }
  default: [support]                         # when no rule holds
```

Each `when` is a CEL expression over `msg`. Its fields are `msg.json`, `msg.headers`,
`msg.bodyText`, `msg.attributes`, `msg.source`, `msg.subject`, `msg.id`, `msg.protocol` and
`msg.contentType`. Rules are evaluated in order. With `match: first`, the first rule that holds
wins. With `match: all`, every rule that holds adds its destinations. A rule that cannot be
evaluated for a message (a missing key) counts as not holding. When no rule holds, `default`
applies. Without a `default`, the router fails over to its `fallback`. Every expression is
compiled at validation time.

## sticky

A `sticky` router sends a message where the last message with the same key went.

```yaml
router:
  type: sticky
  history: { store: redis-main, key: "{{ .json.customer_id }}", ttl: 7d }
  fallback:
    - { type: static, to: [support] }
```

It sends the message where the last call of the same history key went. This is pure,
deterministic affinity, with no model call. It works when every model is down, which makes it a
good router fallback for an AI route that records history.
[History-aware routing](/docs/history/#sticky-routing) has the details.

## Router fallback chain

When a router fails or chooses nothing, the next router of its `fallback` list takes over.

```yaml
router:
  type: ai
  # ...
  on_low_confidence: [support]
  fallback:                                  # tried in order when the AI step fails
    - type: rules
      rules:
        - { when: 'msg.json.type.startsWith("invoice.")', to: [billing] }
    - type: static
      to: [support]
```

`fallback` is an ordered list of routers (`ai`, `rules`, `sticky` or `static`). The next one is
tried when the router before it:

- errors, times out, or is over its budget;
- chooses no destination;
- leaves the message out through its [`when`](#only-the-messages-that-need-a-model) condition.

When every router of the chain fails or chooses nothing, the message is dead-lettered with reason
`no_route`, and `hikyaku dlq replay` runs it through the route again.

`on_low_confidence` is a separate, explicit target for the case where the model answered but was
not sure. Set `fallback_on_low_confidence: true` to hand unsure answers to the fallback chain
instead. `hikyaku_router_fallback_total{route,to}` counts each hop, and the envelope records
which router served the message.

## The AI router

The AI router asks a model which of its `candidates` should get the message. It runs on
[Genkit Go](https://genkit.dev). Every AI route is a Genkit flow named `route/<name>`, so it can
be traced in the Genkit Developer UI and scored with `hikyaku ai test`.

```yaml
router:
  type: ai
  engine: decision                           # decision (default) | generative
  model: typesafe/jev-1.13.0                 # default: models.decision
  instructions: |
    You route customer events for a SaaS company. Pick the team(s) that must see it.
  state: "{{ .bodyText }}"                   # what the model evaluates
  state_json: true
  candidates:
    - { name: billing, description: "Payments, invoices, refunds, subscription changes" }
    - { name: support, description: "Bugs, outages, how-to questions, angry customers" }
    - { name: realtime-dashboard, description: "Anything on-call should see live" }
  multi: true
  min_confidence: 0.6
  on_low_confidence: [support]
```

The model only ever chooses among the candidates, and every candidate must be a declared
destination. Validation rejects anything else, and a name the model makes up is dropped.

### Engines

The router has two engines. A **decision model** answers typed questions about a state, and gives
each answer a calibrated probability: a 0.9 means it is right about nine times in ten. A
**generative model** writes text; here it is asked for a structured decision with a confidence it
reports itself.

| | `engine: decision` (default) | `engine: generative` |
|---|---|---|
| Plugin | System One (`github.com/firebase/genkit/go/plugins/systemone/exp`, experimental) | the Genkit model plugins `googlegenai`, `anthropic`, `openai`, `openrouter` and `ollama` |
| Models | `typesafe/jev-*`, `openrouter-decisions/liquid/d1`, `openrouter-decisions/google/gemini-decision-1`, `cloudflare-decisions/@cf/cloudflare/clef`, `ollama-decisions/clef`, custom servers | any chat model under the `googleai/`, `anthropic/`, `openai/`, `openrouter/` or `ollama/` prefix, such as `googleai/gemini-2.5-flash` or `openrouter/anthropic/claude-sonnet-4.5` |
| Output | typed answers with **calibrated probabilities** | `GenerateData[RouteDecision]`, with a self-reported confidence |
| Why | cheap, fast, built for classification, with a real confidence for thresholds | flexible, explains itself, works with any provider |

Decision models do not generate text: they answer typed questions about a state. That is exactly
the routing problem, so `decision` is the default. `openrouter/<vendor>/<model>` (a generative
model through Genkit's OpenRouter plugin) and `openrouter-decisions/...` (OpenRouter's System One
decision server) are different providers. Both use `OPENROUTER_API_KEY`.

### Questions built from the candidates

The decision router has no fixed questions. The candidates come from YAML, so it builds its
questions at runtime:

- **`multi: false`** asks one choice question. The options are the candidate names, with their
  descriptions as the criteria. The top answer wins if its probability is at least
  `min_confidence` and it leads the runner-up by more than `min_margin`.
- **`multi: true`** sends **one request with one yes/no question per candidate** ("Should
  *billing* receive this? yes: Payments, invoices, refunds, subscription changes; no:
  otherwise"). The message goes to every candidate whose probability is at least
  `min_confidence`. `min_margin` does not apply.

`instructions` becomes the system message in front of every question. `state` is the message as
the model sees it, rendered from its template and redacted. Set `state_json: true` when it is
JSON.

### The generative engine

The generative engine asks a chat model for a decision of this shape:

```go
type RouteDecision struct {
	Destinations []string `json:"destinations"`
	Confidence   float64  `json:"confidence" jsonschema:"minimum=0,maximum=1"`
	Reason       string   `json:"reason"`
}
```

`engine: generative` calls `genkit.GenerateData[RouteDecision]` with a model of one of those
plugins. A `-tags llmfake` build adds the fake `llmfake/` provider for tests. The candidate names
go into the prompt and into the output schema. After decoding, unknown names are dropped and
`multi` is enforced. **Bad output** (invalid JSON, or names that are not candidates) is retried
like a transport error, then the next model of the chain takes over.

### Building the state

The state is what the model reads. It is untrusted data, never instructions:

```yaml
input:
  max_bytes: 8KiB                            # cap on the whole state, history included
  redact: ["$.card.number", "$.email"]       # JSONPaths removed before the model sees it
  headers: [X-Customer-Tier]                 # headers included (allowlist)
```

`max_bytes` (8KiB by default) caps the whole state. System One servers may silently truncate a
long state, so hikyaku cuts it first. With [history](/docs/history/), it drops the oldest turns
before it ever shortens the current message.

### Thresholds and low confidence

Thresholds decide when an answer is sure enough to act on.

`min_confidence` is the lowest probability accepted. `min_margin` is how far the top answer of a
single choice must lead the second. When the answer falls below them, the message goes to
`on_low_confidence`, or to the router fallback with `fallback_on_low_confidence: true`.

Calibration changes between model releases. So a set of thresholds belongs to a **pinned
decision model version**: `hikyaku lint` warns about `latest`. Tune the thresholds with
[`hikyaku ai test`](/docs/cli/#hikyaku-ai-test), whose report includes reliability bins.

### Shadow mode

Shadow mode lets you watch a new AI route decide, without letting it deliver.

```yaml
shadow: true
```

The AI router decides and logs its decision, but the message is delivered through the fallback
chain. Run a new AI route in shadow next to the rules it will replace, compare, then switch.

### Only the messages that need a model

`when` sends only the messages that match to the model. The rest skip it, with no model call.

```yaml
router:
  type: ai
  when: has(msg.json.text) && msg.json.channel == "chat"
  candidates:
    - name: billing
      description: Payments, invoices, refunds
    - name: support
      description: Bugs, errors, how-to questions
  fallback:
    - type: rules
      rules:
        - when: msg.json.type.startsWith("invoice.")
          to: [billing]
    - type: static
      to: [support]
```

`when` is a CEL condition over `msg`, on any router. A message it is true for reaches the router.
Any other message skips it, with **no model call**, and goes straight to the next router of the
fallback chain. That keeps model spend to the messages that need a model:

- **Gate on the payload:** `has(msg.json.text)` sends free text to the model and typed events to
  the rules.
- **Gate on the caller:** `msg.json.tier == "enterprise"` sends one customer tier to the model.
- **Gate on an opt-in:** `"X-Route-Ai" in msg.headers && msg.headers["X-Route-Ai"] == "yes"`
  lets the sender ask for AI routing.

A condition that cannot be evaluated counts as false. In CEL, reading a field or header that is
missing is an error, so check that it is there first, with `has(msg.json.x)` or
`"X-Name" in msg.headers`. The decision shows the skip (`ai: skipped (when is false)`), and
`hikyaku_router_skipped_total{route,router}` counts it. `hikyaku lint` warns when the last router
of a chain has a `when`: no router is left for the messages it leaves out, so they are
dead-lettered with reason `no_route`.

Four features keep model calls to the messages that need a decision: `when`,
[sticky routing](#sticky) (follow-ups go where the last call went, without asking), the
[cache](#cache) and the [budget](#budget) (a hard cap on calls and tokens).

### Cache

The cache reuses a decision for messages that share a key.

```yaml
cache: { store: redis-main, key: "{{ .json.ticket_id }}", ttl: 10m }
```

Decisions are cached by `key` (by default a hash of the state) for `ttl` (10m by default). The
cache is an in-process LRU of `size` entries (10000 by default), or a shared store, so that every
replica reuses the decisions. A cached decision is marked as such on the envelope.

### Budget

The budget is a hard cap on what a route may spend on model calls.

```yaml
budget: { max_calls_per_minute: 600, max_tokens_per_hour: 2000000, store: redis-main }
```

`max_calls_per_minute` counts model calls per clock minute. `max_tokens_per_hour` counts tokens
per clock hour. When the route has spent either, messages go straight to the router fallback
until the window turns over. Only the call budget is shared through `store`, so it holds across
replicas. The token budget is counted in each process.

### Timeout

`timeout` (10s by default) is the time budget of the whole AI step, model retries and model
fallbacks included. When it runs out, the router fallback takes over.

### What gets recorded

Every decision is recorded as `ai.decision` on the envelope: the model that answered, the
answers and their probabilities, the reason (generative engine), and which fallback served it.
It shows in traces, in `hikyaku tail`, in the dead-letter queue and, with history, in the
routing history of the key.

## Model retries and model fallbacks

When a model fails, hikyaku retries it, then moves to the next model of a chain.

```yaml
models:
  default: googleai/gemini-2.5-flash         # generative default
  decision: typesafe/jev-1.13.0              # decision default
  retry: { attempts: 3, backoff: 500ms, max_backoff: 10s }   # per model, then the next one
  fallback:
    default: [anthropic/claude-sonnet-4-5, ollama/llama3.2]
    decision: [openrouter-decisions/liquid/d1, ollama-decisions/clef]
```

- `models.retry` sets the attempts, backoff and maximum backoff **per model**, before the chain
  moves to the next model. It retries rate limits, outages, timeouts and bad output.
- `models.fallback.default` (generative) and `models.fallback.decision` are the global chains. A
  route overrides them with `fallback_models`.
- Chains can cross providers: `typesafe/jev-1.13.0`, then `openrouter-decisions/liquid/d1`, then
  `ollama-decisions/clef`, a local model that is always there.
- **The System One plugin retries a transport failure twice by itself, inside each attempt.**
  hikyaku counts those as part of the attempt. So one attempt is up to three HTTP calls, and
  `attempts: 3` means at most nine calls to one decision model before the chain moves on.
- `hikyaku_ai_model_fallback_total{route,from,to,reason}` counts every move to the next model.

When every model of the chain has failed, the [router fallback chain](#router-fallback-chain)
takes over.

## Genkit plugins and servers

`genkit:` names the model plugins to start and the decision servers to use.

```yaml
genkit:
  plugins: [systemone, googlegenai, anthropic, ollama]
  systemone:
    servers: [typesafe, openrouter, ollama]
    custom:
      - provider: corp-decisions
        base_url: https://decisions.internal
        api_key: "{{secrets.CORP_DECISIONS_KEY}}"
```

`genkit.plugins` names the plugins to initialize: `systemone`, `googlegenai`, `anthropic`,
`openai`, `openrouter` and `ollama` (and `llmfake` in test builds). A plugin named by a model's
provider prefix is added automatically.

The generative plugins take their key from a declared secret of that name, or else from the
environment: `GEMINI_API_KEY` or `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` and
`OPENROUTER_API_KEY`. `ollama` needs none. `genkit.ollama.url` locates the Ollama server.
`genkit.openai.base_url` points `openai` at any OpenAI-compatible server (OpenAI's by default; a
server of your own may need no key).

`systemone.servers` turns on the known System One servers, each with its key from the
environment: `typesafe` (`TYPESAFE_API_KEY`), `openrouter` (`OPENROUTER_API_KEY`), `workersai`
(`CLOUDFLARE_API_TOKEN`) and `ollama` (no key). `custom` adds any other server that speaks the
System One protocol; its models are `<provider>/<id>`.

### Gemini Decision 1

[Gemini Decision 1](https://aistudio.google.com/docs/models/gemini-decision-1) is Google's decision
model. hikyaku reaches it through OpenRouter's decision server, as
`openrouter-decisions/google/gemini-decision-1`. It needs `OPENROUTER_API_KEY`, not a Gemini API
key.

```yaml
secrets:
  OPENROUTER_API_KEY: env:OPENROUTER_API_KEY

models:
  decision: openrouter-decisions/google/gemini-decision-1
  fallback:
    decision: [openrouter-decisions/liquid/d1]   # optional: another decision model
```

Nothing else changes in a route: the candidates, `multi`, `min_confidence`, `min_margin`, history,
sticky routing, the cache and the budget work as they do with any decision model. The
[ai-triage example](/docs/examples/#ai-triage) passes its five labeled cases on this model with the
thresholds it already has (accuracy 100%, ECE 0.03, about 250 ms per call).

What to know about this model:

- **With `multi: true`, a candidate's name may hold only letters, digits, `_` and `-`**, and at
  most 64 characters. A name with a dot, such as `team.billing`, is refused with an HTTP 400, and
  the router fallback chain takes every message. Without `multi`, any name works.
- **Probabilities stay between 0.01 and 0.99.** With `multi: true`, a `min_confidence` above 0.99
  never matches.
- **Tune the thresholds on this model.** A threshold tuned on another decision model does not
  carry over. Run [`hikyaku ai test`](/docs/cli/#hikyaku-ai-test) with your own cases.
- **Pin the version in production.** `google/gemini-decision-1` follows the newest release. The
  decision's `model` shows the version that answered, such as
  `openrouter-decisions/gemini-decision-1-20261111`. Ask for that version by its dated name:
  `openrouter-decisions/google/gemini-decision-1-20261111`.
- **`GEMINI_API_KEY` does not work for this model.** The Gemini API has a decisions endpoint of its
  own, but it does not speak the System One protocol, so neither the `googlegenai` plugin nor a
  `custom` server reaches it.

## Try it

Two commands test a route before it goes live.

```sh
hikyaku route --dry-run --source webhooks --data @msg.json        # the decision, without delivering
hikyaku ai test --route smart-triage --cases cases.yaml --min-accuracy 0.9
```

`route --dry-run` shows which router answered, the model, the probabilities and the fallback hops
it would take. `ai test` scores the route on labeled cases: accuracy, a confusion matrix,
calibration in reliability bins, and latency. The [Quick start](/docs/quickstart/#route-with-ai-on-a-fake-model)
runs both on a fake decision model.
