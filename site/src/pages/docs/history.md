---
layout: ../../layouts/DocsLayout.astro
title: History-aware routing
---

# History-aware routing

This page explains how the AI router uses past calls, not only the current message. Read it when
follow-ups must go where earlier messages went, or when you want the router to learn from
corrections.

A router that only sees the current message gets these cases wrong:

- a follow-up from the same customer, which should go where the previous event went;
- a customer who has already escalated twice today;
- a conversation that moved from a billing question to a bug report.

Past calls are kept per route and per key in a [store](/docs/spec/#stores), Redis in production.
They are sent to the model as conversation turns. History is designed for the System One decision
engine, whose state takes several role-tagged messages natively. The generative engine reads the
same history as chat messages.

## Turn it on

Add a `history` block to an AI router, and a store to keep the records in.

```yaml
stores:
  - name: redis-main
    type: redis
    addr: redis:6379
    password: "{{secrets.REDIS_PASSWORD}}"
    timeout: 50ms

routes:
  - name: smart-triage
    from: [webhooks]
    router:
      type: ai
      # instructions, state, candidates, thresholds ...
      history:
        store: redis-main                    # default: the first store
        key: "{{ .json.customer_id }}"       # whose history: customer, conversation, session, device
        window: { max_entries: 10, max_age: 24h }   # what is sent to the model
        ttl: 7d                              # what is kept in the store (mandatory)
        record: { state: redacted }          # redacted (default) | summary | none
        sticky: { enabled: true, follow_up_threshold: 0.8 }
        on_store_error: ignore               # ignore (default) | fallback
```

`key` is a per-message template. Everyone who renders the same key shares one history. `window`
bounds what the model reads: the newest `max_entries` (10 by default), no older than `max_age`.
`ttl` bounds what is kept in the store. Without a `stores:` section, `hikyaku run` keeps history
in memory, which is enough on one replica and for local runs.

## What is recorded

One record per routed call. It is appended **after delivery**, so the outcome is known:

```go
type CallRecord struct {
	MsgID       string    `json:"msg_id"`
	At          time.Time `json:"at"`
	Route       string    `json:"route"`
	Key         string    `json:"key"`             // the rendered history.key
	State       string    `json:"state,omitempty"` // the redacted state, a short summary, or empty
	Decision    []Answer  `json:"decision"`        // candidate, probability, chosen, margin
	Model       string    `json:"model"`           // which model in the fallback chain answered
	DeliveredTo []string  `json:"delivered_to"`    // after destination fallbacks
	Outcome     string    `json:"outcome"`         // delivered | fallback | dlq | low_confidence
	Feedback    *Feedback `json:"feedback,omitempty"` // a human or system correction
}
```

## The Redis layout

The Redis store keeps history like this:

- One list per route and key: `{prefix}:{relay}:{route}:hist:{key}`. `prefix` is the store's
  `key_prefix` (`hikyaku` by default), and `relay` is the spec's `name`. The `{key}` part is a
  hash tag, so a Redis Cluster keeps every entry of a key on one slot.
- Each write is one pipeline: `LPUSH`, `LTRIM` to `max_entries × 2`, and `EXPIRE` to the `ttl`.
- A secondary hash, `…:msg:{msg_id} → key`, lets feedback find a record by its message ID. It has
  the same TTL.
- **Writes are asynchronous**: they go through a bounded in-process queue with retries, and they
  never block delivery. **Reads are bounded** by the store's `timeout` (50ms by default), so a
  slow Redis cannot stall routing.
- Standalone Redis (`addr`), Sentinel (`sentinel: { master_name, addrs }`) and Cluster
  (`cluster: { addrs }`) all work, with ACL users, passwords and TLS.

The same store backs the AI [decision cache](/docs/routing/#cache), source dedupe keys, and the
retry-budget and AI call-budget counters. One Redis holds all the shared state. That is what makes
several replicas behave like one; see [Deploy](/docs/deploy/#running-several-replicas).

## When the store fails

History makes routing better, but it never makes routing unavailable. `on_store_error` says what
happens when the store cannot be read:

- **`on_store_error: ignore`** (the default) routes the message without history.
- **`on_store_error: fallback`** hands the message to `router.fallback` instead.

Either way, `hikyaku_history_errors_total{op}` is incremented. The store's own circuit breaker
(on by default) stops hammering a dead Redis until it recovers.

## How history reaches the model

The router turns the stored records into conversation turns, then asks its questions as usual.

1. `Recent(key, window)` fetches the last records within the window, oldest first.
2. The router builds the state as role-tagged turns:
   - the system message is the route's `instructions`, plus one line explaining the history
     format;
   - each past call becomes a pair of turns: a `user` turn with the stored state (or summary),
     and a `model` turn that says what happened, such as
     `routed to support (p=0.91), outcome: delivered`, plus `correct destination: billing` when a
     correction was recorded;
   - the current message is the last `user` turn.
3. The candidate questions are unchanged (one choice, or one yes/no per candidate). But they are
   now answered **in the context of the history**, so the probabilities reflect it.
4. `input.max_bytes` caps the **total** state. The oldest pairs of turns go first. The current
   message is shortened only once no history is left. System One servers may silently truncate a
   long state; this way, the part that is cut is the part that matters least.

`hikyaku history show --route smart-triage --key c-42` prints a key's history exactly as the
model will see it.

## Sticky routing

Sticky routing sends a follow-up where the last call of the same key went, without asking the
candidate questions again.

```yaml
history:
  # ...
  sticky: { enabled: true, mode: model, follow_up_threshold: 0.8 }
```

With `sticky.enabled`, the router adds one built-in yes/no question: "Is the latest message a
follow-up to a previous call in this history?"

- If the probability is at least `follow_up_threshold` (0.8 by default) and the last call was
  delivered, the message goes to **the same destinations as last time**. When the last call has a
  correction, it goes to the corrected ones instead. The candidate questions are skipped, which is
  cheaper and consistent.
- Otherwise the candidate questions decide, as usual.

`sticky.mode` is `model` (the default, as above), `always_last` or `off`. `always_last` is pure
deterministic affinity within the window, with no model call. It works when every model is down,
so it also serves as a router fallback. The [`sticky` router](/docs/routing/#sticky) is the same
thing as a router of its own.

`hikyaku_sticky_total{route,outcome}` counts the follow-ups that stuck and the ones that did not.

## The feedback loop

A correction says where a past message should have gone. Record one, and the router takes it into
account for that key from then on. A correction comes in three ways:

- `POST /v1/feedback` on the [admin API](/docs/observability/#admin-api), with the admin token:

  ```sh
  curl -X POST http://localhost:9090/v1/feedback \
    -H "Authorization: Bearer $HIKYAKU_ADMIN_TOKEN" \
    -d '{"msg_id": "01JC3Z8Q6ZK3V0B9D3T5V7X9YB", "correct": ["billing"], "note": "refund, not a bug"}'
  ```

- `hikyaku history correct <msg_id> --to billing --note "refund, not a bug"`;
- a destination that answers with an `X-Hikyaku-Reroute: billing` header.

The correction updates the stored record. Every later call for that key sees
`correct destination: billing` in its history. So the model adapts per customer by in-context
learning (it learns from the examples in its input), with no retraining.
`hikyaku_feedback_total{route}` counts corrections.

To tune the global prompt and thresholds from real mistakes, export the corrections as an eval
set and score the route on it:

```sh
hikyaku history export --labeled --since 30d --route smart-triage > cases.yaml
hikyaku ai test --route smart-triage --cases cases.yaml
```

## Privacy

History stores message content, so these controls decide what is kept, and for how long.

- `record.state: redacted` (the default) stores the state after `input.redact` has removed its
  JSONPaths.
- `record.state: summary` stores a short template-rendered summary instead, such as
  `record: { state: summary, summary: "{{ .json.type }} / {{ .json.subject }}" }`.
- `record.state: none` stores only the decision and the outcome.
- The `ttl` is mandatory: history is never kept forever.
- `hikyaku history forget --route r --key k` and `DELETE /v1/history/{route}/{key}` delete a
  key's history, for right-to-be-forgotten requests.
- Give hikyaku a dedicated Redis user with an ACL limited to its key prefix, and use TLS.

## The store interface

Read this section if you want to write a store of your own. Stores implement two small
interfaces from the root module's `store/` package. The Redis implementation is its own module,
`stores/redis` (go-redis v9, with standalone, Sentinel and Cluster support). `memory` implements
both for local runs and tests.

```go
type HistoryStore interface {
	Append(ctx context.Context, route, key string, rec CallRecord, o AppendOptions) error
	Recent(ctx context.Context, route, key string, w Window) ([]CallRecord, error)
	Feedback(ctx context.Context, msgID string, fb Feedback) (CallRecord, error)
	Forget(ctx context.Context, route, key string) error
	Records(ctx context.Context, route string, since time.Time, fn func(CallRecord) error) error
}

type KV interface { // the cache, dedupe and counters
	Get(ctx context.Context, k string) ([]byte, bool, error)
	SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error)
	Incr(ctx context.Context, k string, ttl time.Duration) (int64, error)
}

type Deleter interface { // optional: dedupe forgets the key of a message that failed
	Delete(ctx context.Context, k string) error
}
```

## Metrics

| Metric | What it counts |
|---|---|
| `hikyaku_history_reads_total{route,outcome}` | history reads, by outcome |
| `hikyaku_history_errors_total{op}` | store errors, by operation |
| `hikyaku_history_turns{route}` | turns sent to the model (histogram) |
| `hikyaku_sticky_total{route,outcome}` | sticky follow-up decisions |
| `hikyaku_feedback_total{route}` | corrections recorded |
