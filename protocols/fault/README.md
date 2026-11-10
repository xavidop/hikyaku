# protocols/fault

A destination that fails on purpose. Use it to watch retries, circuit breakers, fallbacks and the dead-letter queue (DLQ, where a message goes when every delivery attempt failed) at work, in local runs and in tests. hikyaku's resilience suite is built on it.

The module has two parts:

- the `fault` destination type, which delays and fails by the rules you set;
- `fault.Wrap`, which puts the same faults in front of a real destination. `hikyaku chaos` uses it.

```go
import "github.com/xavidop/hikyaku/protocols/fault"

fault.Register() // registers fault as a destination
```

## Destination

A `fault` destination accepts or fails each delivery attempt. Each attempt goes like this:

1. It waits `latency`.
2. If it is one of the first `fail_first` attempts, it fails.
3. Otherwise it fails at random, with the probability `fail`.
4. An attempt that does not fail is accepted. Its result has status `202`.

The destination keeps the last 1000 messages it accepted in memory. Tests read them through `Sink.Delivered`.

The attempt count and the random source belong to one destination (one group member). They last as long as it does: a reload builds them again.

The keys go under `fault:`:

| Key | Meaning |
|---|---|
| `fail` | Share of attempts that fail, as `50%` or `0.5` (default 0). |
| `fail_first` | Fail the first N attempts, then follow `fail`. |
| `latency` | Added to every attempt. An attempt whose `timeout` runs out while it waits fails as a timeout. |
| `error` | How an attempt fails: `status` (default), `connect` or `timeout`. |
| `status` | Status of a failed attempt with `error: status` (default `503`). |
| `retry_after` | `Retry-After` hint of a failed attempt with `error: status`. |
| `seed` | Random seed, for repeatable runs (default: the clock). |

The destination's health check always passes. Its circuit breaker still opens on failures, as any destination's does.

## Errors

A failed attempt is an error with a class and tags. The class says whether hikyaku retries it by default. The tags are what `retry_on` and `never_on` match:

| `error` | Class | Tags |
|---|---|---|
| `status` | by the status, as HTTP: `429` rate limited, `408` and `504` timeout, another `4xx` permanent, another `5xx` transient | the code, plus `4xx` or `5xx` (`408` also `timeout`) |
| `connect` | transient | `connect` |
| `timeout` | timeout: the attempt waits until its `timeout` runs out | `timeout` |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries every fault except one: a `4xx` status other than `408` and `429`. That one goes straight to the fallback chain. A `retry_after` goes with every `status` failure. The retry then waits that long, capped at `max_backoff`.

## Wrapping a real destination

Use this when you want the same faults on a destination that really delivers. `fault.Wrap(sink, f)` applies the faults before each attempt reaches `sink`: first the latency, then the failure. An attempt that does not fail is delivered by the real sink. A wrapped sink that serves something of its own, such as an `sse` stream, still starts with the relay.

`hikyaku chaos --destination <name>` wraps a declared destination this way, without editing the spec. Its flags set the keys: `--fail`, `--latency`, `--error`, `--status`, `--retry-after` and `--seed`. There is no `fail_first` flag.

- With `--data`, it pushes `--count` messages (default 100) through the destination's retry and fallback path. Then it reports the attempts, where each message ended up, and the latencies. It exits 1 if a message was lost.
- Without `--data`, it runs the relay as `hikyaku run` does, with the destination wrapped.

## Example

```yaml
destinations:
  - name: flaky
    type: fault
    fault:
      fail: 50%
      latency: 200ms
      error: status
      status: 503
      retry_after: 1s
      seed: 42
    retry:
      attempts: 5
      backoff: 100ms
    fallback: [backup]

  - name: backup
    type: fault
    fault:
      fail_first: 2
```

The same faults on a real destination of an existing spec, with no edit:

```sh
hikyaku chaos -f hikyaku.yaml --destination billing --fail 30% --latency 50ms --data '{"type":"invoice.paid"}' --count 200
```
