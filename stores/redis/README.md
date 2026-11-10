# stores/redis

`github.com/xavidop/hikyaku/stores/redis` is the Redis implementation of hikyaku's `store.Store`. It holds the routing history, the decision cache, source `dedupe` keys, the retry-budget and AI call-budget counters, and the read state of sources that keep one in a store (`state_store`), so the replicas of a relay share one state. It uses go-redis v9 and works with a standalone Redis (or Valkey), a Sentinel-managed one, or a Redis Cluster.

```go
s, err := redis.New(ctx, spec, relayName, logger) // spec is a config.Store
```

- **Bounded:** every operation runs within `timeout` (default 50ms), because reads must never stall routing. That includes pool waits, handshakes and retries.
- **Breaker:** after too many failures (`circuit_breaker`, on by default), every operation returns `store.ErrUnavailable` at once, without touching the network, until `open_for` has passed and the trial calls succeed.
- **Startup:** `New` does not fail when Redis is down. It logs a warning, and the relay routes without history until Redis answers.

## Key layout

`{prefix}` is `key_prefix` (default `hikyaku`), and `{relay}` is the relay's name. The braces around `<key>` and `<msg_id>` are literal: they are Redis Cluster hash tags.

| Key | Type | Holds |
| --- | --- | --- |
| `{prefix}:{relay}:{route}:hist:{<key>}` | list | The route's records for a history key, as JSON, newest first. Trimmed to the route's `keep`, with the history TTL. |
| `{prefix}:{relay}:msg:{<msg_id>}` | hash | `route` and `key` of a message's record, so feedback can find it. It has the same TTL, and it is deleted when its record is trimmed or forgotten. |
| `{prefix}:{relay}:kv:<k>` | string | The KV surface: decision cache, dedupe keys, counters, source state. |

An append runs `LPUSH`, `LTRIM`, `PEXPIRE` and `HSET` in one pipeline. The pipeline is `MULTI`/`EXEC` except in cluster mode, where the list and the index live in different slots. A feedback is one Lua script on the list. The script replaces only the record's `feedback` member and keeps the rest byte for byte.

## Configuration

From the design's example spec:

```yaml
secrets:
  REDIS_PASSWORD: env:REDIS_PASSWORD

stores:                                     # shared state backends, referenced by name
  - name: redis-main
    type: redis                             # redis | memory (dev/tests)
    addr: redis:6379                        # or sentinel: {...} / cluster: { addrs: [...] }
    username: hikyaku
    password: "{{secrets.REDIS_PASSWORD}}"
    tls: { enabled: true }
    db: 0
    key_prefix: hikyaku
    timeout: 50ms                           # reads must never stall routing
    retry: { attempts: 2, backoff: 10ms }
```

Sentinel and Cluster take the place of `addr` (declare `SENTINEL_PASSWORD` under `secrets:` too):

```yaml
    sentinel: { master_name: mymaster, addrs: ["sentinel-1:26379", "sentinel-2:26379"], password: "{{secrets.SENTINEL_PASSWORD}}" }
    cluster: { addrs: ["redis-1:6379", "redis-2:6379", "redis-3:6379"] }   # db is ignored
```

`retry.attempts` and `retry.backoff` become go-redis retries. `circuit_breaker` takes `failure_ratio`, `min_requests`, `window`, `open_for` and `half_open_max`.

## Tests

```sh
GOWORK=off go test -race ./...                 # miniredis, in process
GOWORK=off go test -tags integration ./...     # Redis 7 and Valkey 8 containers (needs Docker)
```
