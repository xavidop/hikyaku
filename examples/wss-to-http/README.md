# wss-to-http

This example subscribes to a WebSocket event stream, keeps the events that matter, reshapes them with jq, and forwards them to an HTTP API. It shows four things:

- **Resilient subscription.** When the stream drops, hikyaku reconnects with jittered backoff (a growing, randomized wait between tries). When the primary URL is down, it rotates to a failover URL. It keeps trying forever (`max_attempts: 0`).
- **Filtering and reshaping.** A CEL filter drops heartbeats, and a jq transform reshapes each event.
- **Protected delivery.** Retries and a circuit breaker (after enough failures, hikyaku stops calling the API for a while) guard the API. When it stays down, the events fall back to a file spool.
- **Archive.** Every event is archived as it arrived.

Two specs ship with the example:

- `hikyaku.yaml`: subscribes to `wss://stream.example.com/events`, with `STREAM_TOKEN` as the bearer token.
- `hikyaku.llmfake.yaml`: the same pipeline, offline. hikyaku serves the WebSocket itself (a `ws` listen source on `:8081`) and posts to an HTTP endpoint of its own (`:8082`), which writes what it receives to `out/api.ndjson`.

## Run it offline

Start the relay with the offline spec:

```sh
hikyaku run -f hikyaku.llmfake.yaml
```

Push two events into the WebSocket. Node 22 has a WebSocket client built in:

```sh
node -e 'const w=new WebSocket("ws://localhost:8081/events");w.onopen=()=>{w.send(JSON.stringify({id:"e1",type:"order.created",ts:1,payload:{total:42}}));w.send(JSON.stringify({type:"heartbeat"}));setTimeout(()=>w.close(),200)}'
cat out/api.ndjson       # {"at":1,"data":{"total":42},"id":"e1","type":"order.created"}
cat out/archive.ndjson   # both events, as they arrived
```

The heartbeat never reaches the API, because the route's filter drops it. The archive keeps it.

Now stop the receiving end. The API's retries and breaker kick in, and the events land in `out/spool.ndjson` until it is back. Or let hikyaku fail the API for you: this chaos test makes every attempt on `api` fail and pushes 5 events through it, so they take the fallback path:

```sh
hikyaku chaos -f hikyaku.llmfake.yaml --destination api --fail 100% --data '{"id":"e2","type":"order.created"}' --count 5
```
