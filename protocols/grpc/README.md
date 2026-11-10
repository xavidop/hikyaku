# protocols/grpc

The gRPC protocol for hikyaku. It gives you the `grpc` source, in `listen` and `subscribe` modes, and the `grpc` destination. Use it when the systems you relay between speak gRPC.

It is built on [`google.golang.org/grpc`](https://pkg.go.dev/google.golang.org/grpc) and [`google.golang.org/protobuf`](https://pkg.go.dev/google.golang.org/protobuf). hikyaku needs no generated code for your services. It learns the shape of each method from a descriptor set or from server reflection. It then builds requests and responses as [`dynamicpb`](https://pkg.go.dev/google.golang.org/protobuf/types/dynamicpb) messages and maps them to and from JSON.

Two terms this page uses throughout:

- A **descriptor set** is a file that describes your protobuf services, messages and methods. `protoc` or `buf` writes it. hikyaku reads it instead of your `.proto` files.
- **Server reflection** is a gRPC service a server can expose to describe its own methods. With it, a client such as `grpcurl` needs no proto file.

gRPC has four kinds of method, and hikyaku handles all of them:

- **unary**: one request, one response;
- **client-streaming**: the client sends many requests, the server answers once;
- **server-streaming**: one request, the server streams many responses;
- **bidi** (bidirectional streaming): both sides stream.

```go
import "github.com/xavidop/hikyaku/protocols/grpc"

grpc.Register() // registers grpc as a source and a destination
```

## Sources

A `grpc` source brings messages into hikyaku over gRPC. It runs in one of two modes:

- `listen`: hikyaku is the gRPC server, and callers send it messages.
- `subscribe`: hikyaku is the client. It calls a server-streaming method on another server, and each response becomes a message.

A source with a `url` and no `listen` defaults to `subscribe`. Any other source defaults to `listen`.

### listen

In `listen` mode hikyaku is the gRPC server. Use it when your clients call hikyaku directly. A listen source always serves:

- **`hikyaku.relay.v1.Relay/Send`** (see [The Relay service](#the-relay-service)). One call is one message, with its own ID, subject, headers, content type and body.
- **The services of `descriptor_set`**: all of them, or the ones named in `services`. hikyaku serves them generically, whatever the kind of each method. A unary or server-streaming call is one message. In a client-streaming or bidi call, each request is one message (see [Streams](#streams)). When it serves all services, hikyaku skips the ones it serves itself (Relay, health, reflection). Naming one of those in `services` is an error.
- **`grpc.health.v1.Health`**: the standard health check. It says `SERVING` for the server and for each service while the source runs, and `NOT_SERVING` once it stops.
- **Server reflection**, v1 and v1alpha, unless `reflection: false`. It describes the Relay service and every file of the descriptor set, so `grpcurl` and other reflection clients work without a proto file.

Every call becomes one or more envelopes (an envelope is the message as hikyaku carries it):

| Field | Relay/Send | A described method |
|---|---|---|
| `ID` | `Message.id`, else the `idempotency-key` metadata, else a new ULID | the `idempotency-key` metadata (with `-<n>` appended, from 0, in a client or bidi stream), else a new ULID |
| `Subject` | `Message.subject`, else `/hikyaku.relay.v1.Relay/Send` | the full method name, such as `/acme.orders.v1.Orders/Create` |
| `ContentType` | `Message.content_type`, else `application/json` for a JSON body, `text/plain; charset=utf-8` for other UTF-8, `application/octet-stream` for the rest | `application/json` |
| `Body` | `Message.body` | the request as JSON (see [JSON mapping](#json-mapping)) |
| `Headers` | the call's metadata, then `Message.headers` | the call's metadata |
| `Attributes` | `grpc.method`, `grpc.peer` and `remote_addr` (the caller's address), `idempotency_key` when the caller chose the ID | the same, plus `grpc.message_index` in a client or bidi stream |
| `Trace` | the W3C `traceparent` metadata | the same |

A ULID is a unique ID that sorts by time. hikyaku makes one when the caller gives no ID.

gRPC metadata (the call's key-value pairs, like HTTP headers) become envelope headers. hikyaku writes their names with each word capitalized: `x-tenant` becomes `X-Tenant`. It leaves out pseudo-headers, `content-type` and gRPC's own `grpc-*` keys. A binary value (a `-bin` key) stays base64-encoded, as it is on the wire. A listen source also drops the call's credentials: `authorization`, `proxy-authorization`, `cookie`, `x-api-key`, `api-key` and the `auth.api_key` header. No destination sees them.

**Answers.** With `ack: delivered` (the default), a call waits until hikyaku knows what happened to its messages. It waits up to `respond.timeout` (default 30s). The answer depends on the outcome. Dead-lettered means the message went to the dead-letter queue (DLQ), where a message goes when every delivery attempt failed.

| Outcome | Relay/Send | A described method |
|---|---|---|
| delivered, unrouted, filtered | `OK`, `SendReply{id, status}` | `OK` with the output message: the `respond.from` destination's reply mapped into it, or an empty one without `respond` (or when that destination did not take the message) |
| dead-lettered | `OK`, `status: dead_lettered` | `OK` with the `respond.from` reply when there is one (another required destination failed), else an empty output; with `respond` and no reply (`respond.from` itself failed, and the message is in the dead-letter queue), `UNAVAILABLE` |
| failed (the relay could not keep the message; retry) | `UNAVAILABLE` | `UNAVAILABLE` |
| no outcome within the wait | `OK`, `status: pending` (the relay still has the message); `DEADLINE_EXCEEDED` with `respond` | `OK` with an empty output; `DEADLINE_EXCEEDED` with `respond` |
| the reply does not fit the output type | | `INTERNAL` |

With `ack: accepted`, a call is answered as soon as the relay holds its messages (`env.OnQueued`). Relay/Send answers `status: accepted`; a described method answers an empty output. [Streams](#streams) says how a streaming call is answered.

A call to a described method also gets response metadata:

- `hikyaku-id`: the message ID. In a client stream, the ID of the last message.
- `hikyaku-status`: `delivered`, `dead_lettered`, `unrouted`, `filtered`, `accepted` or `pending`. In a client stream, the worst one.
- The headers of the `respond.from` reply that gRPC accepts. Credentials, framing and `grpc-*` keys are left out.

A bidi call carries `hikyaku-id` and `hikyaku-status` in its trailers instead (see [Streams](#streams)).

**Backpressure.** Backpressure is what hikyaku does when its buffer is full. A listen source never makes a caller wait for room. It answers `RESOURCE_EXHAUSTED` with the trailer `retry-after: 1`, the way an http source answers 503. A grpc destination of another relay reads that trailer as its retry hint. A message larger than `max_body_bytes` is refused by gRPC itself, with `RESOURCE_EXHAUSTED`, before it reaches the relay.

**Shutdown.** When the relay stops:

- health turns `NOT_SERVING`;
- calls waiting for an outcome are answered at once: `pending`, or `UNAVAILABLE` with `respond`. The relay still delivers their messages before it exits;
- new messages get `UNAVAILABLE`;
- the server stops gracefully, and cuts calls still running after 5s.

A bidi call answers its waiting messages the same way and ends at once: `OK` if the client had closed its side, else `UNAVAILABLE`.

**Hot reload.** The grpc listen sources of one process share their ports. On a reload, the new source starts on the port the old one holds. New connections go to the new source. The port closes with its last source. Each source runs its own server, so a reload may change `tls`, `auth` and the services.

| Key | Meaning |
|---|---|
| `listen` | Address, such as `:50051`. `path` does not apply: calls are routed by method. |
| `tls` | `cert_file` + `key_file`, and `client_ca_file` for mTLS (a client without a certificate the CA signed is refused during the handshake). |
| `auth` | `bearer`, `jwt`, `api_key` or `ip_allow`. Checked on every call except health checks; a failed check answers `UNAUTHENTICATED`. The checks read the call's metadata as headers (`authorization: Bearer ...`, the API key header in lowercase), and use the client address as the peer. `hmac` is refused, because a gRPC call carries no raw body to sign. |
| `ack` | `delivered` (default) or `accepted`, as above. |
| `respond` | `from`: the destination whose reply answers a described method, mapped into its output type; `timeout`: how long a call waits (default 30s). Relay/Send always answers `{id, status}`. Needs `ack: delivered`. |
| `max_body_bytes` | Largest request message gRPC accepts (default 4MiB, gRPC's own). |
| `descriptor_set` | A `FileDescriptorSet` file. Its services are served too. Build it with `protoc --include_imports --descriptor_set_out=FILE ...` or `buf build -o FILE`. A well-known type the file lacks (`google/protobuf/*.proto`) is found among the ones linked into hikyaku. |
| `services` | Full names of the `descriptor_set` services to serve (default all). |
| `reflection` | Serve server reflection (default `true`). |

#### Streams

hikyaku serves a described method of any kind. Each message gets an answer from the table above. Whether that answer comes per call or per message depends on the kind:

- **Client-streaming:** each request is one message. The call is answered once all of them have settled (reached a final outcome). The answer is the reply to the last message. Any failed message fails the call.
- **Server-streaming:** the request is one message, and the response stream carries the answer. When the `respond.from` reply is a JSON array, each element is streamed as one response (`[]` streams none). When it is NDJSON (one JSON value per line), each line is one response. Any other answer gives the one response a unary call gets: the reply mapped into the output type, or an empty output. The stream then ends `OK`. An error status ends it instead, with no response. The response metadata are the same as for a unary call.
- **Bidi:** each request is one message. Each gets the response a unary call of the method would get, in request order, once its outcome is final (with `ack: accepted`, once the relay holds it). A message with no outcome within `respond.timeout` gets an empty response, or `DEADLINE_EXCEEDED` with `respond`. The call ends `OK` once the client has closed its side and every response is sent.

In a bidi call:

- **Backpressure:** at most 64 messages wait for their outcome at once. Past that, the call reads no more requests until the oldest is answered. HTTP/2 flow control then holds a fast client back.
- **Failures:** the first message whose answer would be an error status ends the call with that status, once the responses before it are sent. Those statuses are `UNAVAILABLE` for a failed message, `DEADLINE_EXCEEDED`, and `INTERNAL` for a reply that does not fit. A request the relay refuses (`RESOURCE_EXHAUSTED` with `retry-after: 1` on a full buffer) ends the call the same way. The client knows what to send again: the requests that got no response. Note that the messages read after the failed one may already be with the relay.
- **Metadata:** the headers go out with the first response, so they carry none of hikyaku's. The trailers carry `hikyaku-id` (the last message answered) and `hikyaku-status` (the worst status of the messages answered; from best to worst: `delivered` or `accepted`, then `filtered`, `unrouted`, `pending`, `dead_lettered`). The headers of a `respond.from` reply are not sent, because they belong to one message.

### subscribe

In `subscribe` mode hikyaku is the client. Use it to follow a server-streaming method on another server. hikyaku calls `method` on `url` with the request in `body`. Every response the server streams becomes an envelope.

hikyaku resolves the method from `descriptor_set`, or through the server's reflection (v1, or v1alpha for older servers) on every connect.

When the stream ends or breaks, hikyaku calls it again, with exponential backoff (a wait that doubles each time) and jitter (a random part, so clients do not retry in step). A failed attempt (the dial, the reflection or the call) moves on to the next URL in `url` + `failover_urls`. A stream that delivered a message, or stayed up for 30s, resets the failure count. Each reconnect is logged at warn. After `max_attempts` failed attempts in a row, the source fails, and the relay stops.

**Acknowledgements.** A gRPC stream has no acknowledgements (no way to tell the server a message is done). So a message is delivered at least once only while the stream is up. hikyaku cannot get back:

- what the server streamed while the source was disconnected, unless the server replays it on the next call;
- a message in hikyaku's memory buffer when it stops. Use `buffer.type: disk` to keep those.

A message whose outcome is `failed` is logged at warn, since the server cannot redeliver it. While the relay applies backpressure, the source stops reading, and HTTP/2 flow control holds the server back.

| Field | Value |
|---|---|
| `ID` | a new ULID |
| `Subject` | the method, such as `/acme.prices.v1.Prices/Watch` |
| `ContentType` | `application/json` |
| `Body` | the response as JSON |
| `Headers` | the stream's header metadata |
| `Attributes` | `grpc.method`; `grpc.url` (the URL, without a password); `ordering_key` (the method: one stream is one ordered unit) |

A response that is larger than `max_body_bytes` once rendered as JSON is logged at error and dropped. The stream carries on. hikyaku reads responses up to four times that size (at least 16MiB), so one oversized response does not break the stream.

| Key | Meaning |
|---|---|
| `url` | `grpc://host:port` (plaintext) or `grpcs://host:port` (TLS). The port defaults to 80 and 443. No path, query or credentials: the method goes in `method`, credentials in `headers`. |
| `failover_urls` | URLs rotated through when an attempt fails. |
| `method` | The server-streaming method, `/package.Service/Method`. |
| `body` | The request as JSON (default `{}`). |
| `headers` | Static metadata of the call (and of its reflection calls), such as `authorization: Bearer {{secrets.TOKEN}}`. A `-bin` key takes a base64 value. Templates, `grpc-*`, `content-type`, `user-agent`, `te` and connection-specific headers are refused. |
| `tls` | For `grpcs://` URLs: `ca_file`, `cert_file` + `key_file` (mTLS), `server_name`, `insecure_skip_verify`. Without it, the system roots verify the server. |
| `descriptor_set` | A `FileDescriptorSet` that describes the method. The method and `body` are checked when the source is built. |
| `reflection` | Resolve the method through server reflection when there is no `descriptor_set` (default `true`; `false` needs `descriptor_set`). |
| `reconnect` | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |
| `max_body_bytes` | Largest message relayed, as JSON (default 4MiB). |
| `ack` | `delivered` (default) or `accepted`. It only decides when a failed message is logged, since a stream has nothing to acknowledge. |

## Destination

A `grpc` destination sends each message to a gRPC server. Use it when the system you deliver to exposes a gRPC method. The destination calls `method` on `url` once per delivery. It uses one long-lived connection: dialed on first use, kept open, and closed by `Close`. The method may be unary, client-streaming, server-streaming or bidi.

Without `descriptor_set`, hikyaku resolves the method through the server's reflection on the first delivery. It keeps the result until the relay restarts or reloads. So the server may be down when the relay starts.

- **Request:** the message body, or the rendered `body` template, parsed as JSON into the method's input type. Unknown fields are ignored. A body that does not parse, or does not fit the type, is a permanent failure. An empty body is an empty request. A body whose content type is `application/protobuf` or `application/x-protobuf` is decoded as binary protobuf. A streaming call sends the request as the one message of its stream, then closes its side.
- **Metadata:** three parts, in this order. First, the message's headers that gRPC accepts: without `Authorization`, `Cookie`, framing or `grpc-*` keys, and without values that are not printable ASCII; a `-bin` header is base64-decoded. Then `idempotency-key`: the caller's idempotency key, or else the message ID. It is the same on every attempt and every fallback. Then the configured `headers`, rendered per message.
- **Result:** for a unary or client-streaming method, the response as JSON. For a server-streaming or bidi method, every response the server streams until it ends the stream, as a JSON array (`[]` for none). The array holds at most 10MiB. Responses past that are read and dropped, and a warning is logged, the way an http destination cuts a response at 10MiB. The result has `ContentType: application/json`, `Status: 0`, the response's header metadata as `Headers`, and the URL as `Endpoint`. A server can ask for another destination with the `x-hikyaku-reroute` response metadata, as with the http `X-Hikyaku-Reroute` header.
- **Batches:** with `batch:`, a client-streaming or bidi method takes a whole batch in one call: one request per message, in order. The call has one set of metadata: the keys whose values all the messages agree on, plus an `idempotency-key` made from the messages' keys, `batch-<hash>`. A batch tried again, here or on a fallback, carries the same key. A batch of one keeps its message's metadata. A message that does not fit, or whose template fails, fails the batch. A unary or server-streaming method takes one request per call. So it gets the batch combined into one message (NDJSON, or `format: json_array`), as other protocols do. Map it into the request with `body`. For example, `{"orders": {{ .bodyText }}}` with `format: json_array` fills a repeated `orders` field.
- **Health:** the server's `grpc.health.v1.Health/Check` must say `SERVING` for the method's service, or for the server as a whole when it does not know the service. A server without the health service counts as healthy once the connection is ready.

| Key | Meaning |
|---|---|
| `url` | `grpc://host:port` or `grpcs://host:port`, as for sources. It cannot be a per-message template: the connection is dialed before any message. |
| `method` | The method, `/package.Service/Method`: unary, client-, server- or bidi-streaming. |
| `body` | Per-message template of the request JSON (default: the message body). |
| `headers` | Metadata, values may be per-message templates (`x-tenant: "{{ .json.tenant }}"`). Static ones also go with the reflection and health calls. A configured `idempotency-key` replaces the message's. |
| `tls` | For `grpcs://`: `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |
| `descriptor_set` | A `FileDescriptorSet` that describes the method; checked when the destination is built. |
| `reflection` | Resolve the method through server reflection when there is no `descriptor_set` (default `true`; `false` needs `descriptor_set`). |

**Timeouts and errors.** The attempt's timeout is the destination's `timeout`. The relay's context deadline becomes the call's deadline. A streaming call must end within it: a stream the server keeps open fails with `DEADLINE_EXCEEDED`. Each delivery is one attempt, because the relay owns retries.

Every failure carries the tag `grpc:<CODE>`, where `CODE` is the gRPC status name in capitals, such as `grpc:UNAVAILABLE`. It also carries a broad tag that the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) acts on:

| Status | Class | Tags |
|---|---|---|
| `UNAVAILABLE` | transient | `grpc:UNAVAILABLE`, `5xx`, and `connect` when the call never reached the server (refused, DNS, TLS) |
| `ABORTED`, `INTERNAL`, `UNKNOWN` | transient | `grpc:<CODE>`, `5xx` |
| `RESOURCE_EXHAUSTED` | rate limited, with the server's `grpc-retry-pushback-ms` or `retry-after` metadata (trailers first) as the retry hint | `grpc:RESOURCE_EXHAUSTED`, `429` |
| `DEADLINE_EXCEEDED` | timeout | `grpc:DEADLINE_EXCEEDED`, `timeout` |
| `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `PERMISSION_DENIED`, `UNAUTHENTICATED`, `FAILED_PRECONDITION`, `UNIMPLEMENTED`, `OUT_OF_RANGE`, `DATA_LOSS`, `CANCELLED` | permanent | `grpc:<CODE>`, `4xx` |
| The message does not fit the input type | permanent | none |
| A template fails to render | permanent | `template` |
| One message of a batch does not fit, or its template fails | that message's class, for the whole batch | that message's tags |

A more specific tag wins over a broad one. So `retry_on: [grpc:NOT_FOUND]` retries a `NOT_FOUND` even with `never_on: [4xx]`, and `never_on: [grpc:INTERNAL]` stops an `INTERNAL` that `5xx` would retry. A failure to resolve the method through reflection is classified the same way. A server with no reflection answers `UNIMPLEMENTED`: set `descriptor_set` then. A status that ends a stream after some responses is classified the same way too: the delivery fails, and the responses are dropped.

## JSON mapping

This section says how hikyaku turns protobuf messages into JSON and back. It follows grpc-gateway:

- Field names are lowerCamelCase: `order_id` becomes `orderId`. On the way in, either spelling is accepted.
- On the way out, fields with no value are included.
- On the way in, unknown fields are ignored.

The proto3 JSON mapping applies: 64-bit integers are strings, `bytes` are base64, enums are names, and the well-known types have their own forms (`Timestamp` is RFC 3339). A `google.protobuf.Any` is resolved from the descriptor set (or reflection) first, then from the types linked into hikyaku.

## The Relay service

The Relay service is the simplest way to send a message to a listen source: one call, one message, no proto file of your own. `relaypb/relay.proto` defines it, and every listen source serves it:

```proto
service Relay {
  rpc Send(Message) returns (SendReply);
}
message Message { string id = 1; string subject = 2; map<string, string> headers = 3; string content_type = 4; bytes body = 5; }
message SendReply { string id = 1; string status = 2; }
```

`relaypb` holds its Go code. With reflection on, a client needs no proto file:

```sh
grpcurl -plaintext -d '{"subject": "orders", "body": "eyJpZCI6MX0="}' localhost:50051 hikyaku.relay.v1.Relay/Send
```

### Regenerating the code

The generated files are committed: `relaypb/*.pb.go`, and for the tests `testdata/echo.pb` (a descriptor set) and `internal/echopb`. Regenerate them from this directory with:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
go generate ./...
```

`go generate` runs `protoc` with the commands in `grpc.go`. Install `protoc` from the [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) or with `brew install protobuf`.

## Example

A relay that serves an orders API over gRPC, follows a price stream, and delivers to two gRPC services:

```yaml
secrets:
  INGEST_TOKEN: env:INGEST_TOKEN
  PRICES_TOKEN: env:PRICES_TOKEN

sources:
  - name: rpc
    type: grpc
    listen: ":50051"
    descriptor_set: /etc/hikyaku/orders.pb
    services: [acme.orders.v1.Orders]
    tls: { cert_file: /etc/hikyaku/tls.crt, key_file: /etc/hikyaku/tls.key }
    auth: { bearer: { tokens: ["{{secrets.INGEST_TOKEN}}"] } }
    respond: { from: orders-api, timeout: 10s }

  - name: prices
    type: grpc
    mode: subscribe
    url: grpcs://prices.example.com:443
    failover_urls: [grpcs://prices-eu.example.com:443]
    method: /acme.prices.v1.Prices/Watch
    body: '{"symbols": ["ACME"]}'
    headers: { authorization: "Bearer {{secrets.PRICES_TOKEN}}" }
    reconnect: { backoff: 500ms, max_backoff: 30s, max_attempts: 0 }

destinations:
  - name: orders-api
    type: grpc
    url: grpc://orders.internal:9090
    method: /acme.orders.v1.Orders/Create
    timeout: 5s
    retry: { attempts: 3, never_on: [4xx, grpc:INTERNAL] }

  - name: audit
    type: grpc
    url: grpc://audit.internal:9090
    method: /acme.audit.v1.Audit/Append   # client-streaming: a batch is one call
    batch: { max_messages: 500, max_wait: 1s }
```

## Tests

`go test ./...` runs everything against in-process servers on loopback ports, and through the relay's engine. It covers a unary proxy, a bidi call answered message by message, a server stream relayed from an upstream one, and batches.

`go test -tags integration ./...` adds a suite against [`moul/grpcbin`](https://github.com/moul/grpcbin) in a container (testcontainers-go). It covers: server-streaming subscriptions resolved through its reflection, which is v1alpha only; unary and streaming calls (client, server and bidi, and batches in one call); metadata; the error classes of real status codes; and a relay in front of it that serves its streaming methods from a descriptor set built from its reflection. The image is published for `linux/amd64` only and runs emulated on other hosts.
