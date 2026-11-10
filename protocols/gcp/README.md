# protocols/gcp

This module holds the Google Cloud protocols for hikyaku: the `pubsub` and `gcs` source and destination types. It is built on [`cloud.google.com/go/pubsub/v2`](https://pkg.go.dev/cloud.google.com/go/pubsub/v2) and [`cloud.google.com/go/storage`](https://pkg.go.dev/cloud.google.com/go/storage). Use it when hikyaku has to read from, or write to, Pub/Sub or Cloud Storage.

```go
import "github.com/xavidop/hikyaku/protocols/gcp"

gcp.Register() // registers pubsub and gcs as sources and destinations
```

| Type | Source | Destination |
|---|---|---|
| `pubsub` | subscribe: StreamingPull on a subscription | publish to a topic (native batches) |
| `gcs` | poll: list a bucket, read each object | one object per message (a batch: one NDJSON object) |

A subscribe source receives messages as the service pushes them, for as long as the relay runs. A poll source checks for new data every `interval`.

## Project, credentials and endpoints

Every type takes a `gcp:` block. It names the project, the credentials and, for local development, the endpoint.

| Key | Meaning |
|---|---|
| `project` | Project ID. A pubsub `subscription` or `topic` given as a full path (`projects/<project>/subscriptions/<id>`, `projects/<project>/topics/<id>`) needs no project. gcs does not use it. |
| `credentials_file` | A JSON credentials file. Its `"type"` picks the credential: `service_account`, `authorized_user`, `impersonated_service_account` or `external_account` (workload identity federation). |
| `credentials_json` | The same JSON inline; use `{{secrets.X}}`. Errors never quote it. |
| `endpoint` | A custom endpoint. On a `*.googleapis.com` host (a regional or private endpoint, such as `us-east1-pubsub.googleapis.com:443`), TLS and the credentials apply as usual. Any other host is an emulator: no credentials (setting them is an error), and pubsub dials it in plain text, as `PUBSUB_EMULATOR_HOST` does. pubsub takes `host:port`; gcs takes the JSON API base URL, and a bare `host:port` becomes `http://host:port/storage/v1/`. |

Without `credentials_file` or `credentials_json`, the clients use [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

## pubsub source

A `pubsub` source subscribes to `subscription` with StreamingPull, a stream Pub/Sub pushes messages down. The subscription is durable, so it is the resume point: whatever was not acked (acknowledged: Pub/Sub was told the message is done) is delivered again after a restart.

- **Flow control.** The source keeps at most `prefetch` messages in flight (the client's `MaxOutstandingMessages`). The client extends the ack deadline (the time Pub/Sub waits for an ack before it redelivers) of every message it holds, for up to an hour. So a slow delivery (retries, fallbacks, a model destination) does not expire.
- **Ack.** A message is acked once the relay is done with it: delivered, dead-lettered, unrouted or filtered. A `Failed` message (the DLQ failed too, or shutdown cut the delivery) is nacked (handed back), so Pub/Sub redelivers it under the subscription's retry policy and dead-letter topic. With `ack: accepted`, the source acks as soon as the relay holds the message. This is only safe with `buffer.type: disk`, which keeps the message on disk until it is delivered.
- **Ordering.** The Receive callback waits until the relay settles its message. So the messages of one ordering key reach the relay one at a time, in order (on a subscription with message ordering enabled). Each such message carries `ordering_key`, the attribute the relay's `ordering` serializes on.
- **Too large.** A message over `max_body_bytes` is logged at error level and acked, because a nack would bring it back forever.
- **Shutdown.** When the relay stops, the source stops pulling and `Start` returns. The client's graceful shutdown keeps extending the deadlines of the messages in flight, and sends each ack or nack as the relay settles it. `Close` waits for that, then closes the client. The stream stays open while it drains, so Pub/Sub may push it a few more messages (or a nacked one back) that it no longer reads. Those are redelivered once the subscription's ack deadline passes, and each counts as one delivery attempt.
- **Failures.** The client retries a broken stream itself. When it gives up (the subscription is missing, the credentials are refused), the source logs at warn and receives again after the `reconnect` backoff. A Receive that delivered a message, or ran for 30s, resets the failure count. After `max_attempts` failures in a row, the source fails and the relay stops.
- **Health** is the last Receive error, else whether the subscription exists. Credentials that may consume the subscription but not read it (`roles/pubsub.subscriber`) count as healthy.

Each message maps to the envelope (the message as hikyaku carries it) like this:

| Field | Value |
|---|---|
| `ID` | the `Idempotency-Key` attribute (as written by a hikyaku pubsub destination), else the Pub/Sub message ID |
| `Subject` | the `subject` attribute, else the subscription ID |
| `ContentType` | the `content-type` attribute, else detected: `application/json` for valid JSON, `text/plain; charset=utf-8` for other UTF-8, `application/octet-stream` for binary |
| `Body` | the message data |
| `Headers` | every attribute, except `Idempotency-Key` and the `hikyaku-*` ones |
| `Attributes` | `pubsub.message_id`, `pubsub.publish_time` (RFC 3339), `pubsub.subscription` (the full name), `pubsub.ordering_key` and `ordering_key` (when the message has one), `pubsub.delivery_attempt` (when the subscription has a dead-letter policy) |

The keys of a `pubsub` source:

| Key | Meaning |
|---|---|
| `subscription` | The subscription ID, or its full `projects/<project>/subscriptions/<id>` path. Required. |
| `gcp` | Project, credentials and endpoint (see above). |
| `prefetch` | Messages in flight at once (default 64). |
| `ack` | `delivered` (default) or `accepted`. |
| `max_body_bytes` | Largest message (default 10MiB). |
| `reconnect` | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

## pubsub destination

A `pubsub` destination publishes each message to `topic` and waits for the server's message ID, which the result carries in the `pubsub-message-id` header. It keeps one publisher per topic.

- **Attributes.** The message's headers become attributes (a header with several values is joined with `, `). Left out are the headers the http destinations drop (hop-by-hop headers, `Authorization`, `Cookie`, ...), the reserved `goog*` keys, and any header over Pub/Sub's limits (256-byte key, 1024-byte value). Then come `Idempotency-Key` (the relay's idempotency key, by default the envelope ID), `content-type` and `subject`, then the `headers:` block, which wins. A hikyaku pubsub source reads `Idempotency-Key`, `content-type` and `subject` back. So a message keeps its ID across hops, and consumers can drop the copies a retry publishes again.
- **Ordering.** With `key`, the publisher enables message ordering, and each message gets the rendered ordering key (an empty one means unordered). After a failed publish, Pub/Sub's client pauses that key. The next attempt resumes it before publishing. For strict per-key order through retries, also order the route (`ordering.key`).
- **One attempt.** The client's own publish retries are off: the relay owns retries, backoff, breakers and fallbacks. A publish whose attempt timed out may still land later.
- **Batches.** As a `BatchSink`, the destination renders every message of a batch (a template error publishes none of them), publishes them all, then waits for all of them. The batch fails with the first failure. The messages a retry publishes again carry their `Idempotency-Key`, so consumers can dedupe them (drop the copies they already have).
- **Health** checks that `topic` exists. A templated topic is known only per message, so it is not checked. Credentials that may publish but not read the topic (`roles/pubsub.publisher`) count as healthy.
- **Close** flushes and stops the publishers.

The keys of a `pubsub` destination:

| Key | Meaning |
|---|---|
| `topic` | Per-message template of the topic: an ID (needs `gcp.project`) or a full `projects/<project>/topics/<id>` path. Required. |
| `key` | Per-message template of the ordering key. |
| `headers` | Attributes set on every message; values may be per-message templates. |
| `body` | Per-message template of the data (default the message body). |
| `gcp` | Project, credentials and endpoint (see above). |

How its failures are classified and tagged:

| Failure | Class | Tags |
|---|---|---|
| `UNAVAILABLE`, `INTERNAL`, `ABORTED`, `UNKNOWN`, `DATA_LOSS` | transient | `pubsub:<CODE>`, `grpc:<CODE>`, `5xx`, plus `connect` when the connection failed |
| `RESOURCE_EXHAUSTED` | rate limited (the server's `RetryInfo` is the retry-after hint) | `pubsub:RESOURCE_EXHAUSTED`, `grpc:RESOURCE_EXHAUSTED`, `429` |
| `DEADLINE_EXCEEDED`, or the attempt's timeout | timeout | `pubsub:DEADLINE_EXCEEDED`, `grpc:DEADLINE_EXCEEDED`, `timeout` |
| `NOT_FOUND`, `PERMISSION_DENIED`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `UNAUTHENTICATED` (and `ALREADY_EXISTS`, `OUT_OF_RANGE`, `UNIMPLEMENTED`) | permanent | `pubsub:<CODE>`, `grpc:<CODE>`, `4xx` |
| `CANCELLED` (the relay canceled the attempt) | transient | `pubsub:CANCELLED`, `grpc:CANCELLED` |
| A message over Pub/Sub's 10MB limit | permanent | `pubsub:MESSAGE_TOO_LARGE`, `4xx` |
| The ordering key is paused by an earlier failure | transient | none, so the default policy retries it |
| A template failed (`topic`, `key`, `headers`, `body`) | permanent | `template` |
| The destination is closed | transient | none |

The `5xx` and `4xx` tags make the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) treat gRPC codes the way it treats HTTP statuses. A failure with neither tag falls back to its class: it is retried unless it is permanent. Name a code to change that. The more specific tag wins, so `retry_on: [pubsub:NOT_FOUND]` retries a topic that is being created despite `never_on: [4xx]`, and `never_on: [pubsub:INTERNAL]` stops an `INTERNAL` that `5xx` would retry.

## gcs source

Use a `gcs` source to turn the objects dropped in a bucket into messages. It polls `bucket`, or the objects under `prefix`: once at start, then every `interval`. It reads the objects in name order.

- **Messages.** Each object is one message. With `split: lines`, each non-empty line is one message. With any other `split`, a jq program like an http poll source's (such as `.items[]`), each output of the program run on the object's JSON is one message. When the object is not JSON or the program fails, the object is sent whole with the `gcs.split_error` attribute, so a route or the dead-letter queue deals with it instead of every poll retrying it.
- **After.** Once every message of an object is final (delivered, dead-lettered, unrouted or filtered), the object is handled as `after` says: deleted (`after: delete`, the default); copied to `move_to` + its name and then deleted (`after: move`); or recorded in `state_store` as `gcs:<bucket>:<name>:<generation>` and left in place (`after: keep`). A generation is the version number Cloud Storage gives each write of an object. Delete and move touch only the generation that was read: when the object was replaced meanwhile, the new version is read on the next poll. Objects under `move_to` are never read.
- **Failed messages.** When a message is not final, the object is left alone, and the next poll reads it again: every message of it, with the same IDs, so destinations that dedupe drop the copies. Within a process, an object whose messages are still in flight is not read again.
- **Keep.** `after: keep` remembers objects in the store, so a restart, or a replica sharing the store, does not read them twice, and a new generation of an object is read as new. A process also remembers the last 100,000 recorded objects, so it does not ask the store again on every poll. The record is keyed by bucket, so sources that share a store and a bucket share their records: give them separate stores (or key prefixes) to keep them apart. A kept object is listed on every poll, so prefer a `prefix` per day, say, over a growing bucket.
- **Too large.** An object (or a line) over `max_body_bytes` is logged at error level and counts as handled: the `after:` action applies to it, as an ack would. A line that is too large is dropped from its object.
- **Timeouts.** Each list page, object open and `after:` action gets `timeout`, and a read fails after `timeout` without progress. A read held back by the relay's backpressure (the relay stops reading until it catches up) does not time out. Cloud Storage's client retries transient failures within that time. A poll that still fails is logged, and the source skips to the next interval. A gcs source therefore takes no `retry` key (validation refuses one): the client's retries within `timeout` take its place.
- **Health** is the last poll's error (tagged like the destination's errors).

Each message maps to the envelope like this:

| Field | Value |
|---|---|
| `ID` | `<generation>-<line>`: line 0 for a whole object, else the line (or split item) number |
| `Subject` | the object name |
| `ContentType` | a whole object: its content type, else detected (as for pubsub); a line: detected; a jq split item: `application/json` |
| `Headers` | the object's custom metadata |
| `Attributes` | `gcs.bucket`, `gcs.name`, `gcs.generation`, `gcs.size`, `gcs.line` (split messages: the 1-based line or item number), `gcs.split_error` (an object the jq split could not take), `ordering_key` (the object name) |

The keys of a `gcs` source:

| Key | Meaning |
|---|---|
| `bucket` | The bucket (a `gs://` prefix is dropped). Required. |
| `prefix` | Only objects under this prefix. |
| `after` | `delete` (default), `move` or `keep`. |
| `move_to` | Prefix moved objects get, with `after: move` (required then). |
| `state_store` | Store (by name) that records the objects read with `after: keep` (default the first store). |
| `split` | `lines`, or a jq program such as `.items[]`. |
| `interval` | Time between polls (default 30s). |
| `timeout` | Per-request timeout (default 10s). |
| `max_body_bytes` | Largest message (default 10MiB). |
| `gcp` | Credentials and endpoint (see above). |

## gcs destination

A `gcs` destination writes each message as one object, with a does-not-exist precondition: the write only goes through when no object has that key yet. A retried delivery of the same key therefore never writes twice. When the object already exists (HTTP 412), the destination takes it for the earlier attempt and reports success, with the result header `gcs-existing: true`. A fresh write reports `gcs-generation`. The result's endpoint is `gs://<bucket>/<key>`.

The key must be unique per message: an existing object is taken as this message's earlier delivery. The default key, `<relay>/<yyyy>/<mm>/<dd>/<id>.json`, is unique and uses the message's receive date, which a retry keeps. A template should include `{{ .id }}`.

- **Objects.** The content type is `content_type`, else the message's. The message's headers become custom metadata, except the dropped ones (as for pubsub) and `Content-Type`; then the `headers:` block. Each write is one request with a CRC32C checksum. The client's own retries are off, because the relay owns retries.
- **Batches.** As a `BatchSink`, the destination writes a batch as one NDJSON object: one body per line, with JSON bodies compacted and other bodies as they are (a body with a newline spans several lines). The content type is `content_type`, else `application/x-ndjson`. The default key ends in `<first message's id>-<hash of the batch's ids>.ndjson`, so a retry of the batch writes the same key, and another batch never collides with it. A `key` template, and the `headers:` block (the only metadata of a batch), render against the first message.
- **Health** checks that the bucket exists. Credentials that may write objects but not read the bucket's metadata (403) count as healthy.

The keys of a `gcs` destination:

| Key | Meaning |
|---|---|
| `bucket` | The bucket written to. Required. |
| `key` | Per-message template of the object name (default `<relay>/<yyyy>/<mm>/<dd>/<id>.json`, `.ndjson` for a batch). |
| `content_type` | The objects' content type (default the message's; a batch's `application/x-ndjson`). |
| `headers` | Metadata set on every object; values may be per-message templates. |
| `body` | Per-message template of the object's content (default the message body). |
| `gcp` | Credentials and endpoint (see above). |

How its failures are classified and tagged:

| Failure | Class | Tags |
|---|---|---|
| HTTP 429 | rate limited (with `Retry-After`) | `429`, `4xx`, `gcs:429` |
| HTTP 408, 504 | timeout | `408` + `4xx` or `504` + `5xx`, `gcs:<code>`, `timeout` |
| Other 5xx | transient | `<code>`, `5xx`, `gcs:<code>` |
| Other 4xx (403, a missing bucket's 404, ...) | permanent | `<code>`, `4xx`, `gcs:<code>` |
| The connection failed (refused, reset, DNS) | transient | `connect` |
| The attempt's timeout ran out | timeout | `timeout` |
| A template failed, or `key` rendered empty | permanent | `template` |
| The destination is closed | transient | none |

## Example

A spec that uses every type: orders from a subscription, partner drops from a bucket, events published per region, and an archive written in batches.

```yaml
secrets:
  GCS_READER: env:GCS_READER

sources:
  - name: orders
    type: pubsub
    subscription: orders-relay
    gcp: { project: shop-prod }
    prefetch: 128
    reconnect: { backoff: 1s, max_backoff: 30s, max_attempts: 0 }

  - name: drops
    type: gcs
    bucket: partner-drops
    prefix: incoming/
    split: lines
    after: move
    move_to: processed/
    interval: 1m
    gcp: { credentials_json: "{{secrets.GCS_READER}}" }

destinations:
  - name: events
    type: pubsub
    topic: projects/shop-prod/topics/events-{{ .json.region }}
    key: "{{ .json.customer_id }}"
    headers: { source: hikyaku }
    retry: { attempts: 5, retry_on: [connect, timeout, 5xx, 429] }

  - name: archive
    type: gcs
    bucket: shop-archive
    key: "orders/{{ .json.customer_id }}/{{ .id }}.json"
    content_type: application/json
    batch: { max_messages: 500, max_wait: 5s }
```

For local development, point `gcp.endpoint` at the emulators: `localhost:8085` for `gcloud beta emulators pubsub start`, and `http://localhost:4443/storage/v1/` for `fsouza/fake-gcs-server -scheme http`.

## Tests

The unit tests run on in-process fakes: `pstest` for Pub/Sub and `fake-gcs-server`'s `fakestorage` for Cloud Storage. The integration tests (`-tags integration`) run against containers through testcontainers: the Pub/Sub emulator (`gcr.io/google.com/cloudsdktool/google-cloud-cli:592.0.0-emulators`) and `fsouza/fake-gcs-server:1.56.1`.

```sh
GOWORK=off go test -race ./...
GOWORK=off go test -race -tags integration ./...
```
