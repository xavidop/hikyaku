# protocols/azure

This module holds the Azure messaging protocols for hikyaku: the `servicebus` and `eventhubs` source and destination types. It is built on the Azure SDK for Go ([`azservicebus`](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus), [`azeventhubs/v2`](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2) and [`azidentity`](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity)). Use it when hikyaku has to read from, or write to, Service Bus or Event Hubs.

```go
import "github.com/xavidop/hikyaku/protocols/azure"

azure.Register() // registers servicebus and eventhubs as sources and destinations
```

Both are subscribe sources: they receive messages as the service delivers them, for as long as the relay runs. Every message becomes one envelope (the message as hikyaku carries it).

## Connecting

Every entry has an `azure:` block, which says how to reach the namespace and how to sign in. It may also have a `tls:` block, for a custom CA or a client certificate.

| Key | Meaning |
|---|---|
| `azure.connection_string` | `Endpoint=sb://<namespace>/;SharedAccessKeyName=...;SharedAccessKey=...`, from `{{secrets.X}}`. An `EntityPath` names the entity (for eventhubs, the hub). The emulators' `UseDevelopmentEmulator=true` works too. |
| `azure.namespace` | Instead of a connection string: the fully qualified namespace, such as `<name>.servicebus.windows.net`, with Microsoft Entra credentials. |
| `azure.tenant_id`, `azure.client_id`, `azure.client_secret` | Client secret credentials, when `client_id` and `client_secret` are set (all three are then required). Otherwise the namespace uses `DefaultAzureCredential` (environment, workload identity, managed identity, Azure CLI, ...), in `tenant_id` when it is set. |

The connection string is never logged, and never appears in errors, attributes or `/debug`: logs and endpoints name the namespace and the entity only.

## Service Bus

### Source

A `servicebus` source receives from a `queue`, or from a `topic` subscription (`topic` + `subscription`), in PeekLock mode. In PeekLock mode, Service Bus locks a received message for this receiver instead of removing it. The receiver then settles the message: it completes it (removes it) or abandons it (unlocks it for another receive).

| Key | Meaning |
|---|---|
| `queue` | The queue received from. |
| `topic`, `subscription` | Instead of `queue`: the topic subscription received from. |
| `prefetch` | Messages in flight at most: received and not yet settled (default 64, at most 5000). Each receive asks for as many messages as there are free slots. |
| `max_body_bytes` | Largest message (default 10MiB). A larger one is logged and completed: it would otherwise come back forever. |
| `ack` | `delivered` (default): settle when the relay is done with the message. `accepted`: complete as soon as the relay holds it (only safe with `buffer.type: disk`). |
| `reconnect` | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal`, `none`), `max_attempts` (0 = forever): failed receives in a row before the source gives up and the relay stops. The SDK also retries each call three times within the same backoff. |

**Settlement.** A message is completed (`CompleteMessage`) once its outcome is final: delivered, dead-lettered by the relay, unrouted or filtered. A message that failed (the relay's DLQ failed too, or the relay shut down before it finished) is abandoned (`AbandonMessage`). Service Bus then redelivers it at once, counts the delivery, and moves it to the entity's own dead-letter queue after `MaxDeliveryCount`. Settlement never blocks the relay, and it outlives the source's context, so messages in flight are still settled at shutdown. Messages received but not yet handed to the relay when the source stops are abandoned at once.

**Locks.** A message lock expires unless it is renewed. Every second, the source looks at its messages in flight and renews (`RenewMessageLock`) the lock of each one that has used half of what was left of its lock. A lock that is lost anyway (`locklost`) is logged. Service Bus redelivers the message, and its completion fails (logged at warn).

**Sessions.** A session groups the messages that share a session ID; one receiver handles a session at a time, in order. When the entity requires sessions, the first receive says so, and the source switches to sessions. 8 workers each accept the next session with messages (`AcceptNextSessionForQueue` / `ForSubscription`), receive from it until it has had no message for a second, and release it once its messages are settled. The session lock is renewed halfway through, instead of the message locks. Messages of one session reach the relay in order. `ordering_key` is the session ID, so `ordering: {}` on the source keeps each session in order through delivery.

**Close and health.** `Close` waits for the messages in flight to be settled (until its context ends), then closes the receivers and the connection. `Health` is the last receive's failure, else a peek of one message (without sessions), which checks the connection, the credentials and the entity.

How a message maps to the envelope:

| Envelope field | Value |
|---|---|
| `ID` | The `Idempotency-Key` application property when present (up to 256 characters; also the `idempotency_key` attribute), else `MessageId`, else a new ULID |
| `Subject` | The message's `Subject` (label), else the queue or topic name |
| `Headers` | The application properties, each value as text, under the property names as sent |
| `ContentType` | The message's `ContentType`, else `application/json` for a JSON body, `text/plain; charset=utf-8` for UTF-8 text, `application/octet-stream` otherwise |
| `Body` | The data section (several are joined; an `amqp-value` body is its text or JSON) |

| Attribute | Value |
|---|---|
| `servicebus.message_id` | `MessageId` |
| `servicebus.sequence_number` | The sequence number (int64) |
| `servicebus.delivery_count` | Deliveries so far, this one included (int64) |
| `servicebus.enqueued_time` | When it was enqueued (RFC 3339, UTC) |
| `servicebus.session_id` | The session, when the entity has sessions |
| `servicebus.correlation_id` | `CorrelationId`, when set |
| `servicebus.entity` | The queue, or `<topic>/subscriptions/<subscription>` |
| `ordering_key` | The session ID, else the entity |

### Destination

A `servicebus` destination keeps one sender for its `queue` or `topic`.

| Key | Meaning |
|---|---|
| `queue` or `topic` | The entity sent to. It cannot be a per-message template: a sender is opened per entity. |
| `group_id` | Per-message template of the `SessionId` (at most 128 characters; required by an entity with sessions). |
| `headers` | Application properties set on every message; values may be per-message templates. |
| `body` | Per-message template of the body (default: the message body). |

Each message carries the envelope ID as `MessageId`. An entity with duplicate detection therefore drops what a retry or a fallback sends again (an ID longer than 128 characters is replaced by its SHA-256). `Subject` is the envelope's subject, and `ContentType` its content type. The application properties are the envelope's headers (several values joined with `, `; `Authorization`, `Cookie`, hop-by-hop and framing headers and `Content-Type` are left out), then `headers`, then `Idempotency-Key` when no header carries it already.

`Deliver` sends one message. A destination with `batch:` sends a batch with `SendMessageBatch`, starting a new batch whenever one is full, one session's messages at a time and in order. The batch succeeds or fails as a whole, so a retry may send a message twice: enable duplicate detection on the entity to drop those. `Health` opens the sender's link (connecting and authorizing) when it is not open. `Close` closes the sender and the connection.

## Event Hubs

### Source

An `eventhubs` source reads every partition of an event hub in a consumer group, with a partition client per partition it owns. A partition is one of the ordered streams an event hub is split into. A consumer group is a named view of the hub, with reading positions of its own.

| Key | Meaning |
|---|---|
| `topic` | The event hub (or the connection string's `EntityPath`). |
| `group` | The consumer group (default `$Default`). Give the relay a consumer group of its own: it opens partitions with an owner level, which disconnects other readers of the group. |
| `start` | Where a partition without a checkpoint starts: `latest` (default) or `earliest`. |
| `state_store` | The store (by name) that keeps the checkpoints and the leases (default: the first store, else the in-process memory store). Use a `redis` store: with the memory store, a restart reads from `start:` again, and replicas cannot share the partitions. |
| `max_body_bytes` | Largest event (default 10MiB). A larger one is logged and skipped: the checkpoint passes it. |
| `ack` | `delivered` (default) or `accepted` (an event counts as final once the relay holds it). |
| `reconnect` | As for servicebus: failures in a row before the source gives up. A partition that fails to receive is opened again after the last event received. |

**Checkpoints.** A checkpoint is the position in a partition up to which every event is done, so a restart knows where to resume. A partition's checkpoint is the sequence number up to which every event was settled final (delivered, dead-lettered, unrouted or filtered): a contiguous watermark over the events, in the order they were read. It is written every second, and exactly on `Close`. An event that failed (the relay's DLQ failed too, or the relay stopped first) holds the checkpoint below it for the rest of the run. Reading goes on, and after a restart the partition is read again from that event (at least once), so later events are delivered again too. That event is logged at warn.

A new owner of a partition starts after its checkpoint. Without one, it starts at the earliest event (`start: earliest`), or after the partition's last event (`start: latest`). That start is written at once, so a restart before the first checkpoint resumes there instead of skipping to the newest event. A checkpoint past the partition's last event (the hub was recreated) is ignored, with a warning.

The checkpoints live under `eventhubs:<namespace>:<hub>:<group>:<partition>`. A hikyaku store can set a key only once (`SetNX`) or increment a counter (`Incr`), so a checkpoint is a register built from those. Each owner of a partition takes a new generation (`...:gen`, which is also its owner level), writes where it starts (`...:g<N>:base`), and adds to it with counters, one per bit (`...:g<N>:b<k>`). A reader takes the newest generation that has a start, so a value only grows, and never runs ahead of what was settled:

- An `Incr` is not idempotent (it is not safe to run twice), and a store may run one twice (the Redis store retries a command whose reply was lost). So each write adds the binary digits of half the distance to the watermark: even an `Incr` that ran twice leaves the checkpoint at or below the watermark, and `Incr`'s answer says it ran twice. Anything stranger, or an error (whose outcome is never guessed), starts a new generation at the watermark, written with `SetNX`, which is idempotent.
- The distance halves every second. So while the relay runs, the checkpoint trails the watermark by about a second of events (a crash reads those again). `Close` writes the watermark exactly, unless another owner took the partition meanwhile.
- A generation's keys expire after 30 days, and a long-lived owner starts a new one every 7 days. So a relay stopped for more than about 23 days restarts from `start:`.

**Replicas.** Replicas of a relay share the partitions of a consumer group through leases in the same store. A lease is a short-lived claim on a partition: the replica that holds it reads the partition. Time is cut into 10s epochs. A replica owns a partition for an epoch when it wrote that epoch's lease first. The owner renews its partitions for the next epoch after a third of the current one; the others take the leases still free after two thirds. Each replica takes at most its share: the partitions divided by the replicas alive (each registers in every epoch). So the partitions spread out, and the partitions of a replica that stopped move to the others within two epochs (about 20s). A replica restarted with the same store waits for its previous leases to run out, about 20s at most. The leases decide who reads, and Event Hubs enforces it: each new owner opens the partition with an owner level higher than every reader before it, which disconnects an old reader still reading (it stops, and does not take the partition back until a later epoch). When the store is down, a replica keeps the partitions it reads.

**Close and health.** When the source stops, its partition readers stop receiving. The checkpoints of the events still in flight are written as they settle, and on `Close`, which then closes the connection. `Health` is what fails now (the leases, a partition reader), else the event hub's properties.

How an event maps to the envelope:

| Envelope field | Value |
|---|---|
| `ID` | `MessageId`, else the `Idempotency-Key` property (also the `idempotency_key` attribute), else `<partition>-<sequence number>` |
| `Subject` | The event hub |
| `Headers` | The event's properties, each value as text |
| `ContentType` | The event's `ContentType`, else detected as for servicebus |
| `Body` | The data section (as for servicebus) |

| Attribute | Value |
|---|---|
| `eventhubs.partition` | The partition ID |
| `eventhubs.sequence_number` | The sequence number (int64) |
| `eventhubs.offset` | The offset |
| `eventhubs.enqueued_time` | When it was enqueued (RFC 3339, UTC) |
| `eventhubs.partition_key` | The partition key, when the producer set one |
| `eventhubs.correlation_id` | `CorrelationId`, when set |
| `ordering_key` | The partition ID |

### Destination

An `eventhubs` destination keeps one producer for its event hub.

| Key | Meaning |
|---|---|
| `topic` | The event hub (or the connection string's `EntityPath`). Not a per-message template. |
| `key` | Per-message template of the partition key: events with one key go to one partition, in order. Without it (or when it renders empty), Event Hubs picks the partition. |
| `headers` | Properties set on every event; values may be per-message templates. |
| `body` | Per-message template of the body (default: the message body). |

Each event carries the envelope ID as `MessageId`, its content type, and properties built as for servicebus (`Idempotency-Key` included). `Deliver` sends a batch of one. A destination with `batch:` sends `EventDataBatch`es, one partition key at a time and in order, starting a new batch whenever one is full. Event Hubs does not deduplicate, so a retried batch may write events twice. Consumers dedupe (drop the copies) on `MessageId`; hikyaku's source takes it as the envelope ID.

## Errors

Each delivery is one attempt (the SDK retries once inside it, to recover a link that went stale); the relay owns retries. Errors carry these classes and tags for `retry_on` and `never_on` (`<proto>` is `servicebus` or `eventhubs`):

| Failure | Class | Tags |
|---|---|---|
| The attempt's timeout ran out | timeout | `timeout` |
| The connection was lost (`connlost`), the sender is closed (`closed`), the link was detached | transient | `<proto>:connlost`, `<proto>:closed`, `<proto>:detach-forced`, ..., plus `connect` |
| The service timed out | timeout | `<proto>:timeout`, `timeout` |
| Throttled (`server-busy`) or the entity is full (`resource-limit-exceeded`) | rate limited, `Retry-After` 10s | `<proto>:server-busy` or `<proto>:resource-limit-exceeded`, plus `429` |
| The entity does not exist | permanent | `<proto>:notfound` or `<proto>:not-found` |
| The credentials are refused | permanent | `<proto>:unauthorized`, `<proto>:unauthorized-access`, or `<proto>:credentials` (a Microsoft Entra failure) |
| The message is too large | permanent | `<proto>:message_too_large`, `<proto>:message-size-exceeded` |
| Not allowed, entity disabled, a bad argument | permanent | `<proto>:not-allowed`, `<proto>:entity-disabled`, `<proto>:argument-error`, ... |
| Another broker error | transient | `<proto>:<code or condition>`, plus `5xx` |
| A network error | transient | `connect` |
| A `group_id`, `key`, `headers` or `body` template failed | permanent | `template` |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) therefore retries the transient and throttled failures, and sends the permanent ones to the fallback or the DLQ.

## Tests

`go test ./...` runs the unit tests on fakes of the SDK clients. The integration suites run the official emulators with testcontainers-go (`modules/azure`):

- Service Bus: `mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.1` with `mcr.microsoft.com/azure-sql-edge:latest` (`testdata/servicebus.json`).
- Event Hubs: `mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.2.1` with Azurite `mcr.microsoft.com/azure-storage/azurite:3.36.0` (`testdata/eventhubs.json`).

The images are several GB and take minutes to start, so the suites need `HIKYAKU_AZURE_EMULATORS=1` as well as the `integration` tag (they skip otherwise):

```sh
cd protocols/azure
HIKYAKU_AZURE_EMULATORS=1 GOWORK=off go test -race -tags integration -run Emulator -timeout 30m ./...
```

## Example

A spec that uses every type. The `telemetry` source keeps its checkpoints and leases in the shared Redis store, so a restart resumes and replicas share the partitions.

```yaml
secrets:
  SB_CONN: env:SERVICEBUS_CONNECTION_STRING
  TENANT: env:AZURE_TENANT_ID
  CLIENT_ID: env:AZURE_CLIENT_ID
  CLIENT_SECRET: env:AZURE_CLIENT_SECRET

stores:
  - name: shared
    type: redis
    addr: redis:6379

sources:
  - name: orders
    type: servicebus
    queue: orders
    prefetch: 32
    azure: { connection_string: "{{secrets.SB_CONN}}" }
    ordering: {}            # keeps each session in order

  - name: telemetry
    type: eventhubs
    topic: telemetry
    group: hikyaku
    start: earliest
    state_store: shared
    azure:
      namespace: telemetry-ns.servicebus.windows.net   # DefaultAzureCredential

destinations:
  - name: audit
    type: servicebus
    topic: audit
    group_id: "{{ .json.customer_id }}"
    headers: { X-Source: "{{ .source }}" }
    azure: { connection_string: "{{secrets.SB_CONN}}" }

  - name: archive
    type: eventhubs
    topic: archive
    key: "{{ .json.device_id }}"
    batch: { max_messages: 200, max_wait: 500ms }
    azure:
      namespace: telemetry-ns.servicebus.windows.net
      tenant_id: "{{secrets.TENANT}}"
      client_id: "{{secrets.CLIENT_ID}}"
      client_secret: "{{secrets.CLIENT_SECRET}}"
```
