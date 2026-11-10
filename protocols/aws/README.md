# protocols/aws

This module holds the AWS protocols for hikyaku, built on [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2). It adds `sqs`, `sns` and `eventbridge` as sources and destinations, and `s3` as a poll source and a destination. Use it when hikyaku has to read from, or write to, these AWS services.

```go
import "github.com/xavidop/hikyaku/protocols/aws"

aws.Register() // registers sqs, sns, eventbridge and s3 as sources and destinations
```

## Region and credentials: the `aws` block

The `aws` block says which region a source or destination talks to, and which credentials it uses. Every source and destination builds its own client from the SDK's default chain (the environment, the shared config and credentials files, SSO, web identity, ECS and EC2 roles), then applies its `aws` block on top. Leave the block out when the defaults are what you want.

| Key | Meaning |
|---|---|
| `region` | Region. Without it: `AWS_REGION`, the profile's region, then the region in `queue_url` or in the topic or bus ARN. With none of these, the source or destination fails to start. |
| `profile` | Shared config profile. |
| `access_key_id`, `secret_access_key` | Static credentials, set together. Use `{{secrets.X}}`. |
| `session_token` | Session token of temporary static credentials. |
| `role_arn` | Role assumed through STS on top of the base credentials. The session is named `hikyaku-<relay>-<name>`. The credentials are cached and renewed a minute before they expire. |
| `endpoint` | Custom endpoint URL, such as LocalStack's `http://localhost:4566`. It applies to every service client, STS included. Against it, S3 uses path-style addressing and checksums only where the API requires them. |

Each delivery makes one attempt. The SDK's own retryer is capped at one attempt, so the relay's retry policy, circuit breakers and fallbacks decide what happens next. Calls made on the source side (receive, delete, change visibility, list, get) keep the SDK's standard retries.

## Sources: `sqs`, `sns`, `eventbridge`

All three source types read an SQS queue. Use `sqs` to read a queue directly. Use `sns` to read the queue that is subscribed to an SNS topic: it unwraps the notification SNS writes. Use `eventbridge` to read the queue that an EventBridge rule targets: it unwraps the event.

They run in `subscribe` mode (the default): the source keeps receiving while the relay runs, instead of checking at an interval the way a poll source does. Each one long-polls `queue_url` with ReceiveMessage. A long poll waits up to `wait_time` for a message, instead of returning empty at once. The receive asks for every message attribute, and for the `ApproximateReceiveCount`, `SentTimestamp` and `MessageGroupId` system attributes.

### Acknowledgement and redelivery

This section says when the source deletes a message (acknowledges it: tells SQS it is done), and when SQS delivers it again. SQS hides a received message from other consumers for its visibility timeout. Delete the message before that timeout ends, and it is gone for good. Otherwise SQS makes it visible again and delivers it once more.

- **Final outcome** (delivered, dead-lettered, unrouted, filtered): the message is deleted. Deletes are batched: one DeleteMessageBatch per 10 settled messages, or every 100ms. A failed delete is tried up to three times. After that, SQS redelivers the message when its visibility timeout ends. Its ID lets idempotent destinations (safe to send twice) drop the duplicate.
- **`Failed`** (the DLQ failed too, or shutdown cut the delivery): ChangeMessageVisibility makes the message visible again after a backoff based on its receive count. The backoff starts at 1s and doubles per receive, with equal jitter, capped at the visibility timeout. SQS then redelivers the message. A queue redrive policy can move it to an SQS dead-letter queue after `maxReceiveCount` receives.
- **`ack: accepted`**: the message is deleted as soon as the relay holds it, before it is delivered. Use it only with `buffer.type: disk`, which keeps the message on disk until it is delivered.
- **Heartbeat**: while a message is in flight for longer than half the visibility timeout, the source extends its visibility (ChangeMessageVisibilityBatch, back to the full timeout). That way SQS does not hand it to another consumer while the relay still works on it. The visibility timeout is `visibility_timeout`, else the queue's own, read with GetQueueAttributes at start. When the credentials cannot read it, 30s is used.
- **Shutdown**: messages received but not yet handed to the relay are made visible again at once, for the next consumer. Messages in flight finish, and their deletes are flushed when the relay closes the source.
- **Resume**: there is nothing to store. SQS keeps every message that was not deleted, so a restart goes on where the last run stopped.
- **Receive failures** are retried with the `reconnect` backoff. After `reconnect.max_attempts` failures in a row, the source fails and the relay stops. The default, 0, retries forever.
- **Oversize**: a message whose body (after unwrapping) is over `max_body_bytes` is logged and deleted. Left in the queue, it would come back forever.

### Envelope

The envelope is the message as hikyaku carries it. This is how an SQS message maps to it. The `ordering_key` attribute is the one the relay's `ordering` serializes on: messages with the same key are delivered one at a time, in order.

| Field | `sqs` |
|---|---|
| `ID` | The `Idempotency-Key` message attribute, else the SQS message ID. |
| `Subject` | The queue name (the last segment of `queue_url`). |
| `Body` | The message body, base64-decoded when the `content-transfer-encoding` attribute is `base64`. |
| `ContentType` | The `content-type` attribute, else detected: `application/json`, `text/plain; charset=utf-8`, or `application/octet-stream`. |
| `Headers` | The `String` and `Number` message attributes, by name. `Binary` attributes, `content-type` and `content-transfer-encoding` are not headers. |
| `Attributes` | `sqs.message_id`, `sqs.receive_count` (an int), `sqs.sent_timestamp` (RFC 3339), and on a FIFO queue `sqs.group_id` and `ordering_key` (the message group). |

**`sns`** unwraps the notification SNS writes to the queue (`Type: Notification`):

- `Body` is `Message`, base64-decoded when its `content-transfer-encoding` attribute says `base64`.
- `ID` is the `Idempotency-Key` message attribute, else the SNS `MessageId`.
- `Subject` is `Subject`, else the topic name.
- `Headers` are the `String` and `Number` entries of `MessageAttributes`.
- `ContentType` is the `content-type` attribute, else detected.
- Attributes: `sns.topic_arn` and `sns.message_id`, besides the `sqs.*` ones.

A `SubscriptionConfirmation` or `UnsubscribeConfirmation` message is logged and deleted. A body that is not a notification, such as a subscription with raw message delivery, passes through as a plain SQS message, with its message attributes as headers.

**`eventbridge`** unwraps the event EventBridge sends to the queue:

- `Body` is `detail`, and `ContentType` is `application/json`.
- `ID` is the event `id`.
- `Subject` is `detail-type`.
- Attributes: `eventbridge.source`, `eventbridge.id`, `eventbridge.time`, `eventbridge.account`, `eventbridge.region`, and `eventbridge.resources` when the event lists any.

A body that is not an event, such as the output of an input transformer, passes through as a plain SQS message.

### Keys

The keys of an `sqs`, `sns` or `eventbridge` source:

| Key | Meaning |
|---|---|
| `queue_url` | The queue read (required). |
| `wait_time` | Long-poll wait, up to 20s (default 20s), rounded up to whole seconds. |
| `prefetch` | Messages per receive, 1 to 10 (default 10). |
| `visibility_timeout` | How long a received message stays hidden, up to 12h (default: the queue's). |
| `max_body_bytes` | Largest body (default 10MiB). |
| `ack` | `delivered` (default) or `accepted`. |
| `reconnect` | Backoff of failing receives: `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |
| `aws` | See above. |

The credentials need `sqs:ReceiveMessage`, `sqs:DeleteMessage` and `sqs:ChangeMessageVisibility`. Without `visibility_timeout`, they also need `sqs:GetQueueAttributes`. Before the source starts, its health check calls GetQueueAttributes too. Once it runs, its health is the last receive's error.

## Source: `s3`

Use an `s3` source to turn the objects dropped in a bucket into messages. It is a poll source (mode `poll`, the default): it lists the objects under `prefix` when it starts, then every `interval` (ListObjectsV2, every page). It skips "directories" (keys ending in `/`) and reads each object it has not handled yet. What it does with an object:

- **Messages**: the whole object is one message. With `split: lines`, each non-empty line is one message (a trailing `\r` is dropped). Any other `split` is a jq expression, as for http poll: each output of the expression, run over the object's JSON, is one message. An object that does not split is passed on whole, and the failure is logged.
- **`after`**, once every message of the object is final:
  - `delete` (default) deletes the object.
  - `move` copies it to `move_to` + key, then deletes it. Keys under `move_to` are not polled, and `prefix` must not be under `move_to`. Give `move_to` its trailing slash: `done/`.
  - `keep` leaves it, and records `s3:<bucket>:<key>:<etag>` in the `state_store` KV store (SetNX, no TTL). A recorded object is skipped, until a new version (a new ETag) appears.
- **At least once**: when a message of an object fails, the object is left as it is, and the next poll reads it again. Its messages keep their IDs, so idempotent destinations (safe to send twice) drop the ones they already have. Within one process, an object is not read again while its messages are in flight.
- **Oversize**: an object over `max_body_bytes` is logged and handled as `after` says, without messages.
- **Shutdown**: an object whose messages were not all handed over stays for the next run.
- **Poll failures**: a listing is retried with `retry` (default 3 attempts, 1s backoff). Then it is logged, and the source skips to the next interval. An object that cannot be read is tried again on the next poll.

Each message maps to the envelope like this:

| Field | Value |
|---|---|
| `ID` | The object's ETag, or `<etag>-<n>` with a split (the line number, or the jq output's position from 1). |
| `Subject` | The key. |
| `Body` | The object, or the line or jq output. |
| `ContentType` | The object's content type, detected when it is missing or a generic octet-stream. A line is detected on its own. A jq output is `application/json`. |
| `Headers` | The object's user metadata (`x-amz-meta-*`), without the prefix. |
| `Attributes` | `s3.bucket`, `s3.key`, `s3.etag`, `s3.size` (the object's bytes), `s3.line` (with `split: lines`) or `s3.item` (with a jq split), and `ordering_key` (the key). |

The keys of an `s3` source:

| Key | Meaning |
|---|---|
| `bucket` | The bucket polled (required). |
| `prefix` | Only objects under this prefix. |
| `interval` | Time between polls (default 30s). |
| `after` | `delete` (default), `move` or `keep`. |
| `move_to` | With `after: move`: the prefix objects are moved under. |
| `state_store` | With `after: keep`: the store that remembers the objects read (default: the relay's default store). Use a shared store, such as Redis, so a restart and the other replicas skip them too. |
| `split` | `lines`, or a jq expression such as `.records[]`. |
| `max_body_bytes` | Largest object (default 10MiB). |
| `timeout` | Bounds each list, state, delete and move call (default 10s). An object's download gets `timeout` or 5m, whichever is longer. |
| `retry` | Retries of a failed listing within one poll, with `retry_on`, `never_on` and `respect_retry_after` matched as for a destination. |
| `aws` | See above. |

Things to know:

- Two objects with the same content have the same ETag, so they get the same message IDs. Destinations that drop repeated IDs (FIFO queues, idempotent HTTP endpoints) deliver only the first. Make such objects differ, for example with a timestamp.
- An object overwritten while its messages are in flight is deleted (or moved) once they are final. The new content is lost unless it is read before that. Use `after: keep` for objects that change in place.
- With `after: keep`, every poll lists the whole prefix, and checks the store once per object not yet known to this process.
- Replicas that poll the same bucket can read the same object at the same time. Delivery stays at least once, and the IDs let destinations drop duplicates.

The credentials need `s3:ListBucket` and `s3:GetObject`, plus `s3:DeleteObject` (`delete`, `move`) and `s3:PutObject` (`move`). Before the first poll, the health check calls HeadBucket. After it, the health is the last poll's error.

## Destinations

Use these destinations to write messages to SQS, SNS, EventBridge or S3. Each delivery is one attempt. Every destination supports the relay's native batching (`batch:` on the destination): it gets the messages of a batch together, and each section below says how it writes them. A batch call that reports any failed entry fails as a whole. The relay retries all of it, and the deduplication IDs (FIFO) or the message IDs let destinations drop what was already written.

### Message attributes (`sqs`, `sns`)

This is how the body and the headers of a message travel on SQS and SNS.

The body is sent as text when SQS and SNS can carry it: valid UTF-8 without control characters (tabs and line breaks aside). Otherwise it is sent as base64, with the `content-transfer-encoding: base64` attribute, which hikyaku sources decode. An empty body is a permanent error, because neither service carries one.

Message attributes are `String` attributes, in this order, at most 10 (the service limit):

1. `content-transfer-encoding` (base64 bodies).
2. `content-type`: the message's content type, or a `content-type` entry of `headers`.
3. `Idempotency-Key`: the idempotency key the relay set on this delivery (the envelope ID, unless a source or transform set one), or an `Idempotency-Key` entry of `headers`.
4. The `headers` block, in name order (per-message templates; empty values are skipped).
5. The message's headers, in name order. The first value of each is sent. Hop-by-hop and framing headers, `Content-Type`, `Authorization` and `Cookie` do not travel.

Attributes past the tenth are dropped. So are names SQS and SNS refuse (characters other than `A-Z a-z 0-9 _ - .`, an `AWS.` or `Amazon.` prefix) and values they cannot carry. Each kind of drop is logged once per destination.

### `sqs`

An `sqs` destination sends each message to `queue_url` with SendMessage. A batch is SendMessageBatch, in calls of up to 10 messages and 256KiB.

| Key | Meaning |
|---|---|
| `queue_url` | The queue (required, not a template). A URL ending in `.fifo` is a FIFO queue. |
| `group_id` | Per-message template of the message group. FIFO: default the envelope source. A standard queue gets a group only when `group_id` is set (fair queues). |
| `delay` | Delivery delay, up to 15m, in whole seconds (rounded up). Standard queues only: a FIFO queue's delay is set on the queue. |
| `headers` | Per-message templates of message attributes. |
| `body` | Per-message template of the body (default: the message body). |
| `aws` | See above. |

A FIFO queue (first in, first out) delivers the messages of one message group in order, and drops a message whose deduplication ID it has seen in the last 5 minutes. On a FIFO queue, `MessageDeduplicationId` is the idempotency key, so SQS drops a retry or a replay within that window. A group or deduplication ID that SQS would refuse (over 128 characters, or characters outside printable ASCII) is replaced by its SHA-256. The result carries the `sqs-message-id` header (one value per message for a batch), and `sqs-sequence-number` on a FIFO queue. The health check is GetQueueAttributes on the queue.

### `sns`

An `sns` destination publishes each message to `topic`. A batch is PublishBatch, per topic, in calls of up to 10 messages and 256KiB.

| Key | Meaning |
|---|---|
| `topic` | Per-message template of the topic ARN (required). |
| `subject` | Per-message template of the subject. Line breaks and control characters become spaces, and it is cut to 100 characters. Without it, or when it renders empty, no subject is sent. |
| `group_id` | Per-message template of the message group. FIFO topics (`.fifo`): default the envelope source. A standard topic gets a group only when `group_id` is set (for SQS fair queues behind it). |
| `headers` | Per-message templates of message attributes. |
| `body` | Per-message template of the message (default: the message body). |
| `aws` | See above. |

A FIFO topic also gets the idempotency key as `MessageDeduplicationId`. The result carries `sns-message-id`, and `sns-sequence-number` on a FIFO topic. The health check is GetTopicAttributes on a static topic. A templated topic has nothing to check until a message names it.

### `eventbridge`

An `eventbridge` destination puts each message on `event_bus` as an event (PutEvents). A batch is PutEvents in calls of up to 10 events and 256KiB.

| Key | Meaning |
|---|---|
| `event_bus` | Bus name or ARN (default: the account's default bus). |
| `event_source` | The events' `source` (default `hikyaku.<relay>`). The `aws.` prefix is reserved for AWS. |
| `detail_type` | Per-message template of `detail-type` (default: the message subject, else `hikyaku.message`), cut to 128 characters. |
| `body` | Per-message template of the detail source (default: the message body). |
| `aws` | See above. |

The detail is the body when it is a JSON object. Otherwise it is `{"body": "<text>"}`, or `{"body_base64": "..."}` for a body that is not UTF-8. EventBridge events carry no headers, so `headers` is refused. When `FailedEntryCount` is not 0, the call fails with each failed entry's `ErrorCode` as tags. The result carries `eventbridge-event-id`. The health check is DescribeEventBus.

### `s3`

An `s3` destination writes each message to `bucket` as one object (PutObject).

| Key | Meaning |
|---|---|
| `bucket` | The bucket (required, not a template). |
| `key` | Per-message template of the object key (default `<relay>/<yyyy>/<mm>/<dd>/<id>.json`, by the time the message was received, in UTC). A leading `/` is dropped. A key that renders empty is a permanent error. |
| `content_type` | The objects' content type (default: the message's). |
| `headers` | Per-message templates of user metadata. |
| `body` | Per-message template of the object (default: the message body). |
| `aws` | See above. |

The object's metadata holds the idempotency key (`idempotency-key`), the `headers` block, then the message's headers, with the same exclusions as message attributes. It is cut to S3's 2KiB. Entries whose name is not a header token, or whose value is not printable ASCII, are dropped (logged once).

A batch is one object of NDJSON: one body per line, with JSON bodies compacted onto their line. Its key comes from the batch's first message (default `<relay>/<yyyy>/<mm>/<dd>/<first id>.ndjson`), and its content type is `application/x-ndjson` (or `content_type`). Its metadata is the first message's idempotency key and the `headers` block, rendered against the first message. The messages' own headers are left out. An `s3` source with `split: lines` reads such an object back message by message. The result carries `s3-key`, `s3-etag` and `s3-version-id` (on a versioned bucket). The health check is HeadBucket.

## Errors

Every failure is a `*plugin.Error`, with a class (how it failed: transient, permanent, rate limited or timeout) and tags that retry policies match. An AWS error code `<Code>` is tagged `aws:<Code>` and `<service>:<Code>` (`sqs`, `sns`, `eventbridge`, `s3`), so `retry_on` and `never_on` can name it. The more specific tag wins: `retry_on: [s3:NoSuchBucket]` retries a bucket that is being created despite `never_on: [4xx]`.

| Failure | Class | Tags |
|---|---|---|
| Throttling: `Throttling`, `ThrottlingException`, `RequestThrottled`, `SlowDown`, `TooManyRequestsException`, `KMSThrottlingException`, `KmsThrottled`, `OverLimit`, ... | rate limited | `aws:<Code>`, `<service>:<Code>`, `429` |
| Missing queue, topic, bus or bucket, refused content, bad credentials: `NonExistentQueue`, `AWS.SimpleQueueService.NonExistentQueue`, `QueueDoesNotExist`, `NotFound`, `NoSuchBucket`, `AccessDenied`, `InvalidParameterValue`, `InvalidMessageContents`, `ValidationException`, `AuthorizationError`, `ResourceNotFoundException`, `InvalidClientTokenId`, ... | permanent | `aws:<Code>`, `<service>:<Code>`, the status (`404`), `4xx` |
| AWS failing: `InternalError`, `InternalFailure`, `ServiceUnavailable`, `ExpiredToken`, ... | transient | `aws:<Code>`, `<service>:<Code>`, the status when it is a 5xx (`503`), `5xx` |
| Another code | by its HTTP status: 429 rate limited, 408 and 504 timeout, other 5xx transient, other 4xx permanent. Without a status, by who is at fault. | as above |
| A batch call with failed entries | the worst entry's class (permanent first) | every entry's `aws:<Code>` and `<service>:<Code>`, then the class tag |
| AWS cannot be reached (refused, reset, DNS) | transient | `connect` |
| The attempt's timeout ran out | timeout | `timeout` |
| A per-message template failed, or an empty SQS/SNS body | permanent | `template` (template), `4xx` (empty body) |

A transient AWS error is tagged `5xx`, not `4xx` or its status, even when its status is a 4xx, such as `ExpiredToken`. That way the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries it. A failure whose tags match neither list falls back to its class: it is retried unless it is permanent.

## Example

A spec that uses every type, plus one `sqs` destination that talks to LocalStack for local development:

```yaml
sources:
  - name: orders
    type: sqs
    queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo
    visibility_timeout: 60s
    aws: { role_arn: arn:aws:iam::123456789012:role/hikyaku }

  - name: alerts
    type: sns
    queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/alerts-subscription

  - name: billing-events
    type: eventbridge
    queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/billing-rule-target

  - name: drops
    type: s3
    bucket: partner-drops
    prefix: incoming/
    split: lines
    after: move
    move_to: processed/
    interval: 1m

destinations:
  - name: work-queue
    type: sqs
    queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/work.fifo
    group_id: "{{ .json.customer_id }}"
    headers: { tenant: "{{ .json.tenant }}" }
    batch: { max_messages: 10, max_wait: 200ms }

  - name: fanout
    type: sns
    topic: arn:aws:sns:eu-west-1:123456789012:order-events
    subject: "Order {{ .json.id }}"

  - name: bus
    type: eventbridge
    event_bus: orders
    detail_type: "Order {{ .json.status }}"

  - name: archive
    type: s3
    bucket: hikyaku-archive
    key: "orders/{{ .json.customer_id }}/{{ .id }}.json"
    batch: { max_messages: 500, max_wait: 30s }

  - name: local-queue
    type: sqs
    queue_url: http://localhost:4566/000000000000/dev
    aws: { region: us-east-1, endpoint: http://localhost:4566, access_key_id: test, secret_access_key: test }
```
