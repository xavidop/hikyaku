# protocols/mqtt

This module adds the `mqtt` type to hikyaku, as a source and as a destination. Use it to read messages from an MQTT broker, or to publish messages to one. It speaks MQTT 5 and MQTT 3.1.1.

`protocol_version` picks the client library:

- `"5"` (the default) uses [`github.com/eclipse/paho.golang`](https://github.com/eclipse/paho.golang).
- `"3.1.1"` uses [`github.com/eclipse/paho.mqtt.golang`](https://github.com/eclipse/paho.mqtt.golang).

Both libraries sit behind one small client interface. So the source and the destination behave the same over either version.

```go
import "github.com/xavidop/hikyaku/protocols/mqtt"

mqtt.Register() // registers mqtt as a source and a destination
```

## Connecting

Every `mqtt` source and destination connects to one broker, named by `url`. This section covers the URL, TLS, the credentials, the client identifier and the keepalive. It applies to sources and destinations alike.

| URL scheme | Transport | Default port |
|---|---|---|
| `mqtt://`, `tcp://` | TCP | 1883 |
| `mqtts://`, `ssl://`, `tls://` | TLS | 8883 |
| `ws://`, `wss://` | WebSocket (subprotocol `mqtt`); include the broker's path, such as `ws://broker:8080/mqtt` | 80, 443 |

A `tls` block configures TLS. It takes `ca_file`, `cert_file` + `key_file` (for mutual TLS, where the client shows a certificate too), `server_name` and `insecure_skip_verify`. A `tls` block on an `mqtt://` or `ws://` URL switches that connection to TLS (port 8883 for TCP).

The connection authenticates with `username` and `password`. Use `{{secrets.X}}` for the password. A URL may carry them instead (`mqtt://user:pass@broker`). When both are set, the fields win. Passwords never appear in logs, errors or attributes: URLs are shown with the password replaced by `xxxxx`.

The client identifier is `client_id`. The default is `hikyaku-<relay>-<name>`, where `<name>` is the source or destination name. A broker gives a client identifier's session to its latest connection. So two connections with one identifier keep taking the session from each other. Two cases need care:

- Replicas of one relay share the default `client_id`. Give each replica its own, such as `client_id: "orders-{{env.HOSTNAME}}"`, and share the load with `group`.
- A source and a destination with the same name share the default `client_id`. Set one of them explicitly.

Every connection sends a keepalive (a ping that tells the broker the client is still there) every 30s.

## Source

A source subscribes to one or more topic filters on the broker at `url`. It turns every PUBLISH (a published message) into one envelope. `mode: subscribe` is the only mode.

Some MQTT terms the keys use:

- **QoS** (quality of service) is a message's delivery level: `0` at most once, `1` at least once, `2` exactly once.
- **A persistent session** is what the broker keeps for a client between connections: its subscriptions, and the QoS 1 and 2 messages that arrived while it was away.
- **A shared subscription** (MQTT 5) lets several clients subscribe as one group. The broker hands each message to one member of the group.
- **Prefetch** is how many messages the broker may send ahead, before the relay has acknowledged them.

| Key | Meaning |
|---|---|
| `url` | The broker, such as `mqtt://broker:1883` (see Connecting). |
| `failover_urls` | Brokers tried in turn when a connect fails. |
| `protocol_version` | `"5"` (default) or `"3.1.1"`. |
| `topic` / `topics` | The topic filter, or several filters. `+` takes one whole level, `#` the whole rest of the topic. |
| `group` | MQTT 5 only: a shared subscription. Each filter is subscribed as `$share/<group>/<filter>`, and the broker hands each message to one member of the group. |
| `qos` | Subscription QoS: `0`, `1` (default) or `2`. The broker delivers each message at the lower of this and its publish QoS. |
| `clean_session` | `false` (default) keeps a persistent session; `true` starts clean each time (see Sessions). |
| `client_id` | See Connecting. A persistent session belongs to the client identifier. |
| `username`, `password` | See Connecting. |
| `tls` | See Connecting. |
| `ack` | `delivered` (default): acknowledge once the relay finished the message. `accepted`: acknowledge once the relay holds it (safe with `buffer.type: disk`). |
| `max_body_bytes` | Largest payload handed to the relay (default 1MiB). A larger message is logged and acknowledged, so the broker does not redeliver it forever. |
| `prefetch` | MQTT 5 only: the receive maximum. That is how many QoS 1 and 2 messages the broker may have waiting for acknowledgement (default 64, at most 65535). With MQTT 3.1.1 the broker's own limit applies (mosquitto's `max_inflight_messages`). |
| `reconnect` | `backoff` (500ms), `max_backoff` (30s), `jitter` (`full`, `equal` or `none`; default `full`), `max_attempts` (0 = forever). |

### Envelope

Each message becomes one envelope (the message as hikyaku carries it). Its fields come from the packet:

| Field | Value |
|---|---|
| `ID` | The MQTT 5 user property `Idempotency-Key` (a destination of this package sets it), else the correlation data when it is text, else a new ULID |
| `Subject` | The topic |
| `Headers` | MQTT 5 user properties, spelled like HTTP headers (`x-tenant` becomes `X-Tenant`); a repeated property keeps every value. None over 3.1.1 |
| `ContentType` | The MQTT 5 content type; else `application/json` for a JSON payload, `text/plain; charset=utf-8` for other UTF-8 text, `application/octet-stream` otherwise |
| `Attributes` | `mqtt.topic`, `mqtt.qos` (0-2), `mqtt.retained`, `mqtt.duplicate` (a redelivery), `mqtt.packet_id` (0 for QoS 0), and `ordering_key` (the topic) |

A retained message is the last message the broker keeps on a topic, for clients that subscribe later. Retained messages arrive when the source subscribes, with `mqtt.retained: true`. Over MQTT 5 the source subscribes with retain handling 1 (send retained messages only to a new subscription). So a resumed session does not get the retained messages again.

### Acknowledgements

To acknowledge a message is to tell the broker the message is done. This section says when the source does that, and what happens when a message fails.

Acknowledgements are manual. The source acknowledges a QoS 1 message (PUBACK), or receives a QoS 2 message (PUBREC), only when the relay's outcome is final: delivered, dead-lettered, unrouted or filtered. The broker holds the message until then.

MQTT requires acknowledgements in the order the messages arrived (MQTT 3.1.1 and 5, §4.6). The relay finishes messages in any order. So each connection keeps an ordered queue: a message is acknowledged once it and every message received before it are settled. A slow message holds back the acknowledgements behind it. The broker stops sending once `prefetch` of them are waiting.

MQTT has no negative acknowledgement (no way to hand a message back to the broker). When a message fails (the relay could not even dead-letter it), it and every message after it stay unacknowledged. After a backoff (`reconnect.backoff`, growing while failures repeat), the source drops the connection and reconnects. The broker then redelivers them from the session, with `mqtt.duplicate: true`. Messages that arrive on the connection between the failure and the reconnect are not handed to the relay: they come back after the reconnect too. Messages received before the failure can still be acknowledged until the connection drops. With `clean_session: true` the broker forgets unacknowledged messages when the connection drops, so a failed message is lost.

QoS 0 messages need no acknowledgement and are never redelivered.

### Sessions

A session is what the broker remembers about a client between connections: its subscriptions and its queued QoS 1 and 2 messages. `clean_session` decides whether the broker keeps it.

With `clean_session: false` (the default) the session is persistent. Over MQTT 5 the source asks the broker to keep it for one day after a disconnect. An MQTT 3.1.1 broker keeps it as long as its own settings say (mosquitto: `persistent_client_expiration`, by default forever). The broker queues the QoS 1 and 2 messages published while the source is away. After a reconnect or a restart of the relay, it redelivers whatever was not acknowledged. The broker also keeps the subscriptions. The source subscribes on its first connection after it starts, so changed topics apply. It subscribes again whenever the broker answers without a session (after the broker lost it). If the broker refuses that subscription while it resumed a session, the source keeps the session's subscriptions and logs a warning.

A persistent session keeps subscriptions you removed from the spec. To drop them, run once with `clean_session: true`, or change `client_id`.

### Connection loss and shutdown

This section covers what the source does when the connection drops, when the relay is busy, and when the relay stops.

When the connection drops, the source reconnects with backoff and jitter (a random part added to each wait). Each failed connect moves on to the next of `url` and `failover_urls`. A refused subscription counts as a failed connect, unless a resumed session keeps its subscriptions. A connection that delivered a message, or stayed up for 30s, resets the failure count. Each reconnect is logged at warn. After `max_attempts` failed connects in a row, the source fails and the relay stops. Messages handed over on a dropped connection can no longer be acknowledged, so the broker redelivers them: delivery is at least once.

While the relay applies backpressure (it stops reading until it catches up), the source stops reading from the connection. A connection that cannot be read for longer than the keepalive (30s) may be dropped and redialed. Its unacknowledged messages are redelivered.

When the relay shuts down, the source stops handing messages over but keeps the connection, so the messages in flight can still be acknowledged. Close waits for them to settle, then disconnects. The broker redelivers whatever is left unacknowledged.

`Health` is nil while the source is connected and subscribed.

## Destination

A destination publishes each message to a topic on the broker at `url`. The topic can change per message.

A destination keeps one connection to `url`. It connects on first use (a delivery, or a health check). When the connection drops, it reconnects in the background with backoff (500ms, doubling to 30s, full jitter). Each delivery publishes one message and waits for the broker's PUBACK (QoS 1) or PUBCOMP (QoS 2). A QoS 0 delivery returns once the message is written. The connection starts a clean session. So when a dropped connection loses an acknowledgement, the relay retries that delivery, not the session.

| Key | Meaning |
|---|---|
| `url` | The broker (see Connecting). It cannot be a per-message template. |
| `protocol_version` | `"5"` (default) or `"3.1.1"`. |
| `topic` | Per-message template of the topic, such as `devices/{{ .json.device }}/state`. It cannot contain `+` or `#`. |
| `qos` | Publish QoS: `0`, `1` (default) or `2`. |
| `retain` | Publish retained messages. |
| `body` | Per-message template of the payload (default: the message body). |
| `headers` | MQTT 5: per-message templates of extra user properties. |
| `client_id`, `username`, `password`, `tls` | See Connecting. |

Over MQTT 5 a message is published with:

- its content type,
- its headers as user properties, sorted by name, except hop-by-hop headers, `Authorization`, `Cookie`, `Content-Type` and `Content-Length`,
- the `headers` templates,
- `Idempotency-Key`, the relay's idempotency key (the envelope ID, or what the `idempotency_key` attribute set). A source of this package uses it as the envelope ID, so a message keeps its ID across brokers.

Header values that MQTT cannot carry (not UTF-8, or with a NUL) are left out. A `headers` template that renders one fails the delivery (permanent, `template`). MQTT 3.1.1 has no properties, so the headers, the content type and the idempotency key are not sent. The destination logs this at warn when `headers` is set.

`Health` connects if the destination never did, and is nil while it is connected. Close disconnects and stops reconnecting.

Each delivery is one attempt, because the relay owns retries. A failed delivery carries a class and tags. `retry_on` and `never_on` match the tags:

| Failure | Class | Tags |
|---|---|---|
| No connection: the connect failed (refused, DNS, TLS, credentials), or the connection dropped before the broker acknowledged | transient | `connect` |
| The attempt's timeout ran out | timeout | `timeout` |
| The broker refused the publish (MQTT 5 reason code `0x80` or more on the PUBACK or PUBREC) | see below | `mqtt:0x<code>`, such as `mqtt:0x87` |
| The broker cannot take it at all (QoS above its maximum, retain unavailable) | permanent | |
| The topic or body template failed, or the topic it rendered is invalid | permanent | `template` |

| Reason code | Class |
|---|---|
| `0x83` implementation specific error, `0x87` not authorized, `0x90` topic name invalid, `0x95` packet too large, `0x99` payload format invalid | permanent |
| `0x97` quota exceeded | rate limited (tagged `429` too) |
| `0x80` unspecified error, `0x88` server unavailable, `0x89` server busy, and any other code | transient |

A failure whose tags match neither `retry_on` nor `never_on` falls back to its class: it is retried unless it is permanent. So the default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries connection failures, timeouts, `0x97` and every transient code (`0x80`, `0x88`, `0x89` and so on). It gives up at once on the permanent ones. List a code in `never_on` to stop retrying it. For example, `never_on: [4xx, "mqtt:0x89"]` hands a busy broker's messages to the fallback at once. `retry_on` is only needed to retry a permanent code. An MQTT 3.1.1 broker has no reason codes: it refuses a publish by closing the connection, which is a `connect` failure.

## Example

One source reads device telemetry over TLS, with a second broker as failover and a shared subscription across replicas. Two destinations publish commands and state, one over MQTT 5 and one over MQTT 3.1.1.

```yaml
secrets:
  MQTT_PASSWORD: env:MQTT_PASSWORD

sources:
  - name: telemetry
    type: mqtt
    url: mqtts://broker.internal:8883
    failover_urls: [mqtts://broker-2.internal:8883]
    tls: { ca_file: /etc/hikyaku/ca.pem }
    username: relay
    password: "{{secrets.MQTT_PASSWORD}}"
    client_id: "telemetry-{{env.HOSTNAME}}"
    topics: ["devices/+/telemetry", "devices/+/alerts"]
    group: relays          # MQTT 5 shared subscription across replicas
    qos: 1
    prefetch: 256
    max_body_bytes: 256KiB
    reconnect: { backoff: 500ms, max_backoff: 30s, max_attempts: 0 }

destinations:
  - name: device-commands
    type: mqtt
    url: mqtt://broker.internal:1883
    topic: "devices/{{ .json.device_id }}/commands"
    qos: 1
    headers: { x-origin: "{{ .source }}" }
    retry: { attempts: 5 }

  - name: device-state
    type: mqtt
    url: mqtt://legacy-broker:1883
    protocol_version: "3.1.1"
    topic: "state/{{ .json.device_id }}"
    retain: true
```
