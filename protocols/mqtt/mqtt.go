// Package mqtt is hikyaku's MQTT protocol: the mqtt source and destination
// type, over MQTT 5 (protocol_version "5", the default, with
// github.com/eclipse/paho.golang) and MQTT 3.1.1 (protocol_version "3.1.1",
// with github.com/eclipse/paho.mqtt.golang). Both libraries sit behind one
// small client interface (client.go), so the source and the destination are
// written once.
//
// A source subscribes to its topic filters (topic or topics, at qos,
// default 1; with group, a shared subscription $share/<group>/<filter>, MQTT 5
// only) and turns every PUBLISH into an envelope: Subject is the topic, MQTT 5
// user properties become headers, the ID is the Idempotency-Key user property
// or the correlation data when there is one, and the attributes are
// mqtt.topic, mqtt.qos, mqtt.retained, mqtt.duplicate, mqtt.packet_id and
// ordering_key (the topic).
//
// Acknowledgements are manual. A QoS 1 or 2 message is acknowledged (PUBACK or
// PUBREC) once the relay's outcome is final (delivered, dead-lettered,
// unrouted or filtered), and MQTT requires acknowledgements in the order the
// messages arrived, so each connection keeps an ordered queue: a message is
// acknowledged once it and every message before it are settled. MQTT has no
// negative acknowledgement: when a message fails (the relay could not even
// dead-letter it), it and every message after it stay unacknowledged, and
// the source drops the connection after a backoff and reconnects, so the
// broker redelivers them from the persistent session (clean_session: false,
// the default). QoS 0 messages need no acknowledgement.
//
// The source reconnects with reconnect.* backoff and rotates through
// failover_urls; it keeps its connection after its context ends, so the
// messages in flight can still be acknowledged, and Close disconnects.
//
// A destination keeps one connection, dialed on first use and redialed in
// the background whenever it drops. It publishes each message to its topic
// (a per-message template) at qos (default 1), optionally retained, and waits
// for PUBACK (QoS 1) or PUBCOMP (QoS 2); QoS 0 returns once written. Over MQTT
// 5 the envelope's headers, the headers templates and the Idempotency-Key
// become user properties; MQTT 3.1.1 has no properties, so they are dropped.
package mqtt

import (
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/plugin"
)

// kind is the protocol type of the source and the destination.
const kind = "mqtt"

// Register makes the mqtt source and destination type available.
func Register() {
	plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(ctx, spec)
	})
	plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSink(ctx, spec)
	})
}

// Content types of received messages without an MQTT 5 content type.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// detectContentType names a payload: JSON when it parses as JSON, text when
// it is UTF-8, else binary.
func detectContentType(b []byte) string {
	switch {
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}
