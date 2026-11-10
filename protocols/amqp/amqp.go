// Package amqp is hikyaku's AMQP 0-9-1 protocol (RabbitMQ): the amqp source
// and destination type, on github.com/rabbitmq/amqp091-go.
//
// A source subscribes to one queue with manual acknowledgements and a
// prefetch window (basic.qos, default 64). Each delivery becomes one
// envelope: the message ID (else its Idempotency-Key header, else a new ULID)
// is the envelope ID, the routing key is the subject, the headers table and
// the content-type, content-encoding, correlation-id and reply-to properties
// are the headers, and the amqp.* attributes carry the queue, exchange,
// routing key, delivery tag, redelivered flag, message ID and timestamp.
// ordering_key is the queue.
//
// A message is acknowledged (basic.ack) once the relay is done with it:
// delivered, dead-lettered, unrouted or filtered. One the relay failed
// (the dead-letter queue failed too, or shutdown cut its delivery) is
// returned to the queue (basic.nack with requeue) after a delay that grows
// with the message's failed attempts, so a message that keeps failing does
// not spin. Tags are acknowledged one by one, in whatever order messages
// finish. A message over max_body_bytes is logged and acknowledged.
//
// The source reconnects with reconnect.* backoff when the connection or the
// channel closes, rotating through url and failover_urls. Messages in
// flight on the old channel can no longer be acknowledged: the broker
// redelivers them, so they are seen twice. At shutdown the consumer is
// cancelled and the deliveries not yet handed to the relay go back to the
// queue; the connection is closed once the messages in flight are settled.
//
// A destination publishes each message to exchange (default the default
// exchange) with a per-message routing_key, persistent by default, with the
// envelope ID as message ID. Publisher confirms are always on: a delivery
// succeeds once the broker confirms the message. A nack, a message returned
// as unroutable (with mandatory: true), a broker exception and a blocked
// connection (a resource alarm) are errors classified for the retry policy.
// DeliverBatch publishes a whole batch, then waits for every confirm.
package amqp

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/plugin"
)

// kind is the protocol type.
const kind = "amqp"

// Register makes the amqp source and destination type available.
func Register() {
	plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		s, err := newSource(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
	plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newSink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
}

// Content types of messages that carry none.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// detectContentType is the content type of a body without one: JSON when it
// parses as JSON, text when it is UTF-8, else binary.
func detectContentType(b []byte) string {
	switch {
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}

// redactURL hides a URL's password, for logs and errors. A URL that does not
// parse loses everything before its host instead, so a password in it never
// shows.
func redactURL(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Redacted()
	}
	scheme, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return "(invalid URL)"
	}
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		return scheme + "://xxxxx@" + rest[i+1:]
	}
	return raw
}

// connectionName is the connection_name client property (and the consumer
// tag) of a source or destination, as the broker shows it.
func connectionName(relay, name string) string {
	if relay == "" {
		return "hikyaku-" + name
	}
	return "hikyaku-" + relay + "-" + name
}
