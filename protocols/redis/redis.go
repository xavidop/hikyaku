// Package redis is hikyaku's Redis protocol: the redis source and
// destination type, over Redis Streams and Pub/Sub.
//
// A source reads either a stream (stream, with group) or a pub/sub channel
// (channel).
//
// A stream source is one consumer of a consumer group. At startup it creates
// the group (and the stream) when they are missing, at the start of the
// stream (start: earliest) or at its end (latest, the default). It reads the
// entries it left pending in an earlier run first, then new ones. An entry
// is acknowledged (XACK) once its outcome is final: delivered,
// dead-lettered, unrouted or filtered. A failed entry stays pending. Every
// half ack_wait the source reclaims (XAUTOCLAIM) the group's entries idle
// longer than ack_wait, which redelivers what this consumer failed and what
// a dead consumer left behind. The source also refreshes its own entries in
// flight (XCLAIM ... JUSTID), so neither it nor another consumer reclaims
// an entry that is still being delivered. At most prefetch entries are in
// flight at once.
//
// A pub/sub source subscribes to channel, or pattern-subscribes when the
// channel holds *, ? or [. Pub/sub is at most once: a message published
// while the source is disconnected is lost, and there is nothing to
// acknowledge.
//
// Both reconnect with backoff and jitter (reconnect.*) and rotate through
// failover_urls.
//
// A destination appends each message to a stream (XADD, trimmed with
// MAXLEN ~ max_len) or publishes its body to a channel (PUBLISH, the
// channel a per-message template). A stream entry carries the fields id
// (the envelope ID), body, content_type, subject and one h:<Header> per
// header; a stream source reads them back into the same envelope. A batch
// is one MULTI/EXEC of XADDs, or one pipeline of PUBLISHes.
//
// Envelope attributes: redis.stream, redis.entry_id, redis.group,
// redis.consumer and redis.deliveries for streams; redis.channel and
// redis.pattern for pub/sub; ordering_key is the stream or the channel.
package redis

import (
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// kind is the protocol type, as sources and destinations name it.
const kind = "redis"

// Register makes the redis source and destination type available.
func Register() {
	plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(ctx, spec)
	}, plugin.WithDescription("Redis Streams (consumer groups, XREADGROUP) or Pub/Sub (SUBSCRIBE, PSUBSCRIBE)"))
	plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSink(ctx, spec)
	}, plugin.WithDescription("Redis Streams (XADD) or Pub/Sub (PUBLISH)"))
}

// newSource builds a stream or a pub/sub source.
func newSource(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
	opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	if opts.mode == modeStream {
		return newStreamSource(spec, opts), nil
	}
	return newPubSubSource(spec, opts), nil
}

// Content types of received messages that do not say theirs.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// sniff is the content type of a body that came without one: JSON when it
// parses as JSON, text when it is UTF-8, else binary.
func sniff(b []byte) string {
	switch {
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	default:
		return contentBinary
	}
}

// clientName is the name a connection gives itself (CLIENT SETNAME), so
// CLIENT LIST shows which relay and entry it belongs to. Redis refuses
// names with spaces, so anything outside [A-Za-z0-9._:-] becomes _.
func clientName(relay, name string) string {
	parts := []string{"hikyaku"}
	for _, p := range []string{relay, name} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', strings.ContainsRune("._:-", r):
			return r
		}
		return '_'
	}, strings.Join(parts, "-"))
}

var propagator = propagation.TraceContext{}

// headerCarrier reads a W3C trace context (traceparent, tracestate) from an
// envelope's headers, matching names case-insensitively.
type headerCarrier struct{ env *envelope.Envelope }

func (c headerCarrier) Get(k string) string { return c.env.Header(k) }
func (headerCarrier) Set(string, string)    {}
func (headerCarrier) Keys() []string        { return nil }

// traceOf is the trace context a message carries in its headers.
func traceOf(env *envelope.Envelope) trace.SpanContext {
	return trace.SpanContextFromContext(propagator.Extract(context.Background(), headerCarrier{env}))
}
