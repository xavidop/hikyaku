// Package nats is hikyaku's NATS protocol: the nats (core NATS) and
// jetstream source and destination types, built on
// github.com/nats-io/nats.go and its jetstream package.
//
// A nats source subscribes to subject (wildcards * and > allowed), as a
// member of the queue group when queue is set, so the members share the
// messages. Core NATS has no acknowledgements: delivery is at most once. A
// message the relay fails, or has not finished when it stops, is lost (and
// logged). Use jetstream for at-least-once delivery.
//
// A jetstream source reads stream through a durable pull consumer (durable,
// default the source name), created or updated at start with explicit
// acks, ack_wait, and max_ack_pending set to prefetch, so a restart resumes
// where the last run stopped. A message is acked once its outcome is final
// (delivered, dead-lettered, unrouted or filtered) and nak'ed when it
// failed, with a delay that grows with its delivery count, so the server
// redelivers it. While the relay works on a message for longer than half of
// ack_wait, the source tells the server it is still in progress, so it is
// not redelivered under the relay. At shutdown, messages not yet handed to
// the relay are nak'ed at once.
//
// A nats destination publishes each message to subject (a per-message
// template) and flushes, so a delivery means the server has it. A
// jetstream destination publishes with the envelope ID as Nats-Msg-Id, so
// the stream drops the copies of a retried message, and waits for the
// stream's ack. It also delivers batches, publishing them asynchronously
// and waiting for every ack.
//
// Every connection is named hikyaku-<relay>-<name>, tries url and then
// failover_urls, and reconnects with the reconnect.* backoff, forever
// unless reconnect.max_attempts is set. URLs are logged without their
// credentials.
//
// Envelopes: Subject is the message subject, Headers are the NATS headers,
// and the ordering_key attribute is the subject. A nats source sets the
// attributes nats.subject and nats.reply; a jetstream source sets
// jetstream.stream, jetstream.sequence, jetstream.consumer,
// jetstream.delivered and jetstream.subject.
package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// The source and destination types.
const (
	KindNATS      = "nats"
	KindJetStream = "jetstream"
)

// Register makes the nats and jetstream source and destination types
// available.
func Register() {
	plugin.RegisterSource(KindNATS, func(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(spec)
	})
	plugin.RegisterSource(KindJetStream, func(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newStreamSource(spec)
	})
	plugin.RegisterSink(KindNATS, func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSink(spec)
	})
	plugin.RegisterSink(KindJetStream, func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newStreamSink(spec)
	})
}

// Headers the protocol reads or writes.
const (
	headerMsgID       = "Nats-Msg-Id"
	headerIdempotency = "Idempotency-Key"
	headerContentType = "Content-Type"
)

// Content types of received messages that carry no Content-Type header.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
)

// headerValue returns the first value of a header. NATS headers are case
// sensitive, but publishers spell the common ones differently, so the
// exact spelling wins and any other spelling does too.
func headerValue(h nats.Header, name string) string {
	if v := h.Get(name); v != "" {
		return v
	}
	for k, vs := range h {
		if len(vs) > 0 && strings.EqualFold(k, name) {
			return vs[0]
		}
	}
	return ""
}

// delHeader removes every spelling of a header.
func delHeader(h nats.Header, name string) {
	for k := range h {
		if strings.EqualFold(k, name) {
			delete(h, k)
		}
	}
}

// copyHeaders copies NATS headers into an envelope's, keeping their
// spelling (templates and CEL see them under canonical names).
func copyHeaders(h nats.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// contentType is a received message's content type: its Content-Type
// header, else JSON, text or binary from what the body holds.
func contentType(h nats.Header, data []byte) string {
	if ct := headerValue(h, headerContentType); ct != "" {
		return ct
	}
	switch {
	case len(data) == 0:
		return ""
	case json.Valid(data):
		return contentJSON
	case utf8.Valid(data):
		return contentText
	}
	return contentBinary
}

// traceOf reads a W3C trace context (traceparent) from the headers, so a
// trace started upstream continues through the relay.
func traceOf(h nats.Header) trace.SpanContext {
	if len(h) == 0 {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), headerCarrier(h)))
}

// headerCarrier reads NATS headers for a propagator, matching names in any
// case.
type headerCarrier nats.Header

func (c headerCarrier) Get(key string) string { return headerValue(nats.Header(c), key) }

func (c headerCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// redacted stands for a credential in a logged URL.
const redacted = "xxxxx"

// redactURL hides a URL's credentials, for logs, attributes and results.
// A NATS URL with a user and no password carries a token, so the whole
// user part goes.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || (u.User == nil && strings.Contains(raw, "@")) {
		// Unparsable, or user:pass@host without a scheme (an opaque URL):
		// cut everything before the host.
		if at := strings.LastIndex(raw, "@"); at >= 0 {
			if i := strings.Index(raw, "://"); i >= 0 && i < at {
				return raw[:i+3] + redacted + "@" + raw[at+1:]
			}
			return redacted + "@" + raw[at+1:]
		}
		return raw
	}
	if u.User == nil {
		return u.String()
	}
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), redacted)
	} else {
		u.User = url.User(redacted)
	}
	return u.String()
}

// nameChars may not appear in stream and consumer names.
const nameChars = ">*. /\\\t\r\n"

// checkName checks a stream or consumer name.
func checkName(field, name string) error {
	if name == "" {
		return fmt.Errorf("%s is empty", field)
	}
	if strings.ContainsAny(name, nameChars) {
		return fmt.Errorf("%s %q may not contain '.', '*', '>', '/', '\\' or whitespace", field, name)
	}
	return nil
}

// durableName turns a source name (which may contain '.') into a consumer
// name.
func durableName(name string) string {
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(nameChars, r) {
			return '_'
		}
		return r
	}, name)
}

// checkSubject checks a subject: tokens separated by '.', none empty, no
// whitespace. With wildcards, a token may be * and the last one may be >.
func checkSubject(subject string, wildcards bool) error {
	if subject == "" {
		return errors.New("is empty")
	}
	if strings.ContainsAny(subject, " \t\r\n") {
		return errors.New("contains whitespace")
	}
	tokens := strings.Split(subject, ".")
	for i, tok := range tokens {
		switch {
		case tok == "":
			return errors.New("has an empty token")
		case !strings.ContainsAny(tok, "*>"):
		case !wildcards:
			return errors.New("has a wildcard (* or >): a message is published to one subject")
		case tok == "*", tok == ">" && i == len(tokens)-1:
		default:
			return fmt.Errorf("has the token %q: * must be a whole token and > the last one", tok)
		}
	}
	return nil
}

// checkHeaderKey checks a header name: printable ASCII without ':'.
func checkHeaderKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		if c := k[i]; c <= ' ' || c > '~' || c == ':' {
			return false
		}
	}
	return true
}

// newEnvelope is the envelope of a received message: body, subject,
// headers, content type, trace and ordering key.
func newEnvelope(source, kind, subject string, h nats.Header, data []byte) *envelope.Envelope {
	env := envelope.New(source, kind, data)
	env.Subject = subject
	env.Headers = copyHeaders(h)
	env.ContentType = contentType(h, data)
	env.Trace = traceOf(h)
	env.Attributes["ordering_key"] = subject
	return env
}
