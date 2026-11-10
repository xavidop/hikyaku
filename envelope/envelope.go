// Package envelope defines the protocol-agnostic message that flows through a
// relay. Every source turns what it receives into an Envelope, and every sink
// delivers one. The shape is CloudEvents-compatible: ID, Source, Subject,
// ContentType and Body map onto id, source, subject, datacontenttype and data.
package envelope

import (
	"context"
	"encoding/json"
	"maps"
	"net/textproto"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/trace"
)

// Envelope is one message in flight.
//
// Fields are plain data so sources and sinks can fill and read them directly.
// An envelope is owned by one goroutine at a time: the engine clones it before
// handing copies to destinations that run in parallel. SetAttribute and
// Attribute are safe for concurrent use; direct access to Attributes is not.
type Envelope struct {
	// ID is a ULID unless the source took one from the message (an
	// Idempotency-Key header, a broker message ID). It is the default
	// idempotency key, sent on every attempt and every fallback.
	ID       string
	Source   string // the source name from the spec
	Protocol string // "http", "wss", "stdin", ...
	// Subject is the path, topic, channel or routing key the message came on.
	Subject     string
	Headers     map[string][]string
	ContentType string
	Body        []byte
	// Attributes carry protocol metadata and what the relay learned about the
	// message: ai.decision, delivery.attempts, dlq, ...
	Attributes map[string]any
	ReceivedAt time.Time
	Trace      trace.SpanContext

	mu       sync.Mutex
	parsed   any
	parseErr error
	parsedOK bool
	ack      *acker
	queued   *hook
	stream   Stream
}

// NewID returns a new ULID string.
func NewID() string { return ulid.Make().String() }

// New returns an envelope with a fresh ID, the receive time set to now and
// empty header and attribute maps.
func New(source, protocol string, body []byte) *Envelope {
	return &Envelope{
		ID:         NewID(),
		Source:     source,
		Protocol:   protocol,
		Headers:    map[string][]string{},
		Body:       body,
		Attributes: map[string]any{},
		ReceivedAt: time.Now().UTC(),
	}
}

// JSON returns the body parsed as JSON, parsed once and cached. A body that is
// not JSON returns its parse error (and a nil value) every time.
func (e *Envelope) JSON() (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.parsedOK {
		e.parsedOK = true
		e.parsed, e.parseErr = nil, nil
		if len(e.Body) > 0 {
			var v any
			if err := json.Unmarshal(e.Body, &v); err != nil {
				e.parseErr = err
			} else {
				e.parsed = v
			}
		}
	}
	return e.parsed, e.parseErr
}

// JSONOrNil is JSON without the error: nil when the body is not JSON.
func (e *Envelope) JSONOrNil() any {
	v, _ := e.JSON()
	return v
}

// SetBody replaces the body and drops the cached JSON view.
func (e *Envelope) SetBody(b []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Body = b
	e.parsed, e.parseErr, e.parsedOK = nil, nil, false
}

// BodyText is the body as a string.
func (e *Envelope) BodyText() string { return string(e.Body) }

// Header returns the first value of a header, matched case-insensitively.
func (e *Envelope) Header(name string) string {
	if v, ok := e.Headers[name]; ok && len(v) > 0 {
		return v[0]
	}
	if v, ok := e.Headers[textproto.CanonicalMIMEHeaderKey(name)]; ok && len(v) > 0 {
		return v[0]
	}
	for k, v := range e.Headers {
		if len(v) > 0 && equalFold(k, name) {
			return v[0]
		}
	}
	return ""
}

// SetHeader replaces a header with one value, under its canonical name.
func (e *Envelope) SetHeader(name, value string) {
	if e.Headers == nil {
		e.Headers = map[string][]string{}
	}
	e.DelHeader(name)
	e.Headers[textproto.CanonicalMIMEHeaderKey(name)] = []string{value}
}

// DelHeader removes every spelling of a header.
func (e *Envelope) DelHeader(name string) {
	for k := range e.Headers {
		if equalFold(k, name) {
			delete(e.Headers, k)
		}
	}
}

// SetAttribute sets one attribute; safe for concurrent use.
func (e *Envelope) SetAttribute(key string, value any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Attributes == nil {
		e.Attributes = map[string]any{}
	}
	e.Attributes[key] = value
}

// Attribute reads one attribute; safe for concurrent use.
func (e *Envelope) Attribute(key string) (any, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.Attributes[key]
	return v, ok
}

// AttributesCopy returns a shallow copy of the attributes; safe for
// concurrent use.
func (e *Envelope) AttributesCopy() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return maps.Clone(e.Attributes)
}

// Clone returns a copy that can be changed without touching e: headers,
// attributes and body are copied (attribute values shallowly). The clone
// shares e's acknowledgement, so acking either acks both.
func (e *Envelope) Clone() *Envelope {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := &Envelope{
		ID:          e.ID,
		Source:      e.Source,
		Protocol:    e.Protocol,
		Subject:     e.Subject,
		ContentType: e.ContentType,
		ReceivedAt:  e.ReceivedAt,
		Trace:       e.Trace,
		ack:         e.ack,
		queued:      e.queued,
		stream:      e.stream,
	}
	if e.Headers != nil {
		c.Headers = make(map[string][]string, len(e.Headers))
		for k, v := range e.Headers {
			c.Headers[k] = append([]string(nil), v...)
		}
	}
	if e.Body != nil {
		c.Body = append([]byte(nil), e.Body...)
	}
	c.Attributes = maps.Clone(e.Attributes)
	if c.Attributes == nil {
		c.Attributes = map[string]any{}
	}
	return c
}

// AckStatus is how the relay finished with a message.
type AckStatus int

const (
	// Delivered: every required destination succeeded, on its primary or a
	// fallback.
	Delivered AckStatus = iota
	// DeadLettered: at least one required destination failed and the message
	// is safely in the dead-letter queue.
	DeadLettered
	// Unrouted: no route takes messages from this source, or the routers chose
	// no destination. Counted and logged.
	Unrouted
	// Filtered: every route that takes the source filtered the message out.
	Filtered
	// Failed: the message was not handled (the DLQ failed too, or the relay
	// shut down first). Sources treat it as a nack and redeliver if they can.
	Failed
)

func (s AckStatus) String() string {
	switch s {
	case Delivered:
		return "delivered"
	case DeadLettered:
		return "dead_lettered"
	case Unrouted:
		return "unrouted"
	case Filtered:
		return "filtered"
	case Failed:
		return "failed"
	}
	return "unknown"
}

// Acked reports whether the source may acknowledge the message: anything but
// Failed.
func (s AckStatus) Acked() bool { return s != Failed }

// Reply is what a destination answered, kept so a sync source (HTTP with
// respond.from) can pass it back to its caller.
type Reply struct {
	Status      int
	Headers     map[string][]string
	Body        []byte
	ContentType string
}

// Outcome is handed to the source's ack callback once the relay is done.
type Outcome struct {
	Status AckStatus
	// Replies maps a destination name to the reply of whichever endpoint
	// served it (the primary or a fallback).
	Replies map[string]*Reply
	// Err is the last error, set when Status is DeadLettered or Failed.
	Err error
}

type acker struct {
	once sync.Once
	fn   func(context.Context, Outcome)
}

// OnAck registers the source's acknowledgement callback. It runs exactly once,
// on the first Ack, for this envelope and every clone made after OnAck.
func (e *Envelope) OnAck(fn func(context.Context, Outcome)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ack = &acker{fn: fn}
}

// Ack finishes the message. Only the first call has an effect; envelopes
// without a callback ignore it.
func (e *Envelope) Ack(ctx context.Context, o Outcome) {
	e.mu.Lock()
	a := e.ack
	e.mu.Unlock()
	if a == nil || a.fn == nil {
		return
	}
	a.once.Do(func() { a.fn(ctx, o) })
}

// Stream is a way back to a caller waiting for incremental output, such as
// the LLM proxy's server-sent events. The source sets it; a sink that
// streams (a model destination asked to stream) writes each chunk. The
// first Write commits the response, so a sink that has written must not be
// retried or fall back: it reports a permanent error instead.
type Stream interface {
	// Write sends one chunk (one event's data).
	Write(chunk []byte) error
	// Committed reports whether a chunk was written.
	Committed() bool
}

// SetStream attaches a stream back to the caller. Clones made after it
// share it.
func (e *Envelope) SetStream(s Stream) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stream = s
}

// Stream is the stream back to the caller, nil when there is none.
func (e *Envelope) Stream() Stream {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stream
}

type hook struct {
	once sync.Once
	fn   func()
}

// OnQueued registers fn to run once the relay holds the message: when the
// engine takes it from the source (memory buffer), or once the write-ahead
// log has it (disk buffer). A source with ack: accepted acknowledges then.
// Clones made after OnQueued share it.
func (e *Envelope) OnQueued(fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queued = &hook{fn: fn}
}

// Queued runs the OnQueued callback, once. The engine calls it.
func (e *Envelope) Queued() {
	e.mu.Lock()
	h := e.queued
	e.mu.Unlock()
	if h == nil || h.fn == nil {
		return
	}
	h.once.Do(h.fn)
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
