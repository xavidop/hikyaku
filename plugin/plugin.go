// Package plugin defines the interfaces every protocol and router implements:
// a Source produces envelopes, a Sink delivers them, a Router decides where
// they go. Protocol modules register factories for their kinds (RegisterSource,
// RegisterSink); cmd/hikyaku/wiring.go links them all into the binary.
package plugin

import (
	"context"
	"errors"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/store"
)

// Source is where messages come from. Start runs until ctx ends (or the
// source runs out, such as stdin at EOF, when it returns nil) and sends each
// message on out. A listen source that cannot hand a message over at once
// applies backpressure to its caller instead of blocking (HTTP 503 with
// Retry-After); a subscribe or poll source simply blocks.
type Source interface {
	Name() string
	Start(ctx context.Context, out chan<- *envelope.Envelope) error
	Health(ctx context.Context) error
}

// Sink is where messages go. Deliver makes one attempt; the engine owns
// retries, timeouts, breakers and fallbacks. Errors should be *Error (see
// errors.go) so the retry policy can classify them.
type Sink interface {
	Name() string
	Deliver(ctx context.Context, env *envelope.Envelope) (*Result, error)
	Health(ctx context.Context) error
	Close(ctx context.Context) error
}

// Starter is a sink that serves something of its own, such as an sse
// destination's event stream. The engine starts it when the relay runs;
// one-shot commands (send, route, ai test, dlq replay) never do, so they do
// not take a running relay's port.
type Starter interface {
	Start(ctx context.Context) error
}

// BatchSink is a sink with a native batch call: one Kafka produce, one SQS
// SendMessageBatch, one S3 object. The engine calls it for a destination
// with batch: set; a sink without it gets one combined envelope instead
// (batch.format, ndjson or json_array). A batch succeeds or fails as a
// whole: an error retries (and falls back with) every message of it, so a
// sink whose call reports partial failures returns an error, and the
// broker's message IDs (the envelope IDs) dedupe what was already written.
type BatchSink interface {
	DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*Result, error)
}

// ErrNoBatch is what DeliverBatch returns for a batch the sink has no
// batch call for after all, such as a grpc destination whose method, found
// through server reflection, takes one request per call. The engine then
// delivers the combined envelope with Deliver, as for a sink without
// DeliverBatch.
var ErrNoBatch = errors.New("no batch call")

// Deduper is a source that applies its own dedupe: block, such as the http
// source, which answers a duplicate itself. The engine leaves dedupe to it,
// and it reports each duplicate it drops through Env.Deduped.
type Deduper interface {
	Dedupes() bool
}

// Closer is a source that holds a connection its acknowledgements need,
// such as a broker consumer. Start returns once ctx ends, but the engine
// still finishes (and acks) the messages in flight; it calls Close after
// they are all done.
type Closer interface {
	Close(ctx context.Context) error
}

// OnSettle wires a subscribe source's acknowledgement to the relay's
// outcome. settle runs once per message. With ack "accepted" it runs as
// soon as the relay holds the message (OnQueued), with final true.
// Otherwise ("delivered", the default) it runs when the message is
// finished: final is true when the broker may forget the message
// (delivered, dead-lettered, unrouted or filtered) and false when it
// failed and should be redelivered.
func OnSettle(env *envelope.Envelope, ackMode string, settle func(ctx context.Context, final bool)) {
	if ackMode == "accepted" {
		env.OnQueued(func() { settle(context.Background(), true) })
		return
	}
	env.OnAck(func(ctx context.Context, o envelope.Outcome) { settle(ctx, o.Status.Acked()) })
}

// Result is what a sink got back from one successful delivery.
type Result struct {
	// Status is the protocol status (an HTTP code), 0 when the protocol has
	// none.
	Status      int
	Headers     map[string][]string
	Body        []byte
	ContentType string
	// Endpoint names what served the delivery: a URL, a file, a group member.
	Endpoint string
	// Reroute holds the destinations a downstream asked for with
	// X-Hikyaku-Reroute: a correction for the feedback loop.
	Reroute []string
}

// Reply converts a result into the envelope's reply type.
func (r *Result) Reply() *envelope.Reply {
	if r == nil {
		return nil
	}
	return &envelope.Reply{Status: r.Status, Headers: r.Headers, Body: r.Body, ContentType: r.ContentType}
}

// Router decides which declared destinations get a message.
type Router interface {
	Route(ctx context.Context, env *envelope.Envelope) (Decision, error)
}

// RouterFunc adapts a function to Router.
type RouterFunc func(ctx context.Context, env *envelope.Envelope) (Decision, error)

// Route calls f.
func (f RouterFunc) Route(ctx context.Context, env *envelope.Envelope) (Decision, error) {
	return f(ctx, env)
}

// Decision is a router's answer.
type Decision struct {
	// Destinations are destination names declared in the spec.
	Destinations []string
	// Router is the kind that decided: static, rules, ai, sticky.
	Router string
	// Model is the model that answered, for an AI decision.
	Model string
	// Confidence is the decision's confidence (the chosen option's
	// probability, or the lowest chosen one for multi), 0 when not an AI one.
	Confidence float64
	Reason     string
	// LowConfidence: the model answered but was not sure; Destinations are
	// on_low_confidence.
	LowConfidence bool
	// Shadow: an AI router in shadow mode decided and logged, and the chain
	// must deliver through its fallback instead.
	Shadow bool
	// Cached: the decision came from the decision cache.
	Cached bool
	// Hops lists the routers tried before this one, with why they failed.
	Hops []string
	// Attributes are set on the envelope (ai.decision, ...).
	Attributes map[string]any
	// History, when set, is the call record to append after delivery.
	History *HistoryNote
}

// HistoryNote is the record an AI router wants appended once delivery is
// done. The engine fills DeliveredTo, Outcome and any reroute feedback.
type HistoryNote struct {
	Store   store.HistoryStore
	Route   string
	Key     string
	Record  store.CallRecord
	Options store.AppendOptions
}
