package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

const (
	// defaultFlushTimeout bounds a nats destination's flush when the
	// attempt has no deadline of its own.
	defaultFlushTimeout = 10 * time.Second
	// asyncAckTimeout fails an asynchronous publish (a batch) whose ack
	// never comes, so it is not kept forever.
	asyncAckTimeout = time.Minute
)

// Result headers of a jetstream delivery: one value per message.
const (
	resultStream    = "jetstream-stream"
	resultSequence  = "jetstream-sequence"
	resultDuplicate = "jetstream-duplicate"
)

// dropHeaders are not forwarded from the incoming message: hop-by-hop
// headers, framing, and the caller's own credentials. NATS control headers
// (Nats-*) are not forwarded either: they steer the hop they were set on
// (a rollup, an expected stream, a TTL). A destination's headers may set
// any of them.
var dropHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "host": true,
	"content-length": true, "authorization": true, "cookie": true, "accept-encoding": true,
}

func dropHeader(k string) bool {
	if !checkHeaderKey(k) {
		return true
	}
	l := strings.ToLower(k)
	return dropHeaders[l] || strings.HasPrefix(l, "nats-")
}

// sink is what the nats and jetstream destinations share: one connection,
// made on first use or when the relay starts, and how a message is built
// from an envelope.
type sink struct {
	name         string
	kind         string
	opts         sinkOptions
	log          *slog.Logger
	flushTimeout time.Duration

	mu     sync.Mutex
	conn   *conn
	js     jetstream.JetStream
	closed bool
}

func newSinkBase(spec plugin.Spec, kind string) (*sink, error) {
	opts, err := parseSink(spec, kind)
	if err != nil {
		return nil, err
	}
	return &sink{
		name: spec.Name, kind: kind, opts: opts, log: spec.Env.Log(), flushTimeout: defaultFlushTimeout,
	}, nil
}

// Name is the destination name.
func (s *sink) Name() string { return s.name }

// Start connects when the relay runs, so the first message does not wait
// for the connection and disconnects are logged from the start. A server
// that is down does not fail it: the connection keeps trying.
func (s *sink) Start(context.Context) error {
	if _, _, err := s.connection(); err != nil && !errors.Is(err, errClosed) {
		return err
	}
	return nil
}

// connection returns the connection, making it on first use.
func (s *sink) connection() (*conn, jetstream.JetStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, errClosed
	}
	if s.conn == nil {
		// Without a reconnect buffer, a publish while disconnected fails at
		// once instead of going out later behind the relay's back (its
		// retry would deliver the message twice).
		c, err := dial(s.opts.conn, s.log, "destination", s.name, nil, nats.ReconnectBufSize(-1))
		if err != nil {
			return nil, nil, fmt.Errorf("connect: %w", err)
		}
		if s.kind == KindJetStream {
			js, err := jetstream.New(c.nc, jetstream.WithPublishAsyncTimeout(asyncAckTimeout))
			if err != nil {
				c.nc.Close()
				return nil, nil, fmt.Errorf("jetstream: %w", err)
			}
			s.js = js
		}
		s.conn = c
	}
	return s.conn, s.js, nil
}

// ready returns the connection when it is connected; otherwise a transient
// error tagged connect that says why it is not.
func (s *sink) ready() (*conn, jetstream.JetStream, error) {
	c, js, err := s.connection()
	if err == nil {
		err = c.health()
	}
	if err != nil {
		return nil, nil, plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	return c, js, nil
}

// Health is nil when the destination is connected (and, for jetstream,
// the account answers), else why not.
func (s *sink) Health(ctx context.Context) error {
	_, js, err := s.ready()
	if err != nil {
		return err
	}
	if js != nil {
		actx, cancel := context.WithTimeout(ctx, healthTimeout)
		defer cancel()
		if _, err := js.AccountInfo(actx); err != nil {
			return fmt.Errorf("jetstream: %w", err)
		}
	}
	return nil
}

// Close waits for the acks of asynchronous publishes still out, then
// drains the connection (what it holds goes out) and closes it.
func (s *sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	c, js := s.conn, s.js
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	if js != nil {
		select {
		case <-js.PublishAsyncComplete():
		case <-ctx.Done():
		}
		js.CleanupPublisher()
	}
	c.close(ctx)
	return nil
}

// message builds what is published for an envelope: the subject (a
// per-message template), the body (or the body template), and the headers:
// the envelope's (see dropHeaders), Content-Type, Idempotency-Key (the
// envelope ID, unless the relay set it) and the destination's own, last.
func (s *sink) message(env *envelope.Envelope) (*nats.Msg, error) {
	subject, err := s.opts.subject.Render(env)
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, fmt.Errorf("subject: %w", err))
	}
	if err := checkSubject(subject, false); err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, fmt.Errorf("subject %q %v", subject, err))
	}
	data := env.Body
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, fmt.Errorf("body: %w", err))
		}
		data = []byte(b)
	}
	h := nats.Header{}
	for k, vs := range env.Headers {
		if dropHeader(k) {
			continue
		}
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	if env.ContentType != "" {
		delHeader(h, headerContentType)
		h.Set(headerContentType, env.ContentType)
	}
	if headerValue(h, headerIdempotency) == "" {
		h.Set(headerIdempotency, env.ID)
	}
	for k, t := range s.opts.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		delHeader(h, k)
		h.Set(k, v)
	}
	return &nats.Msg{Subject: subject, Data: data, Header: h}, nil
}

// Sink is a nats destination: it publishes each message to subject on
// one connection and flushes, so a delivery means the server has it.
type Sink struct{ *sink }

func newSink(spec plugin.Spec) (*Sink, error) {
	b, err := newSinkBase(spec, KindNATS)
	if err != nil {
		return nil, err
	}
	return &Sink{b}, nil
}

// Deliver publishes the message and flushes: one attempt.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	msg, err := s.message(env)
	if err != nil {
		return nil, err
	}
	c, _, err := s.ready()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		// Published now, it could still go out after the failure is
		// reported, and the retry would send it twice.
		return nil, deliverError(ctx, err)
	}
	before := c.nc.LastError()
	if err := c.nc.PublishMsg(msg); err != nil {
		return nil, deliverError(ctx, err)
	}
	fctx, cancel := withDeadline(ctx, s.flushTimeout)
	defer cancel()
	err = c.nc.FlushWithContext(fctx)
	// The server refuses a publish with an -ERR, which arrives before the
	// flush's PONG.
	if now := c.nc.LastError(); deniedPublish(now, before, msg.Subject) {
		return nil, plugin.Wrap(plugin.Permanent, []string{tagPermissions}, now)
	}
	if err != nil {
		return nil, deliverError(fctx, err)
	}
	return &plugin.Result{Endpoint: c.url()}, nil
}

// StreamSink is a jetstream destination: it publishes each message to
// subject and waits for the stream's ack. The envelope ID is the message's
// Nats-Msg-Id, so the stream drops the copies of a retried message.
type StreamSink struct{ *sink }

func newStreamSink(spec plugin.Spec) (*StreamSink, error) {
	b, err := newSinkBase(spec, KindJetStream)
	if err != nil {
		return nil, err
	}
	return &StreamSink{b}, nil
}

// Deliver publishes the message and waits for the stream's ack: one
// attempt.
func (s *StreamSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	msg, err := s.message(env)
	if err != nil {
		return nil, err
	}
	opts := s.publishOptions(env, msg)
	c, js, err := s.ready()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, deliverError(ctx, err)
	}
	before := c.nc.LastError()
	ack, err := js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		// A refused publish gets no answer: the attempt times out.
		if now := c.nc.LastError(); deniedPublish(now, before, msg.Subject) {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagPermissions}, now)
		}
		return nil, deliverError(ctx, err)
	}
	return s.result(c, []*jetstream.PubAck{ack}), nil
}

// DeliverBatch publishes every message asynchronously, then waits for
// every ack. The batch fails as a whole when one message fails; the
// stream drops the copies of those it already stored when the batch is
// retried.
func (s *StreamSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	msgs := make([]*nats.Msg, len(envs))
	opts := make([][]jetstream.PublishOpt, len(envs))
	for i, env := range envs {
		m, err := s.message(env)
		if err != nil {
			return nil, fmt.Errorf("message %s: %w", env.ID, err)
		}
		msgs[i], opts[i] = m, s.publishOptions(env, m)
	}
	c, js, err := s.ready()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, deliverError(ctx, err)
	}
	before := c.nc.LastError()
	refused := func() error {
		now := c.nc.LastError()
		for _, m := range msgs {
			if deniedPublish(now, before, m.Subject) {
				return plugin.Wrap(plugin.Permanent, []string{tagPermissions}, now)
			}
		}
		return nil
	}
	futures := make([]jetstream.PubAckFuture, len(msgs))
	for i, m := range msgs {
		if futures[i], err = js.PublishMsgAsync(m, opts[i]...); err != nil {
			return nil, deliverError(ctx, fmt.Errorf("message %s: %w", envs[i].ID, err))
		}
	}
	acks := make([]*jetstream.PubAck, len(futures))
	var failed error
	for i, f := range futures {
		select {
		case acks[i] = <-f.Ok():
		case err := <-f.Err():
			if failed == nil {
				failed = fmt.Errorf("message %s: %w", envs[i].ID, err)
			}
		case <-ctx.Done():
			if err := refused(); err != nil {
				return nil, err
			}
			return nil, deliverError(ctx, fmt.Errorf("waiting for %d of %d acks: %w", len(futures)-i, len(futures), ctx.Err()))
		}
	}
	if failed != nil {
		if err := refused(); err != nil {
			return nil, err
		}
		return nil, deliverError(ctx, failed)
	}
	return s.result(c, acks), nil
}

// publishOptions are a message's publish options: its Nats-Msg-Id (the
// envelope ID, or the Nats-Msg-Id the destination's headers set) and the
// stream it must land in.
func (s *StreamSink) publishOptions(env *envelope.Envelope, msg *nats.Msg) []jetstream.PublishOpt {
	id := headerValue(msg.Header, headerMsgID)
	if id == "" {
		id = env.ID
	}
	delHeader(msg.Header, headerMsgID)
	opts := []jetstream.PublishOpt{jetstream.WithMsgID(id)}
	if s.opts.stream != "" {
		opts = append(opts, jetstream.WithExpectStream(s.opts.stream))
	}
	return opts
}

// result reports where the messages were stored: one value per message.
func (s *StreamSink) result(c *conn, acks []*jetstream.PubAck) *plugin.Result {
	h := map[string][]string{}
	for _, a := range acks {
		h[resultStream] = append(h[resultStream], a.Stream)
		h[resultSequence] = append(h[resultSequence], strconv.FormatUint(a.Sequence, 10))
		h[resultDuplicate] = append(h[resultDuplicate], strconv.FormatBool(a.Duplicate))
	}
	return &plugin.Result{Headers: h, Endpoint: c.url()}
}

// withDeadline gives ctx a deadline when it has none.
func withDeadline(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
