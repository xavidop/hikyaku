package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// maxIDLen bounds an envelope ID taken from a header; a longer value is
// ignored and the envelope gets a ULID.
const maxIDLen = 256

// Source is a nats source: a core NATS subscription, in a queue group when
// queue is set. Core NATS has no acknowledgements, so delivery is at most
// once: a message the relay fails, or has not taken when it stops, is lost
// and logged.
type Source struct {
	name string
	opts sourceOptions
	log  *slog.Logger

	started atomic.Bool
	lost    atomic.Int64 // failed, cut by a shutdown, or too large
	flights inflight[*envelope.Envelope]

	mu      sync.Mutex
	conn    *conn
	stopped bool
	closing bool
}

func newSource(spec plugin.Spec) (*Source, error) {
	opts, err := parseSource(spec, KindNATS)
	if err != nil {
		return nil, err
	}
	return &Source{name: spec.Name, opts: opts, log: spec.Env.Log()}, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is nil while the source is subscribed and connected, else why
// not.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	c, stopped := s.conn, s.stopped
	s.mu.Unlock()
	switch {
	case c == nil:
		return errNotStarted
	case stopped:
		return errStopped
	}
	return c.health()
}

// Lost counts the messages the source could not keep: failed by the
// relay, cut by a shutdown, or over max_body_bytes.
func (s *Source) Lost() int64 { return s.lost.Load() }

// Dropped counts the messages the server sent faster than the relay took
// them, past the subscription's pending limits (prefetch messages or
// 64MiB): nats.go drops them as a slow consumer.
func (s *Source) Dropped() int64 {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return 0
	}
	return c.dropped.Load()
}

// Start subscribes and hands every message to the relay until ctx ends,
// then returns nil. It fails when the server refuses the subscription, or
// when the connection gave up after reconnect.max_attempts failed connects
// to each server.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	defer s.stop()
	lctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	c, err := dial(s.opts.conn, s.log, "source", s.name, func(err error) {
		if deniedSubscription(err, s.opts.subject, s.opts.queue) {
			cancel(fmt.Errorf("subscribe to %s: %w", s.opts.subject, err))
		}
	})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if !s.attach(c) {
		c.nc.Close()
		return nil
	}
	// A synchronous subscription: nats.go applies pending limits to it
	// (not to a channel one), and NextMsg waits on the relay's context.
	var sub *nats.Subscription
	if s.opts.queue != "" {
		sub, err = c.nc.QueueSubscribeSync(s.opts.subject, s.opts.queue)
	} else {
		sub, err = c.nc.SubscribeSync(s.opts.subject)
	}
	if err != nil {
		if s.isClosing() {
			return nil
		}
		return fmt.Errorf("subscribe to %s: %w", s.opts.subject, err)
	}
	defer s.unsubscribe(c, sub)
	if err := sub.SetPendingLimits(s.opts.prefetch, defaultPendingBytes); err != nil {
		return fmt.Errorf("subscribe to %s: %w", s.opts.subject, err)
	}
	s.log.Info("nats source subscribed", "source", s.name, "subject", s.opts.subject, "queue", s.opts.queue)
	for {
		msg, err := sub.NextMsgWithContext(lctx)
		if err == nil {
			s.handle(lctx, msg, out)
			continue
		}
		switch {
		case ctx.Err() != nil, s.isClosing():
			return nil
		case lctx.Err() != nil:
			return context.Cause(lctx)
		case errors.Is(err, nats.ErrSlowConsumer):
			// Logged by the error handler; count what was dropped so far.
			if n, err := sub.Dropped(); err == nil {
				c.dropped.Store(int64(n))
			}
		case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrBadSubscription):
			return s.gaveUp(c)
		default:
			return fmt.Errorf("subscription to %s: %w", s.opts.subject, err)
		}
	}
}

// handle hands one message to the relay.
func (s *Source) handle(ctx context.Context, msg *nats.Msg, out chan<- *envelope.Envelope) {
	if int64(len(msg.Data)) > s.opts.maxBytes {
		s.lost.Add(1)
		s.log.Warn("nats message over max_body_bytes dropped", "source", s.name, "subject", msg.Subject,
			"bytes", len(msg.Data), "max_body_bytes", s.opts.maxBytes)
		return
	}
	env := newEnvelope(s.name, KindNATS, msg.Subject, msg.Header, msg.Data)
	for _, h := range []string{headerMsgID, headerIdempotency} {
		if id := headerValue(msg.Header, h); id != "" && len(id) <= maxIDLen {
			env.ID = id
			break
		}
	}
	env.Attributes["nats.subject"] = msg.Subject
	if msg.Reply != "" {
		env.Attributes["nats.reply"] = msg.Reply
	}
	s.flights.add(env)
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) { s.settle(env, final) })
	select {
	case out <- env:
	case <-ctx.Done():
		// Stopping before the relay took it: core NATS cannot redeliver it.
		s.flights.remove(env)
		s.lost.Add(1)
	}
}

// settle is the message's acknowledgement. Core NATS has none to send: a
// message that failed is lost, and logged.
func (s *Source) settle(env *envelope.Envelope, final bool) {
	s.flights.remove(env)
	if !final {
		s.lost.Add(1)
		s.log.Warn("nats message failed and is lost: core NATS cannot redeliver it (use jetstream for at-least-once)",
			"source", s.name, "id", env.ID, "subject", env.Subject)
	}
}

// unsubscribe ends the subscription when Start returns. What it still
// holds for the relay is dropped: core NATS cannot redeliver it.
func (s *Source) unsubscribe(c *conn, sub *nats.Subscription) {
	if n, _, err := sub.Pending(); err == nil && n > 0 {
		s.lost.Add(int64(n))
		s.log.Warn("nats source stopping: messages not yet handed to the relay are dropped (core NATS cannot redeliver them)",
			"source", s.name, "messages", n)
	}
	if n, err := sub.Dropped(); err == nil && n > 0 {
		c.dropped.Store(int64(n))
	}
	_ = sub.Unsubscribe()
}

// gaveUp is Start's failure once the connection closed by itself.
func (s *Source) gaveUp(c *conn) error { return gaveUp(c, s.opts.conn.reconnect.maxAttempts) }

// gaveUp is a source's failure once its connection closed by itself:
// reconnect.max_attempts reconnects to each server failed.
func gaveUp(c *conn, maxAttempts int) error {
	c.waitClosed()
	reason := c.reason()
	if reason == nil {
		reason = nats.ErrConnectionClosed
	}
	if maxAttempts > 0 {
		return fmt.Errorf("gave up after reconnect.max_attempts (%d) reconnects to each server: %w", maxAttempts, reason)
	}
	return fmt.Errorf("connection closed: %w", reason)
}

func (s *Source) attach(c *conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conn = c
	return true
}

func (s *Source) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

func (s *Source) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Close waits until the relay has settled the messages it took (or ctx
// ends), then drains and closes the connection.
func (s *Source) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	if err := s.flights.wait(ctx); err != nil {
		s.log.Warn("nats source closing before the relay finished its messages", "source", s.name, "messages", s.flights.len())
	}
	c.close(ctx)
	return nil
}
