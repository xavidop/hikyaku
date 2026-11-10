package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a jetstream source; tests shorten them.
const (
	// defaultAPITimeout bounds one JetStream API call: setting the consumer
	// up, a health check.
	defaultAPITimeout = 10 * time.Second
	// defaultAckTimeout bounds how long an ack waits for the server to
	// confirm it.
	defaultAckTimeout = 5 * time.Second
	// defaultDrainWait bounds how long stopping waits for the consumer to
	// hand back what it buffered.
	defaultDrainWait = 5 * time.Second
	// healthTimeout bounds the account check of a health check, which must
	// stay cheap.
	healthTimeout = 2 * time.Second
)

// StreamSource is a jetstream source: a durable pull consumer on a stream.
// See the package comment for its acknowledgements.
type StreamSource struct {
	name string
	opts sourceOptions
	log  *slog.Logger
	rnd  func() float64

	progressEvery time.Duration // how often a busy message is reported in progress (ack_wait/2)
	apiTimeout    time.Duration
	ackTimeout    time.Duration
	drainWait     time.Duration

	started atomic.Bool
	flights inflight[*flight]

	mu       sync.Mutex
	conn     *conn
	js       jetstream.JetStream
	pending  chan *flight // received, on their way to the relay
	setupErr error        // why the consumer is not set up
	stopped  bool
	closing  bool
	late     []jetstream.Msg // received while stopping; given back once the consumer is closed
}

// flight is a received message until it is settled.
type flight struct {
	msg       jetstream.Msg
	env       *envelope.Envelope
	delivered uint64 // the server's delivery count

	mu    sync.Mutex
	done  bool
	timer *time.Timer // reports the message in progress
}

func newStreamSource(spec plugin.Spec) (*StreamSource, error) {
	opts, err := parseSource(spec, KindJetStream)
	if err != nil {
		return nil, err
	}
	return &StreamSource{
		name: spec.Name, opts: opts, log: spec.Env.Log(), rnd: rand.Float64,
		progressEvery: opts.ackWait / 2, apiTimeout: defaultAPITimeout,
		ackTimeout: defaultAckTimeout, drainWait: defaultDrainWait,
	}, nil
}

// Name is the source name.
func (s *StreamSource) Name() string { return s.name }

// Health is nil while the source is connected, its consumer is set up and
// the account answers (AccountInfo), else why not.
func (s *StreamSource) Health(ctx context.Context) error {
	s.mu.Lock()
	c, js, setupErr, stopped := s.conn, s.js, s.setupErr, s.stopped
	s.mu.Unlock()
	switch {
	case c == nil:
		return errNotStarted
	case stopped:
		return errStopped
	}
	if err := c.health(); err != nil {
		return err
	}
	if setupErr != nil {
		return fmt.Errorf("jetstream consumer %s on stream %s: %w", s.opts.durable, s.opts.stream, setupErr)
	}
	actx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	if _, err := js.AccountInfo(actx); err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	return nil
}

// Start creates or updates the durable consumer and hands every message to
// the relay until ctx ends, then returns nil. A consumer that cannot be set
// up (the stream does not exist yet, the server is down) is retried with
// the reconnect backoff; Start fails once reconnect.max_attempts setups in
// a row failed, or the connection gave up.
func (s *StreamSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	defer s.stop()
	c, err := dial(s.opts.conn, s.log, "source", s.name, nil)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	js, err := jetstream.New(c.nc)
	if err != nil {
		c.nc.Close()
		return fmt.Errorf("jetstream: %w", err)
	}
	pending := make(chan *flight, s.opts.prefetch)
	if !s.attach(c, js, pending) {
		c.nc.Close()
		return nil
	}
	// The pump stops when Start returns, whatever the reason.
	pctx, stopPump := context.WithCancel(ctx)
	pumped := make(chan struct{})
	go s.pump(pctx, pending, out, pumped)
	defer func() {
		stopPump()
		<-pumped
		s.giveBack(pending)
	}()

	rc := s.opts.conn.reconnect
	failures := 0
	for {
		cons, err := s.setup(ctx)
		if err == nil {
			failures = 0
			s.setSetupErr(nil)
			if s.consume(ctx, cons, pending) {
				return nil
			}
			// The consumption ended by itself: set the consumer up again
			// after a pause.
			if !backoff.Sleep(ctx, backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, 1, s.rnd)) {
				return nil
			}
			continue
		}
		switch {
		case ctx.Err() != nil, s.isClosing():
			return nil
		case c.isClosed(), errors.Is(err, nats.ErrConnectionClosed):
			return s.gaveUp(c)
		}
		failures++
		s.setSetupErr(err)
		if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
			return fmt.Errorf("giving up after %d failed attempts to set up consumer %s on stream %s: %w",
				failures, s.opts.durable, s.opts.stream, err)
		}
		wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, failures, s.rnd)
		s.log.Warn("jetstream consumer setup failed; retrying", "source", s.name, "stream", s.opts.stream,
			"durable", s.opts.durable, "failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			return nil
		}
	}
}

// setup creates the durable consumer, or updates it. An existing consumer
// keeps what it was created with (where it started, and settings an
// operator changed); the relay sets what it relies on: explicit acks,
// ack_wait, max_ack_pending and the filter subject.
func (s *StreamSource) setup(ctx context.Context) (jetstream.Consumer, error) {
	actx, cancel := context.WithTimeout(ctx, s.apiTimeout)
	defer cancel()
	cfg := jetstream.ConsumerConfig{Durable: s.opts.durable, DeliverPolicy: s.opts.deliver}
	cur, err := s.js.Consumer(actx, s.opts.stream, s.opts.durable)
	switch {
	case err == nil:
		if info := cur.CachedInfo(); info != nil {
			cfg = info.Config
		}
	case !errors.Is(err, jetstream.ErrConsumerNotFound):
		return nil, err
	}
	cfg.AckPolicy = jetstream.AckExplicitPolicy
	cfg.AckWait = s.opts.ackWait
	cfg.MaxAckPending = s.opts.prefetch
	cfg.FilterSubject, cfg.FilterSubjects = s.opts.subject, nil
	return s.js.CreateOrUpdateConsumer(actx, s.opts.stream, cfg)
}

// consume reads the consumer until ctx ends (true) or the consumption
// stops by itself (false): the consumer was deleted, the server lost it,
// or the connection closed.
func (s *StreamSource) consume(ctx context.Context, cons jetstream.Consumer, pending chan<- *flight) bool {
	recheck := make(chan struct{}, 1)
	cc, err := cons.Consume(func(m jetstream.Msg) { s.receive(ctx, m, pending) },
		jetstream.PullMaxMessages(s.opts.prefetch),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) { s.consumeError(err, recheck) }))
	if err != nil {
		s.setSetupErr(err)
		s.log.Warn("jetstream consume failed", "source", s.name, "stream", s.opts.stream, "durable", s.opts.durable, "err", err)
		return false
	}
	closed := cc.Closed()
	s.log.Info("jetstream source consuming", "source", s.name, "stream", s.opts.stream,
		"durable", s.opts.durable, "subject", s.opts.subject)
	for {
		select {
		case <-ctx.Done():
			// Drain: what the consumer buffered goes through receive,
			// which sets it aside. Once the consumer is closed, the
			// messages are given back to the server: a nak before that
			// could land the redelivery on this consumer, still pulling.
			cc.Drain()
			s.waitClosed(closed)
			s.giveBackLate()
			return true
		case <-closed:
			s.log.Warn("jetstream consumption stopped; setting the consumer up again", "source", s.name,
				"stream", s.opts.stream, "durable", s.opts.durable)
			return false
		case <-recheck:
			ictx, cancel := context.WithTimeout(ctx, s.apiTimeout)
			_, err := cons.Info(ictx)
			cancel()
			if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				s.log.Warn("jetstream consumer is gone; setting it up again", "source", s.name,
					"stream", s.opts.stream, "durable", s.opts.durable, "err", err)
				cc.Stop()
				s.waitClosed(closed)
				return false
			}
		}
	}
}

// consumeError hears what the consumption runs into. Missing heartbeats
// and pulls nobody answers can mean the server lost the consumer, so the
// consumer is checked.
func (s *StreamSource) consumeError(err error, recheck chan<- struct{}) {
	switch {
	case errors.Is(err, jetstream.ErrNoHeartbeat), errors.Is(err, nats.ErrNoResponders):
		select {
		case recheck <- struct{}{}:
		default:
		}
		s.log.Debug("jetstream consume", "source", s.name, "err", err)
	case errors.Is(err, jetstream.ErrConsumerDeleted), errors.Is(err, jetstream.ErrBadRequest):
		s.log.Warn("jetstream consume", "source", s.name, "stream", s.opts.stream, "durable", s.opts.durable, "err", err)
	default:
		s.log.Debug("jetstream consume", "source", s.name, "err", err)
	}
}

func (s *StreamSource) waitClosed(closed <-chan struct{}) {
	t := time.NewTimer(s.drainWait)
	defer t.Stop()
	select {
	case <-closed:
	case <-t.C:
		s.log.Warn("jetstream consumer did not stop in time", "source", s.name, "wait", s.drainWait.String())
	}
}

// receive takes one message from the consumer. It starts reporting the
// message in progress at once, and queues it for the relay (pump), so the
// consumer never waits on the relay with messages whose ack_wait runs.
func (s *StreamSource) receive(ctx context.Context, m jetstream.Msg, pending chan<- *flight) {
	if int64(len(m.Data())) > s.opts.maxBytes {
		// Redelivering it would fail the same way, forever.
		s.log.Warn("jetstream message over max_body_bytes terminated", "source", s.name, "subject", m.Subject(),
			"bytes", len(m.Data()), "max_body_bytes", s.opts.maxBytes)
		if err := m.TermWithReason("hikyaku: message exceeds max_body_bytes"); err != nil {
			s.log.Warn("jetstream term failed", "source", s.name, "err", err)
		}
		return
	}
	if ctx.Err() != nil {
		s.setAside(m) // stopping: the server redelivers it once this consumer is closed
		return
	}
	f := s.track(m)
	select {
	case pending <- f:
	case <-ctx.Done():
		if f.finish() {
			s.flights.remove(f)
			s.setAside(f.msg)
		}
	}
}

// setAside keeps a message received while stopping for giveBackLate.
func (s *StreamSource) setAside(m jetstream.Msg) {
	s.mu.Lock()
	s.late = append(s.late, m)
	s.mu.Unlock()
}

// giveBackLate naks the messages set aside while stopping, now that the
// consumer is closed and cannot be handed their redelivery.
func (s *StreamSource) giveBackLate() {
	s.mu.Lock()
	late := s.late
	s.late = nil
	s.mu.Unlock()
	for _, m := range late {
		if err := m.Nak(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			s.log.Debug("jetstream nak failed", "source", s.name, "subject", m.Subject(), "err", err)
		}
	}
}

// track turns a message into an envelope and keeps it in flight until it
// is settled, reporting it in progress every ack_wait/2.
func (s *StreamSource) track(m jetstream.Msg) *flight {
	env := newEnvelope(s.name, KindJetStream, m.Subject(), m.Headers(), m.Data())
	env.Attributes["jetstream.subject"] = m.Subject()
	f := &flight{msg: m, env: env}
	if meta, err := m.Metadata(); err == nil {
		f.delivered = meta.NumDelivered
		env.ID = fmt.Sprintf("%s-%d", meta.Stream, meta.Sequence.Stream)
		env.Attributes["jetstream.stream"] = meta.Stream
		env.Attributes["jetstream.sequence"] = int64(meta.Sequence.Stream)
		env.Attributes["jetstream.consumer"] = meta.Consumer
		env.Attributes["jetstream.delivered"] = int64(meta.NumDelivered)
	} else {
		s.log.Warn("jetstream message without metadata", "source", s.name, "subject", m.Subject(), "err", err)
	}
	if id := headerValue(m.Headers(), headerMsgID); id != "" && len(id) <= maxIDLen {
		env.ID = id
	}
	s.flights.add(f)
	f.mu.Lock()
	f.timer = time.AfterFunc(s.progressEvery, func() { s.progress(f) })
	f.mu.Unlock()
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) { s.settle(f, final) })
	return f
}

// progress tells the server the relay is still working on the message, so
// it does not redeliver it when ack_wait runs out.
func (s *StreamSource) progress(f *flight) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.done {
		return
	}
	if err := f.msg.InProgress(); err != nil {
		s.log.Debug("jetstream in-progress report failed", "source", s.name, "id", f.env.ID, "err", err)
	}
	f.timer.Reset(s.progressEvery)
}

// finish marks the message settled, once: false when it already was.
func (f *flight) finish() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.done {
		return false
	}
	f.done = true
	if f.timer != nil {
		f.timer.Stop()
	}
	return true
}

// settle is the message's acknowledgement: ack when its outcome is final,
// else nak with a delay that grows with its delivery count, so the server
// redelivers it later.
func (s *StreamSource) settle(f *flight, final bool) {
	if !f.finish() {
		return
	}
	defer s.flights.remove(f)
	if final {
		s.ack(f)
		return
	}
	rc := s.opts.conn.reconnect
	delay := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, int(min(max(f.delivered, 1), 64)), s.rnd)
	if err := f.msg.NakWithDelay(delay); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		s.log.Warn("jetstream nak failed; the server redelivers the message after ack_wait", "source", s.name,
			"id", f.env.ID, "err", err)
	}
}

// ack acknowledges a message. With ack: delivered it waits for the server
// to confirm (a double ack); with ack: accepted it runs as the relay takes
// the message and does not hold it up.
func (s *StreamSource) ack(f *flight) {
	var err error
	if s.opts.ack == ackAccepted {
		err = f.msg.Ack()
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), s.ackTimeout)
		err = f.msg.DoubleAck(ctx)
		cancel()
	}
	if err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		s.log.Warn("jetstream ack failed; the server may redeliver the message", "source", s.name,
			"id", f.env.ID, "err", err)
	}
}

// giveUp gives a message back to the server at once (a nak without delay):
// the relay is stopping and did not take it, or did not finish it.
func (s *StreamSource) giveUp(f *flight) {
	if !f.finish() {
		return
	}
	defer s.flights.remove(f)
	if err := f.msg.Nak(); err != nil && !errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
		s.log.Debug("jetstream nak failed", "source", s.name, "id", f.env.ID, "err", err)
	}
}

// pump hands received messages to the relay in order, waiting while it is
// busy (backpressure), until ctx ends.
func (s *StreamSource) pump(ctx context.Context, pending <-chan *flight, out chan<- *envelope.Envelope, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case f := <-pending:
			select {
			case out <- f.env:
			case <-ctx.Done():
				s.giveUp(f)
			}
		case <-ctx.Done():
			return
		}
	}
}

// giveBack gives back the messages still waiting for the pump.
func (s *StreamSource) giveBack(pending <-chan *flight) {
	for {
		select {
		case f := <-pending:
			s.giveUp(f)
		default:
			return
		}
	}
}

// gaveUp is Start's failure once the connection closed by itself.
func (s *StreamSource) gaveUp(c *conn) error { return gaveUp(c, s.opts.conn.reconnect.maxAttempts) }

func (s *StreamSource) attach(c *conn, js jetstream.JetStream, pending chan *flight) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conn, s.js, s.pending = c, js, pending
	return true
}

func (s *StreamSource) setSetupErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setupErr = err
}

func (s *StreamSource) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
}

func (s *StreamSource) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// Close waits until the relay has settled the messages it took, so their
// acks go out, then drains and closes the connection. Messages still
// unsettled when ctx ends are given back to the server at once.
func (s *StreamSource) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	c, pending := s.conn, s.pending
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	s.giveBack(pending)
	if err := s.flights.wait(ctx); err != nil {
		left := s.flights.snapshot()
		s.log.Warn("jetstream source closing before the relay finished its messages; giving them back",
			"source", s.name, "messages", len(left))
		for _, f := range left {
			s.giveUp(f)
		}
	}
	c.close(ctx)
	return nil
}
