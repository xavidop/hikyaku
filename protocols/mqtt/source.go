package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a source; tests shorten them.
const (
	defaultSubscribeTimeout  = 10 * time.Second
	defaultDisconnectTimeout = 5 * time.Second
	// defaultStableAfter: a connection up this long resets the failure count
	// even if it delivered nothing.
	defaultStableAfter = 30 * time.Second
	// maxIDBytes caps an ID taken from a message.
	maxIDBytes = 256
)

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
	errConnecting = errors.New("connecting")
)

// Source is an mqtt source: a subscription that hands every message to the
// relay and acknowledges it once settled.
type Source struct {
	name    string
	opts    sourceOptions
	connect connectFunc
	log     *slog.Logger
	rnd     func() float64

	subscribeTimeout  time.Duration
	disconnectTimeout time.Duration
	stableAfter       time.Duration

	started atomic.Bool
	// stopCtx ends when Close is called, so a source closed while it runs
	// stops too.
	stopCtx  context.Context
	stopFunc context.CancelFunc

	// handoff guards the hand-over of messages to the relay: once Start
	// returns, stopped is set, and no message reaches out any more.
	handoff sync.RWMutex
	stopped bool

	mu         sync.Mutex
	status     error // nil while connected
	cur        *session
	subscribed bool // subscribed at least once in this process
	closed     bool
}

// session is one connection and its acknowledgement queue.
type session struct {
	c      client
	q      *ackQueue
	srv    server
	failed chan struct{} // closed on the first failed message
}

func newSource(_ context.Context, spec plugin.Spec) (*Source, error) {
	opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	stopCtx, stopFunc := context.WithCancel(context.Background())
	return &Source{
		name: spec.Name, opts: opts, connect: connectorFor(opts.conn.version), log: spec.Env.Log(),
		rnd:              rand.Float64,
		subscribeTimeout: defaultSubscribeTimeout, disconnectTimeout: defaultDisconnectTimeout,
		stableAfter: defaultStableAfter,
		stopCtx:     stopCtx, stopFunc: stopFunc,
		status: errNotStarted,
	}, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is nil while the source is connected, else why not.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur != nil && !closed(s.cur.c.Lost()) {
		return nil
	}
	if s.status == nil {
		return errNotConnected
	}
	return s.status
}

func (s *Source) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Start subscribes until ctx ends, then returns nil, keeping the connection
// so the messages in flight can still be acknowledged (Close disconnects).
// It reconnects with backoff, rotating through failover_urls, and fails only
// once reconnect.max_attempts connects in a row have failed.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	// Deferred calls run last first: ctx ends, which releases a handler
	// waiting to hand a message over, before the hand-over is shut.
	defer s.stopHandoff()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.stopCtx, cancel)
	defer stop()

	rc := s.opts.reconnect
	failures, forced, i := 0, 0, 0
	for {
		st, err := s.run(ctx, s.opts.servers[i], out, forced)
		if ctx.Err() != nil {
			s.setStatus(errStopped)
			return nil
		}
		s.setStatus(err)
		srv := s.opts.servers[i]
		if st.forced {
			// The session waited its backoff before dropping the connection:
			// reconnect now, and the broker redelivers what failed.
			if st.acked > 0 {
				forced = 0
			}
			forced++
			failures = 0
			s.log.Info("mqtt source reconnecting so the broker redelivers a failed message",
				"source", s.name, "url", srv.shown)
			continue
		}
		forced = 0
		if st.connected && (st.delivered > 0 || st.uptime >= s.stableAfter) {
			failures = 0
		} else {
			failures++
			if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
				return fmt.Errorf("giving up after %d failed connects in a row: %w", failures, err)
			}
			i = (i + 1) % len(s.opts.servers)
		}
		wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, max(failures, 1), s.rnd)
		s.log.Warn("mqtt source reconnecting", "source", s.name, "url", s.opts.servers[i].shown,
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// stopHandoff makes sure no message reaches the relay once Start returned.
func (s *Source) stopHandoff() {
	s.handoff.Lock()
	defer s.handoff.Unlock()
	s.stopped = true
}

// runStats is what one connection did.
type runStats struct {
	connected bool
	forced    bool // dropped on purpose after a failed message
	delivered int64
	acked     int
	uptime    time.Duration
}

// run connects to srv, subscribes, and receives until the connection ends,
// a message fails (the connection is then dropped after a backoff), or ctx
// ends (the connection is then kept for Close).
func (s *Source) run(ctx context.Context, srv server, out chan<- *envelope.Envelope, forced int) (runStats, error) {
	ss := &session{srv: srv, failed: make(chan struct{})}
	var failOnce sync.Once
	ss.q = newAckQueue(func() { failOnce.Do(func() { close(ss.failed) }) })
	var delivered atomic.Int64
	onMessage := func(m *message) {
		if s.receive(ctx, ss.q, m, out) {
			delivered.Add(1)
		}
	}
	s.setStatus(errConnecting)
	cctx, cancel := context.WithTimeout(ctx, s.opts.conn.connectTimeout)
	c, err := s.connect(cctx, srv, s.opts.conn, onMessage)
	cancel()
	if err != nil {
		ss.q.close()
		return runStats{}, err
	}
	ss.c = c
	if err := s.subscribe(ctx, ss); err != nil {
		if !c.SessionPresent() || closed(c.Lost()) {
			s.drop(ss)
			return runStats{connected: true}, err
		}
		// A resumed session keeps its subscriptions: carry on with them. The
		// next connection tries again to apply the configured filters.
		s.log.Warn("mqtt source could not subscribe again; the resumed session keeps its subscriptions",
			"source", s.name, "url", srv.shown, "err", err)
	}
	if !s.adopt(ss) {
		s.drop(ss) // closed meanwhile
		return runStats{}, errStopped
	}
	s.log.Info("mqtt source connected", "source", s.name, "url", srv.shown,
		"session_present", c.SessionPresent(), "filters", s.opts.filters)
	start := time.Now()
	stats := func(forced bool) runStats {
		return runStats{connected: true, forced: forced, delivered: delivered.Load(), acked: ss.q.ackedCount(), uptime: time.Since(start)}
	}
	lost := func() (runStats, error) {
		why := lostReason(c) // before drop, whose disconnect would be the reason
		s.drop(ss)
		return stats(false), fmt.Errorf("%s: %w", srv.shown, why)
	}
	select {
	case <-ctx.Done():
		return stats(false), nil
	case <-c.Lost():
		return lost()
	case <-ss.failed:
	}
	// A message failed: MQTT cannot nack it, so drop the connection after a
	// backoff. Messages before it may still settle and be acknowledged
	// meanwhile; it and every later one are redelivered.
	n := forced + 1
	if ss.q.ackedCount() > 0 {
		n = 1
	}
	rc := s.opts.reconnect
	wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, n, s.rnd)
	s.log.Warn("mqtt message failed; it and every later message stay unacknowledged until the source reconnects",
		"source", s.name, "url", srv.shown, "wait", wait.String())
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return stats(false), nil
	case <-c.Lost():
		return lost()
	case <-t.C:
	}
	s.drop(ss)
	return stats(true), errors.New("reconnecting so the broker redelivers a failed message")
}

// subscribe subscribes on the first connection of the process and whenever
// the broker has no session (it remembers the subscriptions of one).
func (s *Source) subscribe(ctx context.Context, ss *session) error {
	s.mu.Lock()
	need := !s.subscribed || !ss.c.SessionPresent()
	s.mu.Unlock()
	if !need {
		return nil
	}
	sctx, cancel := context.WithTimeout(ctx, s.subscribeTimeout)
	defer cancel()
	if err := ss.c.Subscribe(sctx, s.opts.filters, s.opts.qos); err != nil {
		return fmt.Errorf("%s: %w", ss.srv.shown, err)
	}
	s.mu.Lock()
	s.subscribed = true
	s.mu.Unlock()
	return nil
}

// adopt makes ss the current session, unless the source is closed.
func (s *Source) adopt(ss *session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.cur, s.status = ss, nil
	return true
}

// drop ends a session: nothing more is acknowledged on it, and it is
// disconnected.
func (s *Source) drop(ss *session) {
	s.mu.Lock()
	if s.cur == ss {
		s.cur = nil
	}
	s.mu.Unlock()
	ss.q.close()
	ctx, cancel := context.WithTimeout(context.Background(), s.disconnectTimeout)
	defer cancel()
	_ = ss.c.Disconnect(ctx)
}

// lostReason is why a client's connection ended, waiting briefly for the
// client library to say.
func lostReason(c client) error {
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-c.Done():
		return c.Err()
	case <-t.C:
		return errNotConnected
	}
}

// receive hands one message to the relay and reports whether it did. A
// QoS 1 or 2 message joins the acknowledgement queue first; one that cannot
// be handed over (the source stopped, the connection is gone, or an earlier
// message failed) stays unacknowledged, and the broker redelivers it.
func (s *Source) receive(ctx context.Context, q *ackQueue, m *message, out chan<- *envelope.Envelope) bool {
	s.handoff.RLock()
	defer s.handoff.RUnlock()
	if s.stopped || ctx.Err() != nil {
		return false
	}
	var e *ackEntry
	if m.qos > 0 {
		if q.failedAlready() {
			return false // redelivered after the reconnect
		}
		e = q.add(m)
	}
	if int64(len(m.payload)) > s.opts.maxBytes {
		s.log.Warn("mqtt message over max_body_bytes dropped", "source", s.name, "topic", m.topic,
			"bytes", len(m.payload), "max_body_bytes", s.opts.maxBytes)
		if e != nil {
			q.settle(e, true) // acknowledged: redelivering it would loop forever
		}
		return false
	}
	env := s.envelope(m)
	if e != nil {
		plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) { q.settle(e, final) })
	}
	select {
	case out <- env:
		return true
	case <-ctx.Done():
	case <-m.lost:
	}
	if e != nil {
		q.drop(e)
	}
	return false
}

// envelope turns a message into an envelope.
func (s *Source) envelope(m *message) *envelope.Envelope {
	env := envelope.New(s.name, kind, m.payload)
	env.Subject = m.topic
	for _, p := range m.userProps {
		k := textproto.CanonicalMIMEHeaderKey(p.key)
		env.Headers[k] = append(env.Headers[k], p.value)
	}
	if id := messageID(m); id != "" {
		env.ID = id
	}
	env.ContentType = m.contentType
	if env.ContentType == "" {
		env.ContentType = detectContentType(m.payload)
	}
	env.Attributes["mqtt.topic"] = m.topic
	env.Attributes["mqtt.qos"] = int(m.qos)
	env.Attributes["mqtt.retained"] = m.retained
	env.Attributes["mqtt.duplicate"] = m.duplicate
	env.Attributes["mqtt.packet_id"] = int(m.packetID)
	env.Attributes["ordering_key"] = m.topic
	return env
}

// messageID is the ID a message carries: its Idempotency-Key user property,
// else its correlation data when that is text. "" when it has neither.
func messageID(m *message) string {
	for _, p := range m.userProps {
		if strings.EqualFold(p.key, "Idempotency-Key") && usableID(p.value) {
			return p.value
		}
	}
	if id := string(m.correlation); usableID(id) {
		return id
	}
	return ""
}

func usableID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= maxIDBytes && utf8.ValidString(id) && !strings.ContainsRune(id, 0)
}

// Close disconnects, once the messages handed to the relay are settled and
// acknowledged (or ctx ends). The broker keeps a persistent session and
// redelivers whatever is left unacknowledged.
func (s *Source) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ss := s.cur
	s.cur = nil
	s.status = errStopped
	s.mu.Unlock()
	s.stopFunc()
	if ss == nil {
		return nil
	}
	_ = ss.q.waitIdle(ctx)
	ss.q.close()
	// The DISCONNECT gets its own short budget: waiting for the messages in
	// flight may have used up ctx.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.disconnectTimeout)
	defer cancel()
	if err := ss.c.Disconnect(dctx); err != nil {
		return fmt.Errorf("source %s: disconnect: %w", s.name, err)
	}
	return nil
}
