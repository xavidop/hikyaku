package amqp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a source; tests shorten them.
const (
	// defaultStableAfter: a connection up this long resets the failure
	// count even if it delivered nothing.
	defaultStableAfter = 30 * time.Second
	// defaultNackBackoff and defaultNackMaxBackoff bound the delay before a
	// failed message goes back to the queue, growing with its failed
	// attempts (equal jitter, so it never drops near zero).
	defaultNackBackoff    = 500 * time.Millisecond
	defaultNackMaxBackoff = 10 * time.Second
	// defaultStopWait bounds the consumer cancel at shutdown, and the drain
	// of what the client already received after it.
	defaultStopWait = 5 * time.Second
	// defaultCloseWait bounds the close handshake when Close's context has
	// no deadline.
	defaultCloseWait = 5 * time.Second
)

// maxTracked bounds the failed-attempt counts a source remembers.
const maxTracked = 10000

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
	errSourceGone = errors.New("closed")
)

// Source is an amqp source: one queue, consumed with manual acks.
type Source struct {
	name string
	opts sourceOptions
	log  *slog.Logger
	dial dialFunc
	rnd  func() float64

	stableAfter    time.Duration
	nackBackoff    time.Duration
	nackMaxBackoff time.Duration
	stopWait       time.Duration

	started atomic.Bool

	mu       sync.Mutex
	status   error
	sess     *session
	stopping bool // Start's context ended: failed messages go back at once
	closed   bool
	failures map[string]int // failed attempts by message, for the nack delay
}

func newSource(_ context.Context, spec plugin.Spec) (*Source, error) {
	opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	return &Source{
		name: spec.Name, opts: opts, log: spec.Env.Log(), dial: dialBroker, rnd: rand.Float64,
		stableAfter: defaultStableAfter, nackBackoff: defaultNackBackoff, nackMaxBackoff: defaultNackMaxBackoff,
		stopWait: defaultStopWait, status: errNotStarted,
	}, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is nil while the source consumes and the broker still has its
// queue (a passive declare, on a channel of its own so a missing queue
// closes that channel and not the consumer's), else why not.
func (s *Source) Health(ctx context.Context) error {
	s.mu.Lock()
	ss, status := s.sess, s.status
	s.mu.Unlock()
	if status != nil {
		return status
	}
	if ss == nil || ss.dead.Load() || ss.conn.IsClosed() {
		return errors.New("not connected")
	}
	done := make(chan error, 1)
	go func() {
		ch, err := ss.conn.Channel()
		if err != nil {
			done <- err
			return
		}
		_, err = ch.QueueDeclarePassive(s.opts.queue, false, false, false, false, nil)
		_ = ch.Close()
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("queue %s: %w", s.opts.queue, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Start consumes until ctx ends, then returns nil. When the connection or
// the channel closes it reconnects with backoff, rotating through url and
// failover_urls; it fails only when reconnect.max_attempts connects in a
// row failed.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	rc := s.opts.reconnect
	failures, i := 0, 0
	for {
		st, err := s.session(ctx, s.opts.urls[i], out)
		if ctx.Err() != nil || s.isClosed() {
			s.setStatus(errStopped)
			return nil
		}
		s.setStatus(err)
		if st.connected && (st.delivered > 0 || st.uptime >= s.stableAfter) {
			failures = 0
		} else {
			failures++
			if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
				return fmt.Errorf("giving up after %d failed connects in a row: %w", failures, err)
			}
			i = (i + 1) % len(s.opts.urls)
		}
		wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, max(failures, 1), s.rnd)
		s.log.Warn("amqp source reconnecting", "source", s.name, "url", redactURL(s.opts.urls[i]),
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// sessionStats is what one connection did.
type sessionStats struct {
	connected bool
	delivered int
	uptime    time.Duration
}

// session connects to rawURL and consumes until the connection or the
// channel ends, or ctx does.
func (s *Source) session(ctx context.Context, rawURL string, out chan<- *envelope.Envelope) (sessionStats, error) {
	ss, deliveries, err := s.open(ctx, rawURL)
	if err != nil {
		return sessionStats{}, err
	}
	s.log.Info("amqp source consuming", "source", s.name, "url", ss.url, "queue", s.opts.queue, "prefetch", s.opts.prefetch)
	start := time.Now()
	n, err := s.consume(ctx, ss, deliveries, out)
	return sessionStats{connected: true, delivered: n, uptime: time.Since(start)}, err
}

// open dials, opens a channel with the prefetch window, declares the
// topology when asked, and starts the consumer.
func (s *Source) open(ctx context.Context, rawURL string) (*session, <-chan amqp091.Delivery, error) {
	shown := redactURL(rawURL)
	c, err := s.dial(ctx, rawURL, s.opts.conn.config(rawURL))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", shown, err)
	}
	ss := &session{src: s, url: shown, conn: c, tag: s.opts.conn.name, timers: map[uint64]*time.Timer{}}
	ss.connClosed = c.NotifyClose(make(chan *amqp091.Error, 1))
	fail := func(what string, err error) (*session, <-chan amqp091.Delivery, error) {
		ss.close(time.Now().Add(defaultCloseWait))
		return nil, nil, fmt.Errorf("%s: %s: %w", shown, what, err)
	}
	ch, err := c.Channel()
	if err != nil {
		return fail("open a channel", err)
	}
	ss.ch = ch
	ss.chClosed = ch.NotifyClose(make(chan *amqp091.Error, 1))
	if err := ch.Qos(s.opts.prefetch, 0, false); err != nil {
		return fail("set the prefetch", err)
	}
	if s.opts.declare {
		if err := s.declare(ch); err != nil {
			return fail("declare", err)
		}
	}
	// The session is published before the consumer starts, so Health is
	// right as soon as the broker sees the consume: the relay is connected
	// by then, whatever it reported about the connects before.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ss.close(time.Now().Add(defaultCloseWait))
		return nil, nil, errSourceGone
	}
	s.sess, s.status = ss, nil
	s.mu.Unlock()
	deliveries, err := ch.Consume(s.opts.queue, ss.tag, false, false, false, false, nil)
	if err != nil {
		return fail("consume queue "+s.opts.queue, err)
	}
	return ss, deliveries, nil
}

// declare declares the queue (durable) and, with an exchange, the exchange
// (topic, durable) and the queue's binding to it.
func (s *Source) declare(ch channel) error {
	q := s.opts.queue
	if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
		return fmt.Errorf("queue %s: %w", q, err)
	}
	if ex := s.opts.exchange; ex != "" {
		if err := ch.ExchangeDeclare(ex, exchangeType, true, false, false, false, nil); err != nil {
			return fmt.Errorf("exchange %s: %w", ex, err)
		}
		if err := ch.QueueBind(q, s.opts.bindingKey, ex, false, nil); err != nil {
			return fmt.Errorf("binding of queue %s to exchange %s with %q: %w", q, ex, s.opts.bindingKey, err)
		}
	}
	return nil
}

// consume hands each delivery to the relay until the session ends or ctx
// does. It blocks while out is full: the prefetch window then holds the
// broker back. It returns the messages handed over (or acknowledged as
// oversize).
func (s *Source) consume(ctx context.Context, ss *session, deliveries <-chan amqp091.Delivery, out chan<- *envelope.Envelope) (int, error) {
	n := 0
	for {
		select {
		case <-ctx.Done():
			s.stop(ss, deliveries)
			return n, nil
		case e := <-ss.chClosed:
			return n, ss.ended("channel", e)
		case e := <-ss.connClosed:
			return n, ss.ended("connection", e)
		case d, ok := <-deliveries:
			if !ok {
				if e := ss.closeReason(); e != nil {
					return n, ss.ended("channel", e)
				}
				return n, ss.ended("consumer", nil)
			}
			if int64(len(d.Body)) > s.opts.maxBytes {
				s.log.Warn("amqp message over max_body_bytes acknowledged and dropped", "source", s.name, "queue", s.opts.queue,
					"delivery_tag", d.DeliveryTag, "message_id", d.MessageId, "bytes", len(d.Body), "max_body_bytes", s.opts.maxBytes)
				ss.ack(d.DeliveryTag)
				n++
				continue
			}
			env := s.envelope(ss, &d)
			select {
			case out <- env:
				n++
			case <-ctx.Done():
				ss.nack(d.DeliveryTag) // never handed over
				s.stop(ss, deliveries)
				return n, nil
			case e := <-ss.chClosed:
				return n, ss.ended("channel", e)
			case e := <-ss.connClosed:
				return n, ss.ended("connection", e)
			}
		}
	}
}

// stop runs when Start's context ends: pending nacks go out now, the
// consumer is cancelled, and the deliveries the client already received
// but never handed over go back to the queue. The session stays open for
// the acks of the messages in flight; Close closes it.
func (s *Source) stop(ss *session, deliveries <-chan amqp091.Delivery) {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	ss.flushNacks()
	cancelled := make(chan error, 1)
	go func() { cancelled <- ss.ch.Cancel(ss.tag, false) }()
	wait := time.NewTimer(s.stopWait)
	defer wait.Stop()
	select {
	case err := <-cancelled:
		if err != nil {
			s.log.Debug("amqp consumer cancel failed", "source", s.name, "err", err)
			return
		}
	case <-wait.C:
		s.log.Warn("amqp consumer cancel timed out; the broker redelivers what it sent once the connection closes", "source", s.name)
		return
	}
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return
			}
			ss.nack(d.DeliveryTag)
		case <-wait.C:
			return
		}
	}
}

// envelope turns a delivery into an envelope that settles with the broker
// once the relay is done with it.
func (s *Source) envelope(ss *session, d *amqp091.Delivery) *envelope.Envelope {
	env := envelope.New(s.name, kind, d.Body)
	fromMessage := true
	if d.MessageId != "" {
		env.ID = d.MessageId
	} else if key, ok := tableString(d.Headers, headerIdempotencyKey); ok && key != "" && len(key) <= 256 {
		env.ID = key
	} else {
		fromMessage = false
	}
	env.Subject = d.RoutingKey
	env.Headers = headersFromTable(d.Headers)
	if d.ContentType != "" {
		env.ContentType = d.ContentType
		env.SetHeader(headerContentType, d.ContentType)
	} else {
		env.ContentType = detectContentType(d.Body)
	}
	if d.ContentEncoding != "" {
		env.SetHeader(headerContentEncoding, d.ContentEncoding)
	}
	if d.CorrelationId != "" {
		env.SetHeader(headerCorrelationID, d.CorrelationId)
	}
	if d.ReplyTo != "" {
		env.SetHeader(headerReplyTo, d.ReplyTo)
	}
	env.Attributes["amqp.queue"] = s.opts.queue
	env.Attributes["amqp.exchange"] = d.Exchange
	env.Attributes["amqp.routing_key"] = d.RoutingKey
	env.Attributes["amqp.delivery_tag"] = int64(d.DeliveryTag)
	env.Attributes["amqp.redelivered"] = d.Redelivered
	if d.MessageId != "" {
		env.Attributes["amqp.message_id"] = d.MessageId
	}
	if !d.Timestamp.IsZero() {
		env.Attributes["amqp.timestamp"] = d.Timestamp.UTC().Format(time.RFC3339)
	}
	env.Attributes["ordering_key"] = s.opts.queue

	m := tracked{tag: d.DeliveryTag, exchange: d.Exchange, key: d.RoutingKey, body: d.Body, id: env.ID, fromMessage: fromMessage}
	if count, ok := tableInt(d.Headers, "x-delivery-count"); ok && count >= 0 {
		m.count = int(count) + 1
	}
	plugin.OnSettle(env, s.opts.ack, func(_ context.Context, final bool) { s.settle(ss, m, final) })
	return env
}

// tracked is what settling a message needs to know about it.
type tracked struct {
	tag         uint64
	exchange    string
	key         string
	body        []byte
	id          string
	fromMessage bool // id came from the message (message ID, Idempotency-Key)
	count       int  // the broker's delivery count (quorum queues) plus one, 0 when unknown
}

// failureKey identifies a message across redeliveries: its ID when the
// publisher gave it one, else a digest of where it was published and its
// body.
func (m tracked) failureKey() string {
	if m.fromMessage {
		return "id:" + m.id
	}
	h := sha256.New()
	h.Write([]byte(m.exchange))
	h.Write([]byte{0})
	h.Write([]byte(m.key))
	h.Write([]byte{0})
	h.Write(m.body)
	return "sum:" + hex.EncodeToString(h.Sum(nil))
}

// settle acknowledges a finished message, or returns a failed one to the
// queue after a delay that grows with its failed attempts.
func (s *Source) settle(ss *session, m tracked, final bool) {
	if final {
		s.forgetFailures(m)
		ss.ack(m.tag)
		return
	}
	attempt, stopping := s.failed(m)
	if stopping {
		ss.nack(m.tag)
		return
	}
	ss.nackAfter(m.tag, backoff.Delay(s.nackBackoff, s.nackMaxBackoff, "equal", attempt, s.rnd))
}

// failed counts a failed attempt of m and returns its number, and whether
// the source is stopping.
func (s *Source) failed(m tracked) (int, bool) {
	key := ""
	if m.count == 0 {
		key = m.failureKey()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.count > 0 {
		return m.count, s.stopping
	}
	if s.failures == nil || len(s.failures) >= maxTracked {
		s.failures = map[string]int{}
	}
	s.failures[key]++
	return s.failures[key], s.stopping
}

// forgetFailures drops the failed attempts of a message that finished.
func (s *Source) forgetFailures(m tracked) {
	s.mu.Lock()
	empty := len(s.failures) == 0
	s.mu.Unlock()
	if empty {
		return
	}
	key := m.failureKey()
	s.mu.Lock()
	delete(s.failures, key)
	s.mu.Unlock()
}

func (s *Source) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close releases the connection once the relay settled the messages in
// flight: pending nacks go out first, then the channel and the connection
// close (the broker requeues whatever is still unacknowledged).
func (s *Source) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ss := s.sess
	s.mu.Unlock()
	if ss == nil {
		return nil
	}
	ss.flushNacks()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultCloseWait)
	}
	ss.close(deadline)
	return nil
}

// session is one connection and its consuming channel. Acknowledgements go
// to the channel the message came on: delivery tags mean nothing on
// another one.
type session struct {
	src        *Source
	url        string // without its password
	conn       conn
	ch         channel
	tag        string // the consumer tag
	connClosed chan *amqp091.Error
	chClosed   chan *amqp091.Error

	dead atomic.Bool // the channel or the connection closed

	mu     sync.Mutex
	timers map[uint64]*time.Timer // delayed nacks by delivery tag
}

// ended marks the session dead and closes what is left of it. It returns
// why it ended: e is the broker's exception, nil after a clean close.
func (ss *session) ended(what string, e *amqp091.Error) error {
	ss.dead.Store(true)
	ss.close(time.Now().Add(defaultCloseWait))
	switch {
	case e != nil:
		return fmt.Errorf("%s: %s closed: %w", ss.url, what, e)
	case what == "consumer":
		return fmt.Errorf("%s: the broker cancelled the consumer (was queue %s deleted?)", ss.url, ss.src.opts.queue)
	}
	return fmt.Errorf("%s: %s closed", ss.url, what)
}

// closeReason is the exception that closed the channel or the connection,
// when one is waiting (the client notifies it before it closes the
// consumer's deliveries).
func (ss *session) closeReason() *amqp091.Error {
	for _, c := range []chan *amqp091.Error{ss.chClosed, ss.connClosed} {
		select {
		case e := <-c:
			if e != nil {
				return e
			}
		default:
		}
	}
	return nil
}

// usable reports whether acknowledgements can still reach the broker on
// this session's channel.
func (ss *session) usable() bool {
	return !ss.dead.Load() && !ss.ch.IsClosed()
}

// ack acknowledges one delivery. On a channel that is gone it does
// nothing: the broker redelivers the message.
func (ss *session) ack(tag uint64) {
	if !ss.usable() {
		ss.lost("ack", tag, nil)
		return
	}
	if err := ss.ch.Ack(tag, false); err != nil {
		ss.lost("ack", tag, err)
	}
}

// nack returns one delivery to the queue.
func (ss *session) nack(tag uint64) {
	if !ss.usable() {
		ss.lost("nack", tag, nil)
		return
	}
	if err := ss.ch.Nack(tag, false, true); err != nil {
		ss.lost("nack", tag, err)
	}
}

// lost notes an acknowledgement that could not be sent: the broker
// redelivers the message, so the relay sees it again.
func (ss *session) lost(op string, tag uint64, err error) {
	ss.src.log.Debug("amqp "+op+" dropped: the channel the message came on is gone, so the broker redelivers it and it is seen twice",
		"source", ss.src.name, "delivery_tag", tag, "err", err)
}

// nackAfter returns a delivery to the queue after d.
func (ss *session) nackAfter(tag uint64, d time.Duration) {
	ss.mu.Lock()
	if ss.timers == nil { // flushed: the source is stopping
		ss.mu.Unlock()
		ss.nack(tag)
		return
	}
	ss.timers[tag] = time.AfterFunc(d, func() {
		ss.mu.Lock()
		_, pending := ss.timers[tag]
		delete(ss.timers, tag)
		ss.mu.Unlock()
		if pending {
			ss.nack(tag)
		}
	})
	ss.mu.Unlock()
}

// flushNacks sends the delayed nacks now; later ones go out at once.
func (ss *session) flushNacks() {
	ss.mu.Lock()
	timers := ss.timers
	ss.timers = nil
	ss.mu.Unlock()
	for tag, t := range timers {
		if t.Stop() {
			ss.nack(tag)
		}
	}
}

// close closes the connection (and with it the channel), giving the broker
// until deadline to answer.
func (ss *session) close(deadline time.Time) {
	ss.dead.Store(true)
	if ss.conn.IsClosed() {
		return
	}
	if err := ss.conn.CloseDeadline(deadline); err != nil {
		ss.src.log.Debug("amqp connection close", "source", ss.src.name, "url", ss.url, "err", err)
	}
}
