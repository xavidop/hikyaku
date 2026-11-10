package azure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a servicebus source; tests shorten them.
const (
	// defaultRenewEvery is how often the locks of messages in flight are
	// checked; a lock is renewed once half its duration has passed.
	defaultRenewEvery = time.Second
	// defaultSettleTimeout bounds one complete or abandon.
	defaultSettleTimeout = 30 * time.Second
	// defaultRenewTimeout bounds one lock renewal.
	defaultRenewTimeout = 10 * time.Second
	// defaultSessionIdle: a session that has had no message for this long
	// is drained; it is released once its messages are settled.
	defaultSessionIdle = time.Second
	// defaultMaxSessions is how many sessions are received at once.
	defaultMaxSessions = 8
	// defaultLockDuration is assumed for a message without LockedUntil.
	defaultLockDuration = 30 * time.Second
)

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
	// errSessionful: the entity requires sessions.
	errSessionful = errors.New("the entity requires sessions")
)

// sbSource is a servicebus source: a PeekLock receiver of a queue or
// subscription, or session receivers when the entity requires sessions.
type sbSource struct {
	name   string
	opts   sbSourceOptions
	client sbClient
	log    *slog.Logger
	now    func() time.Time
	rnd    func() float64

	renewEvery    time.Duration
	settleTimeout time.Duration
	renewTimeout  time.Duration
	sessionIdle   time.Duration
	maxSessions   int

	started atomic.Bool
	// slots holds one token per message that may be in flight: prefetch.
	slots chan struct{}
	// inflight counts messages received and not yet settled.
	inflight *counter

	mu       sync.Mutex
	status   error
	closed   bool
	receiver *sbLink              // the receiver, without sessions
	sessions map[*sbLink]struct{} // session receivers not yet closed
}

// sbLink is one receiver (or session receiver) and its messages in flight,
// whose locks it renews.
type sbLink struct {
	recv    sbReceiver        // nil for a session
	session sbSessionReceiver // nil without sessions
	settler sbSettler
	close   func(ctx context.Context) error

	inflight *counter
	stop     chan struct{} // stops the renewer
	stopped  chan struct{} // closed once the renewer returned
	once     sync.Once

	mu   sync.Mutex
	msgs map[*sbMsg]struct{}
	lost bool // the session's lock is lost: stop receiving from it

	renewAt time.Time // the session lock's next renewal (renewer only)
}

// sbMsg is a received message in flight.
type sbMsg struct {
	m    *azservicebus.ReceivedMessage
	link *sbLink
	once sync.Once

	mu      sync.Mutex // serializes the lock's renewal and the settlement
	settled bool
	lost    bool      // the lock is lost; Service Bus redelivers the message
	renewAt time.Time // renewer only, under mu
}

func newSBSource(_ context.Context, spec plugin.Spec) (*sbSource, error) {
	opts, err := parseSBSource(spec)
	if err != nil {
		return nil, err
	}
	// The receive loop retries on its own (reconnect.*); the SDK retries
	// each call a few times within that policy.
	retry := azservicebus.RetryOptions{MaxRetries: 3, RetryDelay: opts.reconnect.backoff, MaxRetryDelay: opts.reconnect.maxBackoff}
	client, err := dialServiceBus(opts.conn, retry)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	return buildSBSource(spec, opts, client), nil
}

// buildSBSource makes the source on a client (tests pass a fake).
func buildSBSource(spec plugin.Spec, opts sbSourceOptions, client sbClient) *sbSource {
	s := &sbSource{
		name: spec.Name, opts: opts, client: client, log: spec.Env.Log(), now: time.Now, rnd: rand.Float64,
		renewEvery: defaultRenewEvery, settleTimeout: defaultSettleTimeout, renewTimeout: defaultRenewTimeout,
		sessionIdle: defaultSessionIdle, maxSessions: defaultMaxSessions,
		slots: make(chan struct{}, opts.prefetch), inflight: newCounter(),
		status: errNotStarted, sessions: map[*sbLink]struct{}{},
	}
	for range opts.prefetch {
		s.slots <- struct{}{}
	}
	return s
}

// Name is the source name.
func (s *sbSource) Name() string { return s.name }

func (s *sbSource) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Health is the last receive's failure, else a peek of one message (without
// sessions), which checks the connection and the entity.
func (s *sbSource) Health(ctx context.Context) error {
	s.mu.Lock()
	status, closed, l := s.status, s.closed, s.receiver
	s.mu.Unlock()
	switch {
	case closed:
		return errStopped
	case status != nil:
		return status
	case l == nil:
		return nil
	}
	// An explicit start leaves the receiver's own peek cursor alone, so
	// health checks may run at the same time.
	from := int64(0)
	if _, err := l.recv.PeekMessages(ctx, 1, &azservicebus.PeekMessagesOptions{FromSequenceNumber: &from}); err != nil {
		return fmt.Errorf("peek %s: %w", s.opts.entity.path(), err)
	}
	return nil
}

// Start receives until ctx ends, then returns nil; messages in flight are
// settled as the relay finishes them, and Close closes the receivers. It
// fails only after reconnect.max_attempts failed receives in a row.
func (s *sbSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	s.setStatus(nil)
	s.log.Info("servicebus source receiving", "source", s.name, "namespace", s.opts.conn.namespace,
		"entity", s.opts.entity.path(), "prefetch", s.opts.prefetch)
	err := s.receive(ctx, out)
	if errors.Is(err, errSessionful) {
		s.log.Info("servicebus entity requires sessions; receiving session by session", "source", s.name,
			"entity", s.opts.entity.path(), "sessions", s.maxSessions)
		err = s.receiveSessions(ctx, out)
	}
	if ctx.Err() != nil {
		s.setStatus(errStopped)
		return nil
	}
	if err != nil {
		s.setStatus(err)
	}
	return err
}

// receive runs the receiver of an entity without sessions: it asks for as
// many messages as there are free slots, so at most prefetch are in flight.
func (s *sbSource) receive(ctx context.Context, out chan<- *envelope.Envelope) error {
	recv, err := s.client.newReceiver(s.opts.entity)
	if err != nil {
		return fmt.Errorf("receiver for %s: %w", s.opts.entity.path(), err)
	}
	l := s.openLink(recv, nil)
	s.mu.Lock()
	s.receiver = l
	s.mu.Unlock()
	failures := 0
	for {
		n, ok := s.take(ctx)
		if !ok {
			return nil
		}
		msgs, err := recv.ReceiveMessages(ctx, n, nil)
		s.giveBack(n - len(msgs))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if requiresSessions(err) {
				s.mu.Lock()
				s.receiver = nil
				s.mu.Unlock()
				s.closeLink(ctx, l)
				return errSessionful
			}
			failures++
			if !s.retryWait(ctx, "receive", failures, err) {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("giving up after %d failed receives in a row: %w", failures, err)
			}
			continue
		}
		failures = 0
		s.setStatus(nil)
		s.dispatch(ctx, l, msgs, out)
	}
}

// retryWait records a failure and waits before the next try. It returns
// false when ctx ended or reconnect.max_attempts is used up.
func (s *sbSource) retryWait(ctx context.Context, what string, failures int, err error) bool {
	rc := s.opts.reconnect
	s.setStatus(fmt.Errorf("%s %s: %w", what, s.opts.entity.path(), err))
	if rc.maxAttempts > 0 && failures >= rc.maxAttempts {
		return false
	}
	wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, failures, s.rnd)
	s.log.Warn("servicebus "+what+" failed; retrying", "source", s.name, "entity", s.opts.entity.path(),
		"failures", failures, "wait", wait.String(), "err", err)
	return backoff.Sleep(ctx, wait)
}

// receiveSessions receives an entity that requires sessions: maxSessions
// workers each accept the next session with messages, receive it until it
// is drained, and release it once its messages are settled.
func (s *sbSource) receiveSessions(ctx context.Context, out chan<- *envelope.Envelope) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg   sync.WaitGroup
		once sync.Once
		fail error
	)
	for range s.maxSessions {
		wg.Go(func() {
			if err := s.sessionWorker(ctx, out); err != nil {
				once.Do(func() { fail = err })
				cancel()
			}
		})
	}
	wg.Wait()
	return fail
}

func (s *sbSource) sessionWorker(ctx context.Context, out chan<- *envelope.Envelope) error {
	failures := 0
	for {
		began := s.now()
		sr, err := s.client.acceptNextSession(ctx, s.opts.entity)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if hasCode(err, azservicebus.CodeTimeout) {
				// No session has messages: ask again (not too often).
				if wait := s.sessionIdle - s.now().Sub(began); wait > 0 && !backoff.Sleep(ctx, wait) {
					return nil
				}
				continue
			}
			failures++
			if !s.retryWait(ctx, "accept session", failures, err) {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("giving up after %d failed session accepts in a row: %w", failures, err)
			}
			continue
		}
		failures = 0
		s.setStatus(nil)
		s.runSession(ctx, sr, out)
		if ctx.Err() != nil {
			return nil
		}
	}
}

// runSession receives one session until it has no message for sessionIdle
// (or its lock is lost), then releases it once its messages are settled.
// When ctx ends first, Close settles and closes it.
func (s *sbSource) runSession(ctx context.Context, sr sbSessionReceiver, out chan<- *envelope.Envelope) {
	l := s.openLink(nil, sr)
	s.mu.Lock()
	s.sessions[l] = struct{}{}
	s.mu.Unlock()
	s.log.Debug("servicebus session accepted", "source", s.name, "session", sr.SessionID())
	for !l.lockLost() {
		n, ok := s.take(ctx)
		if !ok {
			return
		}
		rctx, cancel := context.WithTimeout(ctx, s.sessionIdle)
		msgs, err := sr.ReceiveMessages(rctx, n, nil)
		cancel()
		s.giveBack(n - len(msgs))
		if ctx.Err() != nil {
			s.abandon(l, msgs)
			return
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			s.log.Warn("servicebus session receive failed; releasing the session", "source", s.name,
				"session", sr.SessionID(), "err", err)
		}
		if len(msgs) == 0 {
			break
		}
		s.dispatch(ctx, l, msgs, out)
	}
	select {
	case <-l.inflight.idle():
	case <-ctx.Done():
		return
	}
	s.log.Debug("servicebus session released", "source", s.name, "session", sr.SessionID())
	s.closeLink(context.Background(), l)
}

// take waits for a free slot and takes every other free one: the number
// of messages to ask for. False when ctx ended.
func (s *sbSource) take(ctx context.Context) (int, bool) {
	select {
	case <-s.slots:
	case <-ctx.Done():
		return 0, false
	}
	n := 1
	for n < cap(s.slots) {
		select {
		case <-s.slots:
			n++
		default:
			return n, true
		}
	}
	return n, true
}

func (s *sbSource) giveBack(n int) {
	for range n {
		s.slots <- struct{}{}
	}
}

// dispatch hands received messages to the relay, in order. Once ctx ends,
// the rest go back to the entity at once.
func (s *sbSource) dispatch(ctx context.Context, l *sbLink, msgs []*azservicebus.ReceivedMessage, out chan<- *envelope.Envelope) {
	for i, m := range msgs {
		if ctx.Err() != nil {
			s.abandon(l, msgs[i:])
			return
		}
		s.handle(ctx, l, m, out)
	}
}

// abandon returns messages that were never handed to the relay.
func (s *sbSource) abandon(l *sbLink, msgs []*azservicebus.ReceivedMessage) {
	for _, m := range msgs {
		s.settle(context.Background(), s.track(l, m), false)
	}
}

// handle turns one message into an envelope and hands it over.
func (s *sbSource) handle(ctx context.Context, l *sbLink, m *azservicebus.ReceivedMessage, out chan<- *envelope.Envelope) {
	sm := s.track(l, m)
	body := m.Body
	if body == nil && m.RawAMQPMessage != nil {
		b := m.RawAMQPMessage.Body
		body = amqpBody(b.Data, b.Value, b.Sequence)
	}
	if int64(len(body)) > s.opts.maxBytes {
		s.log.Warn("servicebus message over max_body_bytes; completing it", "source", s.name,
			"message_id", m.MessageID, "bytes", len(body), "max_body_bytes", s.opts.maxBytes)
		s.settle(context.Background(), sm, true)
		return
	}
	env := s.envelope(m, body)
	plugin.OnSettle(env, s.opts.ack, func(ctx context.Context, final bool) { s.settle(ctx, sm, final) })
	select {
	case out <- env:
	case <-ctx.Done():
		s.settle(context.Background(), sm, false)
	}
}

// envelope maps a message: ID from Idempotency-Key or MessageId, the
// application properties as headers, Subject (else the entity), the
// content type, and the servicebus.* attributes.
func (s *sbSource) envelope(m *azservicebus.ReceivedMessage, body []byte) *envelope.Envelope {
	env := envelope.New(s.name, kindServiceBus, body)
	if key := idempotencyFrom(m.ApplicationProperties); key != "" {
		env.ID = key
		env.Attributes["idempotency_key"] = key
	} else if m.MessageID != "" {
		env.ID = m.MessageID
	}
	env.Headers = headersFrom(m.ApplicationProperties)
	env.Subject = s.opts.entity.name()
	if m.Subject != nil && *m.Subject != "" {
		env.Subject = *m.Subject
	}
	env.ContentType = contentType(m.ContentType, body)
	a := env.Attributes
	a["servicebus.message_id"] = m.MessageID
	a["servicebus.delivery_count"] = int64(m.DeliveryCount)
	a["servicebus.entity"] = s.opts.entity.path()
	a["ordering_key"] = s.opts.entity.path()
	if m.SequenceNumber != nil {
		a["servicebus.sequence_number"] = *m.SequenceNumber
	}
	if t, ok := rfc3339(m.EnqueuedTime); ok {
		a["servicebus.enqueued_time"] = t
	}
	if m.SessionID != nil && *m.SessionID != "" {
		a["servicebus.session_id"] = *m.SessionID
		a["ordering_key"] = *m.SessionID
	}
	if m.CorrelationID != nil && *m.CorrelationID != "" {
		a["servicebus.correlation_id"] = *m.CorrelationID
	}
	return env
}

// openLink starts tracking a receiver and renewing its locks.
func (s *sbSource) openLink(recv sbReceiver, sr sbSessionReceiver) *sbLink {
	l := &sbLink{recv: recv, session: sr, inflight: newCounter(), msgs: map[*sbMsg]struct{}{},
		stop: make(chan struct{}), stopped: make(chan struct{})}
	if sr != nil {
		l.settler, l.close = sr, sr.Close
		l.renewAt = halfway(s.now(), sr.LockedUntil())
	} else {
		l.settler, l.close = recv, recv.Close
	}
	go s.renewer(l)
	return l
}

// closeLink stops a receiver's renewer and closes it.
func (s *sbSource) closeLink(ctx context.Context, l *sbLink) {
	l.once.Do(func() {
		close(l.stop)
		<-l.stopped
		s.mu.Lock()
		delete(s.sessions, l)
		s.mu.Unlock()
		if err := l.close(ctx); err != nil {
			s.log.Debug("closing servicebus receiver", "source", s.name, "err", err)
		}
	})
}

func (l *sbLink) lockLost() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}

// track counts a received message in flight.
func (s *sbSource) track(l *sbLink, m *azservicebus.ReceivedMessage) *sbMsg {
	sm := &sbMsg{m: m, link: l, renewAt: s.now().Add(defaultLockDuration / 2)}
	if m.LockedUntil != nil {
		sm.renewAt = halfway(s.now(), *m.LockedUntil)
	}
	s.inflight.add()
	l.inflight.add()
	l.mu.Lock()
	l.msgs[sm] = struct{}{}
	l.mu.Unlock()
	return sm
}

// settle completes (final) or abandons a message, once, without blocking
// the caller: Service Bus then forgets it, or redelivers it and counts the
// delivery (dead-lettering it after MaxDeliveryCount).
func (s *sbSource) settle(ctx context.Context, sm *sbMsg, final bool) {
	sm.once.Do(func() {
		go s.doSettle(context.WithoutCancel(ctx), sm, final)
	})
}

func (s *sbSource) doSettle(ctx context.Context, sm *sbMsg, final bool) {
	l := sm.link
	defer func() {
		l.mu.Lock()
		delete(l.msgs, sm)
		l.mu.Unlock()
		s.giveBack(1)
		l.inflight.done()
		s.inflight.done()
	}()
	sm.mu.Lock() // waits out a renewal in progress
	sm.settled = true
	sm.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.settleTimeout)
	defer cancel()
	if final {
		if err := l.settler.CompleteMessage(ctx, sm.m, nil); err != nil {
			s.log.Warn("servicebus complete failed; Service Bus redelivers the message once its lock expires",
				"source", s.name, "message_id", sm.m.MessageID, "err", err)
		}
		return
	}
	if err := l.settler.AbandonMessage(ctx, sm.m, nil); err != nil {
		s.log.Warn("servicebus abandon failed; Service Bus redelivers the message once its lock expires",
			"source", s.name, "message_id", sm.m.MessageID, "err", err)
	}
}

// renewer renews, until the link closes, the locks of its messages in
// flight past half their lock (or the session's lock).
func (s *sbSource) renewer(l *sbLink) {
	defer close(l.stopped)
	t := time.NewTicker(s.renewEvery)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
		}
		if l.session != nil {
			s.renewSession(l)
		} else {
			s.renewMessages(l)
		}
	}
}

func (s *sbSource) renewMessages(l *sbLink) {
	now := s.now()
	l.mu.Lock()
	var due []*sbMsg
	for sm := range l.msgs {
		due = append(due, sm)
	}
	l.mu.Unlock()
	for _, sm := range due {
		sm.mu.Lock()
		if !sm.settled && !sm.lost && !now.Before(sm.renewAt) {
			s.renewMessage(l, sm)
		}
		sm.mu.Unlock()
	}
}

// renewMessage renews one message's lock; sm.mu is held.
func (s *sbSource) renewMessage(l *sbLink, sm *sbMsg) {
	ctx, cancel := context.WithTimeout(context.Background(), s.renewTimeout)
	defer cancel()
	err := l.recv.RenewMessageLock(ctx, sm.m, nil)
	switch {
	case err == nil:
		until := s.now().Add(defaultLockDuration)
		if sm.m.LockedUntil != nil {
			until = *sm.m.LockedUntil
		}
		sm.renewAt = halfway(s.now(), until)
	case hasCode(err, azservicebus.CodeLockLost):
		sm.lost = true
		s.log.Warn("servicebus message lock lost while in flight; Service Bus redelivers the message",
			"source", s.name, "message_id", sm.m.MessageID)
	default:
		s.log.Debug("servicebus lock renewal failed; retrying", "source", s.name, "message_id", sm.m.MessageID, "err", err)
	}
}

func (s *sbSource) renewSession(l *sbLink) {
	if s.now().Before(l.renewAt) || l.lockLost() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.renewTimeout)
	defer cancel()
	err := l.session.RenewSessionLock(ctx, nil)
	switch {
	case err == nil:
		l.renewAt = halfway(s.now(), l.session.LockedUntil())
	case hasCode(err, azservicebus.CodeLockLost):
		l.mu.Lock()
		l.lost = true
		l.mu.Unlock()
		s.log.Warn("servicebus session lock lost; Service Bus redelivers its messages in flight",
			"source", s.name, "session", l.session.SessionID())
	default:
		s.log.Debug("servicebus session lock renewal failed; retrying", "source", s.name,
			"session", l.session.SessionID(), "err", err)
	}
}

// halfway is when a lock held until `until` should be renewed: halfway
// through what is left of it.
func halfway(now, until time.Time) time.Time {
	if until.IsZero() {
		return now.Add(defaultLockDuration / 2)
	}
	if !until.After(now) {
		return now
	}
	return now.Add(until.Sub(now) / 2)
}

// Close waits for the messages in flight to be settled (until ctx ends),
// then closes the receivers and the connection.
func (s *sbSource) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if !s.inflight.wait(ctx) {
		s.log.Warn("servicebus source closing with messages still in flight; Service Bus redelivers them once their locks expire",
			"source", s.name, "in_flight", s.inflight.count())
	}
	s.mu.Lock()
	links := make([]*sbLink, 0, len(s.sessions)+1)
	if s.receiver != nil {
		links = append(links, s.receiver)
	}
	for l := range s.sessions {
		links = append(links, l)
	}
	s.mu.Unlock()
	for _, l := range links {
		s.closeLink(ctx, l)
	}
	return s.client.Close(ctx)
}
