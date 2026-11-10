package amqp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Settings of a destination; tests shorten them.
const (
	// defaultBlockedRetry is the retry hint of a delivery refused while
	// the broker blocks the connection.
	defaultBlockedRetry = 5 * time.Second
	// returnBuffer holds returned messages until the delivery that
	// published them reads them.
	returnBuffer = 256
)

// Sink is an amqp destination: one connection and one channel, in confirm
// mode, that deliveries take turns on.
type Sink struct {
	name     string
	endpoint string // the URL without its password
	opts     sinkOptions
	log      *slog.Logger
	dial     dialFunc

	blockedRetry time.Duration

	// sem serializes deliveries, and the dials and channel opens they make;
	// unlike a mutex, waiting for it honours the caller's context.
	sem chan struct{}

	mu     sync.Mutex
	pc     *pubConn
	closed bool
}

// pubConn is a destination's connection and its publishing channel.
type pubConn struct {
	conn     conn
	ch       channel
	chClosed chan *amqp091.Error
	returns  chan amqp091.Return
	chErr    *amqp091.Error // why the channel closed, once known

	mu      sync.Mutex
	blocked bool
	reason  string
	connErr *amqp091.Error // why the connection closed, once known
}

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	return &Sink{
		name: spec.Name, endpoint: redactURL(opts.url), opts: opts, log: spec.Env.Log(), dial: dialBroker,
		blockedRetry: defaultBlockedRetry, sem: make(chan struct{}, 1),
	}, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// publishing is one message ready to publish.
type publishing struct {
	key string
	msg amqp091.Publishing
}

// Deliver publishes one message and waits for the broker to confirm it.
// It makes one attempt: a connection or channel that closed is reopened by
// the next delivery.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	p, err := s.prepare(env)
	if err != nil {
		return nil, err
	}
	if err := s.send(ctx, []publishing{p}); err != nil {
		return nil, err
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// DeliverBatch publishes every message, then waits for all the confirms.
// The batch fails as a whole, with its most severe failure: the messages
// the broker took are published again on a retry, and the message IDs (the
// envelope IDs) let consumers drop the copies.
func (s *Sink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	ps := make([]publishing, 0, len(envs))
	for _, env := range envs {
		p, err := s.prepare(env)
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	if len(ps) > 0 {
		if err := s.send(ctx, ps); err != nil {
			return nil, err
		}
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// Health is nil when the destination is connected (and not blocked by a
// resource alarm) or can connect now.
func (s *Sink) Health(ctx context.Context) error {
	s.mu.Lock()
	pc, closed := s.pc, s.closed
	s.mu.Unlock()
	if closed {
		return errClosed
	}
	if pc == nil || pc.conn.IsClosed() {
		if err := s.acquire(ctx); err != nil {
			return ctxError(ctx, err)
		}
		defer s.release()
		var err error
		if pc, err = s.channel(ctx); err != nil {
			return err
		}
	}
	if reason, blocked := pc.blockedReason(); blocked {
		return fmt.Errorf("the broker blocks the connection: %s", reason)
	}
	return nil
}

// Close waits for the delivery in flight (within ctx), then closes the
// channel and the connection.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.acquire(ctx) == nil {
		defer s.release()
	}
	s.mu.Lock()
	pc := s.pc
	s.pc = nil
	s.mu.Unlock()
	if pc != nil {
		deadline, ok := ctx.Deadline()
		if !ok {
			deadline = time.Now().Add(defaultCloseWait)
		}
		pc.close(deadline)
	}
	return nil
}

func (s *Sink) acquire(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) release() { <-s.sem }

// prepare renders a message's routing key, headers and body and sets its
// properties: delivery mode, message ID (the envelope ID), content type and
// encoding, correlation ID, reply-to and timestamp.
func (s *Sink) prepare(env *envelope.Envelope) (publishing, error) {
	key, err := s.opts.routingKey.Render(env)
	if err != nil {
		return publishing{}, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
	}
	if len(key) > maxShortstr {
		return publishing{}, plugin.Permanentf("the routing key is %d bytes, over the AMQP limit of %d", len(key), maxShortstr)
	}
	headers := tableFromHeaders(env.Headers)
	for _, h := range s.opts.headers {
		v, err := h.tmpl.Render(env)
		if err != nil {
			return publishing{}, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		setFold(headers, h.name, v)
	}
	if !hasFold(headers, headerIdempotencyKey) {
		headers[headerIdempotencyKey] = env.ID
	}
	body := env.Body
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return publishing{}, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		body = []byte(b)
	}
	msg := amqp091.Publishing{
		Headers:      headers,
		DeliveryMode: amqp091.Transient,
		MessageId:    messageID(env.ID),
		Timestamp:    env.ReceivedAt,
		Body:         body,
	}
	if s.opts.persistent {
		msg.DeliveryMode = amqp091.Persistent
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}
	contentType := env.ContentType
	if contentType == "" {
		contentType = env.Header(headerContentType)
	}
	// A property longer than an AMQP short string travels as a header.
	for _, p := range []struct {
		name  string
		value string
		set   *string
	}{
		{headerContentType, contentType, &msg.ContentType},
		{headerContentEncoding, env.Header(headerContentEncoding), &msg.ContentEncoding},
		{headerCorrelationID, env.Header(headerCorrelationID), &msg.CorrelationId},
		{headerReplyTo, env.Header(headerReplyTo), &msg.ReplyTo},
	} {
		switch {
		case p.value == "":
		case len(p.value) <= maxShortstr:
			*p.set = p.value
		default:
			setFold(headers, p.name, p.value)
		}
	}
	return publishing{key: key, msg: msg}, nil
}

// messageID is the message ID of an envelope: its ID, or the SHA-256 of an
// ID longer than an AMQP short string (the full one is the Idempotency-Key
// header).
func messageID(id string) string {
	if len(id) <= maxShortstr {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// send publishes ps on the destination's channel and waits for their
// confirms.
func (s *Sink) send(ctx context.Context, ps []publishing) error {
	if err := s.acquire(ctx); err != nil {
		return ctxError(ctx, err)
	}
	defer s.release()
	pc, err := s.channel(ctx)
	if err != nil {
		return err
	}
	if reason, blocked := pc.blockedReason(); blocked {
		return blockedError(reason, s.blockedRetry)
	}
	return s.publish(ctx, pc, ps)
}

// channel returns the publishing channel: it dials when there is no open
// connection, and opens a channel when there is none or the last one
// closed (a channel exception leaves the connection open).
func (s *Sink) channel(ctx context.Context) (*pubConn, error) {
	s.mu.Lock()
	closed, pc := s.closed, s.pc
	s.mu.Unlock()
	if closed {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	if pc != nil && pc.conn.IsClosed() {
		s.log.Debug("amqp destination reconnecting", "destination", s.name, "url", s.endpoint, "reason", pc.closeErr())
		s.forget(pc)
		pc = nil
	}
	if pc == nil {
		var err error
		if pc, err = s.connect(ctx); err != nil {
			return nil, err
		}
	}
	if pc.ch == nil || pc.ch.IsClosed() {
		if pc.ch != nil {
			s.log.Debug("amqp destination reopening its channel", "destination", s.name, "url", s.endpoint, "reason", pc.channelErr())
		}
		if err := s.openChannel(pc); err != nil {
			return nil, classify(ctx, "open a channel on "+s.endpoint, err)
		}
	}
	return pc, nil
}

// connect dials the broker and follows the new connection.
func (s *Sink) connect(ctx context.Context) (*pubConn, error) {
	c, err := s.dial(ctx, s.opts.url, s.opts.conn.config(s.opts.url))
	if err != nil {
		return nil, dialError(ctx, s.endpoint, err)
	}
	pc := &pubConn{conn: c}
	connClosed := c.NotifyClose(make(chan *amqp091.Error, 1))
	blocks := c.NotifyBlocked(make(chan amqp091.Blocking, 4))
	go pc.watch(s, connClosed, blocks)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		pc.close(time.Now().Add(defaultCloseWait))
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	s.pc = pc
	s.log.Debug("amqp destination connected", "destination", s.name, "url", s.endpoint)
	return pc, nil
}

// openChannel opens a channel on pc's connection in confirm mode, listening
// for returned messages, and declares the exchange when asked.
func (s *Sink) openChannel(pc *pubConn) error {
	pc.ch = nil
	ch, err := pc.conn.Channel()
	if err != nil {
		return err
	}
	chClosed := ch.NotifyClose(make(chan *amqp091.Error, 1))
	returns := ch.NotifyReturn(make(chan amqp091.Return, returnBuffer))
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("confirm mode: %w", err)
	}
	if s.opts.declare && s.opts.exchange != "" {
		if err := ch.ExchangeDeclare(s.opts.exchange, exchangeType, true, false, false, false, nil); err != nil {
			_ = ch.Close()
			return fmt.Errorf("declare exchange %s: %w", s.opts.exchange, err)
		}
	}
	pc.ch, pc.chClosed, pc.returns, pc.chErr = ch, chClosed, returns, nil
	return nil
}

// forget drops pc if it is still the destination's connection.
func (s *Sink) forget(pc *pubConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pc == pc {
		s.pc = nil
	}
}

// publish publishes ps, then waits for every confirm, collecting the
// messages the broker returned as unroutable meanwhile (a return comes
// before its message's confirm).
func (s *Sink) publish(ctx context.Context, pc *pubConn, ps []publishing) error {
	if err := ctx.Err(); err != nil {
		return ctxError(ctx, err)
	}
	returned := map[string]amqp091.Return{}
	returns := pc.returns
	drain(returns, nil) // returns of earlier, abandoned attempts
	// A write the broker's TCP pushback holds cannot be interrupted: when
	// the attempt runs out during one, the connection is closed under it.
	stop := context.AfterFunc(ctx, func() { _ = pc.conn.CloseDeadline(time.Now()) })
	confirms := make([]confirmation, 0, len(ps))
	var perr error
	for _, p := range ps {
		dc, err := pc.ch.PublishWithDeferredConfirmWithContext(ctx, s.opts.exchange, p.key, s.opts.mandatory, false, p.msg)
		if err != nil {
			perr = err
			break
		}
		confirms = append(confirms, dc)
		// The client gives up notifying a return that waits too long for
		// room, so a long batch reads them as it goes.
		drain(returns, returned)
	}
	if !stop() {
		s.forget(pc)
		return ctxError(ctx, fmt.Errorf("publish to %s: the attempt ran out while writing: %w", s.endpoint, ctx.Err()))
	}
	if perr != nil {
		if e := pc.channelErr(); e != nil {
			perr = e
		}
		return classify(ctx, "publish to "+s.endpoint, perr)
	}
	for _, dc := range confirms {
		if err := awaitConfirm(ctx, dc, &returns, returned); err != nil {
			return ctxError(ctx, fmt.Errorf("waiting for %s to confirm the message: %w", s.endpoint, err))
		}
	}
	drain(returns, returned)
	var errs []error
	for i, p := range ps {
		if r, ok := returned[p.msg.MessageId]; ok {
			errs = append(errs, returnError(r))
			continue
		}
		if confirms[i].Acked() {
			continue
		}
		if e := pc.channelErr(); e != nil {
			// The channel closed before the broker confirmed: publishing
			// to an exchange that does not exist is 404 NOT_FOUND, say.
			errs = append(errs, brokerError("publish to "+s.endpoint, e))
			continue
		}
		if pc.ch.IsClosed() {
			errs = append(errs, plugin.Wrap(plugin.Transient, []string{plugin.TagConnect},
				fmt.Errorf("publish to %s: the channel closed before the broker confirmed the message", s.endpoint)))
			continue
		}
		errs = append(errs, nackError(s.opts.exchange, p.key))
	}
	return batchError(errs, len(ps))
}

// awaitConfirm waits for one confirm, recording the returned messages that
// arrive meanwhile.
func awaitConfirm(ctx context.Context, dc confirmation, returns *chan amqp091.Return, returned map[string]amqp091.Return) error {
	for {
		select {
		case <-dc.Done():
			return nil
		case r, ok := <-*returns:
			if !ok {
				*returns = nil // closed with the channel
				continue
			}
			returned[r.MessageId] = r
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// drain reads the returned messages already waiting, into returned (or
// away when it is nil).
func drain(returns chan amqp091.Return, returned map[string]amqp091.Return) {
	if returns == nil {
		return
	}
	for {
		select {
		case r, ok := <-returns:
			if !ok {
				return
			}
			if returned != nil {
				returned[r.MessageId] = r
			}
		default:
			return
		}
	}
}

// watch follows the connection: it records the broker's resource alarms
// (connection.blocked and unblocked) and why the connection closed.
func (pc *pubConn) watch(s *Sink, closed chan *amqp091.Error, blocks chan amqp091.Blocking) {
	for {
		select {
		case b, ok := <-blocks:
			if !ok {
				blocks = nil
				continue
			}
			pc.mu.Lock()
			pc.blocked, pc.reason = b.Active, b.Reason
			pc.mu.Unlock()
			if b.Active {
				s.log.Warn("amqp broker blocked the destination's connection", "destination", s.name, "url", s.endpoint, "reason", b.Reason)
			} else {
				s.log.Info("amqp broker unblocked the destination's connection", "destination", s.name, "url", s.endpoint)
			}
		case e, ok := <-closed:
			if ok && e != nil {
				pc.mu.Lock()
				pc.connErr = e
				pc.mu.Unlock()
				s.log.Warn("amqp destination connection closed", "destination", s.name, "url", s.endpoint, "err", e)
			}
			return
		}
	}
}

// blockedReason reports whether the broker blocks the connection, and why.
func (pc *pubConn) blockedReason() (string, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.reason, pc.blocked
}

// closeErr is why the connection closed, nil when it closed cleanly or is
// still open.
func (pc *pubConn) closeErr() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.connErr == nil {
		return nil
	}
	return pc.connErr
}

// channelErr is the exception that closed the publishing channel, nil when
// there was none (yet). The client notifies it before it fails the
// channel's pending confirms. Only the delivery holding the destination's
// turn calls it.
func (pc *pubConn) channelErr() *amqp091.Error {
	if pc.chErr == nil {
		select {
		case e := <-pc.chClosed:
			pc.chErr = e
		default:
		}
	}
	return pc.chErr
}

// close closes the connection, giving the broker until deadline to answer.
func (pc *pubConn) close(deadline time.Time) {
	if !pc.conn.IsClosed() {
		_ = pc.conn.CloseDeadline(deadline)
	}
}
