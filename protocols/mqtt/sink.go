package mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// idempotencyProperty is the MQTT 5 user property carrying the idempotency
// key; a source reads it back as the envelope ID.
const idempotencyProperty = "Idempotency-Key"

// dropHeaders are not published as user properties: hop-by-hop headers,
// framing, the caller's own credentials, and what has a property of its own
// (content type, idempotency key).
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
	"Content-Type": true, idempotencyProperty: true,
}

// Sink is an mqtt destination: one connection, dialed on first use and
// redialed in the background whenever it drops.
type Sink struct {
	name     string
	endpoint string // the URL without its password
	opts     sinkOptions
	connect  connectFunc
	log      *slog.Logger
	rnd      func() float64

	reconnect   reconnectOptions
	stableAfter time.Duration

	published atomic.Int64

	mu       sync.Mutex
	started  bool
	closed   bool
	cur      client        // the live connection, nil while there is none
	attempt  chan struct{} // closed when the connect in progress ends; nil when none is
	lastErr  error         // why there is no connection
	stop     context.CancelFunc
	loopDone chan struct{}
}

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	s := &Sink{
		name: spec.Name, endpoint: opts.server.shown, opts: opts, connect: connectorFor(opts.conn.version),
		log: spec.Env.Log(), rnd: rand.Float64,
		reconnect:   reconnectOptions{backoff: defaultBackoff, maxBackoff: defaultMaxBackoff, jitter: defaultJitter},
		stableAfter: defaultStableAfter,
	}
	if opts.conn.version == version311 && len(opts.headers) > 0 {
		s.log.Warn("mqtt destination: MQTT 3.1.1 has no user properties, so headers are not sent", "destination", s.name)
	}
	return s, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver publishes one message and waits for the broker's acknowledgement
// (QoS 1 and 2) or the write (QoS 0). It makes one attempt.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	p, err := s.publication(env)
	if err != nil {
		return nil, err
	}
	c, err := s.connection(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.Publish(ctx, p); err != nil {
		return nil, publishError(ctx, s.endpoint, err)
	}
	s.published.Add(1)
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// Health is nil while the destination is connected. It connects first if
// it never did, waiting for that within ctx.
func (s *Sink) Health(ctx context.Context) error {
	_, err := s.connection(ctx)
	return err
}

// Close disconnects and stops reconnecting.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	stop, done, c := s.stop, s.loopDone, s.cur
	s.cur = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	if c == nil {
		return nil
	}
	if err := c.Disconnect(ctx); err != nil {
		return fmt.Errorf("destination %s: disconnect: %w", s.name, err)
	}
	return nil
}

// connection returns the live connection. The first call starts the
// connection loop; a call while a connect is in progress waits for it
// within ctx; a call while the loop is backing off fails at once.
func (s *Sink) connection(ctx context.Context) (client, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	if !s.started {
		s.startLocked()
	}
	c, attempt, why := s.cur, s.attempt, s.lastErr
	s.mu.Unlock()
	if c == nil && attempt != nil {
		select {
		case <-attempt:
		case <-ctx.Done():
			return nil, ctxError(ctx, fmt.Errorf("waiting to connect to %s: %w", s.endpoint, ctx.Err()))
		}
		s.mu.Lock()
		c, why = s.cur, s.lastErr
		s.mu.Unlock()
	}
	if c == nil || closed(c.Lost()) {
		return nil, notConnected(s.endpoint, why)
	}
	return c, nil
}

// startLocked starts the connection loop, its first connect in progress.
// s.mu is held.
func (s *Sink) startLocked() {
	ctx, cancel := context.WithCancel(context.Background())
	s.started, s.stop, s.loopDone = true, cancel, make(chan struct{})
	s.attempt = make(chan struct{})
	go s.loop(ctx, s.attempt)
}

// loop keeps the connection up: it connects, waits for the connection to
// drop, and connects again, with backoff after failures. A connection that
// published a message or stayed up for stableAfter resets the backoff.
func (s *Sink) loop(ctx context.Context, attempt chan struct{}) {
	defer close(s.loopDone)
	rc := s.reconnect
	failures := 0
	for {
		if attempt == nil {
			attempt = make(chan struct{})
			s.mu.Lock()
			s.attempt = attempt
			s.mu.Unlock()
		}
		cctx, cancel := context.WithTimeout(ctx, s.opts.conn.connectTimeout)
		c, err := s.connect(cctx, s.opts.server, s.opts.conn, nil)
		cancel()
		s.mu.Lock()
		s.attempt = nil
		if err == nil && s.closed {
			s.mu.Unlock()
			close(attempt)
			s.disconnect(c)
			return
		}
		if err == nil {
			s.cur, s.lastErr = c, nil
		} else {
			s.lastErr = err
		}
		s.mu.Unlock()
		close(attempt)
		attempt = nil
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			s.log.Debug("mqtt destination connected", "destination", s.name, "url", s.endpoint)
			before, start := s.published.Load(), time.Now()
			select {
			case <-ctx.Done():
				return // Close disconnects
			case <-c.Lost():
			}
			why := lostReason(c)
			s.mu.Lock()
			if s.cur == c {
				s.cur = nil
			}
			s.lastErr = why
			s.mu.Unlock()
			s.disconnect(c)
			if s.published.Load() > before || time.Since(start) >= s.stableAfter {
				failures = 0
			}
			err = why
		}
		failures++
		wait := backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, failures, s.rnd)
		s.log.Warn("mqtt destination reconnecting", "destination", s.name, "url", s.endpoint,
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			return
		}
	}
}

func (s *Sink) disconnect(c client) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDisconnectTimeout)
	defer cancel()
	_ = c.Disconnect(ctx)
}

// publication renders the PUBLISH of a message.
func (s *Sink) publication(env *envelope.Envelope) (*publication, error) {
	topic, err := s.opts.topic.Render(env)
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
	}
	if !s.opts.topic.IsStatic() {
		if err := validTopic(topic); err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, fmt.Errorf("topic %q: %v", topic, err))
		}
	}
	payload := env.Body
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		payload = []byte(b)
	}
	p := &publication{topic: topic, payload: payload, qos: s.opts.qos, retain: s.opts.retain}
	if s.opts.conn.version != version5 {
		return p, nil // 3.1.1 has no properties
	}
	p.contentType = env.ContentType
	if p.userProps, err = s.userProps(env); err != nil {
		return nil, err
	}
	return p, nil
}

// userProps are the MQTT 5 user properties of a message: its headers (in
// name order, without dropHeaders), then the headers templates, then the
// idempotency key.
func (s *Sink) userProps(env *envelope.Envelope) ([]userProp, error) {
	var props []userProp
	for _, k := range slices.Sorted(maps.Keys(env.Headers)) {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if dropHeaders[ck] || s.opts.headers[ck] != nil || !validString(ck) {
			continue
		}
		for _, v := range env.Headers[k] {
			if validString(v) {
				props = append(props, userProp{key: ck, value: v})
			}
		}
	}
	for _, k := range slices.Sorted(maps.Keys(s.opts.headers)) {
		v, err := s.opts.headers[k].Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		if !validString(v) {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, fmt.Errorf("headers.%s: not valid UTF-8 text", k))
		}
		props = append(props, userProp{key: k, value: v})
	}
	if s.opts.headers[idempotencyProperty] == nil {
		props = append(props, userProp{key: idempotencyProperty, value: idempotencyKey(env)})
	}
	return props, nil
}

// idempotencyKey is the key the relay delivers a message under: the
// idempotency_key attribute, else the envelope ID.
func idempotencyKey(env *envelope.Envelope) string {
	if v, ok := env.Attribute("idempotency_key"); ok {
		if k, ok := v.(string); ok && k != "" {
			return k
		}
	}
	return env.ID
}

// validString reports whether s can be an MQTT string: UTF-8 without NUL.
func validString(s string) bool {
	return len(s) <= 65535 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
