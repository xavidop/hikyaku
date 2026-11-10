package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a pub/sub source; tests shorten them.
const (
	// defaultPingEvery: a subscription quiet this long is pinged.
	defaultPingEvery = 30 * time.Second
	// defaultPingTimeout: a subscription that does not answer a ping
	// within it is dropped and made again.
	defaultPingTimeout = 10 * time.Second
	// defaultSubscribeTimeout bounds dialing and the subscription reply.
	defaultSubscribeTimeout = 10 * time.Second
)

// PubSubSource subscribes to a Redis pub/sub channel or pattern. Pub/sub is
// at most once: Redis keeps nothing for a subscriber that is away, so a
// message published while the source reconnects is lost, and there is
// nothing to acknowledge.
type PubSubSource struct {
	name    string
	opts    sourceOptions
	log     *slog.Logger
	clients []*goredis.Client
	shown   []string
	rnd     func() float64

	pingEvery        time.Duration
	pingTimeout      time.Duration
	subscribeTimeout time.Duration

	life     context.Context // ends at Close, which stops a running Start
	stopLife context.CancelFunc

	mu      sync.Mutex
	cur     int
	status  error
	started bool
	closed  bool
}

func newPubSubSource(spec plugin.Spec, opts sourceOptions) *PubSubSource {
	s := &PubSubSource{
		name: spec.Name, opts: opts, log: spec.Env.Log(), rnd: rand.Float64,
		pingEvery: defaultPingEvery, pingTimeout: defaultPingTimeout, subscribeTimeout: defaultSubscribeTimeout,
	}
	s.life, s.stopLife = context.WithCancel(context.Background())
	for _, sv := range opts.servers {
		s.clients = append(s.clients, sv.client())
		s.shown = append(s.shown, sv.shown)
	}
	return s
}

// Name is the source name.
func (s *PubSubSource) Name() string { return s.name }

// Health is why the subscription is down while it is, else a PING.
func (s *PubSubSource) Health(ctx context.Context) error {
	s.mu.Lock()
	st, c, closed := s.status, s.clients[s.cur], s.closed
	s.mu.Unlock()
	switch {
	case closed:
		return errClosed
	case st != nil:
		return st
	}
	return c.Ping(ctx).Err()
}

// Start subscribes and hands every message over until ctx ends. When the
// subscription fails it subscribes again, with backoff, on the next server
// of url and failover_urls. It returns an error only when it gives up,
// after reconnect.max_attempts failed subscribes in a row.
func (s *PubSubSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return fmt.Errorf("source %s: already started or closed", s.name)
	}
	s.started = true
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopOnClose := context.AfterFunc(s.life, cancel)
	defer stopOnClose()
	r := retrier{rc: s.opts.reconnect, rnd: s.rnd}
	for ctx.Err() == nil {
		subscribed, err := s.session(ctx, out)
		if ctx.Err() != nil {
			break
		}
		s.setStatus(err)
		if subscribed {
			r.success()
		}
		if !r.failure() {
			err = fmt.Errorf("source %s: giving up after %d failed subscribes in a row: %w", s.name, r.failures, err)
			s.setStatus(err)
			return err
		}
		if r.failures > 0 && len(s.clients) > 1 {
			s.rotate()
		}
		wait := r.wait()
		s.log.Warn("redis source resubscribing", "source", s.name, "url", s.url(), "channel", s.opts.channel,
			"failures", r.failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			break
		}
	}
	s.setStatus(errStopped)
	return nil
}

// session subscribes once and reads until the subscription ends. It
// reports whether the subscription was made.
func (s *PubSubSource) session(ctx context.Context, out chan<- *envelope.Envelope) (bool, error) {
	s.mu.Lock()
	client, shown := s.clients[s.cur], s.shown[s.cur]
	s.mu.Unlock()
	ps := client.Subscribe(ctx) // no channel yet: nothing is sent
	defer func() { _ = ps.Close() }()
	// Receive only returns on a message or a broken connection.
	stop := context.AfterFunc(ctx, func() { _ = ps.Close() })
	defer stop()

	if err := s.subscribe(ctx, ps); err != nil {
		return false, err
	}
	s.setStatus(nil)
	s.log.Info("redis source subscribed", "source", s.name, "url", shown, "channel", s.opts.channel, "pattern", s.opts.pattern)

	var activity, busy atomic.Int64
	var dead atomic.Pointer[error]
	done := make(chan struct{})
	defer close(done)
	go s.watchdog(ps, &activity, &busy, &dead, done)
	for {
		msg, err := ps.Receive(ctx)
		if err != nil {
			if reason := dead.Load(); reason != nil {
				return true, *reason
			}
			return true, fmt.Errorf("receive on %s: %w", s.opts.channel, err)
		}
		activity.Add(1)
		if m, ok := msg.(*goredis.Message); ok {
			s.handle(ctx, out, m, &busy)
		}
	}
}

// subscribe sends SUBSCRIBE (or PSUBSCRIBE for a pattern) and waits for
// Redis to confirm it.
func (s *PubSubSource) subscribe(ctx context.Context, ps *goredis.PubSub) error {
	verb := "SUBSCRIBE"
	if s.opts.pattern {
		verb = "PSUBSCRIBE"
	}
	sctx, cancel := context.WithTimeout(ctx, s.subscribeTimeout)
	defer cancel()
	var err error
	if s.opts.pattern {
		err = ps.PSubscribe(sctx, s.opts.channel)
	} else {
		err = ps.Subscribe(sctx, s.opts.channel)
	}
	if err == nil {
		var reply any
		if reply, err = ps.ReceiveTimeout(sctx, s.subscribeTimeout); err == nil {
			if _, ok := reply.(*goredis.Subscription); !ok {
				err = fmt.Errorf("unexpected reply %T", reply)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", verb, s.opts.channel, err)
	}
	return nil
}

// handle hands one message over. While the relay applies backpressure the
// source stops reading, and Redis buffers what is published meanwhile, up
// to its client-output-buffer-limit for pub/sub clients; past it, Redis
// drops the connection and those messages are lost.
func (s *PubSubSource) handle(ctx context.Context, out chan<- *envelope.Envelope, m *goredis.Message, busy *atomic.Int64) {
	body := []byte(m.Payload)
	if int64(len(body)) > s.opts.maxBytes {
		s.log.Warn("redis pub/sub message is over max_body_bytes; dropping it", "source", s.name,
			"channel", m.Channel, "bytes", len(body), "max_body_bytes", s.opts.maxBytes)
		return
	}
	env := envelope.New(s.name, kind, body)
	env.Subject = m.Channel
	env.ContentType = sniff(body)
	env.Attributes["redis.channel"] = m.Channel
	if m.Pattern != "" {
		env.Attributes["redis.pattern"] = m.Pattern
	}
	env.Attributes["ordering_key"] = m.Channel
	select {
	case out <- env:
		return
	default:
	}
	busy.Add(1) // odd while waiting: the watchdog holds off
	defer busy.Add(1)
	select {
	case out <- env:
	case <-ctx.Done():
	}
}

// watchdog pings a subscription that has been quiet for pingEvery and
// closes it when nothing, not even the pong, comes back within
// pingTimeout: a half-open connection would otherwise lose every message
// in silence. A reader waiting for the relay (busy is odd) cannot read the
// pong, so it is never held against the connection.
func (s *PubSubSource) watchdog(ps *goredis.PubSub, activity, busy *atomic.Int64, dead *atomic.Pointer[error], done <-chan struct{}) {
	if s.pingEvery <= 0 {
		return
	}
	seen := activity.Load()
	for {
		select {
		case <-done:
			return
		case <-time.After(s.pingEvery):
		}
		b := busy.Load()
		if a := activity.Load(); a != seen || b%2 == 1 {
			seen = a
			continue
		}
		pctx, cancel := context.WithTimeout(context.Background(), s.pingTimeout)
		err := ps.Ping(pctx)
		cancel()
		if err == nil {
			select {
			case <-done:
				return
			case <-time.After(s.pingTimeout):
			}
			if a := activity.Load(); a != seen || busy.Load() != b {
				seen = a
				continue
			}
			err = fmt.Errorf("no reply to PING within %s", s.pingTimeout)
		}
		err = fmt.Errorf("subscription to %s is unresponsive: %w", s.opts.channel, err)
		dead.Store(&err)
		_ = ps.Close()
		return
	}
}

// Close stops the source and closes the connections. Pub/sub has nothing
// to acknowledge, so there is nothing to wait for.
func (s *PubSubSource) Close(context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.stopLife()
	var errs []error
	for _, c := range s.clients {
		if err := c.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *PubSubSource) url() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shown[s.cur]
}

func (s *PubSubSource) rotate() {
	s.mu.Lock()
	s.cur = (s.cur + 1) % len(s.clients)
	s.mu.Unlock()
}

func (s *PubSubSource) setStatus(err error) {
	s.mu.Lock()
	s.status = err
	s.mu.Unlock()
}
