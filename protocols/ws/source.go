package ws

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a source; tests shorten them.
const (
	defaultPingEvery   = 30 * time.Second
	defaultPingTimeout = 10 * time.Second
	// defaultCloseGrace is how long a peer has to answer the close
	// handshake at shutdown before the connection is cut.
	defaultCloseGrace = 2 * time.Second
	// defaultStableAfter: a connection up this long resets the failure count
	// even if it delivered nothing.
	defaultStableAfter = 30 * time.Second
	defaultDialTimeout = 30 * time.Second
)

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
)

// Source is a ws or wss source, in listen or subscribe mode.
type Source struct {
	name   string
	kind   string
	opts   sourceOptions
	auth   *inbound.Authenticator
	client *http.Client
	log    *slog.Logger

	pingEvery   time.Duration
	pingTimeout time.Duration
	closeGrace  time.Duration
	stableAfter time.Duration
	dialTimeout time.Duration
	rnd         func() float64

	started atomic.Bool
	live    atomic.Int64 // open listen connections

	mu       sync.Mutex
	addr     string
	status   error
	stopping bool
	conns    sync.WaitGroup
}

func newSource(ctx context.Context, spec plugin.Spec) (*Source, error) {
	src, opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	s := &Source{
		name: spec.Name, kind: spec.Kind, opts: opts, log: spec.Env.Log(),
		pingEvery: defaultPingEvery, pingTimeout: defaultPingTimeout, closeGrace: defaultCloseGrace,
		stableAfter: defaultStableAfter, dialTimeout: defaultDialTimeout, rnd: rand.Float64,
		status: errNotStarted,
	}
	if opts.mode == modeListen {
		if s.auth, err = inbound.New(ctx, src.Auth); err != nil {
			return nil, fmt.Errorf("source %s: %w", spec.Name, err)
		}
	} else {
		s.client = httpClient(opts.clientTLS)
	}
	return s, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is nil while listening or connected, else why not.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Addr is the address a listen source is bound to, "" before it listens.
func (s *Source) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Source) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

// Start listens or subscribes until ctx ends, then returns nil. It fails
// only when the listener cannot bind, or when a subscribe source used up
// reconnect.max_attempts.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	if s.opts.mode == modeListen {
		return s.listen(ctx, out)
	}
	return s.subscribe(ctx, out)
}

func (s *Source) listen(ctx context.Context, out chan<- *envelope.Envelope) error {
	h, err := httpmux.Register(s.opts.listen, s.opts.path, httpmux.Options{TLS: s.opts.serverTLS, TLSKey: s.opts.tlsKey},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serve(ctx, out, w, r) }))
	if err != nil {
		s.setStatus(err)
		return err
	}
	s.mu.Lock()
	s.addr, s.status = h.Addr(), nil
	s.mu.Unlock()
	s.log.Info("ws source listening", "source", s.name, "addr", h.Addr(), "path", s.opts.path)

	<-ctx.Done()
	s.mu.Lock()
	s.stopping, s.status = true, errStopped
	s.mu.Unlock()
	h.Release()
	// Every connection saw ctx end: it says goodbye (1001) and returns.
	s.conns.Wait()
	return nil
}

// track counts a connection in, unless the source is stopping.
func (s *Source) track() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return false
	}
	s.conns.Add(1)
	return true
}

// serve accepts one WebSocket client and reads its messages.
func (s *Source) serve(ctx context.Context, out chan<- *envelope.Envelope, w http.ResponseWriter, r *http.Request) {
	if !s.track() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.conns.Done()
	if err := s.auth.Check(r, nil); err != nil {
		s.log.Debug("ws connection refused", "source", s.name, "remote_addr", r.RemoteAddr, "err", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.log.Debug("ws handshake failed", "source", s.name, "remote_addr", r.RemoteAddr, "err", err)
		return // Accept answered the request
	}
	defer func() { _ = c.CloseNow() }()
	c.SetReadLimit(s.opts.maxBytes)
	s.live.Add(1)
	defer s.live.Add(-1)

	headers := r.Header.Clone()
	s.auth.StripCredentials(headers)
	remote, subject := r.RemoteAddr, r.URL.Path
	s.log.Debug("ws client connected", "source", s.name, "remote_addr", remote)
	n, err := s.pump(ctx, c, out, func(env *envelope.Envelope) {
		env.Subject = subject
		env.Headers = headers.Clone()
		env.Attributes["remote_addr"] = remote
	})
	s.log.Debug("ws client gone", "source", s.name, "remote_addr", remote, "messages", n, "err", err)
}

// session is what one subscribe connection did.
type session struct {
	connected bool
	delivered int
	uptime    time.Duration
}

// subscribe dials, reads until the connection ends, and reconnects with
// backoff. A failed connect rotates to the next of url and failover_urls; a
// connection that delivered a message or stayed up for stableAfter resets
// the failure count.
func (s *Source) subscribe(ctx context.Context, out chan<- *envelope.Envelope) error {
	rc := s.opts.reconnect
	failures, i := 0, 0
	for {
		st, err := s.session(ctx, s.opts.urls[i], out)
		if ctx.Err() != nil {
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
		s.log.Warn("ws source reconnecting", "source", s.name, "url", redactURL(s.opts.urls[i]),
			"failures", failures, "wait", wait.String(), "err", err)
		if !backoff.Sleep(ctx, wait) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// session dials u and reads from it until the connection ends.
func (s *Source) session(ctx context.Context, u string, out chan<- *envelope.Envelope) (session, error) {
	shown := redactURL(u)
	dctx, cancel := context.WithTimeout(ctx, s.dialTimeout)
	c, _, err := websocket.Dial(dctx, u, s.opts.dial.options(s.client))
	cancel()
	if err != nil {
		return session{}, fmt.Errorf("dial %s: %w", shown, err)
	}
	defer func() { _ = c.CloseNow() }()
	c.SetReadLimit(s.opts.maxBytes)
	s.setStatus(nil)
	s.log.Info("ws source connected", "source", s.name, "url", shown)

	subject := urlPath(u)
	start := time.Now()
	n, err := s.pump(ctx, c, out, func(env *envelope.Envelope) {
		env.Subject = subject
		env.Attributes["url"] = shown
	})
	return session{connected: true, delivered: n, uptime: time.Since(start)}, fmt.Errorf("%s: %w", shown, err)
}

// pump reads messages from c until the connection ends, sending each one on
// out as an envelope (decorate adds what the mode knows). It blocks while
// out is full: TCP flow control then holds the peer back. When ctx ends it
// closes the connection with 1001 Going Away and cuts it if the peer does
// not answer within closeGrace. It returns the messages handed over.
func (s *Source) pump(ctx context.Context, c *websocket.Conn, out chan<- *envelope.Envelope, decorate func(*envelope.Envelope)) (int, error) {
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := context.AfterFunc(ctx, func() {
		// Cancelling the pending read cuts the connection.
		force := time.AfterFunc(s.closeGrace, cancel)
		defer force.Stop()
		_ = c.Close(websocket.StatusGoingAway, "hikyaku is shutting down")
	})
	defer stop()
	var busy atomic.Int64 // odd while waiting for out
	alive := make(chan struct{})
	defer close(alive)
	go keepalive(c, s.pingEvery, s.pingTimeout, alive, &busy)

	n := 0
	for {
		typ, data, err := c.Read(rctx)
		if err != nil {
			return n, err
		}
		if ctx.Err() != nil {
			continue // closing: keep reading until the peer answers
		}
		env := s.newEnvelope(typ, data)
		decorate(env)
		select {
		case out <- env:
			n++
			continue
		default:
		}
		busy.Add(1)
		select {
		case out <- env:
			n++
		case <-ctx.Done():
		}
		busy.Add(1)
	}
}

// newEnvelope turns one message into an envelope.
func (s *Source) newEnvelope(typ websocket.MessageType, data []byte) *envelope.Envelope {
	env := envelope.New(s.name, s.kind, data)
	if typ == websocket.MessageBinary {
		env.ContentType = contentBinary
		env.Attributes["ws.frame"] = "binary"
		return env
	}
	env.ContentType = textContentType(data)
	env.Attributes["ws.frame"] = "text"
	return env
}
