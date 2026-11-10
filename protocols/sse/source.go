package sse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

// defaultStableAfter: a stream up this long resets the failure count even
// if it delivered nothing.
const defaultStableAfter = 30 * time.Second

// minRetry is the floor of a server's retry: a server asking for 0 must not
// turn failed connects into a busy loop.
const minRetry = time.Millisecond

var (
	errNotStarted = errors.New("not started")
	errStopped    = errors.New("stopped")
)

// Source is an sse source, in listen or subscribe mode.
type Source struct {
	name   string
	kind   string
	opts   sourceOptions
	auth   *inbound.Authenticator
	client *http.Client
	log    *slog.Logger

	stableAfter time.Duration
	rnd         func() float64

	started atomic.Bool

	// The subscribe loop's own state, carried from stream to stream.
	lastID string        // sent as Last-Event-ID
	base   time.Duration // the reconnect backoff, or the server's retry

	mu       sync.Mutex
	addr     string
	status   error
	stopping bool
	reqs     sync.WaitGroup
}

func newSource(ctx context.Context, spec plugin.Spec) (*Source, error) {
	src, opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	s := &Source{
		name: spec.Name, kind: spec.Kind, opts: opts, log: spec.Env.Log(),
		stableAfter: defaultStableAfter, rnd: rand.Float64, status: errNotStarted,
		base: opts.reconnect.backoff,
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
	s.log.Info("sse source listening", "source", s.name, "addr", h.Addr(), "path", s.opts.path)

	<-ctx.Done()
	s.mu.Lock()
	s.stopping, s.status = true, errStopped
	s.mu.Unlock()
	// Every request saw ctx end and stopped reading its body.
	h.Release()
	s.reqs.Wait()
	return nil
}

// track counts a request in, unless the source is stopping.
func (s *Source) track() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return false
	}
	s.reqs.Add(1)
	return true
}

// serve takes one pushed event stream: the body of a POST, parsed as it
// arrives, one envelope per event. It answers 200 once the body ends.
func (s *Source) serve(ctx context.Context, out chan<- *envelope.Envelope, w http.ResponseWriter, r *http.Request) {
	if !s.track() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.reqs.Done()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "POST an event stream", http.StatusMethodNotAllowed)
		return
	}
	if err := s.auth.Check(r, nil); err != nil {
		s.log.Debug("sse request refused", "source", s.name, "remote_addr", r.RemoteAddr, "err", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && !isEventStream(ct) {
		http.Error(w, "the body must be "+contentType, http.StatusUnsupportedMediaType)
		return
	}
	// Shutdown interrupts a body read that waits for the client. The
	// deadline is only set while the handler runs.
	rc := http.NewResponseController(w)
	var mu sync.Mutex
	returned := false
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if !returned {
			_ = rc.SetReadDeadline(time.Now())
		}
	})
	defer func() {
		stop()
		mu.Lock()
		returned = true
		mu.Unlock()
	}()

	headers := r.Header.Clone()
	s.auth.StripCredentials(headers)
	remote, path := r.RemoteAddr, r.URL.Path
	p := NewParser(r.Body, s.opts.maxBytes)
	for {
		ev, err := p.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		switch {
		case err == nil:
		case ctx.Err() != nil:
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		case errors.Is(err, ErrTooLarge):
			http.Error(w, fmt.Sprintf("an event is larger than %d bytes", s.opts.maxBytes), http.StatusRequestEntityTooLarge)
			return
		default:
			http.Error(w, "reading the event stream: "+err.Error(), http.StatusBadRequest)
			return
		}
		env := s.newEnvelope(ev, path)
		env.Headers = headers.Clone()
		env.Attributes["remote_addr"] = remote
		select {
		case out <- env:
		case <-ctx.Done():
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// session is what one subscribe stream did.
type session struct {
	connected bool
	delivered int
	uptime    time.Duration
}

// subscribe streams, and reconnects with backoff when the stream ends or
// fails to open. A failed connect rotates to the next of url and
// failover_urls; a stream that delivered an event or stayed up for
// stableAfter resets the failure count.
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
		wait := backoff.Delay(s.base, rc.maxBackoff, rc.jitter, max(failures, 1), s.rnd)
		s.log.Warn("sse source reconnecting", "source", s.name, "url", redactURL(s.opts.urls[i]),
			"failures", failures, "wait", wait.String(), "last_event_id", s.lastID, "err", err)
		if !backoff.Sleep(ctx, wait) {
			s.setStatus(errStopped)
			return nil
		}
	}
}

// session opens one stream from u and reads it until it ends.
func (s *Source) session(ctx context.Context, u string, out chan<- *envelope.Envelope) (session, error) {
	shown := redactURL(u)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return session{}, err
	}
	req.Header = s.opts.header.Clone()
	req.Header.Set("Accept", contentType)
	req.Header.Set("Cache-Control", "no-cache")
	if s.lastID != "" {
		req.Header.Set("Last-Event-ID", s.lastID)
	}
	if s.opts.host != "" {
		req.Host = s.opts.host
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return session{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return session{}, fmt.Errorf("GET %s answered %s", shown, resp.Status)
	}
	if ct := resp.Header.Get("Content-Type"); !isEventStream(ct) {
		return session{}, fmt.Errorf("GET %s answered content type %q, not %s", shown, ct, contentType)
	}
	s.setStatus(nil)
	s.log.Info("sse source connected", "source", s.name, "url", shown, "last_event_id", s.lastID)

	start := time.Now()
	subject := urlPath(u)
	p := NewParser(resp.Body, s.opts.maxBytes)
	p.SetLastID(s.lastID)
	n := 0
	err = func() error {
		for {
			ev, err := p.Next()
			if err != nil {
				return err
			}
			env := s.newEnvelope(ev, subject)
			env.Attributes["url"] = shown
			select {
			case out <- env:
				n++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}()
	// What the stream said carries over to the next one.
	s.lastID = p.LastID()
	if d, ok := p.Retry(); ok {
		s.base = max(d, minRetry)
	}
	if errors.Is(err, io.EOF) {
		err = errors.New("the server ended the stream")
	}
	return session{connected: true, delivered: n, uptime: time.Since(start)}, fmt.Errorf("%s: %w", shown, err)
}

// newEnvelope turns one event into an envelope. Its subject is the event
// name when it has one, else path.
func (s *Source) newEnvelope(ev Event, path string) *envelope.Envelope {
	env := envelope.New(s.name, s.kind, ev.Data)
	env.ContentType = textContentType(ev.Data)
	env.Subject = path
	name := ev.Name
	if name != "" {
		env.Subject = name
	} else {
		name = "message"
	}
	env.Attributes["sse.id"] = ev.ID
	env.Attributes["sse.event"] = name
	return env
}
