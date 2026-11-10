package sse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

// clientBuffer is how many events a client may fall behind before it is
// dropped.
const clientBuffer = 256

// defaultPingEvery is how often an idle stream gets a ": ping" comment.
const defaultPingEvery = 15 * time.Second

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("sse destination is closed")

// Sink is an sse destination: an event stream served on its listen address
// and path, to every client that connects.
type Sink struct {
	name     string
	endpoint string // listen + path
	opts     sinkOptions
	handle   *httpmux.Handle
	auth     *inbound.Authenticator // checks the clients that read the stream
	log      *slog.Logger

	mu        sync.Mutex
	pingEvery time.Duration // under mu: the handler is live from the start
	ring      *ring
	clients   map[*client]struct{}
	closed    bool
}

// event is one encoded event, shared by the ring and every client.
type event struct {
	id   string
	wire []byte
}

// closeGrace is how long a stream may take to end once the sink closes: a
// client that reads gets a clean end, a stuck one is cut.
const closeGrace = time.Second

// client is one connected stream.
type client struct {
	ch    chan *event
	gone  chan struct{} // closed when the sink drops the client
	grace time.Duration // how long a pending write may still take; set before gone closes
	once  sync.Once
}

// drop ends the client's stream: a write still pending after grace fails.
func (c *client) drop(grace time.Duration) {
	c.once.Do(func() {
		c.grace = grace
		close(c.gone)
	})
}

func newSink(ctx context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	s := &Sink{
		name: spec.Name, endpoint: opts.listen + opts.path, opts: opts, log: spec.Env.Log(),
		pingEvery: defaultPingEvery, ring: newRing(opts.replay), clients: map[*client]struct{}{},
	}
	if s.auth, err = inbound.New(ctx, opts.auth); err != nil {
		return nil, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	return s, nil
}

// Start serves the stream: the engine calls it when the relay runs, so
// clients can connect before messages flow. One-shot commands (send,
// route, ai test) never start it, so they do not take the running relay's
// port; their deliveries only reach the replay buffer.
func (s *Sink) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle != nil || s.closed {
		return nil
	}
	h, err := httpmux.Register(s.opts.listen, s.opts.path, httpmux.Options{TLS: s.opts.serverTLS, TLSKey: s.opts.tlsKey}, http.HandlerFunc(s.serve))
	if err != nil {
		return fmt.Errorf("destination %s: %w", s.name, err)
	}
	s.handle = h
	s.log.Info("sse destination serving", "destination", s.name, "addr", h.Addr(), "path", s.opts.path)
	return nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Addr is the address the stream is served on ("" before Start).
func (s *Sink) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return ""
	}
	return s.handle.Addr()
}

// Deliver broadcasts the message as one event to the clients connected now
// and keeps it for replay. It succeeds with no client connected, and never
// waits for a client: one that has fallen clientBuffer events behind is
// dropped.
func (s *Sink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	name := env.Subject
	if s.opts.event != nil {
		var err error
		if name, err = s.opts.event.Render(env); err != nil {
			return nil, plugin.Permanentf("destination %s: event: %v", s.name, err)
		}
	}
	id := clean(env.ID, true)
	ev := &event{id: id, wire: encode(id, clean(name, false), env.Body)}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	s.ring.push(ev)
	dropped := 0
	for c := range s.clients {
		select {
		case c.ch <- ev:
		default:
			delete(s.clients, c)
			c.drop(0)
			dropped++
		}
	}
	s.mu.Unlock()
	if dropped > 0 {
		s.log.Warn("sse clients fell behind; dropped", "destination", s.name, "clients", dropped, "buffer", clientBuffer)
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// Health is nil while the stream is served.
func (s *Sink) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	return nil
}

// Close ends every client's stream and stops serving.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for c := range s.clients {
		c.drop(closeGrace)
	}
	s.clients = map[*client]struct{}{}
	h := s.handle
	s.mu.Unlock()
	if h == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Release() // waits for the handlers, which return once dropped
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// subscribe adds a client and returns what it missed since lastID (nothing
// when the id is unknown), and the ping interval. Both happen under the
// lock that Deliver holds, so the client misses no event and gets none
// twice.
func (s *Sink) subscribe(c *client, lastID string) ([]*event, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, false
	}
	s.clients[c] = struct{}{}
	if lastID == "" {
		return nil, s.pingEvery, true
	}
	return s.ring.after(lastID), s.pingEvery, true
}

func (s *Sink) unsubscribe(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, c)
}

// serve streams events to one client.
func (s *Sink) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "GET the event stream", http.StatusMethodNotAllowed)
		return
	}
	if err := s.auth.Check(r, nil); err != nil {
		s.log.Debug("sse client refused", "destination", s.name, "remote_addr", r.RemoteAddr, "err", err)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // keep proxies such as nginx from buffering the stream
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	c := &client{ch: make(chan *event, clientBuffer), gone: make(chan struct{})}
	backlog, pingEvery, ok := s.subscribe(c, r.Header.Get("Last-Event-ID"))
	if !ok {
		http.Error(w, "the stream is closed", http.StatusServiceUnavailable)
		return
	}
	defer s.unsubscribe(c)

	rc := http.NewResponseController(w)
	// A client that stopped reading blocks a write: once it is dropped, cut
	// that write short. The goroutine ends before the handler returns.
	stopped := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-c.gone:
			_ = rc.SetWriteDeadline(time.Now().Add(c.grace))
		case <-stopped:
		}
	}()
	defer func() {
		close(stopped)
		wg.Wait()
	}()

	w.WriteHeader(http.StatusOK)
	for _, ev := range backlog {
		if _, err := w.Write(ev.wire); err != nil {
			return
		}
	}
	if err := rc.Flush(); err != nil {
		return
	}
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-c.gone:
			return
		case <-r.Context().Done():
			return
		case ev := <-c.ch:
			if _, err := w.Write(ev.wire); err != nil {
				return
			}
		case <-ping.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
}

// encode writes an event in the wire format: id, event (when named), and
// one data line per line of the body.
func encode(id, name string, body []byte) []byte {
	var b bytes.Buffer
	b.Grow(len(body) + len(id) + len(name) + 32)
	if id != "" {
		b.WriteString("id: ")
		b.WriteString(id)
		b.WriteByte('\n')
	}
	if name != "" {
		b.WriteString("event: ")
		b.WriteString(name)
		b.WriteByte('\n')
	}
	for {
		// CR and CRLF end a line in an event stream too, so they split as LF
		// does: no line of the body can break the framing.
		i := bytes.IndexAny(body, "\r\n")
		if i < 0 {
			break
		}
		b.WriteString("data: ")
		b.Write(body[:i])
		b.WriteByte('\n')
		if body[i] == '\r' && i+1 < len(body) && body[i+1] == '\n' {
			i++
		}
		body = body[i+1:]
	}
	b.WriteString("data: ")
	b.Write(body)
	b.WriteString("\n\n")
	return b.Bytes()
}

// clean drops what would break an id or event line: line endings, and for
// an id the NUL that makes clients ignore it.
func clean(s string, id bool) string {
	return strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || (id && r == 0) {
			return -1
		}
		return r
	}, s)
}

// ring keeps the last events for Last-Event-ID replay.
type ring struct {
	buf  []*event
	next int
	full bool
}

func newRing(n int) *ring { return &ring{buf: make([]*event, n)} }

func (r *ring) push(ev *event) {
	r.buf[r.next] = ev
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
}

// after returns the events kept after the one with id, oldest first; nil
// when no kept event has that id.
func (r *ring) after(id string) []*event {
	var all []*event
	if r.full {
		all = append(all, r.buf[r.next:]...)
	}
	all = append(all, r.buf[:r.next]...)
	for i := len(all) - 1; i >= 0; i-- {
		if all[i].id == id {
			return all[i+1:]
		}
	}
	return nil
}
