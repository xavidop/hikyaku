package tcp

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// newTestSource builds a source of kind on 127.0.0.1:0 unless cfg listens
// elsewhere.
func newTestSource(t *testing.T, kind string, cfg config.Source) *Source {
	t.Helper()
	s, _ := newLoggedSource(t, kind, cfg)
	return s
}

// newLoggedSource is newTestSource with its logs recorded.
func newLoggedSource(t *testing.T, kind string, cfg config.Source) (*Source, *records) {
	t.Helper()
	cfg.Name, cfg.Type = "in", kind
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	logs := &records{}
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Logger: logs.logger()}})
	require.NoError(t, err)
	return s, logs
}

// run starts s with a roomy queue. The returned stop (also run at cleanup)
// cancels it and checks that Start returned nil.
func run(t *testing.T, s *Source) (<-chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 64)
	return out, runOut(t, s, out)
}

func runOut(t *testing.T, s *Source, out chan *envelope.Envelope) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("Start did not return after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// listening waits for s to bind and returns its address.
func listening(t *testing.T, s *Source) string {
	t.Helper()
	require.Eventually(t, func() bool { return s.Addr() != "" }, 2*time.Second, 2*time.Millisecond)
	return s.Addr()
}

func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no envelope arrived")
		return nil
	}
}

func noEnvelope(t *testing.T, out <-chan *envelope.Envelope, wait time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected envelope %q", env.BodyText())
	case <-time.After(wait):
	}
}

// dial connects to a tcp source; the connection closes at cleanup.
func dial(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	tc, ok := c.(*net.TCPConn)
	require.True(t, ok)
	return tc
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// waitClosed waits for the peer to close c: a read returns an error.
func waitClosed(t *testing.T, c net.Conn) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, 64)
	for {
		_, err := c.Read(buf)
		if err != nil {
			require.False(t, isTimeout(err), "the source did not close the connection")
			return
		}
	}
}

// newTestSink builds a destination; its kind is the URL's (udp for udp://
// and dtls://, else tcp). It closes at cleanup.
func newTestSink(t *testing.T, cfg config.Endpoint) *Sink {
	t.Helper()
	s, err := buildSink(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func buildSink(cfg config.Endpoint) (*Sink, error) {
	if cfg.Type == "" {
		cfg.Type = kindTCP
		if strings.HasPrefix(cfg.URL, "udp://") || strings.HasPrefix(cfg.URL, "dtls://") {
			cfg.Type = kindUDP
		}
	}
	return newSink(context.Background(), plugin.Spec{Name: "out", Kind: cfg.Type, Config: &cfg})
}

func msg(body string) *envelope.Envelope { return envelope.New("in", "test", []byte(body)) }

// assertClass checks a delivery failure's class and that it carries tags.
func assertClass(t *testing.T, err error, class plugin.Class, tags ...string) {
	t.Helper()
	require.Error(t, err)
	got, _ := plugin.Classify(err)
	assert.Equal(t, class, got, "class of %v", err)
	for _, tag := range tags {
		assert.Contains(t, plugin.Tags(err), tag, "tags of %v", err)
	}
}

// deliver delivers body with a 5s attempt timeout, like the engine.
func deliver(s *Sink, body string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.Deliver(ctx, msg(body))
	return err
}

// peer is a tcp server that reads frames the way a source with framing f
// does, and counts the connections it accepted.
type peer struct {
	ln       net.Listener
	f        framing
	frames   chan string
	accepted atomic.Int64

	mu    sync.Mutex
	conns []net.Conn
	// onConn, when set, serves a connection instead of reading its frames.
	onConn func(c net.Conn)
}

func newPeer(t *testing.T, f framing) *peer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &peer{ln: ln, f: f, frames: make(chan string, 1024)}
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *peer) addr() string { return p.ln.Addr().String() }
func (p *peer) url() string  { return "tcp://" + p.addr() }

func (p *peer) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.accepted.Add(1)
		p.mu.Lock()
		p.conns = append(p.conns, c)
		serve := p.onConn
		p.mu.Unlock()
		if serve != nil {
			go serve(c)
			continue
		}
		go p.read(c)
	}
}

func (p *peer) read(c net.Conn) {
	defer func() { _ = c.Close() }()
	fr := p.f.reader(bufio.NewReader(c), 64<<20)
	for {
		b, err := fr.next()
		if err != nil {
			return
		}
		p.frames <- string(b)
	}
}

func (p *peer) serve(fn func(c net.Conn)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onConn = fn
}

func (p *peer) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *peer) next(t *testing.T) string {
	t.Helper()
	select {
	case f := <-p.frames:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("the peer got no frame")
		return ""
	}
}

// records keeps what a source or sink logs.
type records struct {
	mu   sync.Mutex
	recs []slog.Record
}

// logger logs into r.
func (r *records) logger() *slog.Logger { return slog.New(recordHandler{r: r}) }

// recordHandler adds a logger's With attributes to each record it keeps.
type recordHandler struct {
	r     *records
	attrs []slog.Attr
}

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) WithGroup(string) slog.Handler            { return h }
func (h recordHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return recordHandler{r: h.r, attrs: append(slices.Clone(h.attrs), as...)}
}

func (h recordHandler) Handle(_ context.Context, rec slog.Record) error {
	rec = rec.Clone()
	rec.AddAttrs(h.attrs...)
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	h.r.recs = append(h.r.recs, rec)
	return nil
}

func (r *records) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level == level && rec.Message == msg {
			n++
		}
	}
	return n
}

// attr returns an attribute of the first record with msg.
func (r *records) attr(msg, key string) (slog.Value, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.recs {
		if rec.Message != msg {
			continue
		}
		var v slog.Value
		found := false
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				v, found = a.Value, true
				return false
			}
			return true
		})
		return v, found
	}
	return slog.Value{}, false
}
