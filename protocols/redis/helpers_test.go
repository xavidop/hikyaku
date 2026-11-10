package redis

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// waitFor is how long a test waits for something that should happen.
const waitFor = 5 * time.Second

// startMini runs a miniredis for the test and returns it with an admin
// client.
func startMini(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	return mr, adminClient(t, mr.Addr())
}

func adminClient(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func redisURL(addr string) string { return "redis://" + addr }

// streamConfig is a stream source on addr with test-friendly timings.
func streamConfig(addr string) config.Source {
	return config.Source{URL: redisURL(addr), Stream: "orders", Group: "relay", Consumer: "c1",
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxBackoff: config.Duration(50 * time.Millisecond)}}
}

func newTestSource(t *testing.T, cfg config.Source) plugin.Source {
	t.Helper()
	return newTestSourceLog(t, cfg, nil)
}

func newTestSourceLog(t *testing.T, cfg config.Source, log *slog.Logger) plugin.Source {
	t.Helper()
	cfg.Name, cfg.Type = "in", kind
	src, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "test", Logger: log}})
	require.NoError(t, err)
	switch s := src.(type) {
	case *StreamSource:
		s.block = 200 * time.Millisecond
		s.callTimeout = time.Second
	case *PubSubSource:
		s.subscribeTimeout = time.Second
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = src.(plugin.Closer).Close(ctx)
	})
	return src
}

func newStreamSourceT(t *testing.T, cfg config.Source) *StreamSource {
	t.Helper()
	s, ok := newTestSource(t, cfg).(*StreamSource)
	require.True(t, ok)
	return s
}

func newPubSubSourceT(t *testing.T, cfg config.Source) *PubSubSource {
	t.Helper()
	s, ok := newTestSource(t, cfg).(*PubSubSource)
	require.True(t, ok)
	return s
}

func newTestSink(t *testing.T, ep config.Endpoint) *Sink {
	t.Helper()
	ep.Type = kind
	s, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: kind, Config: &ep, Env: plugin.Env{Relay: "test"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// run starts src. The returned stop (also run at cleanup) cancels it and
// checks that Start returned nil.
func run(t *testing.T, src plugin.Source) (<-chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 64)
	return out, runOut(t, src, out)
}

func runOut(t *testing.T, src plugin.Source, out chan *envelope.Envelope) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- src.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(waitFor):
				t.Error("Start did not return after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(waitFor):
		t.Fatal("no message")
		return nil
	}
}

func noRecv(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected message %s (%s)", env.ID, env.BodyText())
	case <-time.After(d):
	}
}

// settle finishes a message the way the engine does.
func settle(env *envelope.Envelope, status envelope.AckStatus) {
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}

// pendingCount is the size of the group's pending entries list.
func pendingCount(t *testing.T, c *goredis.Client, stream, group string) int64 {
	t.Helper()
	p, err := c.XPending(context.Background(), stream, group).Result()
	require.NoError(t, err)
	return p.Count
}

// eventuallyPending waits for the pending entries list to reach n.
func eventuallyPending(t *testing.T, c *goredis.Client, stream, group string, n int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		p, err := c.XPending(context.Background(), stream, group).Result()
		return err == nil && p.Count == n
	}, waitFor, 10*time.Millisecond, "pending entries never reached %d", n)
}

// xadd appends a raw entry and returns its ID.
func xadd(t *testing.T, c *goredis.Client, stream string, values ...any) string {
	t.Helper()
	id, err := c.XAdd(context.Background(), &goredis.XAddArgs{Stream: stream, Values: values}).Result()
	require.NoError(t, err)
	return id
}

// msg is an envelope as a source would make it.
func msg(body string) *envelope.Envelope {
	env := envelope.New("test", "http", []byte(body))
	env.ContentType = "application/json"
	return env
}

// proxy forwards TCP connections to a server. Drop cuts every open one;
// SetRefuse closes new ones at once; SetBlackhole swallows what is sent
// both ways, as a half-open connection does.
type proxy struct {
	ln     net.Listener
	target string

	mu        sync.Mutex
	conns     map[net.Conn]struct{}
	refuse    bool
	blackhole bool
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		p.Drop()
	})
	return p
}

func (p *proxy) Addr() string { return p.ln.Addr().String() }

func (p *proxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		refuse := p.refuse
		p.mu.Unlock()
		if refuse {
			_ = c.Close()
			continue
		}
		u, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns[c], p.conns[u] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(c, u)
		go p.pipe(u, c)
	}
}

func (p *proxy) pipe(dst, src net.Conn) {
	defer func() { _ = dst.Close(); _ = src.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			hole := p.blackhole
			p.mu.Unlock()
			if !hole {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// Drop closes every open connection.
func (p *proxy) Drop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
	clear(p.conns)
}

func (p *proxy) SetRefuse(v bool) {
	p.mu.Lock()
	p.refuse = v
	p.mu.Unlock()
}

func (p *proxy) SetBlackhole(v bool) {
	p.mu.Lock()
	p.blackhole = v
	p.mu.Unlock()
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

// records keeps what a component logs, for tests that check logs.
type records struct {
	mu   sync.Mutex
	msgs []string
}

func (r *records) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(writerFunc(func(b []byte) (int, error) {
		r.mu.Lock()
		r.msgs = append(r.msgs, string(b))
		r.mu.Unlock()
		return len(b), nil
	}), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (r *records) count(substr string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.msgs {
		if strings.Contains(m, substr) {
			n++
		}
	}
	return n
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(b []byte) (int, error) { return f(b) }

var _ io.Writer = writerFunc(nil)

// classOf is a failure's class and tags, as the engine sees them.
func classOf(err error) (plugin.Class, []string) {
	c, _ := plugin.Classify(err)
	return c, plugin.Tags(err)
}

// requirePluginError requires err to be a *plugin.Error.
func requirePluginError(t *testing.T, err error) {
	t.Helper()
	var pe *plugin.Error
	require.True(t, errors.As(err, &pe), "%v (%T) is not a *plugin.Error", err, err)
}
