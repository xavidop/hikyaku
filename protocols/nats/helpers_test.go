package nats

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// testServer is an embedded NATS server with JetStream on a temporary
// store. It can be stopped and started again on the same port and store,
// as a server restart.
type testServer struct {
	t    *testing.T
	opts *server.Options
	srv  *server.Server
}

func startServer(t *testing.T, mutate ...func(*server.Options)) *testServer {
	t.Helper()
	opts := &server.Options{
		Host: "127.0.0.1", Port: server.RANDOM_PORT, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir(),
	}
	for _, m := range mutate {
		m(opts)
	}
	ts := &testServer{t: t, opts: opts}
	ts.start()
	t.Cleanup(ts.stop)
	return ts
}

func (ts *testServer) start() {
	ts.t.Helper()
	srv, err := server.NewServer(ts.opts.Clone())
	require.NoError(ts.t, err)
	go srv.Start()
	require.True(ts.t, srv.ReadyForConnections(10*time.Second), "nats server not ready")
	ts.srv = srv
	if ts.opts.Port <= 0 {
		ts.opts.Port = srv.Addr().(*net.TCPAddr).Port
	}
}

func (ts *testServer) stop() {
	if ts.srv == nil {
		return
	}
	ts.srv.Shutdown()
	ts.srv.WaitForShutdown()
	ts.srv = nil
}

func (ts *testServer) restart() {
	ts.t.Helper()
	ts.stop()
	ts.start()
}

func (ts *testServer) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", ts.opts.Port) }

// client is a plain connection for the test to publish and inspect with.
func (ts *testServer) client(opts ...nats.Option) *nats.Conn {
	ts.t.Helper()
	nc, err := nats.Connect(ts.url(), opts...)
	require.NoError(ts.t, err)
	ts.t.Cleanup(nc.Close)
	return nc
}

// subscribe subscribes a new client connection to subject and waits until
// the server has the subscription.
func (ts *testServer) subscribe(subject string) *nats.Subscription {
	ts.t.Helper()
	nc := ts.client()
	sub, err := nc.SubscribeSync(subject)
	require.NoError(ts.t, err)
	require.NoError(ts.t, nc.Flush())
	return sub
}

// js is a JetStream context on a new client connection.
func (ts *testServer) js() jetstream.JetStream {
	ts.t.Helper()
	js, err := jetstream.New(ts.client())
	require.NoError(ts.t, err)
	return js
}

// stream creates a file-backed stream on subjects.
func (ts *testServer) stream(name string, subjects ...string) jetstream.JetStream {
	ts.t.Helper()
	js := ts.js()
	_, err := js.CreateStream(ctxT(ts.t), jetstream.StreamConfig{Name: name, Subjects: subjects, Storage: jetstream.FileStorage})
	require.NoError(ts.t, err)
	return js
}

// subscribed waits until the connection named name has a subscription to
// subject on the server.
func (ts *testServer) subscribed(name, subject string) {
	ts.t.Helper()
	require.Eventually(ts.t, func() bool {
		if ts.srv == nil {
			return false
		}
		cz, err := ts.srv.Connz(&server.ConnzOptions{Subscriptions: true})
		if err != nil {
			return false
		}
		for _, ci := range cz.Conns {
			if ci.Name == name && slices.Contains(ci.Subs, subject) {
				return true
			}
		}
		return false
	}, 10*time.Second, 5*time.Millisecond, "%s never subscribed to %s", name, subject)
}

// pulling waits until the durable consumer exists and has a pull request
// waiting: the source is ready for messages.
func pulling(t *testing.T, js jetstream.JetStream, stream, durable string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, err := js.Consumer(ctxT(t), stream, durable)
		if err != nil {
			return false
		}
		info, err := c.Info(ctxT(t))
		return err == nil && info.NumWaiting > 0
	}, 10*time.Second, 10*time.Millisecond, "consumer %s never pulled", durable)
}

func consumerInfo(t *testing.T, js jetstream.JetStream, stream, durable string) *jetstream.ConsumerInfo {
	t.Helper()
	c, err := js.Consumer(ctxT(t), stream, durable)
	require.NoError(t, err)
	info, err := c.Info(ctxT(t))
	require.NoError(t, err)
	return info
}

// ctxT is a context for one test call.
func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// logBuffer collects log lines (to check what is logged, and that secrets
// are not).
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testLogger(t *testing.T, also ...io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.MultiWriter(append(also, t.Output())...), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// fast is a reconnect policy for tests.
func fast() *config.Reconnect {
	return &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxBackoff: config.Duration(50 * time.Millisecond)}
}

func spec(t *testing.T, kind string, cfg any) plugin.Spec {
	return plugin.Spec{Name: "in", Kind: kind, Config: cfg, Env: plugin.Env{Relay: "test", Logger: testLogger(t)}}
}

// closeAtCleanup closes c when the test ends. Close waits for the messages
// the relay holds, so a test that leaves one unsettled would wait forever
// on an unbounded context.
func closeAtCleanup(t *testing.T, c interface{ Close(context.Context) error }) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
}

// newTestSource builds a nats source named "in" (connection hikyaku-test-in).
func newTestSource(t *testing.T, cfg config.Source) *Source {
	t.Helper()
	cfg.Name, cfg.Type = "in", KindNATS
	s, err := newSource(spec(t, KindNATS, &cfg))
	require.NoError(t, err)
	closeAtCleanup(t, s)
	return s
}

// newTestStreamSource builds a jetstream source named "in".
func newTestStreamSource(t *testing.T, cfg config.Source) *StreamSource {
	t.Helper()
	cfg.Name, cfg.Type = "in", KindJetStream
	s, err := newStreamSource(spec(t, KindJetStream, &cfg))
	require.NoError(t, err)
	s.drainWait = 2 * time.Second
	closeAtCleanup(t, s)
	return s
}

func newTestSink(t *testing.T, cfg config.Endpoint) *Sink {
	t.Helper()
	cfg.Type = KindNATS
	s, err := newSink(plugin.Spec{Name: "out", Kind: KindNATS, Config: &cfg, Env: plugin.Env{Relay: "test", Logger: testLogger(t)}})
	require.NoError(t, err)
	closeAtCleanup(t, s)
	return s
}

func newTestStreamSink(t *testing.T, cfg config.Endpoint) *StreamSink {
	t.Helper()
	cfg.Type = KindJetStream
	s, err := newStreamSink(plugin.Spec{Name: "out", Kind: KindJetStream, Config: &cfg, Env: plugin.Env{Relay: "test", Logger: testLogger(t)}})
	require.NoError(t, err)
	closeAtCleanup(t, s)
	return s
}

// run starts src with an output buffer of 64. The returned stop (also run
// at cleanup) cancels it and checks that Start returned nil.
func run(t *testing.T, src plugin.Source) (chan *envelope.Envelope, func()) {
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
			case <-time.After(10 * time.Second):
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
	case <-time.After(10 * time.Second):
		t.Fatal("no envelope arrived")
		return nil
	}
}

// none checks that nothing arrives on out within d.
func none(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected envelope %s (%s): %q", env.ID, env.Subject, env.Body)
	case <-time.After(d):
	}
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

func ack(env *envelope.Envelope, status envelope.AckStatus) {
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}
