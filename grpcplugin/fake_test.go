package grpcplugin

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/plugin"
)

// memSink is an in-process sink: it keeps what it gets.
type memSink struct {
	name   string
	mu     sync.Mutex
	got    []string
	fail   error
	health error
	closed bool
}

func (s *memSink) Name() string { return s.name }

func (s *memSink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if string(env.Body) == "panic" {
		panic("boom")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	s.got = append(s.got, string(env.Body))
	return &plugin.Result{Status: 202, Body: env.Body}, nil
}

func (s *memSink) Health(context.Context) error { return s.health }

func (s *memSink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *memSink) bodies() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.got...)
}

// batchSink is a memSink with a native batch call.
type batchSink struct{ *memSink }

func (s batchSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	for _, e := range envs {
		if _, err := s.Deliver(ctx, e); err != nil {
			return nil, err
		}
	}
	return &plugin.Result{Status: 207}, nil
}

// chanSource emits what the test sends on in, and records how each message
// was settled.
type chanSource struct {
	name    string
	ack     string
	in      chan *envelope.Envelope
	end     chan error // Start returns what is sent here
	settled chan string
	closed  atomic.Bool
}

func newChanSource(spec plugin.Spec) *chanSource {
	return &chanSource{name: spec.Name, ack: AckMode(spec), in: make(chan *envelope.Envelope), end: make(chan error, 1), settled: make(chan string, 100)}
}

func (s *chanSource) Name() string { return s.name }

func (s *chanSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.end:
			return err
		case env := <-s.in:
			if string(env.Body) == "panic" {
				panic("source boom")
			}
			id := string(env.Body)
			plugin.OnSettle(env, s.ack, func(_ context.Context, final bool) {
				if final {
					s.settled <- id + " final"
				} else {
					s.settled <- id + " retry"
				}
			})
			select {
			case out <- env:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (s *chanSource) Health(context.Context) error { return nil }

// nextSettle waits for the source's next settle.
func (s *chanSource) nextSettle(t *testing.T) string {
	t.Helper()
	select {
	case v := <-s.settled:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("no settle")
		return ""
	}
}

func (s *chanSource) Close(context.Context) error {
	s.closed.Store(true)
	return nil
}

// fakeLauncher runs a plugin server in this process, over an in-memory
// connection: a "process" that tests crash and kill at will.
type fakeLauncher struct {
	defs   func() Plugins
	wrap   func(pluginpb.PluginServer) pluginpb.PluginServer
	after  func(*fakeProc) // runs on each launched process
	mu     sync.Mutex
	procs  []*fakeProc
	tries  int
	failed error // launches fail with it while set
}

func (f *fakeLauncher) launch(context.Context) (process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tries++
	if f.failed != nil {
		return nil, f.failed
	}
	srv, err := newServer(f.defs(), slog.New(slog.DiscardHandler))
	if err != nil {
		return nil, err
	}
	var ps pluginpb.PluginServer = srv
	if f.wrap != nil {
		ps = f.wrap(ps)
	}
	lis := bufconn.Listen(1 << 20)
	gs := grpcServer(slog.New(slog.DiscardHandler))(nil)
	pluginpb.RegisterPluginServer(gs, ps)
	go func() { _ = gs.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///plugin",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gs.Stop()
		return nil, err
	}
	p := &fakeProc{gs: gs, conn: conn, srv: srv, rpc: pluginpb.NewPluginClient(conn), id: len(f.procs) + 1}
	f.procs = append(f.procs, p)
	if f.after != nil {
		f.after(p)
	}
	return p, nil
}

func (f *fakeLauncher) proc(i int) *fakeProc {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.procs[i]
}

func (f *fakeLauncher) launches() (ok, tries int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.procs), f.tries
}

func (f *fakeLauncher) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = err
}

type fakeProc struct {
	gs   *grpc.Server
	conn *grpc.ClientConn
	srv  *server
	rpc  pluginpb.PluginClient
	id   int
	dead atomic.Bool
}

func (p *fakeProc) client() pluginpb.PluginClient { return p.rpc }
func (p *fakeProc) exited() bool                  { return p.dead.Load() }
func (p *fakeProc) pid() int                      { return p.id }

// crash stops serving without the host knowing: its next call finds out.
func (p *fakeProc) crash() {
	p.gs.Stop()
	p.srv.shutdown()
}

func (p *fakeProc) kill() {
	p.crash()
	p.dead.Store(true)
	_ = p.conn.Close()
}

func newFakePlugin(t *testing.T, f *fakeLauncher, log *slog.Logger) *Plugin {
	t.Helper()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p, err := newPlugin(context.Background(), "fake", log, f.launch)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// memPlugins serves a "mem" sink (batching with batch) and a "chan" source;
// the instances each launch builds land in sinks and sources.
type memPlugins struct {
	mu      sync.Mutex
	batch   bool
	sinks   []*memSink
	sources []*chanSource
}

func (m *memPlugins) defs() Plugins {
	return Plugins{
		Sinks: map[string]SinkDef{"mem": {Factory: func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
			var o struct {
				Fail bool `yaml:"fail,omitempty"`
			}
			if err := spec.Decode(&o); err != nil {
				return nil, err
			}
			if o.Fail {
				return nil, errors.New("mem: asked to fail")
			}
			s := &memSink{name: spec.Name}
			m.mu.Lock()
			m.sinks = append(m.sinks, s)
			m.mu.Unlock()
			if m.batch {
				return batchSink{s}, nil
			}
			return s, nil
		}}},
		Sources: map[string]SourceDef{"chan": {Factory: func(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
			s := newChanSource(spec)
			m.mu.Lock()
			m.sources = append(m.sources, s)
			m.mu.Unlock()
			return s, nil
		}}},
	}
}

func (m *memPlugins) sink(i int) *memSink {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sinks[i]
}

func (m *memPlugins) source(i int) *chanSource {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sources[i]
}

func (m *memPlugins) count() (sinks, sources int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sinks), len(m.sources)
}
