package grpcplugin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func openMem(t *testing.T, p *Plugin, options map[string]any) plugin.Sink {
	t.Helper()
	s, err := p.sinkFactory("mem")(context.Background(), plugin.Spec{Name: "out", Kind: "mem", Options: options})
	require.NoError(t, err)
	return s
}

// requireConnectError checks a lost-plugin failure and returns a copy of it.
func requireConnectError(t *testing.T, err error) plugin.Error {
	t.Helper()
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Transient, pe.Class)
	assert.Contains(t, pe.Tags, plugin.TagConnect)
	return *pe
}

func TestCrashedPluginIsRelaunchedOnTheNextCall(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	p := newFakePlugin(t, f, nil)
	sink := openMem(t, p, nil)
	ctx := context.Background()

	_, err := sink.Deliver(ctx, msg("a"))
	require.NoError(t, err)
	f.proc(0).crash()
	_, err = sink.Deliver(ctx, msg("lost"))
	requireConnectError(t, err)
	launched, _ := f.launches()
	assert.Equal(t, 1, launched, "the failing call does not relaunch")

	res, err := sink.Deliver(ctx, msg("b"))
	require.NoError(t, err)
	assert.Equal(t, 202, res.Status)
	launched, _ = f.launches()
	assert.Equal(t, 2, launched)
	assert.Equal(t, []string{"a"}, m.sink(0).bodies())
	assert.Equal(t, []string{"b"}, m.sink(1).bodies(), "the handle was re-opened in the new process")
	assert.Equal(t, 2, p.pid())
}

func TestRelaunchBacksOff(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	p := newFakePlugin(t, f, nil)
	clock := time.Unix(1000, 0)
	p.now = func() time.Time { return clock }
	p.rnd = func() float64 { return 0.99 }
	sink := openMem(t, p, nil)
	ctx := context.Background()

	f.proc(0).kill()
	f.fail(errors.New("no binary"))
	_, err := sink.Deliver(ctx, msg("x"))
	pe := requireConnectError(t, err)
	assert.Contains(t, pe.Error(), "plugin fake: relaunch: no binary")
	_, tries := f.launches()
	assert.Equal(t, 2, tries, "the first relaunch is at once")

	_, err = sink.Deliver(ctx, msg("x"))
	pe = requireConnectError(t, err)
	assert.Contains(t, pe.Error(), "plugin fake is down, relaunching in")
	assert.Contains(t, pe.Error(), "no binary")
	assert.InDelta(t, 250*time.Millisecond, pe.RetryAfter, float64(5*time.Millisecond))
	_, tries = f.launches()
	assert.Equal(t, 2, tries, "no relaunch before the backoff passes")

	clock = clock.Add(300 * time.Millisecond)
	_, err = sink.Deliver(ctx, msg("x"))
	requireConnectError(t, err)
	_, err = sink.Deliver(ctx, msg("x"))
	pe = requireConnectError(t, err)
	assert.InDelta(t, 500*time.Millisecond, pe.RetryAfter, float64(10*time.Millisecond), "the backoff doubles")

	clock = clock.Add(time.Second)
	f.fail(nil)
	_, err = sink.Deliver(ctx, msg("back"))
	require.NoError(t, err)
	assert.Equal(t, []string{"back"}, m.sink(1).bodies())

	// A call worked: the next crash is relaunched at once again.
	f.proc(1).kill()
	_, err = sink.Deliver(ctx, msg("again"))
	require.NoError(t, err)
}

func TestCrashLoopBacksOff(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	p := newFakePlugin(t, f, nil)
	frozen := time.Now()
	p.now = func() time.Time { return frozen }
	sink := openMem(t, p, nil)
	f.after = func(fp *fakeProc) { fp.crash() } // every new process dies at once
	f.proc(0).crash()
	_, err := sink.Deliver(context.Background(), msg("x"))
	requireConnectError(t, err)
	_, err = sink.Deliver(context.Background(), msg("x"))
	requireConnectError(t, err) // relaunched, and lost again before anything worked
	_, err = sink.Deliver(context.Background(), msg("x"))
	pe := requireConnectError(t, err)
	assert.Contains(t, pe.Error(), "relaunching in", "a process that keeps dying is relaunched with backoff")
	assert.Positive(t, pe.RetryAfter)
}

func TestLostHandleIsReopened(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	p := newFakePlugin(t, f, nil)
	sink := openMem(t, p, nil)
	r := sink.(*remoteSink).r
	_, err := f.proc(0).rpc.CloseSink(context.Background(), &pluginpb.HandleRequest{Handle: r.handle})
	require.NoError(t, err)

	_, err = sink.Deliver(context.Background(), msg("x"))
	pe := requireConnectError(t, err)
	assert.Contains(t, pe.Error(), "no open sink")
	_, err = sink.Deliver(context.Background(), msg("y"))
	require.NoError(t, err)
	assert.Equal(t, []string{"y"}, m.sink(1).bodies(), "a new sink in the same process")
	launched, _ := f.launches()
	assert.Equal(t, 1, launched)
}

// unbatched says a sink batches, then answers DeliverBatch with
// UNIMPLEMENTED, as a plugin in another language might.
type unbatched struct {
	pluginpb.PluginServer
	calls int
}

func (u *unbatched) DeliverBatch(context.Context, *pluginpb.DeliverBatchRequest) (*pluginpb.DeliverResponse, error) {
	u.calls++
	return nil, status.Error(codes.Unimplemented, "no")
}

func TestBatchFallsBackToSingleDeliveries(t *testing.T) {
	m := &memPlugins{batch: true}
	u := &unbatched{}
	f := &fakeLauncher{defs: m.defs, wrap: func(ps pluginpb.PluginServer) pluginpb.PluginServer { u.PluginServer = ps; return u }}
	p := newFakePlugin(t, f, nil)
	bs, ok := openMem(t, p, nil).(plugin.BatchSink)
	require.True(t, ok)
	for range 2 {
		res, err := bs.DeliverBatch(context.Background(), []*envelope.Envelope{msg("a"), msg("b")})
		require.NoError(t, err)
		assert.Equal(t, 202, res.Status, "the last delivery's result")
	}
	assert.Equal(t, []string{"a", "b", "a", "b"}, m.sink(0).bodies())
	assert.Equal(t, 1, u.calls, "asked once")

	m.sink(0).fail = plugin.Permanentf("full")
	_, err := bs.DeliverBatch(context.Background(), []*envelope.Envelope{msg("c")})
	assert.ErrorContains(t, err, "full", "the batch fails as a whole")
}

func TestNativeBatch(t *testing.T) {
	m := &memPlugins{batch: true}
	p := newFakePlugin(t, &fakeLauncher{defs: m.defs}, nil)
	bs := openMem(t, p, nil).(plugin.BatchSink)
	res, err := bs.DeliverBatch(context.Background(), []*envelope.Envelope{msg("a"), msg("b")})
	require.NoError(t, err)
	assert.Equal(t, 207, res.Status)

	plain := &memPlugins{}
	p2 := newFakePlugin(t, &fakeLauncher{defs: plain.defs}, nil)
	_, ok := openMem(t, p2, nil).(plugin.BatchSink)
	assert.False(t, ok, "a sink that does not batch gets the engine's combining")
}

func TestSinkHealthCloseAndOpenErrors(t *testing.T) {
	m := &memPlugins{}
	p := newFakePlugin(t, &fakeLauncher{defs: m.defs}, nil)
	sink := openMem(t, p, nil)
	require.NoError(t, sink.Health(context.Background()))
	m.sink(0).health = errors.New("disk gone")
	assert.EqualError(t, sink.Health(context.Background()), "disk gone")

	_, err := sink.Deliver(context.Background(), msg("panic"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panicked: boom", "a panic in plugin code fails the call, not the plugin")
	assert.NotContains(t, err.Error(), "goroutine ", "the stack goes to the plugin's log, not into the error")
	assert.Equal(t, plugin.Transient, func() plugin.Class { c, _ := plugin.Classify(err); return c }())
	_, err = sink.Deliver(context.Background(), msg("fine"))
	require.NoError(t, err)

	require.NoError(t, sink.Close(context.Background()))
	assert.True(t, m.sink(0).closed)
	_, err = sink.Deliver(context.Background(), msg("x"))
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Contains(t, err.Error(), "destination out is closed")

	_, err = p.sinkFactory("mem")(context.Background(), plugin.Spec{Name: "bad", Kind: "mem", Options: map[string]any{"fail": true}})
	assert.EqualError(t, err, "plugin fake: mem: asked to fail")
	_, err = p.sinkFactory("mem")(context.Background(), plugin.Spec{Name: "bad", Kind: "mem", Options: map[string]any{"f": func() {}}})
	assert.ErrorContains(t, err, "mem bad: options:")
	_, err = p.sinkFactory("nope")(context.Background(), plugin.Spec{Name: "bad", Kind: "nope"})
	assert.ErrorContains(t, err, `this plugin serves no destination type "nope"`)
	_, err = p.sourceFactory("nope")(context.Background(), plugin.Spec{Name: "bad", Kind: "nope"})
	assert.ErrorContains(t, err, `this plugin serves no source type "nope"`)
}

func TestSourceProxy(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	logs, log := newLogs(t)
	p := newFakePlugin(t, f, log)
	src, err := p.sourceFactory("chan")(context.Background(), plugin.Spec{Name: "in", Kind: "chan",
		Config: &config.Source{MaxBodyBytes: 8, Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxBackoff: config.Duration(5 * time.Millisecond)}}})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() { done <- src.Start(context.Background(), out) }()
	cs := m.source(0)

	// Too large: dropped, and settled so the plugin does not send it again.
	cs.in <- &envelope.Envelope{ID: "big", Body: []byte("much too large")}
	assert.Equal(t, "much too large final", cs.nextSettle(t))

	// The host fills in what the plugin left out, and owns the source name.
	cs.in <- &envelope.Envelope{Source: "other", Body: []byte("one")}
	env := recv(t, out)
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, "chan", env.Protocol)
	assert.NotEmpty(t, env.ID)
	assert.False(t, env.ReceivedAt.IsZero())
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Unrouted})
	assert.Equal(t, "one final", cs.nextSettle(t))

	// A message from a process that has died since is not settled there.
	cs.in <- &envelope.Envelope{ID: "two", Body: []byte("two")}
	old := recv(t, out)
	f.proc(0).crash()
	cs2 := func() *chanSource {
		require.Eventually(t, func() bool { _, n := m.count(); return n == 2 }, 10*time.Second, time.Millisecond, "the stream restarts in a new process")
		return m.source(1)
	}()
	old.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	assert.Eventually(t, func() bool {
		return len(logs.records("settle skipped: the plugin process that sent the message is gone")) == 1
	}, 10*time.Second, time.Millisecond)
	cs2.in <- &envelope.Envelope{ID: "three", Body: []byte("three")}
	env = recv(t, out)
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	assert.Equal(t, "three retry", cs2.nextSettle(t))

	// The plugin's source gives up: so does Start.
	cs2.end <- errors.New("credentials revoked")
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}
	require.Error(t, err)
	assert.Equal(t, "source in: credentials revoked", err.Error())

	require.NoError(t, src.(plugin.Closer).Close(context.Background()))
	assert.True(t, cs2.closed.Load())
}

func TestSourceThatRunsOutOrPanics(t *testing.T) {
	m := &memPlugins{}
	p := newFakePlugin(t, &fakeLauncher{defs: m.defs}, nil)
	open := func() (plugin.Source, *chanSource) {
		_, n := m.count()
		src, err := p.sourceFactory("chan")(context.Background(), plugin.Spec{Name: "in", Kind: "chan"})
		require.NoError(t, err)
		return src, m.source(n)
	}

	src, cs := open()
	done := make(chan error, 1)
	go func() { done <- src.Start(context.Background(), make(chan *envelope.Envelope)) }()
	cs.end <- nil
	assert.NoError(t, <-done, "a plugin source that runs out ends Start with nil")

	src, cs = open()
	go func() { done <- src.Start(context.Background(), make(chan *envelope.Envelope)) }()
	cs.in <- &envelope.Envelope{Body: []byte("panic")}
	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panicked: source boom")
}

func TestSourceGivesUpAfterMaxAttempts(t *testing.T) {
	m := &memPlugins{}
	f := &fakeLauncher{defs: m.defs}
	p := newFakePlugin(t, f, nil)
	src, err := p.sourceFactory("chan")(context.Background(), plugin.Spec{Name: "in", Kind: "chan",
		Config: &config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}}})
	require.NoError(t, err)
	p.base = time.Millisecond
	f.fail(errors.New("gone for good"))
	f.proc(0).kill()
	err = src.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source in: giving up after 2 failed restarts")
}

func TestAckedSourceSettlesWhenQueued(t *testing.T) {
	m := &memPlugins{}
	p := newFakePlugin(t, &fakeLauncher{defs: m.defs}, nil)
	src, err := p.sourceFactory("chan")(context.Background(), plugin.Spec{Name: "in", Kind: "chan", Config: &config.Source{Ack: "accepted"}})
	require.NoError(t, err)
	out, _ := runSource(t, src)
	cs := m.source(0)
	assert.Equal(t, "accepted", cs.ack, "the ack mode reaches the plugin")
	cs.in <- &envelope.Envelope{ID: "a", Body: []byte("a")}
	env := recv(t, out)
	env.Queued()
	assert.Equal(t, "a final", cs.nextSettle(t))
}

// describing serves a fixed Describe answer.
type describing struct {
	pluginpb.PluginServer
	kinds []*pluginpb.Kind
}

func (d *describing) Describe(context.Context, *pluginpb.DescribeRequest) (*pluginpb.DescribeResponse, error) {
	return &pluginpb.DescribeResponse{Kinds: d.kinds}, nil
}

func TestDescribeIsChecked(t *testing.T) {
	m := &memPlugins{}
	sink := func(name string) *pluginpb.Kind {
		return &pluginpb.Kind{Name: name, Role: pluginpb.Role_ROLE_SINK}
	}
	cases := map[string][]*pluginpb.Kind{
		"the plugin serves no kinds":       nil,
		`kind "x" has no role`:             {{Name: "x"}},
		`destination type "x" is declared`: {sink("x"), sink("x")},
		"a kind has no name":               {sink("")},
		`kind "a b": names hold`:           {sink("a b")},
		`destination type "x": options schema`: {
			{Name: "x", Role: pluginpb.Role_ROLE_SINK, OptionsSchemaJson: `{"type": 3}`},
		},
	}
	for want, kinds := range cases {
		f := &fakeLauncher{defs: m.defs, wrap: func(ps pluginpb.PluginServer) pluginpb.PluginServer { return &describing{ps, kinds} }}
		_, err := newPlugin(context.Background(), "fake", slog.New(slog.DiscardHandler), f.launch)
		require.Error(t, err, want)
		assert.Contains(t, err.Error(), want)
		assert.True(t, strings.HasPrefix(err.Error(), "plugin fake: "))
		assert.True(t, f.proc(0).exited(), "a plugin that fails to describe itself is stopped")
	}

	f := &fakeLauncher{defs: m.defs, wrap: func(ps pluginpb.PluginServer) pluginpb.PluginServer {
		return &describing{ps, []*pluginpb.Kind{
			{Name: "src", Role: pluginpb.Role_ROLE_SOURCE, Description: "A source", OptionsSchemaJson: ` {"type":"object"} `},
			sink("any"),
		}}
	}}
	p := newFakePlugin(t, f, nil)
	kinds := p.Kinds()
	require.Len(t, kinds, 2)
	assert.Equal(t, Kind{Name: "src", Role: RoleSource, Description: "A source", Schema: json.RawMessage(`{"type":"object"}`)}, kinds[0])
	assert.Nil(t, kinds[1].Schema)
	x := (&Set{plugins: []*Plugin{p}}).Extensions()
	assert.NotNil(t, x.Sources["src"].Check)
	assert.Nil(t, x.Sinks["any"].Check, "no schema: any options")
}

func TestLaunchFailureAtStart(t *testing.T) {
	f := &fakeLauncher{defs: (&memPlugins{}).defs}
	f.fail(errors.New("exec format error"))
	_, err := newPlugin(context.Background(), "fake", slog.New(slog.DiscardHandler), f.launch)
	assert.EqualError(t, err, "plugin fake: exec format error")
}

func TestPluginEnv(t *testing.T) {
	t.Setenv("HOME", "/home/test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "nope")
	env, err := pluginEnv(map[string]string{"B": "2", "A": "1"}, slog.LevelInfo)
	require.NoError(t, err)
	assert.Contains(t, env, "HOME=/home/test")
	assert.Contains(t, env, logLevelEnv+"=info")
	assert.Equal(t, []string{"A=1", "B=2"}, env[len(env)-2:], "the spec's env comes last, sorted")
	for _, kv := range env {
		assert.False(t, strings.HasPrefix(kv, "AWS_"), kv)
	}
	_, err = pluginEnv(map[string]string{"A=B": "x"}, slog.LevelInfo)
	assert.ErrorContains(t, err, `invalid variable name "A=B"`)
	_, err = pluginEnv(map[string]string{"": "x"}, slog.LevelInfo)
	assert.Error(t, err)
	_, err = pluginEnv(map[string]string{"A": "x\x00"}, slog.LevelInfo)
	assert.ErrorContains(t, err, "NUL")
}

func TestRestartPolicy(t *testing.T) {
	assert.Equal(t, restart{base: defaultBackoff, max: defaultMaxBackoff, jitter: defaultJitter}, restartPolicy(nil))
	assert.Equal(t, restart{base: time.Second, max: time.Minute, jitter: "none", maxAttempts: 3}, restartPolicy(&config.Reconnect{
		Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Minute), Jitter: "none", MaxAttempts: 3,
	}))
}

func TestAckMode(t *testing.T) {
	assert.Equal(t, "delivered", AckMode(plugin.Spec{}))
	assert.Equal(t, "accepted", AckMode(plugin.Spec{Config: &Remote{Ack: "accepted"}}))
	assert.Equal(t, "delivered", AckMode(plugin.Spec{Config: &Remote{Ack: "bogus"}}))
	assert.Equal(t, "accepted", AckMode(plugin.Spec{Config: &config.Source{Ack: "accepted"}}), "in process too")
	assert.Equal(t, "delivered", AckMode(plugin.Spec{Config: &config.Endpoint{}}))
}

func TestRoleString(t *testing.T) {
	assert.Equal(t, "source", RoleSource.String())
	assert.Equal(t, "destination", RoleSink.String())
	assert.Equal(t, "unknown", Role(0).String())
}
