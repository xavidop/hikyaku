package grpcplugin

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/plugin"
)

// serveMem serves memPlugins in this process and returns a client.
func serveMem(t *testing.T, m *memPlugins) pluginpb.PluginClient {
	t.Helper()
	f := &fakeLauncher{defs: m.defs}
	proc, err := f.launch(context.Background())
	require.NoError(t, err)
	t.Cleanup(proc.kill)
	return proc.client()
}

func code(err error) codes.Code { return status.Code(err) }

func TestNewServerChecksDefinitions(t *testing.T) {
	noop := func(context.Context, plugin.Spec) (plugin.Sink, error) { return nil, nil }
	_, err := newServer(Plugins{}, nil)
	assert.EqualError(t, err, "no sinks or sources to serve")
	_, err = newServer(Plugins{Sinks: map[string]SinkDef{"x": {}}}, nil)
	assert.EqualError(t, err, `kind "x" has no Factory`)
	_, err = newServer(Plugins{Sources: map[string]SourceDef{"x": {}}}, nil)
	assert.EqualError(t, err, `kind "x" has no Factory`)
	_, err = newServer(Plugins{Sinks: map[string]SinkDef{"a/b": {Factory: noop}}}, nil)
	assert.ErrorContains(t, err, `kind "a/b": names hold`)
	_, err = newServer(Plugins{Sinks: map[string]SinkDef{"x": {Factory: noop, Options: make(chan int)}}}, nil)
	assert.Error(t, err, "an options value that has no schema")
}

func TestDescribeListsKinds(t *testing.T) {
	c := serveMem(t, &memPlugins{})
	resp, err := c.Describe(context.Background(), &pluginpb.DescribeRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetKinds(), 2)
	assert.Equal(t, "mem", resp.GetKinds()[0].GetName())
	assert.Equal(t, pluginpb.Role_ROLE_SINK, resp.GetKinds()[0].GetRole())
	assert.Equal(t, "chan", resp.GetKinds()[1].GetName())
	assert.Equal(t, pluginpb.Role_ROLE_SOURCE, resp.GetKinds()[1].GetRole())
	assert.Empty(t, resp.GetKinds()[0].GetOptionsSchemaJson(), "no Options, no schema")
}

func TestSpecAsAFactorySeesIt(t *testing.T) {
	var got plugin.Spec
	srv, err := newServer(Plugins{Sinks: map[string]SinkDef{"x": {Factory: func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		got = spec
		return &memSink{name: spec.Name}, nil
	}}}}, nil)
	require.NoError(t, err)
	resp, err := srv.OpenSink(context.Background(), &pluginpb.OpenSinkRequest{Kind: "x", Name: "out", Relay: "r", OptionsJson: `{"n": 3, "f": 1.5, "s": "a: b", "nested": {"list": [1, "two"]}, "none": null}`})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	assert.Equal(t, "sink-1", resp.GetHandle())
	assert.Equal(t, "out", got.Name)
	assert.Equal(t, "x", got.Kind)
	assert.Equal(t, "r", got.Env.Relay)
	assert.NotNil(t, got.Env.Logger)
	assert.Equal(t, &Remote{}, got.Config)
	assert.Equal(t, map[string]any{"n": 3, "f": 1.5, "s": "a: b", "nested": map[string]any{"list": []any{1, "two"}}, "none": nil}, got.Options,
		"options read as YAML reads them: whole numbers are ints, as in process")

	_, err = srv.OpenSink(context.Background(), &pluginpb.OpenSinkRequest{Kind: "x", Name: "out", OptionsJson: "null"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{}, got.Options)

	resp, err = srv.OpenSink(context.Background(), &pluginpb.OpenSinkRequest{Kind: "x", Name: "out", OptionsJson: `{"a": [}`})
	require.NoError(t, err)
	assert.Contains(t, resp.GetError().GetMessage(), "x out: options_json")
	resp, err = srv.OpenSink(context.Background(), &pluginpb.OpenSinkRequest{Kind: "missing"})
	assert.Equal(t, codes.NotFound, code(err))
	assert.Nil(t, resp)
}

func TestSinkCallsOnUnknownHandles(t *testing.T) {
	c := serveMem(t, &memPlugins{})
	ctx := context.Background()
	env := envelopeToPB(msg("x"))
	_, err := c.Deliver(ctx, &pluginpb.DeliverRequest{Handle: "nope", Envelope: env})
	assert.Equal(t, codes.NotFound, code(err))
	_, err = c.DeliverBatch(ctx, &pluginpb.DeliverBatchRequest{Handle: "nope"})
	assert.Equal(t, codes.NotFound, code(err))
	_, err = c.SinkHealth(ctx, &pluginpb.HandleRequest{Handle: "nope"})
	assert.Equal(t, codes.NotFound, code(err))
	_, err = c.CloseSink(ctx, &pluginpb.HandleRequest{Handle: "nope"})
	assert.Equal(t, codes.NotFound, code(err))

	open, err := c.OpenSink(ctx, &pluginpb.OpenSinkRequest{Kind: "mem", Name: "out"})
	require.NoError(t, err)
	assert.False(t, open.GetBatch())
	_, err = c.Deliver(ctx, &pluginpb.DeliverRequest{Handle: open.GetHandle()})
	assert.Equal(t, codes.InvalidArgument, code(err), "no envelope")
	_, err = c.DeliverBatch(ctx, &pluginpb.DeliverBatchRequest{Handle: open.GetHandle(), Envelopes: []*pluginpb.Envelope{env}})
	assert.Equal(t, codes.Unimplemented, code(err), "the sink does not batch")
	resp, err := c.Deliver(ctx, &pluginpb.DeliverRequest{Handle: open.GetHandle(), Envelope: env})
	require.NoError(t, err)
	assert.Equal(t, int32(202), resp.GetResult().GetStatus())
	_, err = c.CloseSink(ctx, &pluginpb.HandleRequest{Handle: open.GetHandle()})
	require.NoError(t, err)
	_, err = c.Deliver(ctx, &pluginpb.DeliverRequest{Handle: open.GetHandle(), Envelope: env})
	assert.Equal(t, codes.NotFound, code(err), "closed")
}

func TestDeliverResponses(t *testing.T) {
	r, err := deliverResponse(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, r.GetOutcome(), "delivered, nothing to report")
	r, err = deliverResponse(nil, plugin.Permanentf("no"))
	require.NoError(t, err, "a failed delivery is a successful call")
	assert.Equal(t, pluginpb.ErrorClass_ERROR_CLASS_PERMANENT, r.GetError().GetClass())
	res, err := outcome(&pluginpb.DeliverResponse{})
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestSourceProtocol(t *testing.T) {
	m := &memPlugins{}
	c := serveMem(t, m)
	ctx := context.Background()

	_, err := c.OpenSource(ctx, &pluginpb.OpenSourceRequest{Kind: "missing"})
	assert.Equal(t, codes.NotFound, code(err))
	for _, call := range []func() error{
		func() error { _, err := c.Settle(ctx, &pluginpb.SettleRequest{Handle: "nope"}); return err },
		func() error { _, err := c.SourceHealth(ctx, &pluginpb.HandleRequest{Handle: "nope"}); return err },
		func() error { _, err := c.CloseSource(ctx, &pluginpb.HandleRequest{Handle: "nope"}); return err },
		func() error {
			st, err := c.StartSource(ctx, &pluginpb.HandleRequest{Handle: "nope"})
			if err != nil {
				return err
			}
			_, err = st.Recv()
			return err
		},
	} {
		assert.Equal(t, codes.NotFound, code(call()))
	}

	open, err := c.OpenSource(ctx, &pluginpb.OpenSourceRequest{Kind: "chan", Name: "in"})
	require.NoError(t, err)
	h := open.GetHandle()
	cs := m.source(0)
	assert.Equal(t, "delivered", cs.ack)
	hr, err := c.SourceHealth(ctx, &pluginpb.HandleRequest{Handle: h})
	require.NoError(t, err)
	assert.Nil(t, hr.GetError())

	streamCtx, cancel := context.WithCancel(ctx)
	st, err := c.StartSource(streamCtx, &pluginpb.HandleRequest{Handle: h})
	require.NoError(t, err)
	cs.in <- &envelope.Envelope{Body: []byte("a"), Source: "spoofed"}
	pe, err := st.Recv()
	require.NoError(t, err)
	assert.NotEmpty(t, pe.GetId(), "an ID is assigned")
	assert.Equal(t, "in", pe.GetSource(), "the source name is the spec's")
	assert.Equal(t, "chan", pe.GetProtocol())
	cs.in <- &envelope.Envelope{ID: "dup", Body: []byte("b")}
	_, err = st.Recv()
	require.NoError(t, err)

	_, err = c.Settle(ctx, &pluginpb.SettleRequest{Handle: h, EnvelopeId: "unknown", Final: true})
	assert.Equal(t, codes.NotFound, code(err))
	_, err = c.Settle(ctx, &pluginpb.SettleRequest{Handle: h, EnvelopeId: pe.GetId(), Final: true})
	require.NoError(t, err)
	assert.Equal(t, "a final", cs.nextSettle(t))
	_, err = c.Settle(ctx, &pluginpb.SettleRequest{Handle: h, EnvelopeId: pe.GetId(), Final: true})
	assert.Equal(t, codes.NotFound, code(err), "settled once")

	// A second stream on the handle replaces the first.
	st2, err := c.StartSource(ctx, &pluginpb.HandleRequest{Handle: h})
	require.NoError(t, err)
	_, err = st.Recv()
	assert.ErrorIs(t, err, io.EOF, "the first stream ends")
	cancel()
	cs.in <- &envelope.Envelope{ID: "c", Body: []byte("c")}
	got, err := st2.Recv()
	require.NoError(t, err)
	assert.Equal(t, "c", got.GetId())

	// Closing fails what is still unsettled, so the source redelivers it.
	_, err = c.CloseSource(ctx, &pluginpb.HandleRequest{Handle: h})
	require.NoError(t, err)
	settled := []string{cs.nextSettle(t), cs.nextSettle(t)}
	assert.ElementsMatch(t, []string{"b retry", "c retry"}, settled)
	assert.True(t, cs.closed.Load(), "a Closer source is closed")
	_, err = st2.Recv()
	assert.ErrorIs(t, err, io.EOF)
}

func TestSourceStreamEnds(t *testing.T) {
	m := &memPlugins{}
	c := serveMem(t, m)
	ctx := context.Background()
	start := func() (pluginpb.Plugin_StartSourceClient, *chanSource) {
		_, n := m.count()
		open, err := c.OpenSource(ctx, &pluginpb.OpenSourceRequest{Kind: "chan", Name: "in"})
		require.NoError(t, err)
		st, err := c.StartSource(ctx, &pluginpb.HandleRequest{Handle: open.GetHandle()})
		require.NoError(t, err)
		return st, m.source(n)
	}

	st, cs := start()
	cs.end <- nil
	_, err := st.Recv()
	assert.ErrorIs(t, err, io.EOF, "a source that runs out ends the stream with OK")

	st, cs = start()
	cs.end <- errors.New("bad credentials")
	_, err = st.Recv()
	assert.Equal(t, codes.Aborted, code(err), "a source that cannot go on aborts it")
	assert.Equal(t, "bad credentials", status.Convert(err).Message())
}

func TestShutdownClosesWhatIsOpen(t *testing.T) {
	m := &memPlugins{}
	srv, err := newServer(m.defs(), nil)
	require.NoError(t, err)
	_, err = srv.OpenSink(context.Background(), &pluginpb.OpenSinkRequest{Kind: "mem", Name: "out"})
	require.NoError(t, err)
	_, err = srv.OpenSource(context.Background(), &pluginpb.OpenSourceRequest{Kind: "chan", Name: "in"})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		srv.shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung")
	}
	assert.True(t, m.sink(0).closed)
	assert.True(t, m.source(0).closed.Load())
}

func TestSettleRunsWhicheverHookTheSourceSet(t *testing.T) {
	var queued, acked []string
	mk := func() *envelope.Envelope {
		env := envelope.New("s", "x", nil)
		env.OnQueued(func() { queued = append(queued, "q") })
		env.OnAck(func(_ context.Context, o envelope.Outcome) { acked = append(acked, o.Status.String()) })
		return env
	}
	settle(context.Background(), mk(), true)
	settle(context.Background(), mk(), false)
	assert.Equal(t, []string{"q"}, queued, "a failed settle does not run OnQueued")
	assert.Equal(t, []string{"delivered", "failed"}, acked)
}

// stubborn is a source whose Start ignores its ctx until released.
type stubborn struct {
	release chan struct{}
	closed  chan struct{}
}

func (s *stubborn) Name() string { return "stubborn" }

func (s *stubborn) Start(context.Context, chan<- *envelope.Envelope) error {
	<-s.release
	return nil
}

func (s *stubborn) Health(context.Context) error { return nil }

func (s *stubborn) Close(context.Context) error {
	close(s.closed)
	return nil
}

func TestCloseDoesNotWaitForeverForAStubbornSource(t *testing.T) {
	src := &stubborn{release: make(chan struct{}), closed: make(chan struct{})}
	t.Cleanup(func() { close(src.release) })
	f := &fakeLauncher{defs: func() Plugins {
		return Plugins{Sources: map[string]SourceDef{"stubborn": {Factory: func(context.Context, plugin.Spec) (plugin.Source, error) { return src, nil }}}}
	}}
	proc, err := f.launch(context.Background())
	require.NoError(t, err)
	t.Cleanup(proc.kill)
	c := proc.client()
	open, err := c.OpenSource(context.Background(), &pluginpb.OpenSourceRequest{Kind: "stubborn", Name: "s"})
	require.NoError(t, err)
	_, err = c.StartSource(context.Background(), &pluginpb.HandleRequest{Handle: open.GetHandle()})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		o, err := proc.(*fakeProc).srv.source(open.GetHandle())
		if err != nil {
			return false
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.cancel != nil
	}, 5*time.Second, time.Millisecond, "the stream is running")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _ = c.CloseSource(ctx, &pluginpb.HandleRequest{Handle: open.GetHandle()})
	assert.Less(t, time.Since(start), 5*time.Second, "bounded by the call's deadline")
	select {
	case <-src.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the source was not closed")
	}
}
