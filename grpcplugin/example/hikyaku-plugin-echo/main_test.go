package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin"
	"github.com/xavidop/hikyaku/plugin"
)

// These test the plugin's types in process; the grpcplugin module's tests
// run the built binary.

func newTestEcho(t *testing.T, hooks bool) (*Echo, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "out.ndjson")
	s, err := newEcho(context.Background(), plugin.Spec{Name: "out", Kind: "echo", Options: map[string]any{"file": file, "test_hooks": hooks}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s.(*Echo), file
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func withHeaders(body string, kv ...string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	for i := 0; i+1 < len(kv); i += 2 {
		env.SetHeader(kv[i], kv[i+1])
	}
	return env
}

func TestEchoOptions(t *testing.T) {
	_, err := newEcho(context.Background(), plugin.Spec{Name: "out", Kind: "echo"})
	assert.EqualError(t, err, "echo out: options.file is required")
	_, err = newEcho(context.Background(), plugin.Spec{Name: "out", Kind: "echo", Options: map[string]any{"file": "x", "typo": 1}})
	assert.ErrorContains(t, err, "echo out: options:")
	_, err = newEcho(context.Background(), plugin.Spec{Name: "out", Kind: "echo", Options: map[string]any{"file": filepath.Join(t.TempDir(), "no", "dir")}})
	assert.ErrorContains(t, err, "echo out: open")
}

func TestEchoDelivers(t *testing.T) {
	e, file := newTestEcho(t, false)
	assert.Equal(t, "out", e.Name())
	env := withHeaders(`{"a":1}`, HeaderFail, "permanent")
	env.ContentType = "application/json"
	res, err := e.Deliver(context.Background(), env)
	require.NoError(t, err, "the hooks are off")
	assert.Equal(t, 200, res.Status)
	assert.Equal(t, []byte(`{"a":1}`), res.Body)
	assert.Equal(t, "application/json", res.ContentType)
	assert.Equal(t, file, res.Endpoint)
	assert.Equal(t, []string{env.ID}, res.Headers["X-Echo-Id"])
	assert.NotContains(t, res.Headers, "X-Echo-Envelope")

	res, err = e.DeliverBatch(context.Background(), []*envelope.Envelope{withHeaders("b"), withHeaders("c")})
	require.NoError(t, err)
	assert.Equal(t, []string{"2"}, res.Headers["X-Echo-Batch"])
	assert.Equal(t, "{\"a\":1}\nb\nc\n", read(t, file))

	require.NoError(t, e.Health(context.Background()))
	require.NoError(t, e.Close(context.Background()))
	require.NoError(t, e.Close(context.Background()))
	assert.Error(t, e.Health(context.Background()))
	_, err = e.Deliver(context.Background(), withHeaders("late"))
	assert.Equal(t, plugin.Permanent, classOf(err))
	_, err = e.DeliverBatch(context.Background(), []*envelope.Envelope{withHeaders("late")})
	assert.Error(t, err)
}

func classOf(err error) plugin.Class {
	c, _ := plugin.Classify(err)
	return c
}

func TestEchoHooks(t *testing.T) {
	e, file := newTestEcho(t, true)
	ctx := context.Background()

	_, err := e.Deliver(ctx, withHeaders("x", HeaderFail, "rate_limited", HeaderRetryAfter, "2s", HeaderTags, "a,b"))
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.RateLimited, pe.Class)
	assert.Equal(t, 2*time.Second, pe.RetryAfter)
	assert.Equal(t, []string{"a", "b"}, pe.Tags)
	for kind, class := range map[string]plugin.Class{"transient": plugin.Transient, "permanent": plugin.Permanent, "timeout": plugin.Timeout} {
		_, err := e.Deliver(ctx, withHeaders("x", HeaderFail, kind))
		assert.Equal(t, class, classOf(err), kind)
	}
	_, err = e.Deliver(ctx, withHeaders("x", HeaderFail, "sometimes"))
	assert.ErrorContains(t, err, `unknown class "sometimes"`)
	_, err = e.Deliver(ctx, withHeaders("x", HeaderFail, "transient", HeaderRetryAfter, "soon"))
	assert.ErrorContains(t, err, HeaderRetryAfter)
	assert.NoFileExists(t, file+".missing")
	_, statErr := os.Stat(file)
	require.NoError(t, statErr)
	assert.Empty(t, read(t, file), "failures write nothing")

	_, err = e.Deliver(ctx, withHeaders("x", HeaderSleep, "a while"))
	assert.ErrorContains(t, err, HeaderSleep)
	res, err := e.Deliver(ctx, withHeaders("napped", HeaderSleep, "1ms"))
	require.NoError(t, err)
	assert.Equal(t, []string{"napped"}, []string{string(res.Body)})
	dctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = e.Deliver(dctx, withHeaders("slow", HeaderSleep, "1m"))
	assert.Equal(t, plugin.Timeout, classOf(err), "a sleep cut by the deadline is a timeout")

	t.Setenv("ECHO_TEST_VAR", "v")
	dctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()
	env := withHeaders("r", HeaderEnv, "ECHO_TEST_VAR")
	env.SetAttribute("k", "v")
	res, err = e.Deliver(dctx, env)
	require.NoError(t, err)
	assert.Equal(t, []string{"v"}, res.Headers["X-Echo-Env-Value"])
	assert.Equal(t, []string{"true"}, res.Headers["X-Echo-Env-Set"])
	assert.NotEmpty(t, res.Headers["X-Echo-Pid"])
	assert.NotEmpty(t, res.Headers["X-Echo-Deadline-Ms"])
	var seen envelope.Envelope
	require.NoError(t, json.Unmarshal([]byte(res.Headers["X-Echo-Envelope"][0]), &seen))
	assert.Equal(t, env.ID, seen.ID)
	assert.Equal(t, "v", seen.Attributes["k"])
}

func newTestTicker(t *testing.T, options map[string]any, ack string) *Ticker {
	t.Helper()
	s, err := newTicker(context.Background(), plugin.Spec{Name: "ticks", Kind: "ticker", Options: options, Config: &grpcplugin.Remote{Ack: ack}})
	require.NoError(t, err)
	return s.(*Ticker)
}

func TestTickerOptions(t *testing.T) {
	tk := newTestTicker(t, nil, "")
	assert.Equal(t, time.Second, tk.interval)
	assert.Equal(t, "delivered", tk.ack)
	assert.Equal(t, "accepted", newTestTicker(t, nil, "accepted").ack)
	for _, bad := range []string{"soon", "0s", "-1s"} {
		_, err := newTicker(context.Background(), plugin.Spec{Name: "ticks", Kind: "ticker", Options: map[string]any{"interval": bad}})
		assert.ErrorContains(t, err, "is not a positive duration", bad)
	}
	_, err := newTicker(context.Background(), plugin.Spec{Name: "ticks", Kind: "ticker", Options: map[string]any{"every": "1s"}})
	assert.Error(t, err)
}

func next(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(10 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func TestTickerRedeliversWhatFails(t *testing.T) {
	log := filepath.Join(t.TempDir(), "settle.log")
	tk := newTestTicker(t, map[string]any{"interval": "5ms", "count": 2, "settle_log": log}, "")
	assert.Equal(t, "ticks", tk.Name())
	require.NoError(t, tk.Health(context.Background()))
	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() { done <- tk.Start(context.Background(), out) }()

	first := next(t, out)
	assert.Equal(t, `{"n":0}`, string(first.Body))
	assert.Equal(t, "ticks", first.Subject)
	assert.Equal(t, "application/json", first.ContentType)
	first.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	again := next(t, out)
	assert.Equal(t, first.ID, again.ID, "the failed message comes back first, as it was")
	assert.Equal(t, `{"n":0}`, string(again.Body))
	again.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	second := next(t, out)
	assert.Equal(t, `{"n":1}`, string(second.Body))
	assert.NotEqual(t, first.ID, second.ID)
	second.Ack(context.Background(), envelope.Outcome{Status: envelope.Filtered})

	select {
	case err := <-done:
		assert.NoError(t, err, "count reached and all settled")
	case <-time.After(10 * time.Second):
		t.Fatal("the ticker did not stop")
	}
	want := strings.Join([]string{"0 " + first.ID + " false", "0 " + first.ID + " true", "1 " + second.ID + " true"}, "\n") + "\n"
	assert.Equal(t, want, read(t, log))
}

func TestTickerKeepsWhatItCouldNotHandOver(t *testing.T) {
	tk := newTestTicker(t, map[string]any{"interval": "1ms"}, "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tk.Start(ctx, make(chan *envelope.Envelope)) }() // nobody reads
	require.Eventually(t, func() bool {
		tk.mu.Lock()
		defer tk.mu.Unlock()
		return tk.pending[0]
	}, 10*time.Second, time.Millisecond, "message 0 is being handed over")
	cancel()
	require.NoError(t, <-done)
	tk.mu.Lock()
	defer tk.mu.Unlock()
	assert.Equal(t, []int{0}, tk.retry, "emitted again on the next start")
	assert.Empty(t, tk.pending)
}
