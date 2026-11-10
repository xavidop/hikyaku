//go:build !windows

package grpcplugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// These tests run the example plugin (example/hikyaku-plugin-echo) as a
// real process.

func TestLaunchDescribe(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	assert.Equal(t, "echo", p.Name())
	kinds := p.Kinds()
	require.Len(t, kinds, 2)

	echo, ticker := kinds[0], kinds[1]
	assert.Equal(t, "echo", echo.Name)
	assert.Equal(t, RoleSink, echo.Role)
	assert.Equal(t, "Appends each message body to a file and replies with it.", echo.Description)
	var schema struct {
		Properties           map[string]map[string]any `json:"properties"`
		Required             []string                  `json:"required"`
		AdditionalProperties *bool                     `json:"additionalProperties"`
	}
	require.NoError(t, json.Unmarshal(echo.Schema, &schema))
	assert.Equal(t, []string{"file"}, schema.Required)
	assert.Contains(t, schema.Properties, "file")
	assert.Contains(t, schema.Properties, "test_hooks", "yaml names, not Go names")
	require.NotNil(t, schema.AdditionalProperties)
	assert.False(t, *schema.AdditionalProperties, "unknown keys are errors")

	assert.Equal(t, "ticker", ticker.Name)
	assert.Equal(t, RoleSource, ticker.Role)
	assert.Contains(t, string(ticker.Schema), `"interval"`)
}

func TestLaunchChecksSHA256(t *testing.T) {
	b, err := os.ReadFile(echoBin)
	require.NoError(t, err)
	sum := sha256.Sum256(b)
	good := hex.EncodeToString(sum[:])

	launchEcho(t, "match", func(c *config.PluginBinary) { c.SHA256 = good })
	launchEcho(t, "upper", func(c *config.PluginBinary) { c.SHA256 = strings.ToUpper(good) })

	_, err = Launch(context.Background(), config.PluginBinary{Name: "tampered", Command: echoBin, SHA256: strings.Repeat("0", 64)}, "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugin tampered: ")
	assert.Contains(t, err.Error(), "does not match its sha256")

	_, err = Launch(context.Background(), config.PluginBinary{Name: "bad", Command: echoBin, SHA256: "xyz"}, "", nil)
	assert.ErrorContains(t, err, "plugin bad: sha256: want 64 hex digits")
}

func TestLaunchFindsTheCommand(t *testing.T) {
	dir := filepath.Dir(echoBin)
	p, err := Launch(context.Background(), config.PluginBinary{Name: "relative", Command: "./" + filepath.Base(echoBin)}, dir, nil)
	require.NoError(t, err, "relative to the spec's directory")
	require.NoError(t, p.Close())

	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	p, err = Launch(context.Background(), config.PluginBinary{Name: "on-path", Command: filepath.Base(echoBin)}, "/nowhere", nil)
	require.NoError(t, err)
	require.NoError(t, p.Close())

	_, err = Launch(context.Background(), config.PluginBinary{Name: "x", Command: "./missing"}, dir, nil)
	assert.ErrorContains(t, err, "no such file")
	_, err = Launch(context.Background(), config.PluginBinary{Name: "x", Command: "hikyaku-no-such-plugin"}, dir, nil)
	assert.ErrorContains(t, err, `command "hikyaku-no-such-plugin" is not on PATH`)
	_, err = Launch(context.Background(), config.PluginBinary{Name: "x"}, dir, nil)
	assert.ErrorContains(t, err, "plugin x: command is required")
	_, err = Launch(context.Background(), config.PluginBinary{Command: echoBin}, dir, nil)
	assert.ErrorContains(t, err, "name is required")

	plain := filepath.Join(t.TempDir(), "plain")
	require.NoError(t, os.WriteFile(plain, []byte("data"), 0o644))
	_, err = Launch(context.Background(), config.PluginBinary{Name: "x", Command: plain}, "", nil)
	assert.ErrorContains(t, err, "is not executable")
}

func TestLaunchRefusesANonPlugin(t *testing.T) {
	script := filepath.Join(t.TempDir(), "not-a-plugin")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\necho hello\n"), 0o755))
	_, err := Launch(context.Background(), config.PluginBinary{Name: "nope", Command: script}, "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugin nope: start ")
}

func TestLaunchHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Launch(ctx, config.PluginBinary{Name: "echo", Command: echoBin}, "", nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDeliverRoundTrip(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	sink, file := openEcho(t, p, "out")
	assert.Equal(t, "out", sink.Name())
	_, batches := sink.(plugin.BatchSink)
	assert.True(t, batches, "the echo sink batches, so its proxy does")

	env := msg(`{"a":1}`, "X-Request", "abc")
	env.Headers["Multi"] = []string{"1", "2"}
	env.Subject = "/orders"
	env.ContentType = "application/json"
	env.SetAttribute("k", "v")
	env.SetAttribute("n", 3)
	env.Trace = trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	res, err := sink.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, 200, res.Status)
	assert.Equal(t, []byte(`{"a":1}`), res.Body)
	assert.Equal(t, "application/json", res.ContentType)
	assert.Equal(t, file, res.Endpoint)
	assert.Equal(t, []string{env.ID}, res.Headers["X-Echo-Id"])
	assert.Equal(t, []string{`{"a":1}`}, lines(t, file))

	// The envelope as the plugin saw it.
	require.Len(t, res.Headers["X-Echo-Envelope"], 1)
	var seen envelope.Envelope
	require.NoError(t, json.Unmarshal([]byte(res.Headers["X-Echo-Envelope"][0]), &seen))
	assert.Equal(t, env.ID, seen.ID)
	assert.Equal(t, "in", seen.Source)
	assert.Equal(t, "http", seen.Protocol)
	assert.Equal(t, "/orders", seen.Subject)
	assert.Equal(t, "application/json", seen.ContentType)
	assert.Equal(t, []byte(`{"a":1}`), seen.Body)
	assert.Equal(t, []string{"abc"}, seen.Headers["X-Request"])
	assert.Equal(t, []string{"1", "2"}, seen.Headers["Multi"])
	assert.Equal(t, map[string]any{"k": "v", "n": float64(3)}, seen.Attributes)
	assert.True(t, env.ReceivedAt.Equal(seen.ReceivedAt), "%s != %s", env.ReceivedAt, seen.ReceivedAt)
	assert.Equal(t, env.Trace.TraceID(), seen.Trace.TraceID())
	assert.Equal(t, env.Trace.SpanID(), seen.Trace.SpanID())

	require.NoError(t, sink.Health(context.Background()))
}

func TestErrorClassesCrossTheWire(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	sink, file := openEcho(t, p, "out")
	cases := []struct {
		name       string
		headers    []string
		class      plugin.Class
		retryAfter time.Duration
		tags       []string
	}{
		{"transient", []string{"X-Echo-Fail", "transient", "X-Echo-Tags", "5xx,503"}, plugin.Transient, 0, []string{"5xx", "503"}},
		{"rate limited", []string{"X-Echo-Fail", "rate_limited", "X-Echo-Retry-After", "1500ms"}, plugin.RateLimited, 1500 * time.Millisecond, []string{"429"}},
		{"permanent", []string{"X-Echo-Fail", "permanent", "X-Echo-Tags", "4xx,400,echo:BAD"}, plugin.Permanent, 0, []string{"4xx", "400", "echo:BAD"}},
		{"timeout", []string{"X-Echo-Fail", "timeout"}, plugin.Timeout, 0, []string{"timeout"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := sink.Deliver(context.Background(), msg("x", c.headers...))
			require.Error(t, err)
			var pe *plugin.Error
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, c.class, pe.Class)
			assert.Equal(t, c.retryAfter, pe.RetryAfter)
			assert.Equal(t, c.tags, pe.Tags)
			assert.Contains(t, err.Error(), "echo: failing as asked")
			class, retryAfter := plugin.Classify(err)
			assert.Equal(t, c.class, class)
			assert.Equal(t, c.retryAfter, retryAfter)
		})
	}
	assert.Empty(t, lines(t, file), "a failed delivery wrote nothing")
}

func TestDeliverBatch(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	sink, file := openEcho(t, p, "out")
	bs, ok := sink.(plugin.BatchSink)
	require.True(t, ok)
	res, err := bs.DeliverBatch(context.Background(), []*envelope.Envelope{msg("a"), msg("b"), msg("c")})
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, res.Headers["X-Echo-Batch"])
	assert.Equal(t, []string{"a", "b", "c"}, lines(t, file))
	res, err = bs.DeliverBatch(context.Background(), nil)
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestDeadlineCrossesTheWire(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	sink, _ := openEcho(t, p, "out")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := sink.Deliver(ctx, msg("x"))
	require.NoError(t, err)
	require.Len(t, res.Headers["X-Echo-Deadline-Ms"], 1, "the plugin's ctx has the host's deadline")
	left, err := strconv.Atoi(res.Headers["X-Echo-Deadline-Ms"][0])
	require.NoError(t, err)
	assert.Greater(t, left, 0)
	assert.LessOrEqual(t, left, 5000)

	res, err = sink.Deliver(context.Background(), msg("x"))
	require.NoError(t, err)
	assert.Empty(t, res.Headers["X-Echo-Deadline-Ms"], "no deadline, none in the plugin")

	ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = sink.Deliver(ctx, msg("slow", "X-Echo-Sleep", "30s"))
	assert.Less(t, time.Since(start), 5*time.Second, "the call ends at the deadline")
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Timeout, pe.Class)
	assert.Contains(t, plugin.Tags(err), plugin.TagTimeout)
}

func TestMinimalEnvironment(t *testing.T) {
	t.Setenv("HIKYAKU_TEST_SECRET", "s3cret")
	p, _ := launchEcho(t, "echo", func(c *config.PluginBinary) {
		c.Env = map[string]string{"ECHO_GREETING": "hola"}
		c.Args = []string{"--ignored"}
	})
	sink, _ := openEcho(t, p, "out")
	env := func(name string) (string, bool) {
		res, err := sink.Deliver(context.Background(), msg("x", "X-Echo-Env", name))
		require.NoError(t, err)
		return res.Headers["X-Echo-Env-Value"][0], res.Headers["X-Echo-Env-Set"][0] == "true"
	}
	v, ok := env("ECHO_GREETING")
	assert.True(t, ok)
	assert.Equal(t, "hola", v, "the spec's env: reaches the plugin")
	_, ok = env("HIKYAKU_TEST_SECRET")
	assert.False(t, ok, "the rest of hikyaku's environment does not")
	_, ok = env("PATH")
	assert.True(t, ok, "the minimal environment does")
	v, _ = env("HIKYAKU_PLUGIN_LOG_LEVEL")
	assert.Equal(t, "debug", v, "the plugin learns the host's log level")
}

func TestPluginLogsReachTheHost(t *testing.T) {
	p, logs := launchEcho(t, "echo")
	_, file := openEcho(t, p, "out")
	require.Eventually(t, func() bool { return len(logs.records("echo opened")) > 0 }, 10*time.Second, 20*time.Millisecond)
	r := logs.records("echo opened")[0]
	assert.Equal(t, "INFO", r["level"])
	assert.Equal(t, "echo", r["plugin"])
	assert.Equal(t, file, r["file"], "attributes survive")
	assert.Equal(t, "out", r["name"])
	assert.NotContains(t, r, "timestamp", "the plugin's own timestamp is dropped")
}

func TestSourceSettles(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	settleLog := filepath.Join(t.TempDir(), "settle.log")
	src := openTicker(t, p, "ticks", map[string]any{"interval": "20ms", "settle_log": settleLog}, nil)
	assert.Equal(t, "ticks", src.Name())
	require.NoError(t, src.Health(context.Background()))
	out, stop := runSource(t, src)

	first := recv(t, out)
	assert.Equal(t, `{"n":0}`, string(first.Body))
	assert.Equal(t, "ticks", first.Source)
	assert.Equal(t, "ticker", first.Protocol)
	assert.Equal(t, "ticks", first.Subject)
	assert.Equal(t, "application/json", first.ContentType)
	n, _ := first.Attribute("ticker.n")
	assert.Equal(t, float64(0), n)
	assert.False(t, first.ReceivedAt.IsZero())

	// Failed: the plugin redelivers the same message.
	first.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	var again *envelope.Envelope
	for again == nil {
		env := recv(t, out)
		if env.ID == first.ID {
			again = env
			continue
		}
		env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	}
	assert.Equal(t, `{"n":0}`, string(again.Body))
	again.Ack(context.Background(), envelope.Outcome{Status: envelope.DeadLettered})

	want := []string{"0 " + first.ID + " false", "0 " + first.ID + " true"}
	require.Eventually(t, func() bool {
		var mine []string
		for _, l := range lines(t, settleLog) {
			if strings.HasPrefix(l, "0 ") {
				mine = append(mine, l)
			}
		}
		return slices.Equal(mine, want)
	}, 10*time.Second, 20*time.Millisecond, "settle calls reach the plugin")

	require.NoError(t, stop(), "Start returns nil when ctx ends")
	closer, ok := src.(plugin.Closer)
	require.True(t, ok)
	require.NoError(t, closer.Close(context.Background()))
	require.NoError(t, closer.Close(context.Background()), "closing twice is fine")
}

func TestSourceSettlesWhenAccepted(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	settleLog := filepath.Join(t.TempDir(), "settle.log")
	src := openTicker(t, p, "ticks", map[string]any{"interval": "20ms", "settle_log": settleLog}, &config.Source{Ack: "accepted"})
	out, stop := runSource(t, src)
	env := recv(t, out)
	env.Queued()
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed}) // too late: accepted settles when queued
	require.Eventually(t, func() bool {
		return slices.Contains(lines(t, settleLog), "0 "+env.ID+" true")
	}, 10*time.Second, 20*time.Millisecond)
	assert.NotContains(t, lines(t, settleLog), "0 "+env.ID+" false")
	require.NoError(t, stop())
}

func TestCountedSourceRunsOut(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	src := openTicker(t, p, "ticks", map[string]any{"interval": "10ms", "count": 2}, nil)
	ctx := context.Background()
	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() { done <- src.Start(ctx, out) }()
	for range 2 {
		recv(t, out).Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	}
	select {
	case err := <-done:
		assert.NoError(t, err, "a source that runs out ends its Start with nil")
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}
}

func TestOptionsRejectedByThePlugin(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	_, err := p.sinkFactory("echo")(context.Background(), plugin.Spec{Name: "out", Kind: "echo", Options: map[string]any{"file": "x", "bogus": 1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugin echo: echo out: options:")
	assert.Contains(t, err.Error(), "bogus")
	_, err = p.sourceFactory("ticker")(context.Background(), plugin.Spec{Name: "ticks", Kind: "ticker", Options: map[string]any{"interval": "-1s"}})
	assert.ErrorContains(t, err, `ticker ticks: options.interval "-1s" is not a positive duration`)
}

// waitExited waits until the host has seen the process exit.
func waitExited(t *testing.T, p *Plugin) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, _, ok := p.current()
		return !ok
	}, 10*time.Second, 10*time.Millisecond)
}

func TestKilledPluginIsRelaunched(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	sink, file := openEcho(t, p, "out")
	pid := p.pid()
	require.NotZero(t, pid)

	// A delivery in flight when the process dies fails as transient,
	// tagged connect.
	errc := make(chan error, 1)
	go func() {
		_, err := sink.Deliver(context.Background(), msg("in-flight", "X-Echo-Sleep", "30s"))
		errc <- err
	}()
	require.Eventually(t, func() bool { return slices.Contains(lines(t, file), "in-flight") }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	var err error
	select {
	case err = <-errc:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery did not fail")
	}
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Transient, pe.Class)
	assert.Equal(t, []string{plugin.TagConnect}, pe.Tags)

	// The next call relaunches the plugin and re-opens the handle.
	res, err := sink.Deliver(context.Background(), msg("after"))
	require.NoError(t, err)
	pid2, _ := strconv.Atoi(res.Headers["X-Echo-Pid"][0])
	assert.NotEqual(t, pid, pid2)
	assert.Equal(t, pid2, p.pid())
	assert.Equal(t, []string{"in-flight", "after"}, lines(t, file))
	assert.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the killed process is reaped")

	// Killed while idle: the next call relaunches it too.
	require.NoError(t, syscall.Kill(pid2, syscall.SIGKILL))
	waitExited(t, p)
	res, err = sink.Deliver(context.Background(), msg("idle"))
	require.NoError(t, err)
	pid3, _ := strconv.Atoi(res.Headers["X-Echo-Pid"][0])
	assert.NotContains(t, []int{pid, pid2}, pid3)
	require.NoError(t, sink.Health(context.Background()))
}

func TestSourceStreamSurvivesAKill(t *testing.T) {
	p, _ := launchEcho(t, "echo")
	src := openTicker(t, p, "ticks", map[string]any{"interval": "10ms"},
		&config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(20 * time.Millisecond), MaxBackoff: config.Duration(100 * time.Millisecond)}})
	out, stop := runSource(t, src)
	first := recv(t, out)
	first.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	pid := p.pid()
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

	// Messages flow again from a new process (which counts from 0).
	require.Eventually(t, func() bool {
		select {
		case env := <-out:
			env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
			return p.pid() != pid && p.pid() != 0 && string(env.Body) == `{"n":0}` && env.ID != first.ID
		default:
			return false
		}
	}, 15*time.Second, 5*time.Millisecond)
	require.NoError(t, stop())
}

// replaceFile swaps a file's content through a rename, so a binary that
// ran keeps its inode (macOS caches code signatures by inode).
func replaceFile(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, data, 0o755))
	require.NoError(t, os.Rename(tmp, path))
}

func TestRelaunchBacksOffAndChecksTheBinary(t *testing.T) {
	good, err := os.ReadFile(echoBin)
	require.NoError(t, err)
	sum := sha256.Sum256(good)
	bin := filepath.Join(filepath.Dir(echoBin), "relaunch-"+strconv.Itoa(os.Getpid()))
	replaceFile(t, bin, good)
	t.Cleanup(func() { _ = os.Remove(bin) })

	p, _ := launchEcho(t, "echo", func(c *config.PluginBinary) { c.Command = bin; c.SHA256 = hex.EncodeToString(sum[:]) })
	sink, _ := openEcho(t, p, "out")
	var clockMu sync.Mutex
	clock := time.Now()
	p.mu.Lock()
	p.now = func() time.Time { // frozen, so the backoff window is too
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	p.mu.Unlock()

	// The binary changes, then the process dies: the relaunch refuses it.
	replaceFile(t, bin, append(append([]byte(nil), good...), 0))
	require.NoError(t, syscall.Kill(p.pid(), syscall.SIGKILL))
	waitExited(t, p)
	_, err = sink.Deliver(context.Background(), msg("x"))
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Transient, pe.Class)
	assert.Contains(t, pe.Tags, plugin.TagConnect)
	assert.Contains(t, err.Error(), "does not match its sha256")

	// Until the backoff passes, calls fail at once, saying when.
	_, err = sink.Deliver(context.Background(), msg("x"))
	require.ErrorAs(t, err, &pe)
	assert.Contains(t, err.Error(), "is down, relaunching in")
	assert.Positive(t, pe.RetryAfter)

	// The right binary is back: once the backoff has passed, a call
	// relaunches it.
	replaceFile(t, bin, good)
	clockMu.Lock()
	clock = clock.Add(time.Minute)
	clockMu.Unlock()
	_, err = sink.Deliver(context.Background(), msg("back"))
	require.NoError(t, err)
}

func TestCloseStopsTheProcess(t *testing.T) {
	p, logs := launchEcho(t, "echo")
	sink, _ := openEcho(t, p, "out")
	src := openTicker(t, p, "ticks", map[string]any{"interval": "1h"}, nil)
	pid := p.pid()
	require.NoError(t, p.Close())
	assert.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the process is gone")
	assert.NotEmpty(t, logs.records("echo closed"), "what the engine left open was closed (flushed) first")

	_, err := sink.Deliver(context.Background(), msg("x"))
	assert.ErrorContains(t, err, "is closed")
	assert.Error(t, src.Health(context.Background()))
	require.NoError(t, p.Close(), "closing twice is fine")
	require.NoError(t, sink.Close(context.Background()))
	_, err = p.sinkFactory("echo")(context.Background(), plugin.Spec{Name: "late", Kind: "echo", Options: map[string]any{"file": "x"}})
	assert.ErrorContains(t, err, "plugin echo is closed")
}

func TestLoadAllRegistersKinds(t *testing.T) {
	_, log := newLogs(t)
	set, err := LoadAll(context.Background(), []config.PluginBinary{{Name: "echo", Command: echoBin}}, "", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	require.Len(t, set.Plugins(), 1)
	set.Register()

	f, ok := plugin.LookupSink("echo")
	require.True(t, ok)
	_, ok = plugin.LookupSource("ticker")
	require.True(t, ok)
	i := slices.IndexFunc(plugin.SinkRegistrations(), func(r plugin.Registration) bool { return r.Kind == "echo" })
	require.GreaterOrEqual(t, i, 0)
	assert.Equal(t, "Appends each message body to a file and replies with it.", plugin.SinkRegistrations()[i].Description)

	// Through the registry, as the engine builds destinations.
	file := filepath.Join(t.TempDir(), "out.ndjson")
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: "echo", Options: map[string]any{"file": file}})
	require.NoError(t, err)
	_, err = sink.Deliver(context.Background(), msg("hi"))
	require.NoError(t, err)
	require.NoError(t, sink.Close(context.Background()))
	assert.Equal(t, []string{"hi"}, lines(t, file))
	_ = f

	// Loading the plugins again (a reload) replaces the kinds.
	again, err := LoadAll(context.Background(), []config.PluginBinary{{Name: "echo", Command: echoBin}}, "", log)
	require.NoError(t, err)
	require.NoError(t, again.Close())
}

func TestLoadAllRefusesClashes(t *testing.T) {
	_, err := LoadAll(context.Background(), []config.PluginBinary{{Name: "a", Command: echoBin}, {Name: "b", Command: echoBin}}, "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `plugin b: destination type "echo" is also served by plugin a`)
	assert.Contains(t, err.Error(), `plugin b: source type "ticker" is also served by plugin a`)

	_, err = LoadAll(context.Background(), []config.PluginBinary{{Name: "a", Command: echoBin}, {Name: "a", Command: echoBin}}, "", nil)
	assert.ErrorContains(t, err, "plugin a is declared twice")

	_, err = LoadAll(context.Background(), []config.PluginBinary{{Name: "a", Command: echoBin}, {Name: "b", Command: "./missing"}}, t.TempDir(), nil)
	assert.ErrorContains(t, err, "plugin b: command")
}

func TestExtensionsValidateOptions(t *testing.T) {
	set, err := LoadAll(context.Background(), []config.PluginBinary{{Name: "echo", Command: echoBin}}, "", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	x := set.Extensions()
	require.Contains(t, x.Sinks, "echo")
	require.Contains(t, x.Sources, "ticker")
	echo, ticker := x.Sinks["echo"], x.Sources["ticker"]
	assert.Equal(t, "Appends each message body to a file and replies with it.", echo.Description)
	assert.NotEmpty(t, echo.Schema)
	require.NotNil(t, echo.Check)

	assert.NoError(t, echo.Check(map[string]any{"file": "out.ndjson", "test_hooks": true}))
	assert.ErrorContains(t, echo.Check(nil), "missing property 'file'")
	assert.ErrorContains(t, echo.Check(map[string]any{"file": "x", "bogus": 1}), "'bogus'")
	assert.ErrorContains(t, echo.Check(map[string]any{"file": 3}), "file: got number, want string")
	assert.NoError(t, ticker.Check(map[string]any{"interval": "250ms", "count": 3}))
	assert.ErrorContains(t, ticker.Check(map[string]any{"count": -1}), "count: ")
	assert.ErrorContains(t, ticker.Check(map[string]any{"interval": "soon"}), "interval: ")

	// hikyaku validate, through the spec loader.
	spec := `
version: 1
name: demo
sources:
  - { name: ticks, type: ticker, options: { interval: 1s } }
destinations:
  - { name: out, type: echo, options: { files: out.ndjson } }
routes:
  - { name: r, from: [ticks], to: [out] }
`
	_, err = config.LoadBytes(context.Background(), "s.yaml", []byte(spec), config.LoadOptions{Extensions: x})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `destination "out".options: `)
	assert.Contains(t, err.Error(), "missing property 'file'")
	_, err = config.LoadBytes(context.Background(), "s.yaml", []byte(strings.Replace(spec, "files:", "file:", 1)), config.LoadOptions{Extensions: x})
	require.NoError(t, err)

	// hikyaku schema.
	s := config.SchemaWith(x)
	b, err := json.Marshal(s.Definitions["Destination"])
	require.NoError(t, err)
	assert.Contains(t, string(b), `"const":"echo"`)
	assert.Contains(t, string(b), `"test_hooks"`)
}

func TestCheckKinds(t *testing.T) {
	mk := func(name string, kinds ...Kind) *Plugin { return &Plugin{name: name, kinds: kinds} }
	err := checkKinds([]*Plugin{
		mk("a", Kind{Name: "http", Role: RoleSink}),
		mk("b", Kind{Name: "stdin", Role: RoleSource}),
		mk("c", Kind{Name: "stdout", Role: RoleSource}, Kind{Name: "x-c", Role: RoleSink}),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `plugin a: destination type "http" clashes with the built-in http destination type`)
	assert.Contains(t, err.Error(), `plugin b: source type "stdin" clashes with the built-in stdin source type`)
	assert.NotContains(t, err.Error(), "plugin c", "a source named like a built-in destination is a different type")

	plugin.RegisterSink("x-go-plugin", func(context.Context, plugin.Spec) (plugin.Sink, error) { return nil, errors.New("unused") })
	err = checkKinds([]*Plugin{mk("d", Kind{Name: "x-go-plugin", Role: RoleSink})})
	assert.ErrorContains(t, err, `plugin d: destination type "x-go-plugin" clashes with the x-go-plugin destination type this binary registers (a Go plugin)`)
}
