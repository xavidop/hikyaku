package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
)

// The resilience suite (design §13): fault sinks behind the real engine,
// asserting that every message reaches its destination, a fallback or the
// DLQ, with the attempt counts the policies promise, and that none is lost.

const resilienceSpec = `
version: 1
name: resilience
defaults:
  timeout: 1s
  retry: { attempts: 3, backoff: 1ms, max_backoff: 5ms }
  retry_budget: { enabled: false }
  circuit_breaker: { enabled: false }
sources: [{ name: in, type: stdin }]
destinations:
  - name: primary
    type: fault
    fault: { fail: %FAIL%, seed: 42 }
    fallback: [secondary]
  - name: secondary
    type: fault
    fault: { fail: %FAIL2%, seed: 7 }
  - name: archive
    type: file
    path: %DIR%/archive.ndjson
dead_letter: { type: file, path: %DIR%/dlq.ndjson, fallback: [stderr] }
routes:
  - { name: r, from: [in], to: [primary, archive] }
`

func newEngine(t *testing.T, fail, fail2 string) (*engine.Engine, *metrics.Metrics, string) {
	t.Helper()
	dir := t.TempDir()
	spec := strings.NewReplacer("%FAIL%", fail, "%FAIL2%", fail2, "%DIR%", dir).Replace(resilienceSpec)
	l, err := config.LoadBytes(context.Background(), "r.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	require.Empty(t, engine.Check(l.Spec))
	m := metrics.New()
	e, err := engine.New(context.Background(), l.Spec, engine.Options{Metrics: m, NoSources: true, Rand: func() float64 { return 0 }})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })
	return e, m, dir
}

type tally struct {
	delivered, viaFallback, deadLettered, lost int
	attempts                                   int
}

func push(t *testing.T, e *engine.Engine, n int) tally {
	t.Helper()
	var out tally
	for i := 0; i < n; i++ {
		env := envelope.New("in", "test", []byte(`{"n":1}`))
		var acked envelope.Outcome
		env.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
		rep := e.Inject(context.Background(), env)
		d := rep.Routes[0].Deliveries[0]
		out.attempts += d.Attempts
		switch {
		case acked.Status == envelope.Failed:
			out.lost++
		case d.DeadLettered:
			out.deadLettered++
		case d.ViaFallback():
			out.viaFallback++
		case d.OK():
			out.delivered++
		default:
			out.lost++
		}
	}
	return out
}

func lines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return strings.Count(string(b), "\n")
}

func TestResilienceNoFaults(t *testing.T) {
	e, m, dir := newEngine(t, "0%", "0%")
	got := push(t, e, 50)
	assert.Equal(t, tally{delivered: 50, attempts: 50}, got, "one attempt each, no fallback")
	assert.Equal(t, 0.0, testutil.ToFloat64(m.Fallback.WithLabelValues("primary", "secondary", "exhausted")))
	assert.Equal(t, 50, lines(t, filepath.Join(dir, "archive.ndjson")))
	assert.Equal(t, 0, lines(t, filepath.Join(dir, "dlq.ndjson")))
}

func TestResiliencePrimaryDownFallbackTakesAll(t *testing.T) {
	e, m, dir := newEngine(t, "100%", "0%")
	got := push(t, e, 20)
	assert.Equal(t, 20, got.viaFallback)
	assert.Zero(t, got.lost)
	assert.Equal(t, 20*(3+1), got.attempts, "3 attempts on the primary, then 1 on the fallback")
	assert.Equal(t, 20.0, testutil.ToFloat64(m.Fallback.WithLabelValues("primary", "secondary", "exhausted")))
	assert.Equal(t, 0, lines(t, filepath.Join(dir, "dlq.ndjson")))
}

func TestResilienceEverythingDownDeadLetters(t *testing.T) {
	e, m, dir := newEngine(t, "100%", "100%")
	got := push(t, e, 10)
	assert.Equal(t, 10, got.deadLettered)
	assert.Zero(t, got.lost, "nothing is dropped: the DLQ has every message")
	assert.Equal(t, 10*(3+3), got.attempts)
	assert.Equal(t, 10, lines(t, filepath.Join(dir, "dlq.ndjson")))
	assert.Equal(t, 10.0, testutil.ToFloat64(m.DLQ.WithLabelValues("primary", "stored")))
	f, err := os.Open(filepath.Join(dir, "dlq.ndjson"))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	envs, err := engine.ReadDLQ(f)
	require.NoError(t, err)
	info, ok := engine.InfoOf(envs[0])
	require.True(t, ok)
	assert.Equal(t, 6, info.Attempts)
	assert.Len(t, info.Hops, 2)
	assert.Equal(t, "exhausted", info.Hops[0].Outcome)
}

func TestResilienceHalfFailingIsAbsorbed(t *testing.T) {
	e, _, dir := newEngine(t, "50%", "50%")
	got := push(t, e, 200)
	assert.Zero(t, got.lost)
	assert.Equal(t, 200, got.delivered+got.viaFallback+got.deadLettered, "every message is accounted for")
	// With 3 attempts per hop at 50%, a hop fails with probability 1/8 and
	// both with 1/64: almost everything is delivered, a few dead-lettered.
	assert.Greater(t, got.delivered, 150)
	assert.Less(t, got.deadLettered, 15)
	assert.Equal(t, got.deadLettered, lines(t, filepath.Join(dir, "dlq.ndjson")))
}

func TestResilienceDeadlineBoundsTheChain(t *testing.T) {
	dir := t.TempDir()
	spec := `
version: 1
name: deadline
sources: [{ name: in, type: stdin }]
destinations:
  - name: slow
    type: fault
    fault: { latency: 50ms, fail: 100% }
    timeout: 1s
    deadline: 200ms
    retry: { attempts: 10, backoff: 1ms }
    fallback: [never]
  - { name: never, type: fault, fault: { latency: 1s } }
dead_letter: { type: file, path: ` + dir + `/dlq.ndjson }
routes: [{ name: r, from: [in], to: [slow] }]
`
	l, err := config.LoadBytes(context.Background(), "d.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{NoSources: true})
	require.NoError(t, err)
	defer e.Close(context.Background())
	start := time.Now()
	rep := e.Inject(context.Background(), envelope.New("in", "test", []byte("x")))
	assert.Less(t, time.Since(start), 800*time.Millisecond, "the deadline spans attempts and fallbacks")
	assert.True(t, rep.Routes[0].Deliveries[0].DeadLettered)
}
