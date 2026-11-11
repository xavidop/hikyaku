package engine

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
)

func TestNewLimiter(t *testing.T) {
	assert.Nil(t, newLimiter(nil))
	assert.Equal(t, 50, newLimiter(&config.RateLimit{PerSecond: 50}).Burst())
	assert.Equal(t, 1, newLimiter(&config.RateLimit{PerSecond: 0.5}).Burst(), "less than one a second still sends one")
	assert.Equal(t, 3, newLimiter(&config.RateLimit{PerSecond: 2.5}).Burst())
	assert.Equal(t, 200, newLimiter(&config.RateLimit{PerSecond: 50, Burst: 200}).Burst())
}

// The burst goes out at once; after it, attempts wait their turn, retries
// included.
func TestRateLimitSpacesAttempts(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: out
    type: fault
    rate_limit: { per_second: 20, burst: 2 }
    retry: { attempts: 2, backoff: 1ms, max_backoff: 1ms }
routes: [{ name: r, from: [in], to: [out] }]
`
	out := &scripted{name: "out", script: []error{nil, nil, err503}}
	setSinks(t, out)
	reg := metrics.New()
	e := build(t, s, Options{Metrics: reg})

	start := time.Now()
	for i := 0; i < 2; i++ {
		require.Equal(t, "delivered", e.Inject(context.Background(), msg("in", `{}`)).Status)
	}
	assert.Less(t, time.Since(start), 40*time.Millisecond, "the burst does not wait")

	// The third message fails once and is retried: two more attempts, 50ms apart.
	require.Equal(t, "delivered", e.Inject(context.Background(), msg("in", `{}`)).Status)
	assert.GreaterOrEqual(t, time.Since(start), 90*time.Millisecond)
	assert.Equal(t, 4, out.count())
	assert.Equal(t, 1, testutil.CollectAndCount(reg.RateLimitWait), "one destination waited")
	assert.Equal(t, uint64(2), histogramCount(t, reg.RateLimitWait.WithLabelValues("out")))
}

// A turn that would come after the deadline is not waited for: the
// fallback takes the message at once.
func TestRateLimitPastDeadlineFallsBack(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: slow
    type: fault
    rate_limit: { per_second: 0.1 }
    deadline: 300ms
    fallback: [queue]
  - { name: queue, type: fault }
routes: [{ name: r, from: [in], to: [slow] }]
`
	slow, queue := &scripted{name: "slow"}, &scripted{name: "queue"}
	setSinks(t, slow, queue)
	e := build(t, s, Options{})
	require.Equal(t, "delivered", e.Inject(context.Background(), msg("in", `{}`)).Status)

	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{}`))
	assert.Less(t, time.Since(start), 200*time.Millisecond, "it does not wait out the deadline")
	assert.Equal(t, "delivered", rep.Status)
	d := rep.Routes[0].Deliveries[0]
	assert.Equal(t, "queue", d.ServedBy)
	assert.Equal(t, outcomeDeadline, d.Hops[0].Outcome)
	assert.Contains(t, d.Hops[0].Error, "rate limit")
	assert.Equal(t, 1, slow.count())
	assert.Equal(t, 1, queue.count())
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, o.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount()
}
