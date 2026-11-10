package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

func policy(r config.Retry) retryPolicy { return newRetryPolicy(config.ResolveRetry(&r)) }

func TestBackoffFullBounds(t *testing.T) {
	p := policy(config.Retry{Backoff: config.Duration(200 * time.Millisecond), MaxBackoff: config.Duration(10 * time.Second)})
	assert.Equal(t, 200*time.Millisecond, p.base(1))
	assert.Equal(t, 400*time.Millisecond, p.base(2))
	assert.Equal(t, 800*time.Millisecond, p.base(3))
	assert.Equal(t, 10*time.Second, p.base(10), "capped at max_backoff")
	assert.Equal(t, 10*time.Second, p.base(1000), "no overflow")
	for _, r := range []float64{0, 0.5, 0.999} {
		w := p.wait(3, func() float64 { return r })
		assert.GreaterOrEqual(t, w, time.Duration(0))
		assert.LessOrEqual(t, w, 800*time.Millisecond, "full jitter is within [0, base]")
	}
	assert.Equal(t, time.Duration(0), p.wait(3, func() float64 { return 0 }))
}

func TestBackoffEqualAndNone(t *testing.T) {
	eq := policy(config.Retry{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Minute), Jitter: "equal"})
	for _, r := range []float64{0, 0.5, 0.999} {
		w := eq.wait(1, func() float64 { return r })
		assert.GreaterOrEqual(t, w, 500*time.Millisecond, "equal jitter keeps half")
		assert.LessOrEqual(t, w, time.Second)
	}
	none := policy(config.Retry{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Minute), Jitter: "none"})
	assert.Equal(t, 4*time.Second, none.wait(3, func() float64 { return 0.7 }))
}

func TestRetryAfterCapped(t *testing.T) {
	p := policy(config.Retry{Backoff: config.Duration(100 * time.Millisecond), MaxBackoff: config.Duration(10 * time.Second)})
	rnd := func() float64 { return 0.5 }
	assert.Equal(t, 2*time.Second, p.waitFor(1, 2*time.Second, rnd), "Retry-After is honoured")
	assert.Equal(t, 10*time.Second, p.waitFor(1, time.Minute, rnd), "and capped by max_backoff")
	assert.Equal(t, 50*time.Millisecond, p.waitFor(1, 0, rnd), "no hint: jittered backoff")
	ignore := policy(config.Retry{Backoff: config.Duration(100 * time.Millisecond), RespectRetryAfter: config.Ptr(false)})
	assert.Equal(t, 50*time.Millisecond, ignore.waitFor(1, 2*time.Second, rnd))
}

func TestRetryOnSpecificity(t *testing.T) {
	p := policy(config.Retry{})
	cases := []struct {
		err   error
		retry bool
	}{
		{plugin.StatusError(429, 0, ""), true},
		{plugin.StatusError(408, 0, ""), true},
		{plugin.StatusError(500, 0, ""), true},
		{plugin.StatusError(503, 0, ""), true},
		{plugin.StatusError(404, 0, ""), false},
		{plugin.StatusError(400, 0, ""), false},
		{context.DeadlineExceeded, true},
		{plugin.Errorf(plugin.Transient, []string{"connect"}, "refused"), true},
		{plugin.Errorf(plugin.Transient, nil, "untagged"), true},
		{plugin.Permanentf("rejected"), false},
		// A protocol tag neither list names falls back to the class.
		{plugin.Errorf(plugin.Transient, []string{"ws:close_1011"}, "closed"), true},
		{plugin.Errorf(plugin.Permanent, []string{"kafka:MESSAGE_TOO_LARGE"}, "too big"), false},
	}
	for _, c := range cases {
		class, _ := plugin.Classify(c.err)
		assert.Equal(t, c.retry, p.retryable(class, plugin.Tags(c.err)), "%v", c.err)
	}
	optOut := policy(config.Retry{NeverOn: config.TagList{"4xx", "kafka:not_leader_for_partition"}})
	assert.False(t, optOut.retryable(plugin.Transient, []string{"kafka:NOT_LEADER_FOR_PARTITION"}), "never_on opts a protocol tag out")
	custom := policy(config.Retry{RetryOn: config.TagList{"5xx", "ws:close_1011", "404"}, NeverOn: config.TagList{"503"}})
	for err, want := range map[error]bool{
		plugin.StatusError(503, 0, ""):                                 false,
		plugin.StatusError(502, 0, ""):                                 true,
		plugin.StatusError(404, 0, ""):                                 true,
		plugin.Errorf(plugin.Transient, []string{"WS:CLOSE_1011"}, ""): true,
	} {
		class, _ := plugin.Classify(err)
		assert.Equal(t, want, custom.retryable(class, plugin.Tags(err)), "%v", err)
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestBreakerTransitions(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	var states []breakerState
	b := newBreaker(config.ResolveBreaker(&config.CircuitBreaker{FailureRatio: 0.5, MinRequests: 4, OpenFor: config.Duration(30 * time.Second), HalfOpenMax: 2}), clk.now, func(s breakerState) { states = append(states, s) })

	// Below min_requests nothing trips, even at 100% failures.
	for i := 0; i < 3; i++ {
		require.True(t, b.allow())
		b.record(true)
	}
	assert.Equal(t, stateClosed, b.current())
	require.True(t, b.allow())
	b.record(true)
	assert.Equal(t, stateOpen, b.current(), "4 failures out of 4 ≥ 50%")
	assert.False(t, b.allow(), "open refuses")

	clk.advance(31 * time.Second)
	assert.Equal(t, stateHalfOpen, b.current())
	assert.True(t, b.allow())
	assert.True(t, b.allow())
	assert.False(t, b.allow(), "half-open lets half_open_max trials through")
	b.record(false)
	b.record(false)
	assert.Equal(t, stateClosed, b.current(), "trials succeeded: closed")

	for i := 0; i < 4; i++ {
		b.allow()
		b.record(true)
	}
	assert.Equal(t, stateOpen, b.current())
	clk.advance(31 * time.Second)
	assert.True(t, b.allow())
	b.record(true)
	assert.Equal(t, stateOpen, b.current(), "a failed trial reopens")
	assert.Equal(t, []breakerState{stateOpen, stateHalfOpen, stateClosed, stateOpen, stateHalfOpen, stateOpen}, states)

	// The window rolls: old failures stop counting.
	b2 := newBreaker(config.ResolveBreaker(&config.CircuitBreaker{MinRequests: 4, Window: config.Duration(10 * time.Second)}), clk.now, nil)
	for i := 0; i < 3; i++ {
		b2.record(true)
	}
	clk.advance(20 * time.Second)
	b2.record(true)
	assert.Equal(t, stateClosed, b2.current())

	off := newBreaker(config.ResolveBreaker(&config.CircuitBreaker{Enabled: config.Ptr(false)}), clk.now, nil)
	for i := 0; i < 100; i++ {
		off.record(true)
	}
	assert.True(t, off.allow())
}

func TestRetryBudget(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	ctx := context.Background()
	b := newRetryBudget(config.RetryBudget{Enabled: config.Ptr(true), Ratio: 0.5, MinPerSecond: 0.1}, "billing", nil, clk.now)
	// window 10s × 0.1/s = 1 retry free, plus half a retry per request.
	for i := 0; i < 4; i++ {
		b.request(ctx)
	}
	allowed := 0
	for i := 0; i < 10; i++ {
		if b.allowRetry(ctx) {
			allowed++
		}
	}
	assert.Equal(t, 3, allowed, "0.5 × 4 requests + 1")
	assert.Equal(t, 0.0, b.remaining())
	clk.advance(11 * time.Second)
	assert.True(t, b.allowRetry(ctx), "the window rolled")

	shared := store.NewMemory("redis-main")
	s := newRetryBudget(config.RetryBudget{Enabled: config.Ptr(true), Ratio: 1, MinPerSecond: 0}, "billing", shared, clk.now)
	s.request(ctx)
	assert.True(t, s.allowRetry(ctx))
	assert.False(t, s.allowRetry(ctx), "shared counters: 1 request, 1 retry")
	shared.SetError(assert.AnError)
	assert.True(t, s.allowRetry(ctx), "a dead store falls back to the local budget")

	var off *retryBudget
	assert.True(t, off.allowRetry(ctx))
	assert.Equal(t, -1.0, off.remaining())
}
