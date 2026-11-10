package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

type transition struct{ from, to breakerState }

func testBreaker(t *testing.T, cb config.CircuitBreaker) (*breaker, *fakeClock, *[]transition) {
	t.Helper()
	clk := newFakeClock()
	var seen []transition
	b := newBreaker(config.ResolveBreaker(&cb), clk.Now, func(from, to breakerState) {
		seen = append(seen, transition{from, to})
	})
	require.NotNil(t, b)
	return b, clk, &seen
}

// call runs one call through b with the given outcome.
func call(t *testing.T, b *breaker, o outcome) error {
	t.Helper()
	tk, err := b.allow()
	if err != nil {
		return err
	}
	b.done(tk, o)
	return nil
}

func TestBreakerOpensAtTheFailureRatio(t *testing.T) {
	b, _, seen := testBreaker(t, config.CircuitBreaker{FailureRatio: 0.5, MinRequests: 4})
	require.NoError(t, call(t, b, outcomeSuccess))
	require.NoError(t, call(t, b, outcomeSuccess))
	require.NoError(t, call(t, b, outcomeFailure))
	assert.Equal(t, stateClosed, b.current(), "3 requests: below min_requests")
	require.NoError(t, call(t, b, outcomeFailure))
	assert.Equal(t, stateOpen, b.current(), "2 of 4 failed")
	assert.ErrorIs(t, call(t, b, outcomeSuccess), store.ErrUnavailable)
	assert.Equal(t, []transition{{stateClosed, stateOpen}}, *seen)
}

func TestBreakerStaysClosedBelowTheRatio(t *testing.T) {
	b, _, _ := testBreaker(t, config.CircuitBreaker{FailureRatio: 0.5, MinRequests: 4})
	for range 10 {
		require.NoError(t, call(t, b, outcomeSuccess))
		require.NoError(t, call(t, b, outcomeSuccess))
		require.NoError(t, call(t, b, outcomeFailure))
	}
	assert.Equal(t, stateClosed, b.current(), "a third of the calls failing is under 0.5")
}

func TestBreakerWindowRolls(t *testing.T) {
	b, clk, _ := testBreaker(t, config.CircuitBreaker{FailureRatio: 0.5, MinRequests: 4, Window: config.Duration(10 * time.Second)})
	for range 3 {
		require.NoError(t, call(t, b, outcomeFailure))
	}
	clk.Advance(11 * time.Second) // those failures leave the window
	require.NoError(t, call(t, b, outcomeFailure))
	assert.Equal(t, stateClosed, b.current(), "only one request in the window")

	for range 3 {
		clk.Advance(time.Second)
		require.NoError(t, call(t, b, outcomeFailure))
	}
	assert.Equal(t, stateOpen, b.current(), "four failures within 10s")
}

func TestBreakerHalfOpen(t *testing.T) {
	const openFor = 30 * time.Second
	b, clk, seen := testBreaker(t, config.CircuitBreaker{MinRequests: 1, OpenFor: config.Duration(openFor), HalfOpenMax: 2})
	require.NoError(t, call(t, b, outcomeFailure))
	require.Equal(t, stateOpen, b.current())

	clk.Advance(openFor - time.Millisecond)
	assert.ErrorIs(t, call(t, b, outcomeSuccess), store.ErrUnavailable)
	clk.Advance(time.Millisecond)
	assert.Equal(t, stateHalfOpen, b.current())

	t1, err := b.allow()
	require.NoError(t, err)
	t2, err := b.allow()
	require.NoError(t, err)
	_, err = b.allow()
	assert.ErrorIs(t, err, store.ErrUnavailable, "half_open_max trials at a time")

	b.done(t1, outcomeSuccess)
	assert.Equal(t, stateHalfOpen, b.current())
	b.done(t2, outcomeSuccess)
	assert.Equal(t, stateClosed, b.current(), "every trial passed")
	assert.Equal(t, []transition{{stateClosed, stateOpen}, {stateOpen, stateHalfOpen}, {stateHalfOpen, stateClosed}}, *seen)

	// Closing starts a fresh window.
	require.NoError(t, call(t, b, outcomeSuccess))
	assert.Equal(t, stateClosed, b.current())
}

func TestBreakerFailedTrialReopens(t *testing.T) {
	const openFor = 30 * time.Second
	b, clk, _ := testBreaker(t, config.CircuitBreaker{MinRequests: 1, OpenFor: config.Duration(openFor), HalfOpenMax: 3})
	require.NoError(t, call(t, b, outcomeFailure))
	clk.Advance(openFor)

	t1, err := b.allow()
	require.NoError(t, err)
	t2, err := b.allow()
	require.NoError(t, err)
	b.done(t1, outcomeFailure)
	assert.Equal(t, stateOpen, b.current(), "one failed trial opens it again")
	b.done(t2, outcomeSuccess) // a trial of the old half-open state: ignored
	assert.Equal(t, stateOpen, b.current())

	clk.Advance(openFor - time.Millisecond)
	assert.ErrorIs(t, call(t, b, outcomeSuccess), store.ErrUnavailable, "open_for restarts from the failed trial")
}

func TestBreakerIgnoredOutcomes(t *testing.T) {
	b, clk, _ := testBreaker(t, config.CircuitBreaker{MinRequests: 1, OpenFor: config.Duration(time.Second), HalfOpenMax: 1})
	for range 5 {
		require.NoError(t, call(t, b, outcomeIgnored))
	}
	assert.Equal(t, stateClosed, b.current(), "a call the caller gave up on is not a failure")

	require.NoError(t, call(t, b, outcomeFailure))
	clk.Advance(time.Second)
	tk, err := b.allow()
	require.NoError(t, err)
	b.done(tk, outcomeIgnored)
	assert.Equal(t, stateHalfOpen, b.current())
	require.NoError(t, call(t, b, outcomeSuccess), "an ignored trial gives its slot back")
	assert.Equal(t, stateClosed, b.current())
}

func TestBreakerDropsStaleOutcomes(t *testing.T) {
	b, _, _ := testBreaker(t, config.CircuitBreaker{FailureRatio: 0.5, MinRequests: 2})
	slow, err := b.allow() // admitted while closed, answers late
	require.NoError(t, err)
	require.NoError(t, call(t, b, outcomeFailure))
	require.NoError(t, call(t, b, outcomeFailure))
	require.Equal(t, stateOpen, b.current())
	b.done(slow, outcomeSuccess)
	assert.Equal(t, stateOpen, b.current())
}

func TestBreakerDisabledIsNil(t *testing.T) {
	b := newBreaker(config.ResolveBreaker(&config.CircuitBreaker{Enabled: config.Ptr(false)}), time.Now, nil)
	assert.Nil(t, b)
	tk, err := b.allow()
	assert.NoError(t, err)
	b.done(tk, outcomeFailure)
	assert.Equal(t, stateClosed, b.current())
}

func TestBreakerDefaults(t *testing.T) {
	b := newBreaker(config.ResolveBreaker(nil), time.Now, nil)
	require.NotNil(t, b, "on by default")
	assert.Equal(t, config.DefaultBreakerRatio, b.ratio)
	assert.Equal(t, config.DefaultBreakerMinReq, b.minRequests)
	assert.Equal(t, config.DefaultBreakerOpen, b.openFor)
	assert.Equal(t, config.DefaultBreakerHalf, b.halfOpenMax)
	assert.Equal(t, config.DefaultBreakerWindow/breakerBuckets, b.width)
}
