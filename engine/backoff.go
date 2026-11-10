package engine

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// retryPolicy is a resolved config.Retry.
type retryPolicy struct {
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
	jitter     string
	retryOn    []string
	neverOn    []string
	respectRA  bool
}

func newRetryPolicy(r config.Retry) retryPolicy {
	return retryPolicy{
		attempts:   max(r.Attempts, 1),
		backoff:    r.Backoff.Std(),
		maxBackoff: r.MaxBackoff.Std(),
		jitter:     r.Jitter,
		retryOn:    lower(r.RetryOn),
		neverOn:    lower(r.NeverOn),
		respectRA:  config.Bool(r.RespectRetryAfter, true),
	}
}

func lower(tags []string) []string {
	out := make([]string, len(tags))
	for i, t := range tags {
		out[i] = strings.ToLower(strings.TrimSpace(t))
	}
	return out
}

// base is the exponential wait before attempt n+1, after attempt n failed:
// backoff doubled per try, capped at maxBackoff.
func (p retryPolicy) base(n int) time.Duration { return backoff.Base(p.backoff, p.maxBackoff, n) }

// wait applies the jitter to base(n). rnd returns a value in [0, 1).
func (p retryPolicy) wait(n int, rnd func() float64) time.Duration {
	return backoff.Jittered(p.base(n), p.jitter, rnd)
}

// waitFor is the wait before the next attempt: the endpoint's Retry-After
// hint when there is one and the policy honours it (capped at max_backoff),
// else the jittered backoff.
func (p retryPolicy) waitFor(n int, retryAfter time.Duration, rnd func() float64) time.Duration {
	if p.respectRA && retryAfter > 0 {
		return min(retryAfter, p.maxBackoff)
	}
	return p.wait(n, rnd)
}

// retryable decides whether a failure is retried. Tags are matched against
// retry_on and never_on, and the more specific match wins: an exact code or
// a class (429, 408, timeout, connect, ws:close_1011) beats a range (4xx,
// 5xx). That is how the default "never 4xx, except 408 and 429" works.
// A failure whose tags match neither list (or that has none, such as a
// protocol-specific kafka:NOT_LEADER_FOR_PARTITION nobody listed) falls back
// to its class: it is retried unless it is permanent.
func (p retryPolicy) retryable(class plugin.Class, tags []string) bool {
	return plugin.Retryable(class, tags, p.retryOn, p.neverOn)
}

func defaultRand() float64 { return rand.Float64() }

// sleep waits d or until ctx ends; false when ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool { return backoff.Sleep(ctx, d) }
