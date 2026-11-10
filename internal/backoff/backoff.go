// Package backoff is the exponential backoff with jitter shared by delivery
// retries and source reconnects.
package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Base is the wait after failure n (1-based): base doubled per failure,
// capped at max.
func Base(base, maxWait time.Duration, n int) time.Duration {
	d := base
	if d <= 0 {
		return 0
	}
	if maxWait < d {
		maxWait = d
	}
	for i := 1; i < n; i++ {
		if d >= maxWait/2 {
			return maxWait
		}
		d *= 2
	}
	return min(d, maxWait)
}

// Jittered applies a jitter mode to a wait: full (default) picks in
// [0, d], equal in [d/2, d], none keeps d. rnd returns [0, 1).
func Jittered(d time.Duration, jitter string, rnd func() float64) time.Duration {
	if rnd == nil {
		rnd = rand.Float64
	}
	switch jitter {
	case "none":
		return d
	case "equal":
		return d/2 + time.Duration(rnd()*float64(d/2))
	default:
		return time.Duration(rnd() * float64(d))
	}
}

// Delay is Jittered(Base(...)).
func Delay(base, maxWait time.Duration, jitter string, n int, rnd func() float64) time.Duration {
	return Jittered(Base(base, maxWait, n), jitter, rnd)
}

// Sleep waits d or until ctx ends; false when ctx ended first.
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
