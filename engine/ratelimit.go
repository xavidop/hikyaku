package engine

import (
	"context"
	"errors"
	"math"
	"time"

	"golang.org/x/time/rate"

	"github.com/xavidop/hikyaku/internal/config"
)

// Destination rate limits (destination rate_limit:). A destination has one
// token bucket, shared by the members of a group. Every attempt takes a
// token first, retries and hedges included, and waits for one when the
// bucket is empty. The wait holds the worker, so a destination slower than
// its traffic fills the buffer and the sources see backpressure, as they
// do with a slow endpoint.
//
// The bucket is this relay's own: two replicas with the same limit send
// twice as much.

// errRateLimitDeadline is the failure of an attempt whose turn would come
// after the destination's deadline: the chain moves on at once.
var errRateLimitDeadline = errors.New("rate limit: the next free slot is past the deadline")

func newLimiter(rl *config.RateLimit) *rate.Limiter {
	if rl == nil || rl.PerSecond <= 0 {
		return nil
	}
	burst := rl.Burst
	if burst <= 0 {
		burst = max(1, int(math.Ceil(rl.PerSecond)))
	}
	return rate.NewLimiter(rate.Limit(rl.PerSecond), burst)
}

// throttle waits for the destination's next token. It fails when ctx ends
// first, or at once when the token would come after ctx's deadline.
func (e *Engine) throttle(ctx context.Context, d *destination) error {
	if d.limiter == nil {
		return nil
	}
	r := d.limiter.Reserve()
	wait := r.Delay()
	if wait <= 0 {
		return nil
	}
	if dl, ok := ctx.Deadline(); ok && time.Now().Add(wait).After(dl) {
		r.Cancel()
		return errRateLimitDeadline
	}
	if e.m != nil {
		e.m.RateLimitWait.WithLabelValues(d.name).Observe(wait.Seconds())
	}
	if !sleep(ctx, wait) {
		r.Cancel()
		return deadlineErr(ctx.Err())
	}
	return nil
}
