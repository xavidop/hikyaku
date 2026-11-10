package redis

import (
	"errors"
	"time"

	"github.com/xavidop/hikyaku/internal/backoff"
)

var (
	errStopped = errors.New("stopped")
	errClosed  = errors.New("closed")
)

// retrier is a source's reconnect state. A failure right after a success
// is a dropped connection and does not count: the reconnect that follows
// is the attempt that counts. Failures in a row before any success do, and
// the source gives up after max_attempts of them (0: never).
type retrier struct {
	rc       reconnectOptions
	rnd      func() float64
	failures int  // failed attempts in a row
	up       bool // the last attempt succeeded
}

// success records a working connection.
func (r *retrier) success() { r.failures, r.up = 0, true }

// failure records a failed attempt; false means the source must give up.
func (r *retrier) failure() bool {
	if r.up {
		r.up = false
		return true
	}
	r.failures++
	return r.rc.maxAttempts == 0 || r.failures < r.rc.maxAttempts
}

// wait is the backoff before the next attempt.
func (r *retrier) wait() time.Duration {
	return backoff.Delay(r.rc.backoff, r.rc.maxBackoff, r.rc.jitter, max(r.failures, 1), r.rnd)
}
