package engine

import (
	"sync"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
)

// breakerState is a circuit breaker's state; the values are what
// hikyaku_breaker_state reports.
type breakerState int

const (
	stateClosed   breakerState = 0
	stateHalfOpen breakerState = 1
	stateOpen     breakerState = 2
)

func (s breakerState) String() string {
	switch s {
	case stateHalfOpen:
		return "half_open"
	case stateOpen:
		return "open"
	}
	return "closed"
}

const breakerBuckets = 10

// breaker is a per-endpoint circuit breaker. Closed, it counts requests and
// failures over a rolling window and opens when the failure ratio reaches
// the threshold (with at least min_requests). Open, it refuses everything
// for open_for, then half-opens: up to half_open_max trial requests go
// through; one failure reopens it, half_open_max successes close it.
type breaker struct {
	enabled  bool
	ratio    float64
	minReq   int
	openFor  time.Duration
	halfMax  int
	bucket   time.Duration
	now      func() time.Time
	onChange func(breakerState)

	mu       sync.Mutex
	state    breakerState
	openedAt time.Time
	halfIn   int
	halfOK   int
	buckets  [breakerBuckets]breakerBucket
}

type breakerBucket struct {
	slot      int64
	req, fail int
}

func newBreaker(cfg config.CircuitBreaker, now func() time.Time, onChange func(breakerState)) *breaker {
	if now == nil {
		now = time.Now
	}
	if onChange == nil {
		onChange = func(breakerState) {}
	}
	window := cfg.Window.Std()
	if window <= 0 {
		window = config.DefaultBreakerWindow
	}
	return &breaker{
		enabled:  config.Bool(cfg.Enabled, true),
		ratio:    cfg.FailureRatio,
		minReq:   max(cfg.MinRequests, 1),
		openFor:  cfg.OpenFor.Std(),
		halfMax:  max(cfg.HalfOpenMax, 1),
		bucket:   max(window/breakerBuckets, time.Millisecond),
		now:      now,
		onChange: onChange,
	}
}

func (b *breaker) setState(s breakerState) {
	if b.state == s {
		return
	}
	b.state = s
	b.onChange(s)
}

// allow reports whether a request may go to the endpoint now.
func (b *breaker) allow() bool {
	if b == nil || !b.enabled {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return false
		}
		b.setState(stateHalfOpen)
		b.halfIn, b.halfOK = 0, 0
		fallthrough
	case stateHalfOpen:
		if b.halfIn >= b.halfMax {
			return false
		}
		b.halfIn++
		return true
	}
	return true
}

// record counts the outcome of a request allow let through.
func (b *breaker) record(failure bool) {
	if b == nil || !b.enabled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	switch b.state {
	case stateHalfOpen:
		if failure {
			b.trip(now)
			return
		}
		b.halfOK++
		if b.halfOK >= b.halfMax {
			b.buckets = [breakerBuckets]breakerBucket{}
			b.setState(stateClosed)
		}
		return
	case stateOpen:
		return
	}
	slot := now.UnixNano() / int64(b.bucket)
	bk := &b.buckets[slot%breakerBuckets]
	if bk.slot != slot {
		*bk = breakerBucket{slot: slot}
	}
	bk.req++
	if failure {
		bk.fail++
	}
	req, fail := 0, 0
	for _, x := range b.buckets {
		if slot-x.slot < breakerBuckets {
			req += x.req
			fail += x.fail
		}
	}
	if req >= b.minReq && float64(fail)/float64(req) >= b.ratio {
		b.trip(now)
	}
}

func (b *breaker) trip(now time.Time) {
	b.openedAt = now
	b.halfIn, b.halfOK = 0, 0
	b.setState(stateOpen)
}

// current is the state, with an elapsed open period reported as half-open.
func (b *breaker) current() breakerState {
	if b == nil || !b.enabled {
		return stateClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == stateOpen && b.now().Sub(b.openedAt) >= b.openFor {
		return stateHalfOpen
	}
	return b.state
}
