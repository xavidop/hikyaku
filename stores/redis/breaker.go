package redis

import (
	"sync"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

// breakerState is where a breaker stands.
type breakerState int

const (
	stateClosed   breakerState = iota // calls go through; outcomes are counted
	stateOpen                         // calls fail fast with store.ErrUnavailable
	stateHalfOpen                     // a few trial calls go through
)

func (s breakerState) String() string {
	switch s {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// outcome is what a call admitted by the breaker tells it.
type outcome int

const (
	outcomeSuccess outcome = iota // Redis answered (a miss counts as an answer)
	outcomeFailure                // Redis failed or was too slow
	outcomeIgnored                // the caller gave up first: nothing learned
)

// breakerBuckets is how many buckets the rolling window is split into.
const breakerBuckets = 10

// bucket counts the outcomes of one slice of the rolling window.
type bucket struct {
	slot     int64 // which slice of time since the breaker's epoch
	requests int
	failures int
}

// ticket is a call the breaker let through.
type ticket struct {
	gen uint64 // the state generation it was admitted under
}

// breaker keeps a dead Redis from being hammered. Closed, it counts outcomes
// over a rolling window and opens once failures reach failure_ratio of at least
// min_requests calls. Open, it rejects every call for open_for. Half-open, it
// lets half_open_max trial calls through: one failure opens it again, and
// half_open_max successes close it. A nil *breaker lets everything through.
type breaker struct {
	ratio       float64
	minRequests int
	openFor     time.Duration
	halfOpenMax int
	width       time.Duration // window / breakerBuckets
	now         func() time.Time
	onChange    func(from, to breakerState)

	mu       sync.Mutex
	epoch    time.Time
	state    breakerState
	gen      uint64 // bumped on every transition; outcomes of older calls are dropped
	openedAt time.Time
	buckets  [breakerBuckets]bucket
	trials   int // half-open: calls let through
	passed   int // half-open: trials that succeeded
}

// newBreaker builds a closed breaker from resolved settings; it returns nil
// when the breaker is disabled.
func newBreaker(cfg config.CircuitBreaker, now func() time.Time, onChange func(from, to breakerState)) *breaker {
	if !config.Bool(cfg.Enabled, true) {
		return nil
	}
	width := cfg.Window.Std() / breakerBuckets
	if width <= 0 {
		width = time.Nanosecond
	}
	return &breaker{
		ratio:       cfg.FailureRatio,
		minRequests: max(cfg.MinRequests, 1),
		openFor:     cfg.OpenFor.Std(),
		halfOpenMax: max(cfg.HalfOpenMax, 1),
		width:       width,
		now:         now,
		onChange:    onChange,
		epoch:       now(),
	}
}

// allow admits a call, or returns store.ErrUnavailable while the breaker is
// open (or half-open with every trial slot taken). An admitted call must be
// reported with done.
func (b *breaker) allow() (ticket, error) {
	if b == nil {
		return ticket{}, nil
	}
	b.mu.Lock()
	from := b.state
	t, err := b.allowLocked(b.now())
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
	return t, err
}

func (b *breaker) allowLocked(now time.Time) (ticket, error) {
	if b.state == stateOpen {
		if now.Sub(b.openedAt) < b.openFor {
			return ticket{}, store.ErrUnavailable
		}
		b.setState(stateHalfOpen, now)
	}
	if b.state == stateHalfOpen {
		if b.trials >= b.halfOpenMax {
			return ticket{}, store.ErrUnavailable
		}
		b.trials++
	}
	return ticket{gen: b.gen}, nil
}

// done reports the outcome of a call allow admitted.
func (b *breaker) done(t ticket, o outcome) {
	if b == nil {
		return
	}
	b.mu.Lock()
	from := b.state
	b.doneLocked(t, o, b.now())
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

func (b *breaker) doneLocked(t ticket, o outcome, now time.Time) {
	if t.gen != b.gen {
		return // admitted before the last transition: stale
	}
	switch b.state {
	case stateClosed:
		if o == outcomeIgnored {
			return
		}
		bk := b.bucket(now)
		bk.requests++
		if o != outcomeFailure {
			return
		}
		bk.failures++
		requests, failures := b.totals(now)
		if requests >= b.minRequests && float64(failures) >= b.ratio*float64(requests) {
			b.setState(stateOpen, now)
		}
	case stateHalfOpen:
		switch o {
		case outcomeFailure:
			b.setState(stateOpen, now)
		case outcomeIgnored:
			b.trials-- // give the slot back
		default:
			b.passed++
			if b.passed >= b.halfOpenMax {
				b.setState(stateClosed, now)
			}
		}
	}
}

// current returns where the breaker stands (closed for a nil breaker).
func (b *breaker) current() breakerState {
	if b == nil {
		return stateClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == stateOpen && b.now().Sub(b.openedAt) >= b.openFor {
		return stateHalfOpen // what the next call will find
	}
	return b.state
}

func (b *breaker) setState(s breakerState, now time.Time) {
	b.state = s
	b.gen++
	b.trials, b.passed = 0, 0
	switch s {
	case stateOpen:
		b.openedAt = now
	case stateClosed:
		b.buckets = [breakerBuckets]bucket{}
		b.epoch = now
	}
}

// bucket returns the bucket of now's slice of time, emptied if it last held
// an older slice.
func (b *breaker) bucket(now time.Time) *bucket {
	slot := b.slot(now)
	bk := &b.buckets[int(((slot%breakerBuckets)+breakerBuckets)%breakerBuckets)]
	if bk.slot != slot {
		*bk = bucket{slot: slot}
	}
	return bk
}

// totals sums the buckets still inside the rolling window.
func (b *breaker) totals(now time.Time) (requests, failures int) {
	cur := b.slot(now)
	for _, bk := range b.buckets {
		if bk.slot <= cur && bk.slot > cur-breakerBuckets {
			requests += bk.requests
			failures += bk.failures
		}
	}
	return requests, failures
}

func (b *breaker) slot(now time.Time) int64 {
	return int64(now.Sub(b.epoch) / b.width)
}

func (b *breaker) notify(from, to breakerState) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}
