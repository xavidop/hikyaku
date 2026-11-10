package engine

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

// budgetWindow is the retry budget's rolling window.
const budgetWindow = 10 * time.Second

// retryBudget caps retries as a fraction of traffic, so an outage does not
// multiply the load on a struggling endpoint (design §7.2): within the
// window, retries may not exceed ratio × first attempts + min_per_second ×
// the window's seconds. With a shared store the counters span replicas.
type retryBudget struct {
	enabled   bool
	ratio     float64
	minPerSec float64
	now       func() time.Time

	shared store.KV
	key    string // the destination, in shared counter keys

	mu       sync.Mutex
	slots    [10]budgetSlot
	sharedRq int64
}

type budgetSlot struct {
	sec          int64
	req, retries int
}

func newRetryBudget(cfg config.RetryBudget, dest string, shared store.KV, now func() time.Time) *retryBudget {
	if now == nil {
		now = time.Now
	}
	return &retryBudget{
		enabled:   config.Bool(cfg.Enabled, true),
		ratio:     cfg.Ratio,
		minPerSec: cfg.MinPerSecond,
		now:       now,
		shared:    shared,
		key:       dest,
	}
}

func (b *retryBudget) slot(now time.Time) *budgetSlot {
	sec := now.Unix()
	s := &b.slots[sec%int64(len(b.slots))]
	if s.sec != sec {
		*s = budgetSlot{sec: sec}
	}
	return s
}

func (b *retryBudget) totals(now time.Time) (req, retries int) {
	sec := now.Unix()
	for _, s := range b.slots {
		if sec-s.sec < int64(len(b.slots)) {
			req += s.req
			retries += s.retries
		}
	}
	return req, retries
}

func (b *retryBudget) allowed(req int) float64 {
	return b.ratio*float64(req) + b.minPerSec*budgetWindow.Seconds()
}

func (b *retryBudget) epochKey(kind string, now time.Time) string {
	return "budget:" + b.key + ":" + kind + ":" + strconv.FormatInt(now.Unix()/int64(budgetWindow.Seconds()), 10)
}

// request counts a first attempt.
func (b *retryBudget) request(ctx context.Context) {
	if b == nil || !b.enabled {
		return
	}
	now := b.now()
	b.mu.Lock()
	b.slot(now).req++
	b.mu.Unlock()
	if b.shared != nil {
		if n, err := b.shared.Incr(ctx, b.epochKey("req", now), 2*budgetWindow); err == nil {
			b.mu.Lock()
			b.sharedRq = n
			b.mu.Unlock()
		}
	}
}

// allowRetry spends one retry if the budget has one.
func (b *retryBudget) allowRetry(ctx context.Context) bool {
	if b == nil || !b.enabled {
		return true
	}
	now := b.now()
	if b.shared != nil {
		n, err := b.shared.Incr(ctx, b.epochKey("retry", now), 2*budgetWindow)
		if err == nil {
			b.mu.Lock()
			req := b.sharedRq
			b.mu.Unlock()
			return float64(n) <= b.allowed(int(req))
		}
		// The store is down: budget locally.
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	req, retries := b.totals(now)
	if float64(retries+1) > b.allowed(req) {
		return false
	}
	b.slot(now).retries++
	return true
}

// remaining is how many retries the window still allows (this replica's
// view).
func (b *retryBudget) remaining() float64 {
	if b == nil || !b.enabled {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	req, retries := b.totals(b.now())
	return max(b.allowed(req)-float64(retries), 0)
}
