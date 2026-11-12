package engine

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
)

const (
	// ShadowAttribute is set to true on the copy a shadow destination gets.
	ShadowAttribute = "shadow"
	// ShadowHeader is the header of that copy, for a receiver that wants to
	// tell it from real traffic.
	ShadowHeader = "X-Hikyaku-Shadow"

	// maxShadows is how many shadow copies may be in flight at once. A
	// shadow destination that cannot keep up loses copies; it never slows
	// the relay down.
	maxShadows = 256

	shadowDelivered = "delivered"
	shadowFailed    = "failed"
	shadowDropped   = "dropped"
)

// shadower sends the copies of shadow destinations (shadow: true). A copy
// is delivered on the side, through the destination's own retries and
// fallbacks. Nothing waits for it, and its failure is counted and logged,
// never dead-lettered.
type shadower struct {
	e      *Engine
	slots  chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	stopped bool
}

func newShadower(e *Engine) *shadower {
	ctx, cancel := context.WithCancel(context.Background())
	return &shadower{e: e, slots: make(chan struct{}, maxShadows), ctx: ctx, cancel: cancel}
}

// split takes the shadow destinations out of a route's decision.
func (e *Engine) splitShadows(names []string) (deliver, shadows []string) {
	for _, n := range names {
		if d := e.dests[n]; d != nil && d.shadow {
			shadows = append(shadows, n)
		}
	}
	if len(shadows) == 0 {
		return names, nil
	}
	for _, n := range names {
		if d := e.dests[n]; d == nil || !d.shadow {
			deliver = append(deliver, n)
		}
	}
	return deliver, shadows
}

// sampled reports whether a message is in a destination's sample. The
// message's ID decides, not chance: a retried or replayed message gets the
// same answer.
func sampled(id, destination string, rate float64) bool {
	if rate >= 1 {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(destination))
	return float64(h.Sum64()>>11)/float64(1<<53) < rate
}

// send copies env to each shadow destination whose sample it is in, and
// returns those destinations. It does not wait for the deliveries.
func (s *shadower) send(names []string, env *envelope.Envelope) []string {
	var sent []string
	for _, name := range names {
		d := s.e.dests[name]
		if d == nil || !sampled(env.ID, name, d.sample) {
			continue
		}
		sent = append(sent, name)
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			s.count(name, shadowDropped)
			continue
		}
		select {
		case s.slots <- struct{}{}:
			s.wg.Add(1)
			s.mu.Unlock()
		default:
			s.mu.Unlock()
			s.count(name, shadowDropped)
			s.e.log.Warn("shadow copy dropped: too many are in flight", "destination", name, "id", env.ID)
			continue
		}
		cp := env.Clone()
		cp.SetAttribute(ShadowAttribute, true)
		cp.SetHeader(ShadowHeader, "true")
		go func() {
			defer func() { <-s.slots; s.wg.Done() }()
			var rep DeliveryReport
			if b := s.e.batchers[name]; b != nil {
				rep = b.submit(s.ctx, cp)
			} else {
				rep = s.e.deliver(s.ctx, name, cp)
			}
			if rep.OK() {
				s.count(name, shadowDelivered)
				return
			}
			s.count(name, shadowFailed)
			s.e.log.Warn("shadow delivery failed", "destination", name, "id", cp.ID, "err", rep.Error)
		}()
	}
	return sent
}

func (s *shadower) count(destination, outcome string) {
	if s.e.m != nil {
		s.e.m.Shadow.WithLabelValues(destination, outcome).Inc()
	}
}

// stop gives the copies in flight a moment to finish, then ends them.
func (s *shadower) stop(wait time.Duration) {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(wait):
	}
	s.cancel()
	<-done
}
