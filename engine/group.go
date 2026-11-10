package engine

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// group is a destination with several members and a strategy.
type group struct {
	strategy string
	members  []*endpoint
	hc       *config.HealthCheck
	hedge    *config.Hedge

	mu      sync.Mutex
	rr      int
	current []int // smooth weighted round robin state
}

func newGroup(d config.Destination, members []*endpoint) *group {
	g := &group{strategy: d.Strategy, members: members, hc: d.HealthCheck, hedge: d.Hedge, current: make([]int, len(members))}
	if g.strategy == "" {
		g.strategy = "failover"
	}
	for _, m := range members {
		m.healthy.Store(true)
		if m.weight <= 0 {
			m.weight = 1
		}
	}
	return g
}

// order is the members to try for one message, best first. Healthy members
// with a closed breaker come first, in strategy order; members known to be
// down follow as a last resort, so a stale probe never fails a message on
// its own.
func (g *group) order() []*endpoint {
	var first []*endpoint
	switch g.strategy {
	case "round_robin":
		g.mu.Lock()
		start := g.rr % len(g.members)
		g.rr++
		g.mu.Unlock()
		first = append(slices.Clone(g.members[start:]), g.members[:start]...)
	case "weighted":
		first = g.weighted()
	case "least_latency":
		first = slices.Clone(g.members)
		sort.SliceStable(first, func(i, j int) bool { return first[i].latency.value() < first[j].latency.value() })
	default: // failover
		first = slices.Clone(g.members)
	}
	var up, down []*endpoint
	for _, m := range first {
		if m.healthy.Load() && m.breaker.current() != stateOpen {
			up = append(up, m)
		} else {
			down = append(down, m)
		}
	}
	return append(up, down...)
}

// weighted picks the first member by smooth weighted round robin (nginx's),
// then orders the rest by weight for failover.
func (g *group) weighted() []*endpoint {
	g.mu.Lock()
	total, best := 0, 0
	for i, m := range g.members {
		g.current[i] += m.weight
		total += m.weight
		if g.current[i] > g.current[best] {
			best = i
		}
	}
	g.current[best] -= total
	g.mu.Unlock()
	rest := make([]*endpoint, 0, len(g.members)-1)
	for i, m := range g.members {
		if i != best {
			rest = append(rest, m)
		}
	}
	sort.SliceStable(rest, func(i, j int) bool { return rest[i].weight > rest[j].weight })
	return append([]*endpoint{g.members[best]}, rest...)
}

// tryGroup delivers to a group: members in order, each with the retry
// policy, until one takes it. A permanent failure stops the group (another
// replica would refuse the same message).
func (e *Engine) tryGroup(ctx context.Context, d *destination, env *envelope.Envelope) attemptResult {
	order := d.group.order()
	if d.group.hedge != nil && len(order) > 1 {
		return e.hedged(ctx, d, order, env)
	}
	var last attemptResult
	total := 0
	for _, m := range order {
		r := e.tryEndpoint(ctx, d, m, env)
		total += r.attempts
		r.attempts = total
		if r.err == nil || r.outcome == outcomePermanent || r.outcome == outcomeDeadline {
			return r
		}
		last = r
	}
	return last
}

// hedged races members: the first starts at once, another one each time
// hedge.after passes with no answer (up to hedge.max extra), and the first
// success wins and cancels the rest. Only idempotent destinations hedge
// (validated), because a hedged message may arrive twice.
func (e *Engine) hedged(ctx context.Context, d *destination, order []*endpoint, env *envelope.Envelope) attemptResult {
	maxExtra := d.group.hedge.Max
	if maxExtra <= 0 {
		maxExtra = 1
	}
	racers := min(len(order), 1+maxExtra)
	hctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan attemptResult, racers)
	start := func(m *endpoint) {
		menv := env.Clone()
		go func() { results <- e.tryEndpoint(hctx, d, m, menv) }()
	}
	start(order[0])
	started, done := 1, 0
	timer := time.NewTimer(d.group.hedge.After.Std())
	defer timer.Stop()
	var last attemptResult
	total := 0
	for done < started {
		select {
		case r := <-results:
			done++
			total += r.attempts
			if r.err == nil {
				r.attempts = total
				return r
			}
			last = r
			if started < racers && done == started {
				// Every racer failed: start the next one now.
				start(order[started])
				started++
				if e.m != nil {
					e.m.Hedges.WithLabelValues(d.name).Inc()
				}
			}
		case <-timer.C:
			if started < racers {
				start(order[started])
				started++
				if e.m != nil {
					e.m.Hedges.WithLabelValues(d.name).Inc()
				}
				timer.Reset(d.group.hedge.After.Std())
			}
		case <-ctx.Done():
			last.attempts = total
			last.outcome, last.err = outcomeDeadline, deadlineErr(ctx.Err())
			return last
		}
	}
	// Racers left that never started fail over in order.
	for _, m := range order[started:] {
		r := e.tryEndpoint(ctx, d, m, env)
		total += r.attempts
		if r.err == nil {
			r.attempts = total
			return r
		}
		last = r
	}
	last.attempts = total
	return last
}

// healthLoop probes a group's members every interval until ctx ends.
func (e *Engine) healthLoop(ctx context.Context, d *destination) {
	hc := d.group.hc
	interval := hc.Interval.Std()
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		e.probe(ctx, d)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (e *Engine) probe(ctx context.Context, d *destination) {
	hc := d.group.hc
	timeout := hc.Timeout.Std()
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	unhealthyAfter := max(hc.UnhealthyAfter, 1)
	if hc.UnhealthyAfter == 0 {
		unhealthyAfter = 3
	}
	healthyAfter := max(hc.HealthyAfter, 1)
	for _, m := range d.group.members {
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := e.probeOne(pctx, m, hc.Path)
		cancel()
		if ctx.Err() != nil {
			return
		}
		m.hmu.Lock()
		if err != nil {
			m.fails++
			m.oks = 0
			if m.healthy.Load() && m.fails >= unhealthyAfter {
				m.healthy.Store(false)
				e.log.Warn("group member unhealthy", "destination", d.name, "member", m.name, "err", err)
			}
		} else {
			m.oks++
			m.fails = 0
			if !m.healthy.Load() && m.oks >= healthyAfter {
				m.healthy.Store(true)
				e.log.Info("group member healthy again", "destination", d.name, "member", m.name)
			}
		}
		healthy := m.healthy.Load()
		m.hmu.Unlock()
		if e.m != nil {
			v := 0.0
			if healthy {
				v = 1
			}
			e.m.MemberHealthy.WithLabelValues(d.name, m.name).Set(v)
		}
	}
}

// probeOne checks one member: GET origin+path for an http member with a
// health_check path, the sink's own Health otherwise.
func (e *Engine) probeOne(ctx context.Context, m *endpoint, path string) error {
	if path == "" || m.origin == "" {
		return m.sink.Health(ctx)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(m.origin, "/")+"/"+strings.TrimPrefix(path, "/"), nil)
	if err != nil {
		return err
	}
	resp, err := e.probeClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return plugin.StatusError(resp.StatusCode, 0, "health check")
	}
	return nil
}

// originOf returns scheme://host of an http(s) URL without templates.
func originOf(e config.Endpoint) string {
	if (e.Type != "http" && e.Type != "https") || strings.Contains(e.URL, "{{") {
		return ""
	}
	u, err := url.Parse(e.URL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
