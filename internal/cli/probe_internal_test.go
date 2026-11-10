package cli

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/redact"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
)

// probeAI is an AI factory whose models are up or down on demand.
type probeAI struct {
	mu    sync.Mutex
	calls int
	down  map[string]bool
	gate  chan struct{} // when set, Health waits for it
}

func (p *probeAI) NewRouter(context.Context, router.AIRequest) (plugin.Router, error) {
	return plugin.RouterFunc(func(context.Context, *envelope.Envelope) (plugin.Decision, error) {
		return plugin.Decision{}, nil
	}), nil
}

func (p *probeAI) Health(_ context.Context, models []string) map[string]error {
	p.mu.Lock()
	gate := p.gate
	p.mu.Unlock()
	if gate != nil {
		<-gate
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	out := map[string]error{}
	for _, m := range models {
		out[m] = nil
		if p.down[m] {
			out[m] = errors.New(m + " is down")
		}
	}
	return out
}

func (p *probeAI) set(f func(p *probeAI)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

func (p *probeAI) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// readiness.models: a route is down only when every model of its chain
// is; the first probe is waited for, and afterwards /readyz answers at once
// from the last probe while a stale one is refreshed in the background.
func TestModelHealthProbe(t *testing.T) {
	ctx := context.Background()
	l, err := config.LoadBytes(ctx, "s.yaml", []byte(`
version: 1
name: probe
sources: [{ name: in, type: stdin }]
destinations: [{ name: a, type: fault }]
routes:
  - name: triage
    from: [in]
    router:
      type: ai
      model: fake/one
      fallback_models: [fake/two]
      candidates: [{ name: a, description: anything }]
      fallback: [{ type: static, to: [a] }]
`), config.LoadOptions{})
	require.NoError(t, err)
	ai := &probeAI{down: map[string]bool{"fake/one": true}}
	e, err := engine.New(ctx, l.Spec, engine.Options{AI: ai, NoSources: true})
	require.NoError(t, err)
	defer e.Close(ctx)

	var mu sync.Mutex
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); defer mu.Unlock(); now = now.Add(d) }
	r := &relay{a: &app{deps: Deps{Now: clock}, redactor: redact.New()}, ai: ai, current: e}

	assert.NoError(t, r.modelHealth(ctx), "fake/one is down, but its fallback model answers")
	assert.Equal(t, 1, ai.count())
	assert.NoError(t, r.modelHealth(ctx))
	assert.Equal(t, 1, ai.count(), "a fresh probe is reused")

	gate := make(chan struct{})
	ai.set(func(p *probeAI) { p.down["fake/two"] = true; p.gate = gate })
	advance(modelProbeTTL)
	start := time.Now()
	assert.NoError(t, r.modelHealth(ctx), "a stale probe answers at once with the last result")
	assert.Less(t, time.Since(start), time.Second, "without waiting for the models")
	close(gate)
	require.Eventually(t, func() bool { return r.modelHealth(ctx) != nil }, 5*time.Second, 5*time.Millisecond, "the background refresh lands")
	assert.ErrorContains(t, r.modelHealth(ctx), "route triage: every model failed (fake/one: fake/one is down; fake/two: fake/two is down)")
	assert.Equal(t, 2, ai.count(), "one refresh, however many calls asked meanwhile")
}
