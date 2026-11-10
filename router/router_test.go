package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

func msg(body string) *envelope.Envelope { return envelope.New("webhooks", "http", []byte(body)) }

func TestStatic(t *testing.T) {
	d, err := NewStatic([]string{"a", "b"}).Route(context.Background(), msg("{}"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, d.Destinations)
	assert.Equal(t, "static", d.Router)
}

func TestRules(t *testing.T) {
	spec := config.RouterSpec{Type: "rules", Rules: []config.Rule{
		{When: `msg.json.type.startsWith("invoice.")`, To: []string{"billing"}},
		{When: `msg.json.priority > 3`, To: []string{"support", "billing"}},
	}}
	r, err := NewRules(spec)
	require.NoError(t, err)
	ctx := context.Background()

	d, err := r.Route(ctx, msg(`{"type":"invoice.paid","priority":5}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations, "first match wins by default")

	d, err = r.Route(ctx, msg(`{"type":"bug","priority":5}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support", "billing"}, d.Destinations)

	_, err = r.Route(ctx, msg(`{"type":"bug"}`))
	assert.ErrorIs(t, err, ErrNoMatch, "missing keys do not match")

	spec.Match = "all"
	spec.Default = []string{"catchall"}
	r, err = NewRules(spec)
	require.NoError(t, err)
	d, err = r.Route(ctx, msg(`{"type":"invoice.paid","priority":5}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing", "support"}, d.Destinations, "every match, each destination once")
	d, err = r.Route(ctx, msg(`not json`))
	require.NoError(t, err)
	assert.Equal(t, []string{"catchall"}, d.Destinations)

	_, err = NewRules(config.RouterSpec{Rules: []config.Rule{{When: "msg.((", To: []string{"x"}}}})
	assert.ErrorContains(t, err, "rules[0].when")
}

type fakeRouter struct {
	d   plugin.Decision
	err error
	n   int
}

func (f *fakeRouter) Route(context.Context, *envelope.Envelope) (plugin.Decision, error) {
	f.n++
	return f.d, f.err
}

func TestChainFallsBackInOrder(t *testing.T) {
	m := metrics.New()
	ai := &fakeRouter{err: errors.New("model unavailable")}
	rulesR := &fakeRouter{err: ErrNoMatch}
	static := &fakeRouter{d: plugin.Decision{Destinations: []string{"support"}, Router: "static"}}
	c := NewChain("triage", []Step{{Kind: "ai", Router: ai}, {Kind: "rules", Router: rulesR}, {Kind: "static", Router: static}}, m, nil)
	d, err := c.Route(context.Background(), msg("{}"))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Equal(t, []string{"ai: model unavailable", "rules: no rule matched"}, d.Hops)
	assert.Equal(t, 1.0, testutil.ToFloat64(m.RouterFallback.WithLabelValues("triage", "static")))
}

func TestChainShadowAndLowConfidence(t *testing.T) {
	shadow := &fakeRouter{d: plugin.Decision{Destinations: []string{"billing"}, Shadow: true, Model: "m", Attributes: map[string]any{"ai.decision": "x"}}}
	low := &fakeRouter{d: plugin.Decision{Destinations: []string{"support"}, LowConfidence: true}}
	rulesR := &fakeRouter{d: plugin.Decision{Destinations: []string{"archive"}, Router: "rules"}}
	c := NewChain("r", []Step{{Kind: "ai", Router: shadow}, {Kind: "ai", Router: low, FallbackOnLow: true}, {Kind: "rules", Router: rulesR}}, nil, nil)
	d, err := c.Route(context.Background(), msg("{}"))
	require.NoError(t, err)
	assert.Equal(t, []string{"archive"}, d.Destinations, "shadow decisions are logged, the chain delivers")
	sh, ok := d.Attributes["ai.shadow"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []string{"billing"}, sh["destinations"])
	assert.Equal(t, "x", sh["decision"])
	assert.Contains(t, d.Hops, "ai: low confidence")

	// Without FallbackOnLow, a low-confidence decision is the answer.
	c = NewChain("r", []Step{{Kind: "ai", Router: low}, {Kind: "rules", Router: rulesR}}, nil, nil)
	d, err = c.Route(context.Background(), msg("{}"))
	require.NoError(t, err)
	assert.True(t, d.LowConfidence)
	assert.Equal(t, []string{"support"}, d.Destinations)
}

func TestChainAllFail(t *testing.T) {
	c := NewChain("r", []Step{
		{Kind: "ai", Router: &fakeRouter{err: errors.New("down")}},
		{Kind: "static", Router: &fakeRouter{d: plugin.Decision{}}},
	}, nil, nil)
	d, err := c.Route(context.Background(), msg("{}"))
	assert.ErrorIs(t, err, ErrNoRoute)
	assert.ErrorContains(t, err, "ai: down; static: chose no destination")
	assert.Len(t, d.Hops, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	next := &fakeRouter{d: plugin.Decision{Destinations: []string{"x"}}}
	_, err = NewChain("r", []Step{{Kind: "static", Router: next}}, nil, nil).Route(ctx, msg("{}"))
	assert.ErrorIs(t, err, ErrNoRoute)
	assert.Zero(t, next.n, "a cancelled context stops the chain")
}

func TestStickyTarget(t *testing.T) {
	delivered := store.CallRecord{Outcome: store.OutcomeDelivered, Decision: []store.Answer{{Candidate: "support", Chosen: true}}, DeliveredTo: []string{"support-b"}}
	to, ok := StickyTarget([]store.CallRecord{delivered})
	assert.True(t, ok)
	assert.Equal(t, []string{"support"}, to, "chosen candidates, not the fallback endpoint that served")

	corrected := delivered
	corrected.Feedback = &store.Feedback{Correct: []string{"billing"}}
	to, _ = StickyTarget([]store.CallRecord{delivered, corrected})
	assert.Equal(t, []string{"billing"}, to, "a correction wins")

	failed := store.CallRecord{Outcome: store.OutcomeDLQ, Decision: delivered.Decision}
	_, ok = StickyTarget([]store.CallRecord{delivered, failed})
	assert.False(t, ok, "only the newest call counts, and it must have been delivered")
	_, ok = StickyTarget(nil)
	assert.False(t, ok)

	sticky := store.CallRecord{Outcome: store.OutcomeFallback, DeliveredTo: []string{"x"}}
	to, ok = StickyTarget([]store.CallRecord{sticky})
	assert.True(t, ok)
	assert.Equal(t, []string{"x"}, to)
}

func TestStickyRouter(t *testing.T) {
	mem := store.NewMemory("m")
	ctx := context.Background()
	require.NoError(t, mem.Append(ctx, "triage", "cust-1", store.CallRecord{MsgID: "1", At: time.Now(), Outcome: store.OutcomeDelivered, Decision: []store.Answer{{Candidate: "billing", Chosen: true}}}, store.AppendOptions{TTL: time.Hour}))
	r, err := NewSticky("triage", mem, config.History{Key: "{{ .json.customer }}"})
	require.NoError(t, err)

	d, err := r.Route(ctx, msg(`{"customer":"cust-1"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, "sticky", d.Router)

	_, err = r.Route(ctx, msg(`{"customer":"cust-2"}`))
	assert.ErrorIs(t, err, ErrNoHistory)
	_, err = r.Route(ctx, msg(`{}`))
	assert.ErrorIs(t, err, ErrNoHistory)

	mem.SetError(errors.New("redis down"))
	_, err = r.Route(ctx, msg(`{"customer":"cust-1"}`))
	assert.ErrorContains(t, err, "redis down")
}

func TestBuild(t *testing.T) {
	spec := &config.Spec{}
	mem := store.NewMemory("default")
	deps := Deps{Spec: spec, Relay: "r", Store: func(string) (store.Store, error) { return mem, nil }}
	route := config.Route{Name: "triage", Router: &config.RouterSpec{
		Type:  "rules",
		Rules: []config.Rule{{When: `msg.json.type == "invoice"`, To: []string{"billing"}}},
		Fallback: []config.RouterSpec{
			{Type: "sticky", History: &config.History{Key: "{{ .json.customer }}"}},
			{Type: "static", To: []string{"support"}},
		},
	}}
	r, err := Build(context.Background(), route, deps)
	require.NoError(t, err)
	d, err := r.Route(context.Background(), msg(`{"type":"bug"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Len(t, d.Hops, 2)

	_, err = Build(context.Background(), config.Route{Name: "x", Router: &config.RouterSpec{Type: "ai"}}, deps)
	assert.ErrorIs(t, err, ErrUnavailable)
	_, err = Build(context.Background(), config.Route{Name: "x"}, deps)
	assert.Error(t, err)
	_, err = Build(context.Background(), config.Route{Name: "x", Router: &config.RouterSpec{Type: "warp"}}, deps)
	assert.ErrorContains(t, err, "unknown router type")
}

type fakeAI struct{ req AIRequest }

func (f *fakeAI) NewRouter(_ context.Context, req AIRequest) (plugin.Router, error) {
	f.req = req
	return NewStatic([]string{"billing"}), nil
}
func (f *fakeAI) Health(context.Context, []string) map[string]error { return nil }

func TestBuildAIGetsStores(t *testing.T) {
	stores := map[string]store.Store{"redis-main": store.NewMemory("redis-main"), "": store.NewMemory("default")}
	ai := &fakeAI{}
	deps := Deps{Spec: &config.Spec{}, Relay: "relay", AI: ai, Store: func(n string) (store.Store, error) {
		s, ok := stores[n]
		if !ok {
			return nil, errors.New("no store " + n)
		}
		return s, nil
	}}
	route := config.Route{Name: "triage", Router: &config.RouterSpec{
		Type:    "ai",
		History: &config.History{Store: "redis-main", Key: "{{ .json.c }}"},
		Cache:   &config.Cache{Store: "redis-main"},
	}}
	_, err := Build(context.Background(), route, deps)
	require.NoError(t, err)
	assert.Same(t, stores["redis-main"], ai.req.Deps.History)
	assert.Same(t, stores["redis-main"], ai.req.Deps.Cache)
	assert.IsType(t, &store.LRU{}, ai.req.Deps.Counters)
	assert.Equal(t, "relay", ai.req.Deps.Relay)

	route.Router.History.Store = "missing"
	_, err = Build(context.Background(), route, deps)
	assert.ErrorContains(t, err, "no store missing")
}

// A router's when condition gates it: a message it is false for (or cannot
// evaluate) skips the router, with no call, for the next one. An ai router
// gated this way only spends on the messages that need a model.
func TestChainWhenGatesARouter(t *testing.T) {
	m := metrics.New()
	ai := &fakeRouter{d: plugin.Decision{Destinations: []string{"billing"}, Router: "ai"}}
	static := &fakeRouter{d: plugin.Decision{Destinations: []string{"support"}, Router: "static"}}
	when, err := transform.CompileCEL(`has(msg.json.text) && "X-Tier" in msg.headers && msg.headers["X-Tier"] == "gold"`)
	require.NoError(t, err)
	c := NewChain("triage", []Step{{Kind: "ai", Router: ai, When: when}, {Kind: "static", Router: static}}, m, nil)

	gold := msg(`{"text":"I was charged twice"}`)
	gold.SetHeader("X-Tier", "gold")
	d, err := c.Route(context.Background(), gold)
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, 1, ai.n, "the gate lets a matching message reach the router")

	d, err = c.Route(context.Background(), msg(`{"text":"hello"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Equal(t, 1, ai.n, "a message the gate leaves out never reaches the router")
	assert.Equal(t, []string{`ai: skipped (when is false)`}, d.Hops)

	d, err = c.Route(context.Background(), msg(`{"type":"invoice.paid"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Equal(t, 1, ai.n)
	assert.Equal(t, 2.0, testutil.ToFloat64(m.RouterSkipped.WithLabelValues("triage", "ai")))

	broken, err := transform.CompileCEL(`msg.json.tier == "gold"`) // no tier field: an evaluation error
	require.NoError(t, err)
	c = NewChain("triage", []Step{{Kind: "ai", Router: ai, When: broken}, {Kind: "static", Router: static}}, m, nil)
	d, err = c.Route(context.Background(), msg(`{"text":"x"}`))
	require.NoError(t, err)
	assert.Equal(t, 1, ai.n, "a condition that cannot be evaluated counts as false")
	require.Len(t, d.Hops, 1)
	assert.Contains(t, d.Hops[0], "ai: skipped (when failed:")
}

func TestBuildCompilesWhen(t *testing.T) {
	route := config.Route{Name: "r", Router: &config.RouterSpec{Type: "static", To: []string{"a"}, When: `msg.json.n > 1`,
		Fallback: []config.RouterSpec{{Type: "static", To: []string{"b"}}}}}
	r, err := Build(context.Background(), route, Deps{})
	require.NoError(t, err)
	d, err := r.Route(context.Background(), msg(`{"n":5}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, d.Destinations)
	d, err = r.Route(context.Background(), msg(`{"n":0}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, d.Destinations)

	route.Router.When = `msg.json.n >`
	_, err = Build(context.Background(), route, Deps{})
	assert.ErrorContains(t, err, "router.when")
}
