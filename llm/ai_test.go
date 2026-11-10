//go:build llmfake

package llm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm/llmfake"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

const triage = `
version: 1
name: ai
models:
  decision: %[1]s
  default: %[1]s
  retry: { attempts: 2, backoff: 1ms, max_backoff: 2ms }
sources: [{ name: webhooks, type: stdin }]
destinations:
  - { name: billing, type: stdout }
  - { name: support, type: stdout }
  - { name: dashboard, type: stdout }
routes:
  - name: triage
    from: [webhooks]
    router:
      type: ai
      engine: %[2]s
      instructions: You route customer events for a SaaS company.
      candidates:
        - { name: billing, description: "Payments, invoices, refunds, subscription changes" }
        - { name: support, description: "Bugs, outages, errors, how-to questions" }
        - { name: dashboard, description: "Signups and usage spikes for the live dashboard" }
      %[3]s
      fallback: [{ type: static, to: [support] }]
`

type harness struct {
	f     *Factory
	spec  *config.Spec
	m     *metrics.Metrics
	mem   *store.Memory
	route plugin.Router
}

func setup(t *testing.T, model, engine, extra string) *harness {
	t.Helper()
	spec := fmt.Sprintf(triage, model, engine, extra)
	l, err := config.LoadBytes(context.Background(), "t.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	require.Empty(t, engine2Check(l.Spec))
	h := &harness{spec: l.Spec, m: metrics.New(), mem: store.NewMemory("default")}
	h.f, err = New(context.Background(), l.Spec, Options{Metrics: h.m, Plugins: llmfake.Plugins()})
	require.NoError(t, err)
	route := l.Spec.Routes[0]
	h.route, err = router.Build(context.Background(), route, router.Deps{
		Spec: l.Spec, Relay: "ai", AI: h.f, Metrics: h.m,
		Store: func(string) (store.Store, error) { return h.mem, nil },
	})
	require.NoError(t, err)
	return h
}

func engine2Check(s *config.Spec) config.Errors { return engine.Check(s) }

func msg(body string) *envelope.Envelope {
	e := envelope.New("webhooks", "http", []byte(body))
	e.ContentType = "application/json"
	return e
}

func decisionOf(t *testing.T, d plugin.Decision) map[string]any {
	t.Helper()
	v, ok := d.Attributes["ai.decision"].(map[string]any)
	require.True(t, ok, "ai.decision is set")
	return v
}

func TestDecisionSingleChoice(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", "min_confidence: 0.5\n      on_low_confidence: [support]")
	d, err := h.route.Route(context.Background(), msg(`{"text":"I was charged twice and want a refund for the invoice"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, "ai", d.Router)
	assert.Equal(t, "llmfake-decisions/clef", d.Model)
	assert.Greater(t, d.Confidence, 0.5)
	ad := decisionOf(t, d)
	answers := ad["answers"].([]store.Answer)
	require.Len(t, answers, 3)
	assert.True(t, answers[0].Chosen)
	assert.Greater(t, answers[0].Margin, 0.0)
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "chosen")))
	flow, ok := h.f.Flow("triage")
	require.True(t, ok)
	out, err := flow.Run(context.Background(), FlowInput{Body: `the app shows an error and crashes, outage`})
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, out.Destinations, "the route/<name> flow is runnable on its own")
}

func TestDecisionShadowIsCounted(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", "min_confidence: 0.5\n      shadow: true")
	d, err := h.route.Route(context.Background(), msg(`{"text":"I was charged twice and want a refund for the invoice"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations, "the fallback chain delivers a shadow decision")
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "shadow")))
	assert.Zero(t, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "chosen")))
}

func TestDecisionLowConfidence(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", "min_confidence: 0.9\n      on_low_confidence: [dashboard]")
	d, err := h.route.Route(context.Background(), msg(`{"text":"hello there"}`))
	require.NoError(t, err)
	assert.True(t, d.LowConfidence)
	assert.Equal(t, []string{"dashboard"}, d.Destinations, "unsure answers go to on_low_confidence")
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "low_confidence")))

	// Without on_low_confidence, the router fallback takes it.
	h = setup(t, "llmfake-decisions/clef", "decision", "min_confidence: 0.9")
	d, err = h.route.Route(context.Background(), msg(`{"text":"hello there"}`))
	require.NoError(t, err)
	assert.Equal(t, "static", d.Router)
	assert.Contains(t, strings.Join(d.Hops, ";"), "not sure")
}

func TestDecisionMulti(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", "multi: true")
	d, err := h.route.Route(context.Background(), msg(`{"text":"the refund failed with an error: invoice outage"}`))
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"billing", "support"}, d.Destinations, "one yes/no per candidate, every likely one chosen")
}

func TestModelFallbackChain(t *testing.T) {
	h := setup(t, "llmfake-decisions/down", "decision", "fallback_models: [llmfake-decisions/garbage, llmfake-decisions/clef]")
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund my invoice"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, "llmfake-decisions/clef", d.Model)
	ad := decisionOf(t, d)
	hops := ad["model_hops"].([]string)
	require.Len(t, hops, 2)
	assert.Contains(t, hops[0], "llmfake-decisions/down")
	assert.Contains(t, hops[1], "llmfake-decisions/garbage", "bad output was retried, then the next model")
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIModelFallback.WithLabelValues("triage", "llmfake-decisions/down", "llmfake-decisions/garbage", "unavailable")))
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIModelFallback.WithLabelValues("triage", "llmfake-decisions/garbage", "llmfake-decisions/clef", "bad_output")))
}

func TestEveryModelDownFallsBackToRules(t *testing.T) {
	h := setup(t, "llmfake-decisions/down", "decision", "")
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund"}`))
	require.NoError(t, err)
	assert.Equal(t, "static", d.Router, "the router fallback takes over when the whole model chain fails")
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.RouterFallback.WithLabelValues("triage", "static")))
}

func TestGenerativeEngine(t *testing.T) {
	h := setup(t, "llmfake/router", "generative", "min_confidence: 0.3")
	d, err := h.route.Route(context.Background(), msg(`{"text":"my subscription invoice is wrong, I need a refund"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, "llmfake/router", d.Model)
	assert.Contains(t, d.Reason, "invoice")
}

func TestGenerativeBadOutputFallsBack(t *testing.T) {
	h := setup(t, "llmfake/bad-json", "generative", "fallback_models: [llmfake/router]")
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund the invoice"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations, "a URL the model made up is never a destination")
	assert.Equal(t, "llmfake/router", d.Model)
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIModelFallback.WithLabelValues("triage", "llmfake/bad-json", "llmfake/router", "bad_output")))
}

const withHistory = `history:
        key: "{{ .json.customer }}"
        ttl: 1h
        sticky: { enabled: true, mode: %s, follow_up_threshold: 0.7 }`

func appendPast(t *testing.T, h *harness, key string, chosen string, fb []string) {
	t.Helper()
	rec := store.CallRecord{MsgID: "past-" + key, At: time.Now(), Route: "triage", Key: key, State: `{"text":"refund for invoice 7","customer":"` + key + `"}`,
		Decision: []store.Answer{{Candidate: chosen, Probability: 0.9, Chosen: true}}, Outcome: store.OutcomeDelivered, DeliveredTo: []string{chosen}}
	if fb != nil {
		rec.Feedback = &store.Feedback{Correct: fb}
	}
	require.NoError(t, h.mem.Append(context.Background(), "triage", key, rec, store.AppendOptions{TTL: time.Hour}))
}

func TestStickyFollowUpReusesTheLastDestination(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", fmt.Sprintf(withHistory, "model"))
	appendPast(t, h, "c-1", "billing", nil)
	d, err := h.route.Route(context.Background(), msg(`{"text":"still no refund for invoice 7, following up again","customer":"c-1"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)
	ad := decisionOf(t, d)
	assert.Equal(t, true, ad["sticky"])
	assert.Greater(t, ad["follow_up"].(float64), 0.7)
	require.NotNil(t, d.History, "the record to append after delivery")
	assert.Equal(t, "c-1", d.History.Key)
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.Sticky.WithLabelValues("triage", "follow_up")))

	// A correction recorded on the last call wins.
	appendPast(t, h, "c-2", "support", []string{"billing"})
	d, err = h.route.Route(context.Background(), msg(`{"text":"still waiting, following up on the same thing","customer":"c-2"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations)

	// Something new for a known customer: the candidate questions decide.
	d, err = h.route.Route(context.Background(), msg(`{"text":"the dashboard shows an error, outage","customer":"c-1"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"support"}, d.Destinations)
	assert.Nil(t, decisionOf(t, d)["sticky"])
}

func TestStickyAlwaysLastNeedsNoModel(t *testing.T) {
	h := setup(t, "llmfake-decisions/down", "decision", fmt.Sprintf(withHistory, "always_last"))
	appendPast(t, h, "c-9", "billing", nil)
	before := llmfake.Calls.Load()
	d, err := h.route.Route(context.Background(), msg(`{"text":"anything at all","customer":"c-9"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"billing"}, d.Destinations, "affinity works with every model down")
	assert.Equal(t, before, llmfake.Calls.Load(), "no model call")
}

func TestDecisionRouterStoreDownIgnore(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", fmt.Sprintf(withHistory, "model"))
	h.mem.SetError(errors.New("redis: connection refused"))
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund the invoice","customer":"c-1"}`))
	require.NoError(t, err, "routing keeps working when the history store is down")
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Equal(t, "ai", d.Router)
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.HistoryErrors.WithLabelValues("read")))

	h = setup(t, "llmfake-decisions/clef", "decision", fmt.Sprintf(withHistory, "model")+"\n        on_store_error: fallback")
	h.mem.SetError(errors.New("redis: connection refused"))
	d, err = h.route.Route(context.Background(), msg(`{"text":"refund the invoice","customer":"c-1"}`))
	require.NoError(t, err)
	assert.Equal(t, "static", d.Router, "on_store_error: fallback hands the message to the router fallback")
}

func TestHistoryTurnsReachTheModel(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", fmt.Sprintf(withHistory, "off"))
	appendPast(t, h, "c-3", "billing", nil)
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund please","customer":"c-3"}`))
	require.NoError(t, err)
	assert.Equal(t, 1, decisionOf(t, d)["history_turns"])
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.HistoryReads.WithLabelValues("triage", "hit")))
	require.NotNil(t, d.History)
	assert.Equal(t, `{"text":"refund please","customer":"c-3"}`, d.History.Record.State, "the redacted state is recorded by default")
}

func TestDecisionCache(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", `cache: { key: "{{ .json.ticket }}", ttl: 1m }`)
	_, err := h.route.Route(context.Background(), msg(`{"text":"refund","ticket":"T-1"}`))
	require.NoError(t, err)
	before := llmfake.Calls.Load()
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund","ticket":"T-1"}`))
	require.NoError(t, err)
	assert.True(t, d.Cached)
	assert.Equal(t, before, llmfake.Calls.Load(), "a cache hit makes no model call")
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "cached")))
}

func TestBudgetSendsToFallback(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", `budget: { max_calls_per_minute: 1 }`)
	d, err := h.route.Route(context.Background(), msg(`{"text":"refund"}`))
	require.NoError(t, err)
	assert.Equal(t, "ai", d.Router)
	d, err = h.route.Route(context.Background(), msg(`{"text":"refund"}`))
	require.NoError(t, err)
	assert.Equal(t, "static", d.Router, "over budget: straight to the router fallback")
	assert.Equal(t, 1.0, testutil.ToFloat64(h.m.AIDecisions.WithLabelValues("triage", "over_budget")))
}

func TestHealth(t *testing.T) {
	h := setup(t, "llmfake-decisions/clef", "decision", "")
	res := h.f.Health(context.Background(), []string{"llmfake-decisions/clef", "llmfake-decisions/down", "llmfake/router", "nobody/none"})
	assert.NoError(t, res["llmfake-decisions/clef"])
	assert.NoError(t, res["llmfake/router"])
	assert.Error(t, res["llmfake-decisions/down"])
	assert.Error(t, res["nobody/none"])
}

// The full path through the engine: the AI routes, the engine delivers, and
// the call record lands in the store after delivery, so the next message of
// the same customer is a follow-up.
func TestEngineRecordsHistoryAfterDelivery(t *testing.T) {
	spec := fmt.Sprintf(triage, "llmfake-decisions/clef", "decision", fmt.Sprintf(withHistory, "model"))
	spec = strings.Replace(spec, "{ name: billing, type: stdout }", "{ name: billing, type: fault }", 1)
	l, err := config.LoadBytes(context.Background(), "t.yaml", []byte(spec+"\nstores: [{ name: default, type: memory }]\n"), config.LoadOptions{})
	require.NoError(t, err)
	f, err := New(context.Background(), l.Spec, Options{Plugins: llmfake.Plugins()})
	require.NoError(t, err)
	plugin.RegisterSink("fault", func(context.Context, plugin.Spec) (plugin.Sink, error) { return okSink{}, nil })
	plugin.RegisterSink("stdout", func(context.Context, plugin.Spec) (plugin.Sink, error) { return okSink{}, nil })
	e, err := engine.New(context.Background(), l.Spec, engine.Options{AI: f, NoSources: true})
	require.NoError(t, err)
	rep := e.Inject(context.Background(), msg(`{"text":"refund for invoice 9","customer":"c-7"}`))
	assert.Equal(t, "delivered", rep.Status)
	mem, _ := e.Store("default")
	require.Eventually(t, func() bool {
		recs, _ := mem.Recent(context.Background(), "triage", "c-7", store.Window{})
		return len(recs) == 1 && recs[0].Outcome == store.OutcomeDelivered
	}, 2*time.Second, 10*time.Millisecond, "appended after delivery, asynchronously")
	rep = e.Inject(context.Background(), msg(`{"text":"still no refund for invoice 9, following up again","customer":"c-7"}`))
	assert.Equal(t, []string{"billing"}, rep.Routes[0].Decision.Destinations)
	assert.Equal(t, true, rep.Routes[0].Decision.Attributes["ai.decision"].(map[string]any)["sticky"])
	e.Close(context.Background())
}

type okSink struct{}

func (okSink) Name() string { return "ok" }
func (okSink) Deliver(context.Context, *envelope.Envelope) (*plugin.Result, error) {
	return &plugin.Result{Status: 200}, nil
}
func (okSink) Health(context.Context) error { return nil }
func (okSink) Close(context.Context) error  { return nil }
