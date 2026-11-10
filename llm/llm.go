// Package llm is hikyaku's AI router on Genkit Go: the only module that
// imports Genkit and the model providers (check_ai_deps.sh enforces it).
//
// The decision engine (default) asks System One decision models typed
// questions built at run time from a route's candidates and gets calibrated
// probabilities back; the generative engine asks any Genkit model for a
// RouteDecision with GenerateData. Both retry each model, walk the model
// fallback chain, read and write history, honour sticky routing, the cache
// and the budget, and run as a Genkit flow per route (route/<name>), so
// every decision is traceable in the Dev UI.
package llm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
)

// Options configure the factory.
type Options struct {
	// Credentials returns a declared secret by name (TYPESAFE_API_KEY,
	// GEMINI_API_KEY, ANTHROPIC_API_KEY, ...). A key not found there is
	// read from the environment by the provider itself.
	Credentials func(name string) (string, bool)
	Logger      *slog.Logger
	Metrics     *metrics.Metrics
	// Plugins are extra Genkit plugins (tests).
	Plugins []api.Plugin
}

// Factory builds AI routers on one Genkit instance; it implements
// router.AIFactory. A reload that keeps genkit:, the model providers and
// their keys keeps the factory: it swaps the router behind each flow, and
// the flows stay registered. A reload that changes them builds a new
// factory, with a Genkit instance of its own, next to the running one.
type Factory struct {
	g    *genkit.Genkit
	spec *config.Spec
	log  *slog.Logger
	m    *metrics.Metrics

	mu    sync.Mutex
	flows map[string]*flowSlot
}

// flowSlot is the route/<name> flow and the router it currently runs.
type flowSlot struct {
	mu   sync.RWMutex
	r    decider
	flow *core.Flow[FlowInput, FlowOutput, struct{}]
}

func (s *flowSlot) current() decider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.r
}

// decider is an engine's decision procedure.
type decider interface {
	decide(ctx context.Context, env *envelope.Envelope) (FlowOutput, error)
	history() *historyStep
	spec() config.RouterSpec
}

// New initializes Genkit with the plugins the spec needs: the ones listed
// under genkit.plugins, plus the providers named by every model of every
// chain. The Genkit instance lives as long as ctx: under the Dev UI, its
// reflection server stops when ctx ends.
func New(ctx context.Context, spec *config.Spec, o Options) (*Factory, error) {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	plugins, err := pluginsFor(spec, o)
	if err != nil {
		return nil, err
	}
	if err := secondReflectionServer(); err != nil {
		return nil, err
	}
	g, err := initGenkit(ctx, plugins)
	if err != nil {
		return nil, err
	}
	countReflectionServer(ctx)
	return &Factory{g: g, spec: spec, log: o.Logger, m: o.Metrics, flows: map[string]*flowSlot{}}, nil
}

// initGenkit turns a plugin's init panic (a missing key, a bad base URL)
// into an error.
func initGenkit(ctx context.Context, plugins []api.Plugin) (g *genkit.Genkit, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("genkit: %v", r)
		}
	}()
	return genkit.Init(ctx, genkit.WithPlugins(plugins...)), nil
}

// Genkit is the underlying instance.
func (f *Factory) Genkit() *genkit.Genkit { return f.g }

// NewRouter builds the AI router of a route and (re)binds its flow.
func (f *Factory) NewRouter(_ context.Context, req router.AIRequest) (plugin.Router, error) {
	spec := req.Spec
	if spec == nil {
		spec = f.spec
	}
	var d decider
	var err error
	switch req.Router.Engine {
	case "", "decision":
		d, err = newDecision(f, spec, req)
	case "generative":
		d, err = newGenerative(f, spec, req)
	default:
		return nil, fmt.Errorf("unknown engine %q", req.Router.Engine)
	}
	if err != nil {
		return nil, err
	}
	slot := f.slot(req.Route.Name)
	slot.mu.Lock()
	slot.r = d
	slot.mu.Unlock()
	return &aiRouter{f: f, route: req.Route.Name, slot: slot, deps: req.Deps}, nil
}

func (f *Factory) slot(route string) *flowSlot {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.flows[route]; ok {
		return s
	}
	s := &flowSlot{}
	s.flow = genkit.DefineFlow(f.g, "route/"+route, func(ctx context.Context, in FlowInput) (FlowOutput, error) {
		d := s.current()
		if d == nil {
			return FlowOutput{}, errors.New("the route has no AI router")
		}
		out, err := d.decide(ctx, in.Envelope())
		return out.normalized(), err
	})
	f.flows[route] = s
	return s
}

// Flow returns a route's flow (hikyaku ai test runs it directly).
func (f *Factory) Flow(route string) (*core.Flow[FlowInput, FlowOutput, struct{}], bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.flows[route]
	if !ok {
		return nil, false
	}
	return s.flow, true
}

// Health checks each model with the smallest possible call: one yes/no
// question to a decision model, a one-word prompt to a generative one.
func (f *Factory) Health(ctx context.Context, models []string) map[string]error {
	out := map[string]error{}
	for _, m := range models {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		out[m] = f.ping(cctx, m)
		cancel()
	}
	return out
}

func (f *Factory) ping(ctx context.Context, model string) error {
	if genkit.LookupModel(f.g, model) == nil {
		return fmt.Errorf("model %s is not available: is its plugin configured?", model)
	}
	if isDecisionModel(model, f.spec.Genkit.SystemOne.Custom) {
		_, err := askDecision(ctx, f.g, model, decisionCall{questions: pingQuestions(), state: "ping"})
		return err
	}
	_, err := genkit.Generate(ctx, f.g, ai.WithModelName(model), ai.WithPrompt("Reply with the word OK."))
	return err
}

// aiRouter is the plugin.Router of an AI route: it runs the route's flow
// and turns its output into a decision with the history record attached.
type aiRouter struct {
	f     *Factory
	route string
	slot  *flowSlot
	deps  router.AIDeps
}

// ErrLowConfidence is a low-confidence answer with no on_low_confidence:
// the router fallback takes the message.
var ErrLowConfidence = errors.New("the model was not sure and the route has no on_low_confidence")

func (r *aiRouter) Route(ctx context.Context, env *envelope.Envelope) (plugin.Decision, error) {
	d := r.slot.current()
	if d == nil {
		return plugin.Decision{Router: "ai"}, errors.New("the route has no AI router")
	}
	out, err := r.slot.flow.Run(ctx, InputOf(env))
	if err != nil {
		return plugin.Decision{Router: "ai"}, err
	}
	rs := d.spec()
	dec := plugin.Decision{
		Destinations:  out.Destinations,
		Router:        "ai",
		Model:         out.Model,
		Confidence:    out.Confidence,
		Reason:        out.Reason,
		LowConfidence: out.LowConfidence,
		Shadow:        rs.Shadow,
		Cached:        out.Cached,
		Attributes:    map[string]any{"ai.decision": out.Attribute()},
	}
	if out.LowConfidence && len(dec.Destinations) == 0 && !rs.FallbackOnLowConfidence {
		return dec, ErrLowConfidence
	}
	if h := d.history(); h != nil && out.Record != nil && !rs.Shadow {
		dec.History = &plugin.HistoryNote{
			Store: h.store, Route: r.route, Key: out.Record.Key, Record: *out.Record,
			Options: h.appendOptions(),
		}
	}
	return dec, nil
}

// isDecisionModel reports whether a model is served by a System One plugin:
// a built-in decision provider, or one of the spec's genkit.systemone.custom
// servers. The custom ones belong to a factory's spec: another spec of the
// process (a reload) may name other ones.
func isDecisionModel(model string, custom []config.SystemOneServer) bool {
	p, _, _ := strings.Cut(model, "/")
	switch p {
	case "typesafe", "openrouter-decisions", "ollama-decisions", "cloudflare-decisions", "llmfake-decisions":
		return true
	}
	for _, c := range custom {
		if c.Provider == p {
			return true
		}
	}
	return false
}
