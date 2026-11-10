// Package router builds the routers of a route: static, rules (CEL),
// sticky (the last destinations of a history key) and ai (built by the llm
// module through AIFactory), chained in the order of router.fallback.
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// Router errors. A chain moves to its next router on any of them.
var (
	ErrNoRoute     = errors.New("no router could decide")
	ErrNoMatch     = errors.New("no rule matched")
	ErrNoHistory   = errors.New("no delivered call in the history of this key")
	ErrUnavailable = errors.New("AI routing is not available in this build (cmd/hikyaku links the llm module)")
)

// AIFactory builds AI routers. The llm module implements it; the root
// module only knows this seam, so it never imports Genkit.
type AIFactory interface {
	NewRouter(ctx context.Context, req AIRequest) (plugin.Router, error)
	// Health checks the models of a chain, by name.
	Health(ctx context.Context, models []string) map[string]error
}

// ModelSinks is an AIFactory that also builds model destinations (type:
// model, the LLM-proxy mode): sinks that answer an OpenAI-style chat
// request with a Genkit model.
type ModelSinks interface {
	NewModelSink(ctx context.Context, spec plugin.Spec) (plugin.Sink, error)
}

// AIRequest is what an AI router is built from.
type AIRequest struct {
	Spec   *config.Spec
	Route  config.Route
	Router config.RouterSpec
	Deps   AIDeps
}

// AIDeps are the relay services an AI router uses.
type AIDeps struct {
	Relay string
	// History is the store of router.history (nil without history).
	History store.HistoryStore
	// Cache backs router.cache: the named store, or an in-process LRU.
	Cache store.KV
	// Counters backs router.budget.
	Counters store.KV
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
}

// Deps are what Build needs.
type Deps struct {
	Spec  *config.Spec
	Relay string
	AI    AIFactory
	// Store returns a declared store by name ("" = the default store).
	Store   func(name string) (store.Store, error)
	Logger  *slog.Logger
	Metrics *metrics.Metrics
}

// Build returns a route's router: its router: and every router.fallback,
// depth first, as one chain.
func Build(ctx context.Context, route config.Route, d Deps) (plugin.Router, error) {
	if route.Router == nil {
		return nil, fmt.Errorf("route %s: no router", route.Name)
	}
	var steps []Step
	var walk func(rs config.RouterSpec, path string) error
	walk = func(rs config.RouterSpec, path string) error {
		r, err := buildOne(ctx, route, rs, d)
		if err != nil {
			return fmt.Errorf("route %s: %s: %w", route.Name, path, err)
		}
		step := Step{Kind: rs.Type, Router: r, FallbackOnLow: rs.FallbackOnLowConfidence}
		if rs.When != "" {
			if step.When, err = transform.CompileCEL(rs.When); err != nil {
				return fmt.Errorf("route %s: %s.when: %w", route.Name, path, err)
			}
		}
		steps = append(steps, step)
		for i, f := range rs.Fallback {
			if err := walk(f, fmt.Sprintf("%s.fallback[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(*route.Router, "router"); err != nil {
		return nil, err
	}
	return NewChain(route.Name, steps, d.Metrics, d.Logger), nil
}

func buildOne(ctx context.Context, route config.Route, rs config.RouterSpec, d Deps) (plugin.Router, error) {
	switch rs.Type {
	case "static":
		return NewStatic(rs.To), nil
	case "rules":
		return NewRules(rs)
	case "sticky":
		if rs.History == nil {
			return nil, errors.New("sticky needs history")
		}
		hist, err := d.Store(rs.History.Store)
		if err != nil {
			return nil, err
		}
		return NewSticky(route.Name, hist, *rs.History)
	case "ai":
		if d.AI == nil {
			return nil, ErrUnavailable
		}
		deps := AIDeps{Relay: d.Relay, Logger: d.Logger, Metrics: d.Metrics}
		if rs.History != nil {
			hist, err := d.Store(rs.History.Store)
			if err != nil {
				return nil, err
			}
			deps.History = hist
		}
		deps.Cache = store.NewLRU(cacheSize(rs.Cache))
		if rs.Cache != nil && rs.Cache.Store != "" {
			s, err := d.Store(rs.Cache.Store)
			if err != nil {
				return nil, err
			}
			deps.Cache = s
		}
		deps.Counters = store.NewLRU(64)
		if rs.Budget != nil && rs.Budget.Store != "" {
			s, err := d.Store(rs.Budget.Store)
			if err != nil {
				return nil, err
			}
			deps.Counters = s
		}
		return d.AI.NewRouter(ctx, AIRequest{Spec: d.Spec, Route: route, Router: rs, Deps: deps})
	}
	return nil, fmt.Errorf("unknown router type %q", rs.Type)
}

func cacheSize(c *config.Cache) int {
	if c == nil || c.Size <= 0 {
		return config.DefaultCacheSize
	}
	return c.Size
}

// NewStatic always routes to the same destinations.
func NewStatic(to []string) plugin.Router {
	return plugin.RouterFunc(func(context.Context, *envelope.Envelope) (plugin.Decision, error) {
		return plugin.Decision{Destinations: append([]string(nil), to...), Router: "static"}, nil
	})
}

type rule struct {
	expr *transform.Expr
	to   []string
}

type rules struct {
	rules []rule
	all   bool
	def   []string
}

// NewRules compiles a rules router.
func NewRules(rs config.RouterSpec) (plugin.Router, error) {
	r := &rules{all: rs.Match == "all", def: rs.Default}
	for i, ru := range rs.Rules {
		x, err := transform.CompileCEL(ru.When)
		if err != nil {
			return nil, fmt.Errorf("rules[%d].when: %w", i, err)
		}
		r.rules = append(r.rules, rule{expr: x, to: ru.To})
	}
	return r, nil
}

// Route evaluates the rules in order. A rule whose expression cannot be
// evaluated for this message (a missing key) does not match.
func (r *rules) Route(_ context.Context, env *envelope.Envelope) (plugin.Decision, error) {
	data := transform.Data(env)
	var to []string
	var matched []string
	for i, ru := range r.rules {
		ok, err := ru.expr.BoolData(data)
		if err != nil || !ok {
			continue
		}
		matched = append(matched, fmt.Sprintf("rules[%d]", i))
		for _, d := range ru.to {
			if !contains(to, d) {
				to = append(to, d)
			}
		}
		if !r.all {
			break
		}
	}
	if len(to) == 0 {
		if len(r.def) == 0 {
			return plugin.Decision{Router: "rules"}, ErrNoMatch
		}
		return plugin.Decision{Destinations: append([]string(nil), r.def...), Router: "rules", Reason: "default"}, nil
	}
	return plugin.Decision{Destinations: to, Router: "rules", Reason: "matched " + strings.Join(matched, ", ")}, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
