package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/xavidop/hikyaku/envelope"
)

// Inject runs one message through every route of its source, synchronously,
// with the full retry, fallback and DLQ path (hikyaku route, send through
// routes, replays). The message's own ack callback runs too.
func (e *Engine) Inject(ctx context.Context, env *envelope.Envelope) *Report {
	return e.handle(ctx, env, nil)
}

// InjectRoute runs a message through one route only: the replay of a routing
// failure.
func (e *Engine) InjectRoute(ctx context.Context, routeName string, env *envelope.Envelope) (*Report, error) {
	if !slices.ContainsFunc(e.routes, func(r *route) bool { return r.name == routeName }) {
		return nil, fmt.Errorf("route %q is not declared", routeName)
	}
	return e.handle(ctx, env, &routeName), nil
}

// DeliverTo pushes a message straight to one destination through its full
// retry and fallback chain; a required destination that fails dead-letters
// it (hikyaku send, dlq replay --to). A destination that batches gets it
// as a batch of one, at once.
func (e *Engine) DeliverTo(ctx context.Context, dest string, env *envelope.Envelope) (DeliveryReport, error) {
	if _, ok := e.dests[dest]; !ok {
		return DeliveryReport{}, fmt.Errorf("destination %q is not declared (one of %v)", dest, e.order)
	}
	var rep DeliveryReport
	if b := e.batchers[dest]; b != nil {
		rep = b.now(ctx, env)
	} else {
		rep = e.deliver(ctx, dest, env)
	}
	if !rep.OK() && rep.Required {
		if err := e.deadLetter(ctx, env, DLQInfo{Destination: dest, Reason: ReasonDelivery, Attempts: rep.Attempts, Error: rep.Error, Hops: rep.Hops}); err != nil {
			return rep, err
		}
		rep.DeadLettered = true
	}
	return rep, nil
}

// DryRun shows what the routes of a source would do with a message: which
// router decides (AI routers really call their models), where it would go,
// and the fallback chains it would walk. Nothing is delivered and no
// history is written.
func (e *Engine) DryRun(ctx context.Context, source string, env *envelope.Envelope) ([]RouteReport, error) {
	routes, ok := e.bySource[source]
	if !ok {
		return nil, fmt.Errorf("no route takes source %q", source)
	}
	env.Source = source
	return e.dryRun(ctx, routes, env)
}

func (e *Engine) dryRun(ctx context.Context, routes []*route, env *envelope.Envelope) ([]RouteReport, error) {
	var out []RouteReport
	for _, r := range routes {
		rr := RouteReport{Route: r.name}
		renv := env.Clone()
		if r.filter != nil {
			ok, err := r.filter.Bool(renv)
			if err == nil && !ok {
				rr.Filtered = true
				out = append(out, rr)
				continue
			}
			if err != nil {
				rr.FilterError = err.Error()
			}
		}
		keep, ferr, err := applyChain(r.transforms, renv)
		if ferr != "" {
			rr.FilterError = ferr
		}
		switch quarantine := invalidTo(err); {
		case len(quarantine) > 0:
			rr.Decision = viewOf(invalidDecision(renv, quarantine, err))
			for _, d := range quarantine {
				rr.Plans = append(rr.Plans, e.planFor(d))
			}
		case err != nil:
			rr.Error = transformError(err)
		case !keep:
			rr.Filtered = true
		default:
			dec, err := r.router.Route(ctx, renv)
			if err != nil {
				rr.Error = err.Error()
				break
			}
			rr.Decision = viewOf(dec)
			for _, d := range dec.Destinations {
				rr.Plans = append(rr.Plans, e.planFor(d))
			}
		}
		out = append(out, rr)
	}
	return out, nil
}

// DryRunRoute is DryRun for one route (hikyaku ai test).
func (e *Engine) DryRunRoute(ctx context.Context, routeName string, env *envelope.Envelope) (RouteReport, error) {
	for _, r := range e.routes {
		if r.name != routeName {
			continue
		}
		if env.Source == "" && len(r.from) > 0 {
			env.Source = r.from[0]
		}
		reps, err := e.dryRun(ctx, []*route{r}, env)
		if err != nil {
			return RouteReport{}, err
		}
		return reps[0], nil
	}
	return RouteReport{}, fmt.Errorf("route %q is not declared", routeName)
}

// Health is the engine's view of its destinations and stores.
type Health struct {
	Ready        bool                `json:"ready"`
	Destinations []DestinationHealth `json:"destinations"`
	Stores       map[string]string   `json:"stores"`
	// Sources holds each source's health: "ok" or its error.
	Sources map[string]string `json:"sources,omitempty"`
	// ListenersUp is false while a listen source is down: the relay cannot
	// take the traffic sent to it, so it is not ready.
	ListenersUp bool `json:"listeners_up"`
}

// DestinationHealth is one destination.
type DestinationHealth struct {
	Name     string         `json:"name"`
	Required bool           `json:"required"`
	Up       bool           `json:"up"`
	Members  []MemberHealth `json:"members"`
}

// MemberHealth is one endpoint.
type MemberHealth struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Breaker string `json:"breaker"`
}

// Health reports readiness: not ready while a required destination can take
// nothing: every member of it, and of every fallback in its chain, is down
// (unhealthy or its breaker open). It also reports each source, and
// whether every listen source is up.
func (e *Engine) Health(ctx context.Context) Health {
	h := Health{Ready: true, Stores: map[string]string{}, ListenersUp: true}
	for i, s := range e.sources {
		if h.Sources == nil {
			h.Sources = map[string]string{}
		}
		if err := s.Health(ctx); err != nil {
			h.Sources[s.Name()] = err.Error()
			if e.spec.Sources[i].Mode == "listen" {
				h.ListenersUp = false
			}
			continue
		}
		h.Sources[s.Name()] = "ok"
	}
	up := map[string]bool{}
	for _, name := range e.order {
		d := e.dests[name]
		dh := DestinationHealth{Name: name, Required: d.required}
		for _, ep := range d.endpoints {
			st := ep.breaker.current()
			mh := MemberHealth{Name: ep.name, Healthy: ep.healthy.Load(), Breaker: st.String()}
			if mh.Healthy && st != stateOpen {
				dh.Up = true
			}
			dh.Members = append(dh.Members, mh)
		}
		up[name] = dh.Up
		h.Destinations = append(h.Destinations, dh)
	}
	for _, name := range e.order {
		if !e.dests[name].required {
			continue
		}
		served := false
		for _, d := range e.chainOf(name) {
			served = served || up[d.name]
		}
		if !served {
			h.Ready = false
		}
	}
	for name, s := range e.Stores() {
		label := name
		if label == "" {
			label = s.Name() + " (default)"
		}
		if err := s.Health(ctx); err != nil {
			h.Stores[label] = err.Error()
		} else {
			h.Stores[label] = "ok"
		}
	}
	return h
}

// DestinationNames lists the destinations in spec order.
func (e *Engine) DestinationNames() []string { return slices.Clone(e.order) }

// SinkHealth is the health of one sink.
type SinkHealth struct {
	Name string
	Err  error
}

// CheckDeadLetter calls Health on the DLQ and on each fallback hop built
// for it, in order (a hop that is a destination is checked by CheckSinks).
func (e *Engine) CheckDeadLetter(ctx context.Context) []SinkHealth {
	var out []SinkHealth
	if e.dlq == nil {
		return out
	}
	for _, h := range e.dlq.hops {
		if h.own {
			out = append(out, SinkHealth{Name: h.name, Err: h.sink.Health(ctx)})
		}
	}
	return out
}

// CheckSinks calls Health on every endpoint (hikyaku doctor).
func (e *Engine) CheckSinks(ctx context.Context) map[string]error {
	out := map[string]error{}
	for _, name := range e.order {
		for _, ep := range e.dests[name].endpoints {
			out[ep.name] = ep.sink.Health(ctx)
		}
	}
	return out
}
