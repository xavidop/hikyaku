package engine

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// destination is a built destination.
type destination struct {
	name       string
	policy     retryPolicy
	timeout    time.Duration
	deadline   time.Duration
	required   bool
	idempotent bool
	idemHeader string
	transforms transform.Chain
	endpoints  []*endpoint // one, or the members of a group
	group      *group
	fallback   []string
	budget     *retryBudget
}

// Hop is one destination tried for a message: the primary or a fallback.
type Hop struct {
	Destination string        `json:"destination"`
	Endpoint    string        `json:"endpoint,omitempty"`
	Attempts    int           `json:"attempts"`
	Outcome     string        `json:"outcome"` // ok, exhausted, permanent, breaker_open, budget, deadline, filtered, error
	Error       string        `json:"error,omitempty"`
	Duration    time.Duration `json:"duration_ns"`
}

// DeliveryReport is how a message fared at one destination, fallbacks
// included.
type DeliveryReport struct {
	Destination string `json:"destination"`
	// ServedBy is the destination that took the message: the primary or a
	// fallback.
	ServedBy     string         `json:"served_by,omitempty"`
	Endpoint     string         `json:"endpoint,omitempty"`
	Attempts     int            `json:"attempts"`
	Hops         []Hop          `json:"hops"`
	Filtered     bool           `json:"filtered,omitempty"`
	Required     bool           `json:"required"`
	DeadLettered bool           `json:"dead_lettered,omitempty"`
	Error        string         `json:"error,omitempty"`
	Result       *plugin.Result `json:"-"`
	Err          error          `json:"-"`
}

// OK reports a delivered (or filtered) message.
func (r DeliveryReport) OK() bool { return r.Err == nil }

// ViaFallback reports a message a fallback served.
func (r DeliveryReport) ViaFallback() bool {
	return r.OK() && r.ServedBy != "" && r.ServedBy != r.Destination
}

// chainOf is a destination with its fallbacks, depth first, each once:
// billing → [billing, billing-dr, billing-queue].
func (e *Engine) chainOf(name string) []*destination {
	var out []*destination
	seen := map[string]bool{}
	var walk func(n string)
	walk = func(n string) {
		d, ok := e.dests[n]
		if !ok || seen[n] {
			return
		}
		seen[n] = true
		out = append(out, d)
		for _, f := range d.fallback {
			walk(f)
		}
	}
	walk(name)
	return out
}

// deliver sends a message to a destination through its fallback chain
// (design §7.2). Each hop gets its own copy of the message, its own
// transforms and its own retry policy; the deadline of the first one spans
// the whole chain. The idempotency key goes out on every attempt of every
// hop.
func (e *Engine) deliver(ctx context.Context, name string, env *envelope.Envelope) DeliveryReport {
	rep := DeliveryReport{Destination: name}
	chain := e.chainOf(name)
	if len(chain) == 0 {
		rep.Err = fmt.Errorf("destination %q is not declared", name)
		rep.Error = rep.Err.Error()
		return rep
	}
	rep.Required = chain[0].required
	if dl := chain[0].deadline; dl > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dl)
		defer cancel()
	}
	ctx, span := metrics.Tracer().Start(ctx, "hikyaku.deliver", trace.WithAttributes(attribute.String("hikyaku.destination", name)))
	defer span.End()
	key := idempotencyKey(env)
	for i, d := range chain {
		start := e.now()
		henv := env.Clone()
		keep, err := d.transforms.Apply(henv)
		if err != nil {
			rep.Hops = append(rep.Hops, Hop{Destination: d.name, Outcome: outcomeError, Error: "transform: " + err.Error()})
			rep.Err = fmt.Errorf("destination %s: transform: %w", d.name, err)
			e.fallbackHop(chain, i, outcomePermanent)
			continue
		}
		if !keep {
			rep.Hops = append(rep.Hops, Hop{Destination: d.name, Outcome: outcomeFiltered})
			rep.Filtered, rep.ServedBy, rep.Err = true, d.name, nil
			break
		}
		henv.SetHeader(d.idemHeader, key)
		r := e.tryHop(ctx, d, henv)
		hop := Hop{Destination: d.name, Endpoint: r.endpoint, Attempts: r.attempts, Outcome: r.outcome, Duration: e.now().Sub(start)}
		rep.Attempts += r.attempts
		if r.err == nil {
			rep.Hops = append(rep.Hops, hop)
			rep.ServedBy, rep.Endpoint, rep.Result, rep.Err = d.name, r.endpoint, r.result, nil
			if r.result != nil && r.result.Endpoint != "" {
				rep.Endpoint = r.result.Endpoint
			}
			break
		}
		hop.Error = r.err.Error()
		rep.Hops = append(rep.Hops, hop)
		rep.Err = fmt.Errorf("destination %s: %w", d.name, r.err)
		if ctx.Err() != nil {
			rep.Hops[len(rep.Hops)-1].Outcome = outcomeDeadline
			e.fallbackHop(chain, i, outcomeDeadline)
			break
		}
		e.fallbackHop(chain, i, r.outcome)
	}
	if rep.Err != nil {
		rep.Error = rep.Err.Error()
		span.SetStatus(codes.Error, rep.Error)
	} else {
		span.SetAttributes(attribute.String("hikyaku.served_by", rep.ServedBy))
	}
	return rep
}

// fallbackHop counts the move from chain[i] to chain[i+1].
func (e *Engine) fallbackHop(chain []*destination, i int, reason string) {
	if i+1 >= len(chain) {
		return
	}
	if e.m != nil {
		e.m.Fallback.WithLabelValues(chain[i].name, chain[i+1].name, reason).Inc()
	}
	e.log.Info("destination fallback", "from", chain[i].name, "to", chain[i+1].name, "reason", reason)
}

// tryHop delivers to one destination of the chain: its group strategy or
// its single endpoint.
func (e *Engine) tryHop(ctx context.Context, d *destination, env *envelope.Envelope) attemptResult {
	ctx, span := metrics.Tracer().Start(ctx, "hikyaku.hop", trace.WithAttributes(attribute.String("hikyaku.destination", d.name)))
	defer span.End()
	var r attemptResult
	if d.group != nil {
		r = e.tryGroup(ctx, d, env)
	} else {
		r = e.tryEndpoint(ctx, d, d.endpoints[0], env)
	}
	if e.m != nil && d.budget != nil {
		e.m.BudgetRemaining.WithLabelValues(d.name).Set(d.budget.remaining())
	}
	if r.err != nil {
		span.SetStatus(codes.Error, r.err.Error())
	}
	span.SetAttributes(attribute.String("hikyaku.outcome", r.outcome), attribute.Int("hikyaku.attempts", r.attempts))
	return r
}

// idempotencyKey is the key sent on every attempt: a source-provided
// idempotency_key attribute, else the envelope ID.
func idempotencyKey(env *envelope.Envelope) string {
	if v, ok := env.Attribute("idempotency_key"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return env.ID
}
