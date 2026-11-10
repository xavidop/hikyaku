package router

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Step is one router of a chain.
type Step struct {
	Kind   string
	Router plugin.Router
	// FallbackOnLow hands a low-confidence answer to the next router.
	FallbackOnLow bool
	// When, if set, gates the router: a message it is false for (or cannot
	// evaluate) skips this router, with no call, for the next one.
	When *transform.Expr
}

type chain struct {
	route string
	steps []Step
	m     *metrics.Metrics
	log   *slog.Logger
}

// NewChain tries each router in order (design §7.4): the next one runs when
// a router errors, times out, is over budget, chose nothing, answered in
// shadow mode, or (FallbackOnLow) was unsure, and in place of a router whose
// when condition leaves the message out.
func NewChain(route string, steps []Step, m *metrics.Metrics, log *slog.Logger) plugin.Router {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &chain{route: route, steps: steps, m: m, log: log}
}

func (c *chain) Route(ctx context.Context, env *envelope.Envelope) (plugin.Decision, error) {
	var hops []string
	var shadow *plugin.Decision
	for i, s := range c.steps {
		if err := ctx.Err(); err != nil {
			hops = append(hops, s.Kind+": "+err.Error())
			break
		}
		if s.When != nil {
			ok, err := s.When.Bool(env)
			if err != nil || !ok {
				why := "when is false"
				if err != nil {
					why = "when failed: " + err.Error()
				}
				hops = append(hops, s.Kind+": skipped ("+why+")")
				if c.m != nil {
					c.m.RouterSkipped.WithLabelValues(c.route, s.Kind).Inc()
				}
				continue
			}
		}
		d, err := s.Router.Route(ctx, env)
		switch {
		case err != nil:
			hops = append(hops, s.Kind+": "+err.Error())
		case d.Shadow:
			hops = append(hops, s.Kind+": shadow decision "+strings.Join(d.Destinations, ", "))
			sd := d
			shadow = &sd
		case d.LowConfidence && s.FallbackOnLow:
			hops = append(hops, s.Kind+": low confidence")
		case len(d.Destinations) == 0:
			hops = append(hops, s.Kind+": chose no destination")
		default:
			if i > 0 {
				if c.m != nil {
					c.m.RouterFallback.WithLabelValues(c.route, s.Kind).Inc()
				}
				c.log.Debug("router fallback served", "route", c.route, "router", s.Kind, "hops", hops)
			}
			d.Hops = append(hops, d.Hops...)
			if shadow != nil {
				if d.Attributes == nil {
					d.Attributes = map[string]any{}
				}
				d.Attributes["ai.shadow"] = shadowAttributes(*shadow)
			}
			return d, nil
		}
		if i+1 < len(c.steps) {
			c.log.Debug("router failed over", "route", c.route, "from", s.Kind, "to", c.steps[i+1].Kind, "reason", hops[len(hops)-1])
		}
	}
	return plugin.Decision{Hops: hops}, fmt.Errorf("%w: %s", ErrNoRoute, strings.Join(hops, "; "))
}

func shadowAttributes(d plugin.Decision) map[string]any {
	out := map[string]any{"destinations": d.Destinations, "model": d.Model, "confidence": d.Confidence}
	if v, ok := d.Attributes["ai.decision"]; ok {
		out["decision"] = v
	}
	return out
}
