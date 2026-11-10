package llm

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm/prompts"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

// RouteDecision is what the generative engine asks a model for (design
// §6.3): the destinations, its own confidence, and a reason.
type RouteDecision struct {
	Destinations []string `json:"destinations"`
	Confidence   float64  `json:"confidence" jsonschema:"minimum=0,maximum=1"`
	Reason       string   `json:"reason"`
}

// FollowUpAnswer is the generative engine's sticky question.
type FollowUpAnswer struct {
	FollowUp float64 `json:"follow_up" jsonschema:"minimum=0,maximum=1"`
}

// generativeRouter is engine: generative: any Genkit model.
type generativeRouter struct{ *routerBase }

func newGenerative(f *Factory, spec *config.Spec, req router.AIRequest) (*generativeRouter, error) {
	b, err := newBase(f, spec, req)
	if err != nil {
		return nil, err
	}
	return &generativeRouter{b}, nil
}

// MessageOpen and MessageClose delimit the message in a generative prompt:
// whatever is between them is data, never instructions.
const (
	MessageOpen  = "<message>"
	MessageClose = "</message>"
)

// system is the generative engine's system prompt: the route's
// instructions, the candidates (the only names the model may answer), the
// answer rules, and the rule that the message is untrusted data.
func (g *generativeRouter) system(withHistory bool) string {
	p := prompts.Generative{Instructions: g.rs.Instructions, Multi: g.rs.Multi, Open: MessageOpen, Close: MessageClose}
	for _, n := range g.names {
		p.Candidates = append(p.Candidates, prompts.Candidate{Name: n, Description: g.descr[n]})
	}
	if withHistory {
		p.History = store.HistoryPreamble
	}
	return prompts.GenerativeSystem(p)
}

// schema is RouteDecision's schema with the candidates as an enum, so the
// provider constrains the names where it can.
func (g *generativeRouter) schema() map[string]any {
	enum := make([]any, len(g.names))
	for i, n := range g.names {
		enum[i] = n
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"destinations": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": enum}},
			"confidence":   map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"reason":       map[string]any{"type": "string"},
		},
		"required": []any{"destinations", "confidence", "reason"},
	}
}

func messages(turns []store.Turn) []*ai.Message {
	msgs := make([]*ai.Message, 0, len(turns))
	for _, t := range turns {
		if t.Role == store.RoleModel {
			msgs = append(msgs, ai.NewModelTextMessage(t.Content))
			continue
		}
		msgs = append(msgs, ai.NewUserTextMessage(MessageOpen+"\n"+t.Content+"\n"+MessageClose))
	}
	return msgs
}

func (g *generativeRouter) decide(ctx context.Context, env *envelope.Envelope) (FlowOutput, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	p, cached, err := g.prepare(ctx, env)
	if err != nil || cached != nil {
		if cached != nil {
			return *cached, nil
		}
		return FlowOutput{}, err
	}
	msgs := messages(p.turns.Turns)
	system := g.system(p.histN > 0)
	stickyOut, follow := g.sticky(ctx, env, p, func(ctx context.Context) (float64, string, error) {
		ans, model, _, err := callChain(ctx, g.route, g.models, g.retry, g.m, g.log, func(ctx context.Context, model string) (FollowUpAnswer, error) {
			out, _, err := genkit.GenerateData[FollowUpAnswer](ctx, g.f.g, ai.WithModelName(model),
				ai.WithSystem(system+"\n\nAnswer one question instead: how likely is it (follow_up, from 0 to 1) that the latest message is a follow-up to a previous call in this history?"),
				ai.WithMessages(msgs...))
			if err != nil {
				return FollowUpAnswer{}, err
			}
			if out == nil || out.FollowUp < 0 || out.FollowUp > 1 {
				return FollowUpAnswer{}, fmt.Errorf("%w: follow_up out of range", ErrBadOutput)
			}
			return *out, nil
		})
		return ans.FollowUp, model, err
	})
	if stickyOut != nil {
		stickyOut.Engine = "generative"
		stickyOut.LatencyMs = time.Since(start).Milliseconds()
		return *stickyOut, nil
	}
	var tokens int
	dec, model, hops, err := callChain(ctx, g.route, g.models, g.retry, g.m, g.log, func(ctx context.Context, model string) (RouteDecision, error) {
		out, resp, err := genkit.GenerateData[RouteDecision](ctx, g.f.g, ai.WithModelName(model), ai.WithSystem(system),
			ai.WithMessages(msgs...), ai.WithOutputSchema(g.schema()))
		if err != nil {
			return RouteDecision{}, err
		}
		if resp != nil && resp.Usage != nil {
			tokens = resp.Usage.InputTokens + resp.Usage.OutputTokens
		}
		if out == nil {
			return RouteDecision{}, fmt.Errorf("%w: no answer", ErrBadOutput)
		}
		return g.clean(*out)
	})
	if err != nil {
		g.count("error")
		return FlowOutput{}, err
	}
	out := FlowOutput{Engine: "generative", Model: model, ModelHops: hops, FollowUp: follow, Tokens: tokens, Reason: dec.Reason, Confidence: dec.Confidence}
	out.LowConfidence = dec.Confidence < g.minConf
	for _, n := range g.names {
		chosen := slices.Contains(dec.Destinations, n)
		a := store.Answer{Candidate: n, Chosen: chosen && !out.LowConfidence}
		if chosen {
			a.Probability = dec.Confidence
		}
		out.Answers = append(out.Answers, a)
	}
	if !out.LowConfidence {
		out.Destinations = dec.Destinations
	}
	return g.finish(ctx, env, p, out, start), nil
}

// clean keeps the declared candidates only (the AI never picks a URL or an
// undeclared destination), enforces multi, and treats an answer with no
// valid destination as bad output, retried like an outage.
func (g *generativeRouter) clean(d RouteDecision) (RouteDecision, error) {
	var keep []string
	for _, n := range d.Destinations {
		n = strings.TrimSpace(n)
		if slices.Contains(g.names, n) && !slices.Contains(keep, n) {
			keep = append(keep, n)
		}
	}
	if len(keep) == 0 {
		return d, fmt.Errorf("%w: none of %v is a candidate", ErrBadOutput, d.Destinations)
	}
	if !g.rs.Multi {
		keep = keep[:1]
	}
	d.Destinations = keep
	d.Confidence = min(max(d.Confidence, 0), 1)
	return d, nil
}
