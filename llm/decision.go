package llm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	systemonex "github.com/firebase/genkit/go/plugins/systemone/exp"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm/prompts"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

// routerBase holds what both engines share.
type routerBase struct {
	f        *Factory
	route    string
	rs       config.RouterSpec
	models   []string
	retry    modelRetry
	state    *stateBuilder
	hist     *historyStep
	cache    *cache
	budget   *budget
	minConf  float64
	timeout  time.Duration
	names    []string
	descr    map[string]string
	m        *metrics.Metrics
	log      *slog.Logger
	maxBytes int
}

func newBase(f *Factory, spec *config.Spec, req router.AIRequest) (*routerBase, error) {
	rs := req.Router
	b := &routerBase{
		f: f, route: req.Route.Name, rs: rs, models: spec.ModelChain(rs), retry: newModelRetry(spec.Models.Retry),
		minConf: rs.MinConfidence, timeout: rs.Timeout.Std(), descr: map[string]string{},
		m: req.Deps.Metrics, log: req.Deps.Logger,
	}
	if b.log == nil {
		b.log = f.log
	}
	if b.m == nil {
		b.m = f.m
	}
	if len(b.models) == 0 {
		return nil, errors.New("no model: set router.model, or models.decision / models.default")
	}
	if len(rs.Candidates) == 0 {
		return nil, errors.New("no candidates")
	}
	if b.timeout <= 0 {
		b.timeout = config.DefaultAITimeout
	}
	if rs.Multi && b.minConf == 0 {
		b.minConf = 0.5 // one yes/no per candidate: "yes" means more likely than not
	}
	for _, c := range rs.Candidates {
		b.names = append(b.names, c.Name)
		b.descr[c.Name] = c.Description
	}
	var err error
	if b.state, err = newStateBuilder(rs); err != nil {
		return nil, err
	}
	b.maxBytes = b.state.maxBytes
	if b.hist, err = newHistory(req.Route.Name, rs.History, req.Deps); err != nil {
		return nil, err
	}
	if b.cache, err = newCache(req.Route.Name, rs.Cache, req.Deps.Cache); err != nil {
		return nil, err
	}
	b.budget = newBudget(req.Route.Name, rs.Budget, req.Deps.Counters)
	return b, nil
}

func (b *routerBase) history() *historyStep   { return b.hist }
func (b *routerBase) spec() config.RouterSpec { return b.rs }

func (b *routerBase) count(outcome string) {
	if b.m != nil {
		b.m.AIDecisions.WithLabelValues(b.route, outcome).Inc()
	}
}

// prepared is a message ready for the model: its state, its history and
// what the cache keys it by.
type prepared struct {
	state    string
	key      string
	recs     []store.CallRecord
	turns    store.Turns
	histN    int
	cacheKey string
}

// prepare runs the steps before the model call: budget, state, history,
// cache (design §6.4 steps 2-3, §6.5).
func (b *routerBase) prepare(ctx context.Context, env *envelope.Envelope) (prepared, *FlowOutput, error) {
	var p prepared
	if !b.budget.allow(ctx) {
		b.count("over_budget")
		return p, nil, ErrOverBudget
	}
	var err error
	if p.state, err = b.state.build(env); err != nil {
		return p, nil, fmt.Errorf("state: %w", err)
	}
	if b.hist != nil {
		if p.key, p.recs, err = b.hist.recent(ctx, env); err != nil {
			b.count("error")
			return p, nil, err
		}
	}
	p.turns = store.BuildTurns(p.recs, p.state, b.maxBytes)
	p.histN = len(p.recs) - p.turns.Dropped
	if b.hist != nil && b.m != nil {
		b.m.HistoryTurns.WithLabelValues(b.route).Observe(float64(p.histN))
	}
	if b.cache != nil {
		p.cacheKey = b.cache.keyFor(env, fingerprint(p.turns))
		if out, ok := b.cache.get(ctx, p.cacheKey); ok {
			b.count("cached")
			out.Record = b.record(env, p, out)
			return p, &out, nil
		}
	}
	return p, nil, nil
}

// sticky applies sticky routing before the candidate questions: always_last
// reuses the last destinations with no model call; model asks the
// follow-up question first and reuses them when it is likely enough.
func (b *routerBase) sticky(ctx context.Context, env *envelope.Envelope, p prepared, followUp func(context.Context) (float64, string, error)) (*FlowOutput, *float64) {
	if b.hist == nil || b.hist.sticky == "off" || p.histN == 0 {
		return nil, nil
	}
	target, ok := router.StickyTarget(p.recs)
	if !ok {
		b.stickyCount("no_history")
		return nil, nil
	}
	prob := 1.0
	model := ""
	if b.hist.sticky == "model" {
		pr, m, err := followUp(ctx)
		if err != nil {
			b.log.Warn("follow-up question failed; asking the candidate questions", "route", b.route, "err", err)
			return nil, nil
		}
		if pr < b.hist.threshold {
			b.stickyCount("new")
			return nil, &pr
		}
		prob, model = pr, m
	}
	b.stickyCount("follow_up")
	out := FlowOutput{
		Destinations: target, Model: model, Confidence: prob, Sticky: true,
		Reason: "a follow-up of the last call of " + p.key, HistoryTurns: p.histN,
	}
	for _, t := range target {
		out.Answers = append(out.Answers, store.Answer{Candidate: t, Probability: prob, Chosen: true})
	}
	if b.hist.sticky == "model" {
		out.FollowUp = &prob
	}
	b.count("sticky")
	out.Record = b.record(env, p, out)
	return &out, out.FollowUp
}

func (b *routerBase) stickyCount(outcome string) {
	if b.m != nil {
		b.m.Sticky.WithLabelValues(b.route, outcome).Inc()
	}
}

// record is the call record appended after delivery, when the route keeps
// history and the message has a key.
func (b *routerBase) record(env *envelope.Envelope, p prepared, out FlowOutput) *store.CallRecord {
	if b.hist == nil || p.key == "" {
		return nil
	}
	return &store.CallRecord{
		MsgID: env.ID, At: nowUTC(), Route: b.route, Key: p.key,
		State:    store.TruncateUTF8(b.hist.recordState(env, p.state), b.maxBytes),
		Decision: out.Answers, Model: out.Model,
	}
}

// finish applies on_low_confidence, the record, the cache and the metrics.
func (b *routerBase) finish(ctx context.Context, env *envelope.Envelope, p prepared, out FlowOutput, start time.Time) FlowOutput {
	out.HistoryTurns = p.histN
	out.LatencyMs = time.Since(start).Milliseconds()
	if out.LowConfidence {
		out.Destinations = slices.Clone(b.rs.OnLowConfidence)
	}
	switch {
	case b.rs.Shadow:
		b.count("shadow") // decided and logged; the fallback chain delivers
	case out.LowConfidence:
		b.count("low_confidence")
	default:
		b.count("chosen")
	}
	out.Record = b.record(env, p, out)
	b.cache.put(ctx, p.cacheKey, out)
	if b.m != nil && out.Model != "" {
		b.m.AILatency.WithLabelValues(b.route, out.Model).Observe(time.Since(start).Seconds())
		if out.Tokens > 0 {
			b.m.AITokens.WithLabelValues(b.route, out.Model).Add(float64(out.Tokens))
		}
	}
	b.budget.spend(out.Tokens)
	return out
}

func fingerprint(t store.Turns) string {
	var sb strings.Builder
	for _, x := range t.Turns {
		sb.WriteString(x.Role)
		sb.WriteByte(0)
		sb.WriteString(x.Content)
		sb.WriteByte(0)
	}
	return sb.String()
}

// decisionRouter is engine: decision (design §6.2).
type decisionRouter struct{ *routerBase }

func newDecision(f *Factory, spec *config.Spec, req router.AIRequest) (*decisionRouter, error) {
	b, err := newBase(f, spec, req)
	if err != nil {
		return nil, err
	}
	return &decisionRouter{b}, nil
}

// choiceID is the question of a single choice; with multi: true each
// candidate's name is its own yes/no question.
const choiceID = "destination"

// questions builds the question set at run time from the candidates: one
// Choice (multi: false) or one Noul per candidate, all in one request
// (design §15 Q1).
func (d *decisionRouter) questions() map[string]systemonex.Question {
	if !d.rs.Multi {
		opts := make([]systemonex.ChoiceOption, 0, len(d.names))
		for _, n := range d.names {
			opts = append(opts, systemonex.ChoiceOption{Name: n, Criteria: d.descr[n]})
		}
		return map[string]systemonex.Question{choiceID: systemonex.ChoiceQuestion{Instructions: "Which destination should receive this message?", Options: opts}}
	}
	qs := make(map[string]systemonex.Question, len(d.names))
	for _, n := range d.names {
		qs[n] = systemonex.NoulQuestion{Instructions: fmt.Sprintf("Should %s receive this message?", n), Yes: d.descr[n], No: "Otherwise"}
	}
	return qs
}

func (d *decisionRouter) decide(ctx context.Context, env *envelope.Envelope) (FlowOutput, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	p, cached, err := d.prepare(ctx, env)
	if err != nil || cached != nil {
		if cached != nil {
			return *cached, nil
		}
		return FlowOutput{}, err
	}
	system := systemPrompt(d.rs.Instructions, p.histN > 0)
	stickyOut, follow := d.sticky(ctx, env, p, func(ctx context.Context) (float64, string, error) {
		res, model, _, err := callChain(ctx, d.route, d.models, d.retry, d.m, d.log, func(ctx context.Context, model string) (decisionResult, error) {
			return askDecision(ctx, d.f.g, model, decisionCall{system: system, turns: p.turns.Turns, questions: followUpQuestion(), state: p.state})
		})
		if err != nil {
			return 0, "", err
		}
		return res.answers[followUpID].Probability, res.model(model), nil
	})
	if stickyOut != nil {
		stickyOut.Engine = "decision"
		stickyOut.LatencyMs = time.Since(start).Milliseconds()
		return *stickyOut, nil
	}
	call := decisionCall{system: system, turns: p.turns.Turns, questions: d.questions(), state: p.state, stateJSON: d.rs.StateJSON && p.histN == 0}
	res, model, hops, err := callChain(ctx, d.route, d.models, d.retry, d.m, d.log, func(ctx context.Context, model string) (decisionResult, error) {
		r, err := askDecision(ctx, d.f.g, model, call)
		if err != nil {
			return r, err
		}
		return r, d.check(r.answers)
	})
	if err != nil {
		d.count("error")
		return FlowOutput{}, err
	}
	out := d.interpret(res.answers)
	out.Engine, out.Model, out.ModelHops, out.FollowUp, out.Tokens = "decision", res.model(model), hops, follow, res.tokens
	return d.finish(ctx, env, p, out, start), nil
}

// check rejects an answer set that does not answer the questions asked.
func (d *decisionRouter) check(ans map[string]systemonex.Answer) error {
	if !d.rs.Multi {
		a, ok := ans[choiceID]
		if !ok || !slices.Contains(d.names, a.Choice) {
			return fmt.Errorf("%w: the choice %q is not a candidate", ErrBadOutput, a.Choice)
		}
		return nil
	}
	for _, n := range d.names {
		if _, ok := ans[n]; !ok {
			return fmt.Errorf("%w: no answer for %s", ErrBadOutput, n)
		}
	}
	return nil
}

// interpret applies min_confidence and min_margin (design §6.2).
func (d *decisionRouter) interpret(ans map[string]systemonex.Answer) FlowOutput {
	var out FlowOutput
	if !d.rs.Multi {
		a := ans[choiceID]
		probs := a.Probabilities
		conf := a.Confidence
		if conf == 0 && probs != nil {
			conf = probs[a.Choice]
		}
		ranked := slices.Clone(d.names)
		sort.SliceStable(ranked, func(i, j int) bool { return probs[ranked[i]] > probs[ranked[j]] })
		second := 0.0
		for _, n := range ranked {
			if n != a.Choice {
				second = probs[n]
				break
			}
		}
		margin := conf - second
		low := conf < d.minConf || (d.rs.MinMargin > 0 && margin < d.rs.MinMargin)
		for _, n := range d.names {
			ansN := store.Answer{Candidate: n, Probability: probs[n]}
			if n == a.Choice {
				ansN.Probability = conf
				ansN.Chosen = !low
				ansN.Margin = margin
			}
			out.Answers = append(out.Answers, ansN)
		}
		out.Confidence, out.LowConfidence = conf, low
		if !low {
			out.Destinations = []string{a.Choice}
			out.Reason = fmt.Sprintf("%s (p=%.2f, margin %.2f)", a.Choice, conf, margin)
		} else {
			out.Reason = fmt.Sprintf("unsure: %s at p=%.2f, margin %.2f", a.Choice, conf, margin)
		}
		return out
	}
	lowest, highest := 1.0, 0.0
	for _, n := range d.names {
		pr := ans[n].Probability
		chosen := pr >= d.minConf
		out.Answers = append(out.Answers, store.Answer{Candidate: n, Probability: pr, Chosen: chosen})
		if chosen {
			out.Destinations = append(out.Destinations, n)
			lowest = min(lowest, pr)
		}
		highest = max(highest, pr)
	}
	if len(out.Destinations) == 0 {
		out.LowConfidence, out.Confidence = true, highest
		out.Reason = fmt.Sprintf("no candidate reached %.2f", d.minConf)
		return out
	}
	out.Confidence = lowest
	out.Reason = "chosen: " + strings.Join(out.Destinations, ", ")
	return out
}

// decisionCall is one request to a decision model.
type decisionCall struct {
	system    string
	turns     []store.Turn
	questions map[string]systemonex.Question
	state     string
	stateJSON bool
}

type decisionResult struct {
	answers map[string]systemonex.Answer
	served  string
	tokens  int
}

// model is the model version that answered (the server's own name for it,
// such as jev-1.13.0), prefixed like the chain's names.
func (r decisionResult) model(chain string) string {
	if r.served == "" {
		return chain
	}
	prefix, _, _ := strings.Cut(chain, "/")
	if strings.Contains(r.served, "/") {
		return prefix + "/" + r.served[strings.LastIndex(r.served, "/")+1:]
	}
	return prefix + "/" + r.served
}

// askDecision makes one decision call: the questions ride on the output
// schema, the history goes as role-tagged turns, the system message in
// front of every question.
func askDecision(ctx context.Context, g *genkit.Genkit, model string, c decisionCall) (decisionResult, error) {
	var msgs []*ai.Message
	if len(c.turns) == 0 {
		msgs = []*ai.Message{ai.NewUserTextMessage(c.state)}
	}
	for _, t := range c.turns {
		if t.Role == store.RoleModel {
			msgs = append(msgs, ai.NewModelTextMessage(t.Content))
		} else {
			msgs = append(msgs, ai.NewUserTextMessage(t.Content))
		}
	}
	opts := []ai.GenerateOption{ai.WithModelName(model), ai.WithOutputSchema(systemonex.Schema(c.questions)), ai.WithMessages(msgs...)}
	if c.system != "" {
		opts = append(opts, ai.WithSystem(c.system))
	}
	if c.stateJSON {
		opts = append(opts, ai.WithConfig(&systemonex.Config{StateJSON: true}))
	}
	answers, resp, err := genkit.GenerateData[map[string]systemonex.Answer](ctx, g, opts...)
	if err != nil {
		return decisionResult{}, err
	}
	if answers == nil {
		return decisionResult{}, fmt.Errorf("%w: no answers", ErrBadOutput)
	}
	r := decisionResult{answers: *answers, served: systemonex.ResponseInfo(resp).Model}
	if resp != nil && resp.Usage != nil {
		r.tokens = resp.Usage.InputTokens + resp.Usage.OutputTokens
	}
	return r, nil
}

// followUpID is the sticky question.
const followUpID = "follow_up"

func followUpQuestion() map[string]systemonex.Question {
	return map[string]systemonex.Question{followUpID: systemonex.NoulQuestion{
		Instructions: prompts.FollowUpInstructions,
		Yes:          prompts.FollowUpYes,
		No:           prompts.FollowUpNo,
	}}
}

func pingQuestions() map[string]systemonex.Question {
	return map[string]systemonex.Question{"ok": systemonex.NoulQuestion{Instructions: prompts.HealthCheckInstructions}}
}
