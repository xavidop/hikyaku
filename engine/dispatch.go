package engine

import (
	"context"
	"errors"
	"slices"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// DecisionView is a routing decision, as reports show it.
type DecisionView struct {
	Destinations  []string       `json:"destinations"`
	Router        string         `json:"router"`
	Model         string         `json:"model,omitempty"`
	Confidence    float64        `json:"confidence,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	LowConfidence bool           `json:"low_confidence,omitempty"`
	Cached        bool           `json:"cached,omitempty"`
	Hops          []string       `json:"router_hops,omitempty"`
	Attributes    map[string]any `json:"attributes,omitempty"`
}

func viewOf(d plugin.Decision) *DecisionView {
	return &DecisionView{
		Destinations: d.Destinations, Router: d.Router, Model: d.Model, Confidence: d.Confidence,
		Reason: d.Reason, LowConfidence: d.LowConfidence, Cached: d.Cached, Hops: d.Hops, Attributes: d.Attributes,
	}
}

// RouteReport is what one route did with a message.
type RouteReport struct {
	Route        string           `json:"route"`
	Filtered     bool             `json:"filtered,omitempty"`
	Decision     *DecisionView    `json:"decision,omitempty"`
	Error        string           `json:"error,omitempty"`
	DeadLettered bool             `json:"dead_lettered,omitempty"`
	Deliveries   []DeliveryReport `json:"deliveries,omitempty"`
	// Shadowed are the shadow destinations a copy was sent to, on the side.
	Shadowed []string `json:"shadowed,omitempty"`
	// Plans are the fallback chains a dry run would walk.
	Plans []Plan `json:"plans,omitempty"`
}

// Report is what the relay did with a message.
type Report struct {
	ID     string        `json:"id"`
	Source string        `json:"source"`
	Status string        `json:"status"`
	Error  string        `json:"error,omitempty"`
	Routes []RouteReport `json:"routes"`
}

// process runs a message through every route that takes its source and acks
// it (design §3): delivered once every required destination has it, on its
// primary or a fallback; dead-lettered once each failure is safely in the
// DLQ; failed (a nack) when the DLQ could not take it either.
func (e *Engine) process(ctx context.Context, env *envelope.Envelope) *Report {
	var rep *Report
	if d := e.dedupers[env.Source]; d != nil {
		key, dup := d.check(ctx, env)
		if dup {
			e.countDeduped(env.Source)
			e.log.Debug("duplicate dropped", "source", env.Source, "id", env.ID)
			rep = e.finish(ctx, env, &Report{ID: env.ID, Source: env.Source}, envelope.Outcome{Status: envelope.Filtered})
			e.tap(Event{Type: "done", Envelope: env, Report: rep})
			return rep
		}
		defer func() {
			if rep.Status == envelope.Failed.String() {
				d.forget(ctx, key)
			}
		}()
	}
	rep = e.handle(ctx, env, nil)
	e.tap(Event{Type: "done", Envelope: env, Report: rep})
	return rep
}

// verdict is what becomes of a processed message (doneFunc).
type verdict int

const (
	final verdict = iota // done for this run
	again                // process it once more, now (its backoff is over)
	left                 // it stays in the disk buffer for the next start
)

// doneFunc gets the report of a processed message. ordered says the
// message holds its key's turn: the disk buffer then retries a failed one
// where it stands (again), so the messages behind it keep waiting.
type doneFunc func(rep *Report, ordered bool) verdict

// run processes a message until its outcome is final for this run.
func (e *Engine) run(ctx context.Context, env *envelope.Envelope, done doneFunc, ordered bool) verdict {
	for {
		rep := e.process(ctx, env)
		if done == nil {
			return final
		}
		if v := done(rep, ordered); v != again {
			return v
		}
	}
}

// handle routes and delivers; only selects a single route (by name) when
// only is set, for DLQ replays of routing failures.
func (e *Engine) handle(ctx context.Context, env *envelope.Envelope, only *string) *Report {
	if env.Trace.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, env.Trace)
	}
	ctx, span := metrics.Tracer().Start(ctx, "hikyaku.message", trace.WithAttributes(
		attribute.String("hikyaku.id", env.ID), attribute.String("hikyaku.source", env.Source)))
	defer span.End()
	if e.m != nil {
		e.m.Received.WithLabelValues(env.Source).Inc()
	}
	e.tap(Event{Type: "received", Envelope: env})

	routes := e.bySource[env.Source]
	if only != nil {
		routes = nil
		for _, r := range e.routes {
			if r.name == *only {
				routes = []*route{r}
			}
		}
	}
	rep := &Report{ID: env.ID, Source: env.Source}
	if len(routes) == 0 {
		e.log.Warn("no route takes this source", "source", env.Source, "id", env.ID)
		if e.m != nil {
			e.m.Unrouted.WithLabelValues(env.Source).Inc()
		}
		return e.finish(ctx, env, rep, envelope.Outcome{Status: envelope.Unrouted})
	}

	replies := map[string]*envelope.Reply{}
	var failed []error
	var dlqErr error // the failure behind the first dead letter
	allFiltered, anyDLQ, dlqFailed := true, false, false
	var held error // a delay held the message when the relay shut down
	for _, r := range routes {
		rr, dl := e.runRoute(ctx, r, env)
		if !rr.Filtered {
			allFiltered = false
		}
		for _, d := range rr.Deliveries {
			if d.OK() && d.Result != nil {
				replies[d.Destination] = d.Result.Reply()
			}
		}
		// Every failure of a required destination, and every routing
		// failure, goes to the DLQ: nothing is dropped silently.
		for _, f := range dl {
			if f.held {
				held = f.err
				continue
			}
			anyDLQ = true
			if dlqErr == nil {
				dlqErr = f.err
			}
			if err := e.deadLetter(ctx, f.env, f.info); err != nil {
				dlqFailed = true
				failed = append(failed, err)
			} else {
				for i := range rr.Deliveries {
					if rr.Deliveries[i].Destination == f.info.Destination {
						rr.Deliveries[i].DeadLettered = true
					}
				}
				if f.info.Destination == "" {
					rr.DeadLettered = true
				}
			}
		}
		rep.Routes = append(rep.Routes, rr)
	}
	out := envelope.Outcome{Status: envelope.Delivered, Replies: replies}
	switch {
	case dlqFailed:
		out.Status, out.Err = envelope.Failed, failed[0]
		span.SetStatus(codes.Error, failed[0].Error())
	case held != nil:
		out.Status, out.Err = envelope.Failed, held
	case anyDLQ:
		// The error tells a source why (a model's 400 or 429, no route).
		out.Status, out.Err = envelope.DeadLettered, dlqErr
	case allFiltered:
		out.Status = envelope.Filtered
	}
	return e.finish(ctx, env, rep, out)
}

func (e *Engine) finish(ctx context.Context, env *envelope.Envelope, rep *Report, out envelope.Outcome) *Report {
	rep.Status = out.Status.String()
	if out.Err != nil && out.Status == envelope.Failed {
		rep.Error = out.Err.Error()
	}
	if e.m != nil {
		e.m.Acked.WithLabelValues(env.Source, out.Status.String()).Inc()
	}
	env.Ack(ctx, out)
	return rep
}

// dlqItem is one dead letter to write, and the failure behind it.
type dlqItem struct {
	env  *envelope.Envelope
	info DLQInfo
	err  error
	// held: not a dead letter. The relay shut down while a delay held the
	// message, so it is nacked (and stays in the disk buffer).
	held bool
}

// runRoute filters, transforms, routes and delivers a message on one route.
// It returns what has to be dead-lettered rather than writing it, so the
// caller decides the ack once.
func (e *Engine) runRoute(ctx context.Context, r *route, env *envelope.Envelope) (RouteReport, []dlqItem) {
	rr := RouteReport{Route: r.name}
	renv := env.Clone()
	if r.filter != nil {
		ok, err := r.filter.Bool(renv)
		if err == nil && !ok {
			rr.Filtered = true
			return rr, nil
		}
	}
	keep, err := r.transforms.Apply(renv)
	if err != nil && isSchemaError(err) && e.m != nil {
		e.m.SchemaInvalid.WithLabelValues(r.name).Inc()
	}
	var dec plugin.Decision
	switch quarantine := invalidTo(err); {
	case len(quarantine) > 0:
		// on_invalid: the refused message is delivered, not dead-lettered.
		dec = invalidDecision(renv, quarantine, err)
	case err != nil:
		reason, tag := ReasonTransform, plugin.TagTransform
		rr.Error = transformError(err)
		if isSchemaError(err) {
			reason, tag = ReasonSchema, plugin.TagSchema
		}
		return rr, []dlqItem{{env: env, info: DLQInfo{Route: r.name, Reason: reason, Error: rr.Error}, err: plugin.Wrap(plugin.Permanent, []string{tag}, err)}}
	case !keep:
		rr.Filtered = true
		return rr, nil
	default:
		if dec, err = r.router.Route(ctx, renv); err != nil {
			rr.Error = err.Error()
			e.log.Error("routing failed", "route", r.name, "id", env.ID, "err", err)
			return rr, []dlqItem{{env: env, info: DLQInfo{Route: r.name, Reason: ReasonNoRoute, Error: rr.Error}, err: plugin.Wrap(plugin.Permanent, []string{plugin.TagNoRoute}, err)}}
		}
	}
	for k, v := range dec.Attributes {
		renv.SetAttribute(k, v)
	}
	rr.Decision = viewOf(dec)
	if e.m != nil {
		e.m.Routed.WithLabelValues(r.name, dec.Router).Inc()
	}
	to, shadows := e.splitShadows(dec.Destinations)
	rr.Shadowed = e.shadows.send(shadows, renv)
	rr.Deliveries = e.deliverAll(ctx, to, renv)

	var dl []dlqItem
	for _, d := range rr.Deliveries {
		if d.OK() {
			continue
		}
		if !d.Required {
			if e.m != nil {
				e.m.Dropped.WithLabelValues(d.Destination).Inc()
			}
			e.log.Warn("best-effort destination failed; message dropped for it", "destination", d.Destination, "id", env.ID, "err", d.Error)
			continue
		}
		if errors.Is(d.Err, errHeld) {
			dl = append(dl, dlqItem{held: true, err: d.Err})
			continue
		}
		dl = append(dl, dlqItem{env: renv, info: DLQInfo{Route: r.name, Destination: d.Destination, Reason: ReasonDelivery, Attempts: d.Attempts, Error: d.Error, Hops: d.Hops}, err: d.Err})
	}
	e.recordHistory(dec, rr.Deliveries)
	return rr, dl
}

// isSchemaError tells a validate step's refusal from a transform that
// broke.
func isSchemaError(err error) bool {
	var se *transform.SchemaError
	return errors.As(err, &se)
}

// SchemaErrorAttribute is the attribute a message refused by a validate
// step carries to its on_invalid destinations: what is wrong with it.
const SchemaErrorAttribute = "schema.error"

// invalidRouter is the router name of an on_invalid decision, in reports
// and in hikyaku_route_decisions_total.
const invalidRouter = "validate"

// invalidTo returns the on_invalid destinations of a schema refusal.
func invalidTo(err error) []string {
	var se *transform.SchemaError
	if errors.As(err, &se) {
		return se.To
	}
	return nil
}

// invalidDecision sends a refused message to its on_invalid destinations,
// with what is wrong in SchemaErrorAttribute.
func invalidDecision(env *envelope.Envelope, to []string, err error) plugin.Decision {
	env.SetAttribute(SchemaErrorAttribute, err.Error())
	return plugin.Decision{Destinations: slices.Clone(to), Router: invalidRouter, Reason: err.Error()}
}

// transformError is the report text of a failed route transform. A schema
// refusal already says what it is.
func transformError(err error) string {
	if isSchemaError(err) {
		return err.Error()
	}
	return "transform: " + err.Error()
}

// deliverAll delivers to every destination in parallel.
func (e *Engine) deliverAll(ctx context.Context, names []string, env *envelope.Envelope) []DeliveryReport {
	out := make([]DeliveryReport, len(names))
	if len(names) == 1 {
		out[0] = e.deliverOne(ctx, names[0], env)
		return out
	}
	var wg sync.WaitGroup
	for i, n := range names {
		wg.Add(1)
		go func(i int, n string) {
			defer wg.Done()
			out[i] = e.deliverOne(ctx, n, env)
		}(i, n)
	}
	wg.Wait()
	return out
}

// deliverOne delivers to one destination of a route: through its batch
// when it batches, else at once.
func (e *Engine) deliverOne(ctx context.Context, name string, env *envelope.Envelope) DeliveryReport {
	if d := e.dests[name]; d != nil && d.delayed() {
		if rep, ok := e.hold(ctx, d, env); !ok {
			return rep
		}
	}
	if b := e.batchers[name]; b != nil {
		return b.submit(ctx, env)
	}
	return e.deliver(ctx, name, env)
}

// recordHistory completes an AI router's call record with what delivery did
// and queues it (design §6.5: appended after delivery, async).
func (e *Engine) recordHistory(dec plugin.Decision, reps []DeliveryReport) {
	note := dec.History
	if note == nil || note.Store == nil {
		return
	}
	rec := note.Record
	rec.DeliveredTo = nil
	outcome := store.OutcomeDelivered
	for _, d := range reps {
		switch {
		case d.OK():
			rec.DeliveredTo = append(rec.DeliveredTo, d.ServedBy)
			if d.ViaFallback() && outcome == store.OutcomeDelivered {
				outcome = store.OutcomeFallback
			}
			if d.Result != nil && len(d.Result.Reroute) > 0 && rec.Feedback == nil {
				rec.Feedback = &store.Feedback{Correct: slices.Clone(d.Result.Reroute), Source: "destination", At: e.now().UTC()}
				if e.m != nil {
					e.m.Feedback.WithLabelValues(note.Route).Inc()
				}
			}
		case d.Required:
			outcome = store.OutcomeDLQ
		}
	}
	if dec.LowConfidence {
		outcome = store.OutcomeLowConfidence
	}
	rec.Outcome = outcome
	if rec.At.IsZero() {
		rec.At = e.now().UTC()
	}
	e.recorder.Record(note.Store, note.Route, note.Key, rec, note.Options)
}

func (e *Engine) tap(ev Event) {
	if e.opts.Tap != nil {
		e.opts.Tap(ev)
	}
}

// Plan is the fallback chain a dry run would walk for one destination, with
// the breaker state of each endpoint.
type Plan struct {
	Destination string            `json:"destination"`
	Chain       []string          `json:"chain"`
	Breakers    map[string]string `json:"breakers"`
	Required    bool              `json:"required"`
	// Shadow is set for a shadow destination, with the share it gets.
	Shadow bool    `json:"shadow,omitempty"`
	Sample float64 `json:"sample,omitempty"`
}

func (e *Engine) planFor(name string) Plan {
	p := Plan{Destination: name, Breakers: map[string]string{}}
	for i, d := range e.chainOf(name) {
		if i == 0 {
			p.Required = d.required
			if d.shadow {
				p.Shadow, p.Sample = true, d.sample
			}
		}
		p.Chain = append(p.Chain, d.name)
		for _, ep := range d.endpoints {
			p.Breakers[ep.name] = ep.breaker.current().String()
		}
	}
	return p
}
