package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
)

// Destination batching (destination batch:). Each message of a route waits
// in its destination's open batch; the batch is flushed when it holds
// max_messages or max_bytes, or max_wait after its first message (also at
// shutdown: an open batch completes on its timer), and is delivered through
// the destination's normal chain: retries, breaker, fallbacks. Every member shares the batch's outcome, so a dead-lettered
// batch writes one DLQ record per message (each member's own report).
//
// At each hop of the chain the members are transformed one by one with that
// hop's transforms (a filtered member is done, filtered), get their
// idempotency header, and are then combined: a sink with a native batch
// call (plugin.BatchSink) receives the members, any other sink one envelope
// whose body is the members' bodies as NDJSON or a JSON array (and so does a
// BatchSink that hands the batch back with plugin.ErrNoBatch).

const (
	defaultBatchMessages = 100
	defaultBatchBytes    = 1 << 20
	defaultBatchWait     = time.Second
	contentNDJSON        = "application/x-ndjson"
)

type batcher struct {
	e        *Engine
	name     string
	max      int
	maxBytes int
	wait     time.Duration
	format   string

	mu  sync.Mutex
	cur *openBatch
}

type openBatch struct {
	ctx     context.Context
	members []*envelope.Envelope
	bytes   int
	reports []DeliveryReport
	done    chan struct{}
	timer   *time.Timer
	flushed bool
}

func newBatcher(e *Engine, name string, b *config.Batch) *batcher {
	out := &batcher{e: e, name: name, max: b.MaxMessages, maxBytes: int(b.MaxBytes), wait: b.MaxWait.Std(), format: b.Format}
	if out.max <= 0 {
		out.max = defaultBatchMessages
	}
	if out.maxBytes <= 0 {
		out.maxBytes = defaultBatchBytes
	}
	if out.wait <= 0 {
		out.wait = defaultBatchWait
	}
	if e.opts.NoWait {
		out.wait = time.Millisecond
	}
	if out.format == "" {
		out.format = "ndjson"
	}
	return out
}

// submit adds env to the open batch and waits for the batch's outcome. A
// worker waiting on a batch gives its worker slot back meanwhile, so a
// batch can hold more messages than there are workers.
func (b *batcher) submit(ctx context.Context, env *envelope.Envelope) DeliveryReport {
	b.mu.Lock()
	ob := b.cur
	if ob == nil {
		ob = &openBatch{ctx: ctx, done: make(chan struct{})}
		b.cur = ob
		ob.timer = time.AfterFunc(b.wait, func() { b.flush(ob) })
	}
	idx := len(ob.members)
	ob.members = append(ob.members, env)
	ob.bytes += len(env.Body)
	full := len(ob.members) >= b.max || ob.bytes >= b.maxBytes
	if full {
		// Closed to new members now, not when flush runs: a submit in
		// between would make the batch bigger than max_messages.
		b.cur = nil
	}
	b.mu.Unlock()
	if full {
		b.flush(ob)
	}
	if holdsSlot(ctx) {
		<-b.e.sem
		defer func() { b.e.sem <- struct{}{} }()
	}
	select {
	case <-ob.done:
		return ob.reports[idx]
	case <-ctx.Done():
		// The batch still delivers the message; the caller just stops waiting
		// (the relay is shutting down past its grace).
		return DeliveryReport{Destination: b.name, Required: b.e.dests[b.name].required, Err: ctx.Err(), Error: ctx.Err().Error()}
	}
}

// now delivers env as a batch of its own, at once: hikyaku send, dlq
// replay --to and chaos hand one message at a time, and the destination
// still gets the batch format it expects.
func (b *batcher) now(ctx context.Context, env *envelope.Envelope) DeliveryReport {
	return b.e.deliverBatch(ctx, b.name, b.format, []*envelope.Envelope{env})[0]
}

// flush closes ob to new members and delivers it.
func (b *batcher) flush(ob *openBatch) {
	b.mu.Lock()
	if ob.flushed {
		b.mu.Unlock()
		return
	}
	ob.flushed = true
	if b.cur == ob {
		b.cur = nil
	}
	ob.timer.Stop()
	b.mu.Unlock()
	go func() {
		ob.reports = b.e.deliverBatch(ob.ctx, b.name, b.format, ob.members)
		close(ob.done)
	}()
}

type slotKey struct{}

// withSlot marks ctx as run by a worker holding a slot of e.sem.
func withSlot(ctx context.Context) context.Context { return context.WithValue(ctx, slotKey{}, true) }

func holdsSlot(ctx context.Context) bool { v, _ := ctx.Value(slotKey{}).(bool); return v }

type batchKey struct{}

// batchMembers are the members of the batch being attempted, for sinks with
// a native batch call.
func batchMembers(ctx context.Context) []*envelope.Envelope {
	m, _ := ctx.Value(batchKey{}).([]*envelope.Envelope)
	return m
}

// deliverBatch is deliver for a batch: the chain of name, walked once for
// all the members still pending. It returns one report per member.
func (e *Engine) deliverBatch(ctx context.Context, name, format string, members []*envelope.Envelope) []DeliveryReport {
	reps := make([]DeliveryReport, len(members))
	chain := e.chainOf(name)
	for i := range reps {
		reps[i].Destination = name
		if len(chain) > 0 {
			reps[i].Required = chain[0].required
		}
	}
	if len(chain) == 0 {
		for i := range reps {
			reps[i].Err = fmt.Errorf("destination %q is not declared", name)
			reps[i].Error = reps[i].Err.Error()
		}
		return reps
	}
	if dl := chain[0].deadline; dl > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dl)
		defer cancel()
	}
	ctx, span := metrics.Tracer().Start(ctx, "hikyaku.deliver_batch", trace.WithAttributes(
		attribute.String("hikyaku.destination", name), attribute.Int("hikyaku.batch_size", len(members))))
	defer span.End()
	if e.m != nil {
		e.m.BatchSize.WithLabelValues(name).Observe(float64(len(members)))
	}
	batchID := envelope.NewID()
	pending := make([]int, len(members))
	for i := range pending {
		pending[i] = i
	}
	var hops []Hop
	attempts := 0
	for i, d := range chain {
		if len(pending) == 0 {
			break
		}
		start := e.now()
		var next, ok []int // next: carried to the following hop
		var envs []*envelope.Envelope
		var terr error
		for _, m := range pending {
			henv := members[m].Clone()
			keep, ferr, err := applyChain(d.transforms, henv)
			if ferr != "" {
				reps[m].FilterError = ferr
				e.filterFailed(scopeDestination, d.name, members[m], ferr)
			}
			switch {
			case err != nil:
				terr = err
				reps[m].Err = fmt.Errorf("destination %s: transform: %w", d.name, err)
				next = append(next, m)
			case !keep:
				reps[m].Filtered, reps[m].ServedBy, reps[m].Err = true, d.name, nil
			default:
				henv.SetHeader(d.idemHeader, idempotencyKey(members[m]))
				ok = append(ok, m)
				envs = append(envs, henv)
			}
		}
		if terr != nil {
			hops = append(hops, Hop{Destination: d.name, Outcome: outcomeError, Error: "transform: " + terr.Error()})
		}
		reason := outcomePermanent
		switch {
		case len(envs) > 0:
			combined := combineBatch(batchID, envs, format)
			combined.SetHeader(d.idemHeader, batchID)
			r := e.tryHop(context.WithValue(ctx, batchKey{}, envs), d, combined)
			hop := Hop{Destination: d.name, Endpoint: r.endpoint, Attempts: r.attempts, Outcome: r.outcome, Duration: e.now().Sub(start)}
			attempts += r.attempts
			if r.err == nil {
				hops = append(hops, hop)
				ep := r.endpoint
				if r.result != nil && r.result.Endpoint != "" {
					ep = r.result.Endpoint
				}
				for _, m := range ok {
					reps[m].ServedBy, reps[m].Endpoint, reps[m].Result, reps[m].Err = d.name, ep, r.result, nil
				}
				break
			}
			hop.Error = r.err.Error()
			herr := fmt.Errorf("destination %s: %w", d.name, r.err)
			for _, m := range ok {
				reps[m].Err = herr
			}
			next = append(next, ok...)
			slices.Sort(next)
			reason = r.outcome
			if ctx.Err() != nil {
				hop.Outcome = outcomeDeadline
				hops = append(hops, hop)
				e.fallbackHop(chain, i, outcomeDeadline)
				pending = nil
				continue
			}
			hops = append(hops, hop)
		case terr == nil:
			hops = append(hops, Hop{Destination: d.name, Outcome: outcomeFiltered})
		}
		if len(next) > 0 {
			e.fallbackHop(chain, i, reason)
		}
		pending = next
	}
	failed := false
	for i := range reps {
		reps[i].Hops, reps[i].Attempts = hops, attempts
		if reps[i].Err != nil {
			reps[i].Error = reps[i].Err.Error()
			reps[i].ServedBy, reps[i].Result = "", nil
			failed = true
		}
	}
	if failed {
		span.SetStatus(codes.Error, "some batch members failed")
	}
	return reps
}

// combineBatch builds the one envelope a sink without a native batch call
// gets: the members' bodies as NDJSON (one per line) or a JSON array.
func combineBatch(id string, envs []*envelope.Envelope, format string) *envelope.Envelope {
	var body bytes.Buffer
	contentType := contentNDJSON
	if format == "json_array" {
		contentType = "application/json"
		body.WriteByte('[')
		for i, m := range envs {
			if i > 0 {
				body.WriteByte(',')
			}
			if json.Valid(m.Body) {
				body.Write(bytes.TrimSpace(m.Body))
			} else {
				s, _ := json.Marshal(string(m.Body))
				body.Write(s)
			}
		}
		body.WriteByte(']')
	} else {
		for _, m := range envs {
			body.Write(bytes.TrimRight(m.Body, "\r\n"))
			body.WriteByte('\n')
		}
	}
	first := envs[0]
	out := envelope.New(first.Source, "batch", body.Bytes())
	out.ID = id
	out.Subject = first.Subject
	out.ContentType = contentType
	out.SetAttribute("batch.size", len(envs))
	out.SetAttribute("batch.first_id", first.ID)
	return out
}
