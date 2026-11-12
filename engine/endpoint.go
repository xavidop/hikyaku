package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

// Hop outcomes: why a destination (or one endpoint of it) stopped.
const (
	outcomeOK          = "ok"
	outcomeExhausted   = "exhausted"
	outcomePermanent   = "permanent"
	outcomeBreakerOpen = "breaker_open"
	outcomeBudget      = "budget"
	outcomeDeadline    = "deadline"
	outcomeFiltered    = "filtered"
	outcomeError       = "error"
)

// ErrBreakerOpen is the failure of an endpoint whose breaker is open.
var ErrBreakerOpen = errors.New("circuit breaker open")

// ErrBudgetSpent is the failure of a retry the retry budget refused.
var ErrBudgetSpent = errors.New("retry budget spent")

// endpoint is one sink with its breaker: a destination's only endpoint, or
// one member of a group.
type endpoint struct {
	dest    string // the destination it belongs to
	name    string // the destination name, or the member name
	sink    plugin.Sink
	breaker *breaker
	weight  int
	origin  string // scheme://host of an http member, for health checks

	healthy atomic.Bool
	hmu     sync.Mutex
	fails   int
	oks     int
	latency ewma
}

// ewma is an exponentially weighted moving average of latency.
type ewma struct {
	mu  sync.Mutex
	v   float64
	set bool
}

const ewmaAlpha = 0.3

func (e *ewma) observe(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	x := float64(d)
	if !e.set {
		e.v, e.set = x, true
		return
	}
	e.v = ewmaAlpha*x + (1-ewmaAlpha)*e.v
}

func (e *ewma) value() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.v
}

// attemptResult is how an endpoint fared after its retries.
type attemptResult struct {
	result   *plugin.Result
	attempts int
	outcome  string
	err      error
	endpoint string
}

// tryEndpoint delivers to one endpoint with the destination's retry policy:
// a timeout per attempt, the breaker before each one, the retry budget
// before each retry, and the wait from Retry-After or the jittered backoff.
func (e *Engine) tryEndpoint(ctx context.Context, d *destination, ep *endpoint, env *envelope.Envelope) attemptResult {
	res := attemptResult{endpoint: ep.name}
	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			res.outcome, res.err = outcomeDeadline, deadlineErr(err)
			return res
		}
		if !ep.breaker.allow() {
			res.outcome, res.err = outcomeBreakerOpen, ErrBreakerOpen
			return res
		}
		if n > 1 && !d.budget.allowRetry(ctx) {
			res.outcome, res.err = outcomeBudget, ErrBudgetSpent
			return res
		}
		if n == 1 {
			d.budget.request(ctx)
		}
		if err := e.throttle(ctx, d); err != nil {
			res.outcome, res.err = outcomeDeadline, err
			return res
		}
		res.attempts = n
		env.SetAttribute("delivery.attempt", n)
		r, dur, err := e.attemptOnce(ctx, d, ep, env, n)
		if err == nil {
			ep.latency.observe(dur)
			res.result, res.outcome, res.err = r, outcomeOK, nil
			return res
		}
		ep.latency.observe(dur + time.Second) // failures look slow to least_latency
		res.err = err
		if ctx.Err() != nil {
			res.outcome, res.err = outcomeDeadline, err
			return res
		}
		class, retryAfter := plugin.Classify(err)
		if !d.policy.retryable(class, plugin.Tags(err)) {
			res.outcome = outcomePermanent
			return res
		}
		if n >= d.policy.attempts {
			res.outcome = outcomeExhausted
			return res
		}
		wait := d.policy.waitFor(n, retryAfter, e.rand)
		e.log.Debug("delivery failed, retrying", "destination", d.name, "endpoint", ep.name, "attempt", n, "of", d.policy.attempts, "wait", wait.String(), "err", err)
		if !e.sleep(ctx, wait) {
			res.outcome = outcomeDeadline
			return res
		}
	}
}

// attemptOnce is one delivery attempt with its timeout, span and metrics.
func (e *Engine) attemptOnce(ctx context.Context, d *destination, ep *endpoint, env *envelope.Envelope, n int) (*plugin.Result, time.Duration, error) {
	actx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	actx, span := metrics.Tracer().Start(actx, "hikyaku.attempt", trace.WithAttributes(
		attribute.String("hikyaku.destination", d.name),
		attribute.String("hikyaku.endpoint", ep.name),
		attribute.Int("hikyaku.attempt", n),
	))
	defer span.End()
	start := e.now()
	var r *plugin.Result
	var err error
	if members := batchMembers(ctx); members != nil {
		if bs, ok := ep.sink.(plugin.BatchSink); ok {
			r, err = bs.DeliverBatch(actx, members)
			if errors.Is(err, plugin.ErrNoBatch) {
				r, err = ep.sink.Deliver(actx, env)
			}
		} else {
			r, err = ep.sink.Deliver(actx, env)
		}
	} else {
		r, err = ep.sink.Deliver(actx, env)
	}
	dur := e.now().Sub(start)
	if err == nil && actx.Err() != nil && ctx.Err() == nil {
		err = actx.Err() // a sink that ignored its deadline
		r = nil
	}
	if err != nil && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		err = plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	class, _ := plugin.Classify(err)
	// A permanent failure means the endpoint is up and said no: it does not
	// count against the breaker.
	ep.breaker.record(err != nil && class != plugin.Permanent)
	outcome := outcomeOK
	if err != nil {
		outcome = class.String()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	if r != nil {
		span.SetAttributes(attribute.Int("hikyaku.status", r.Status))
	}
	if e.m != nil {
		e.m.DeliveryAttempts.WithLabelValues(d.name, ep.name, outcome).Inc()
		e.m.DeliveryDuration.WithLabelValues(d.name, ep.name).Observe(dur.Seconds())
	}
	return r, dur, err
}

func deadlineErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("deadline exceeded")
	}
	return err
}
