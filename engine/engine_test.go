package engine

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const fallbackSpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: billing
    type: fault
    retry: { attempts: 3 }
    fallback: [billing-dr, billing-queue]
  - { name: billing-dr, type: fault, retry: { attempts: 2 } }
  - { name: billing-queue, type: fault }
  - { name: dashboard, type: fault, required: false, retry: { attempts: 1 } }
dead_letter: { type: fault }
routes:
  - { name: r, from: [in], to: [billing, dashboard] }
`

func TestFallbackChainReasons(t *testing.T) {
	billing := &scripted{name: "billing", final: err503}
	dr := &scripted{name: "billing-dr", final: err400}
	queue := &scripted{name: "billing-queue"}
	dash := &scripted{name: "dashboard", final: err503}
	dlq := &scripted{name: "dead_letter"}
	setSinks(t, billing, dr, queue, dash, dlq)
	m := metrics.New()
	e := build(t, fallbackSpec, Options{Metrics: m})

	var acked envelope.Outcome
	env := msg("in", `{"id":1}`)
	env.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep := e.Inject(context.Background(), env)

	assert.Equal(t, "delivered", rep.Status, "a fallback served billing; the dashboard is best-effort")
	assert.Equal(t, envelope.Delivered, acked.Status)
	d := rep.Routes[0].Deliveries[0]
	assert.Equal(t, "billing", d.Destination)
	assert.Equal(t, "billing-queue", d.ServedBy)
	assert.True(t, d.ViaFallback())
	require.Len(t, d.Hops, 3)
	assert.Equal(t, []string{"exhausted", "permanent", "ok"}, []string{d.Hops[0].Outcome, d.Hops[1].Outcome, d.Hops[2].Outcome})
	assert.Equal(t, 3, d.Hops[0].Attempts)
	assert.Equal(t, 1, d.Hops[1].Attempts, "a permanent failure is not retried")
	assert.Equal(t, 5, d.Attempts)
	assert.Equal(t, 3, billing.count())
	assert.Equal(t, 1, dr.count())
	assert.Equal(t, 1, queue.count())
	assert.Equal(t, 0, dlq.count(), "nothing dead-lettered")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Fallback.WithLabelValues("billing", "billing-dr", "exhausted")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Fallback.WithLabelValues("billing-dr", "billing-queue", "permanent")))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Dropped.WithLabelValues("dashboard")))
	assert.Equal(t, 3.0, testutil.ToFloat64(m.DeliveryAttempts.WithLabelValues("billing", "billing", "transient")))
	assert.Contains(t, acked.Replies, "billing")
}

func TestIdempotencyKeyOnEveryAttempt(t *testing.T) {
	billing := &scripted{name: "billing", script: []error{err503, err503}, final: err503}
	dr := &scripted{name: "billing-dr", script: []error{err503}}
	setSinks(t, billing, dr, &scripted{name: "billing-queue"}, &scripted{name: "dashboard"}, &scripted{name: "dead_letter"})
	e := build(t, fallbackSpec, Options{})
	env := msg("in", `{}`)
	e.Inject(context.Background(), env)
	keys := map[string]bool{}
	for i := 0; i < billing.count(); i++ {
		keys[billing.call(i).Header("Idempotency-Key")] = true
		assert.Equal(t, i+1, mustAttr(t, billing.call(i), "delivery.attempt"))
	}
	for i := 0; i < dr.count(); i++ {
		keys[dr.call(i).Header("Idempotency-Key")] = true
	}
	assert.Equal(t, map[string]bool{env.ID: true}, keys, "one key across every attempt and fallback")
}

func mustAttr(t *testing.T, e *envelope.Envelope, k string) any {
	t.Helper()
	v, ok := e.Attribute(k)
	require.True(t, ok)
	return v
}

func TestRequiredFailureDeadLetters(t *testing.T) {
	setSinks(t, &scripted{name: "billing", final: err503}, &scripted{name: "billing-dr", final: err503},
		&scripted{name: "billing-queue", final: err503}, &scripted{name: "dashboard"})
	dlq := &scripted{name: "dead_letter"}
	sinksMu.Lock()
	sinks["dead_letter"] = dlq
	sinksMu.Unlock()
	m := metrics.New()
	e := build(t, fallbackSpec, Options{Metrics: m})
	var acked envelope.Outcome
	env := msg("in", `{"id":7}`)
	env.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep := e.Inject(context.Background(), env)

	assert.Equal(t, envelope.DeadLettered, acked.Status, "acked once the failure is safely in the DLQ")
	assert.Contains(t, plugin.Tags(acked.Err), "5xx", "the outcome carries the failure behind the dead letter")
	assert.Empty(t, rep.Error, "the report's error is for a message that failed")
	assert.True(t, rep.Routes[0].Deliveries[0].DeadLettered)
	require.Equal(t, 1, dlq.count())
	info, ok := InfoOf(dlq.call(0))
	require.True(t, ok)
	assert.Equal(t, "billing", info.Destination)
	assert.Equal(t, ReasonDelivery, info.Reason)
	assert.Equal(t, 10, info.Attempts, "3 + 2 + the default 5 across the chain")
	assert.Len(t, info.Hops, 3)
	assert.Equal(t, "relay", info.Relay)
	assert.Equal(t, env.ID, dlq.call(0).ID, "the original idempotency key is kept for replay")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.DLQ.WithLabelValues("billing", "stored")))
}

// A dead letter is a span of its own, under the message's.
func TestDeadLetterSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	setSinks(t, &scripted{name: "billing", final: err503}, &scripted{name: "billing-dr", final: err503},
		&scripted{name: "billing-queue", final: err503}, &scripted{name: "dashboard"}, &scripted{name: "dead_letter"})
	e := build(t, fallbackSpec, Options{})
	env := msg("in", `{"id":7}`)
	e.Inject(context.Background(), env)

	var dl sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "hikyaku.dead_letter" {
			dl = s
		}
	}
	require.NotNil(t, dl, "a hikyaku.dead_letter span")
	attrs := map[string]string{}
	for _, a := range dl.Attributes() {
		attrs[string(a.Key)] = a.Value.String()
	}
	assert.Equal(t, env.ID, attrs["hikyaku.id"])
	assert.Equal(t, ReasonDelivery, attrs["hikyaku.reason"])
	assert.Equal(t, "billing", attrs["hikyaku.destination"])
	assert.Equal(t, "dead_letter", attrs["hikyaku.dlq.hop"])
	assert.True(t, dl.Parent().IsValid(), "under the message's span")
}

func TestDLQFallbackThenNack(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: a, type: fault, retry: { attempts: 1 } }
  - { name: spare, type: fault }
dead_letter: { type: fault, fallback: [spare] }
routes: [{ name: r, from: [in], to: [a] }]
`
	spare := &scripted{name: "spare"}
	setSinks(t, &scripted{name: "a", final: err503}, &scripted{name: "dead_letter", final: err503}, spare)
	e := build(t, s, Options{})
	var acked envelope.Outcome
	env := msg("in", `{}`)
	env.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	e.Inject(context.Background(), env)
	assert.Equal(t, envelope.DeadLettered, acked.Status)
	assert.Equal(t, 1, spare.count(), "the DLQ fell back to its fallback")

	spare.final = err503
	env2 := msg("in", `{}`)
	env2.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep := e.Inject(context.Background(), env2)
	assert.Equal(t, envelope.Failed, acked.Status, "every DLQ hop failed: nack, never ack")
	assert.False(t, acked.Status.Acked())
	assert.Equal(t, "failed", rep.Status)
	assert.Contains(t, rep.Error, "every dead-letter hop failed")
}

func TestDeadlineAcrossFallbacks(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: a, type: fault, deadline: 150ms, timeout: 1s, retry: { attempts: 10, backoff: 1ms }, fallback: [b] }
  - { name: b, type: fault }
dead_letter: { type: fault }
routes: [{ name: r, from: [in], to: [a] }]
`
	slow := &scripted{name: "a", delay: 60 * time.Millisecond, final: err503}
	b := &scripted{name: "b", delay: time.Second}
	setSinks(t, slow, b, &scripted{name: "dead_letter"})
	e := build(t, s, Options{})
	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{}`))
	assert.Less(t, time.Since(start), 900*time.Millisecond, "the deadline spans attempts and fallbacks")
	d := rep.Routes[0].Deliveries[0]
	assert.False(t, d.OK())
	assert.Equal(t, "deadline", d.Hops[len(d.Hops)-1].Outcome)
	assert.True(t, d.DeadLettered)
}

func TestBreakerOpensAndFallsBack(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: a
    type: fault
    retry: { attempts: 1 }
    circuit_breaker: { min_requests: 2, failure_ratio: 0.5, open_for: 1h }
    fallback: [b]
  - { name: b, type: fault }
routes: [{ name: r, from: [in], to: [a] }]
`
	a := &scripted{name: "a", final: err503}
	b := &scripted{name: "b"}
	setSinks(t, a, b)
	m := metrics.New()
	e := build(t, s, Options{Metrics: m})
	for i := 0; i < 5; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	assert.Equal(t, 2, a.count(), "after 2 failures the breaker opens and a is not called")
	assert.Equal(t, 5, b.count())
	assert.Equal(t, 3.0, testutil.ToFloat64(m.Fallback.WithLabelValues("a", "b", "breaker_open")))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.BreakerState.WithLabelValues("a")))
	h := e.Health(context.Background())
	assert.True(t, h.Ready, "a is required but b keeps the route working; readiness looks at each destination")
	for _, d := range h.Destinations {
		if d.Name == "a" {
			assert.False(t, d.Up)
		}
	}
}

func TestRetryBudgetSendsToFallback(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: a
    type: fault
    retry: { attempts: 5 }
    circuit_breaker: { enabled: false }
    retry_budget: { ratio: 0.01, min_per_second: 0.1 }
    fallback: [b]
  - { name: b, type: fault }
routes: [{ name: r, from: [in], to: [a] }]
`
	setSinks(t, &scripted{name: "a", final: err503}, &scripted{name: "b"})
	m := metrics.New()
	e := build(t, s, Options{Metrics: m})
	for i := 0; i < 3; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	// One free retry in the window: the first message retries once, then
	// every retry is refused and goes to the fallback.
	assert.Equal(t, 3.0, testutil.ToFloat64(m.Fallback.WithLabelValues("a", "b", "budget")))
}

func TestRetryAfterIsHonoured(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: a, type: fault, retry: { attempts: 2, backoff: 1ms, max_backoff: 1s } }
routes: [{ name: r, from: [in], to: [a] }]
`
	a := &scripted{name: "a", script: []error{plugin.StatusError(429, 120*time.Millisecond, "slow down")}}
	setSinks(t, a)
	e := build(t, s, Options{Rand: func() float64 { return 0 }})
	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{}`))
	assert.GreaterOrEqual(t, time.Since(start), 110*time.Millisecond, "the wait follows Retry-After, not the 1ms backoff")
	assert.True(t, rep.Routes[0].Deliveries[0].OK())
	assert.Equal(t, 2, a.count())
}

const groupSpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: g
    type: group
    strategy: %s
    retry: { attempts: 1 }
    members:
      - { name: m1, type: fault, weight: 3 }
      - { name: m2, type: fault, weight: 1 }
dead_letter: { type: fault }
routes: [{ name: r, from: [in], to: [g] }]
`

func groupEngine(t *testing.T, strategy string, m1, m2 *scripted) *Engine {
	setSinks(t, m1, m2)
	return build(t, sprintf(groupSpec, strategy), Options{})
}

func TestGroupFailover(t *testing.T) {
	m1 := &scripted{name: "m1", final: err503}
	m2 := &scripted{name: "m2"}
	e := groupEngine(t, "failover", m1, m2)
	rep := e.Inject(context.Background(), msg("in", `{}`))
	d := rep.Routes[0].Deliveries[0]
	assert.True(t, d.OK())
	assert.Equal(t, "m2", d.Endpoint)
	assert.Equal(t, 2, d.Attempts)
}

func TestGroupFailoverStopsOnPermanent(t *testing.T) {
	m1 := &scripted{name: "m1", final: err400}
	m2 := &scripted{name: "m2"}
	e := groupEngine(t, "failover", m1, m2)
	rep := e.Inject(context.Background(), msg("in", `{}`))
	assert.False(t, rep.Routes[0].Deliveries[0].OK())
	assert.Equal(t, 0, m2.count(), "another replica would refuse the same message")
}

func TestGroupRoundRobin(t *testing.T) {
	m1, m2 := &scripted{name: "m1"}, &scripted{name: "m2"}
	e := groupEngine(t, "round_robin", m1, m2)
	for i := 0; i < 6; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	assert.Equal(t, 3, m1.count())
	assert.Equal(t, 3, m2.count())
}

func TestGroupWeighted(t *testing.T) {
	m1, m2 := &scripted{name: "m1"}, &scripted{name: "m2"}
	e := groupEngine(t, "weighted", m1, m2)
	for i := 0; i < 8; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	assert.Equal(t, 6, m1.count(), "weights 3:1")
	assert.Equal(t, 2, m2.count())
}

func TestGroupLeastLatency(t *testing.T) {
	m1 := &scripted{name: "m1", delay: 30 * time.Millisecond}
	m2 := &scripted{name: "m2"}
	e := groupEngine(t, "least_latency", m1, m2)
	for i := 0; i < 6; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	assert.LessOrEqual(t, m1.count(), 1, "after one measurement the slow member is avoided")
	assert.GreaterOrEqual(t, m2.count(), 5)
}

func TestHedgeFirstWins(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: g
    type: group
    idempotent: true
    hedge: { after: 30ms, max: 1 }
    retry: { attempts: 1 }
    members:
      - { name: slow, type: fault }
      - { name: fast, type: fault }
routes: [{ name: r, from: [in], to: [g] }]
`
	slow := &scripted{name: "slow", delay: 2 * time.Second}
	fast := &scripted{name: "fast"}
	setSinks(t, slow, fast)
	m := metrics.New()
	e := build(t, s, Options{Metrics: m})
	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{}`))
	d := rep.Routes[0].Deliveries[0]
	assert.True(t, d.OK())
	assert.Equal(t, "fast", d.Endpoint)
	assert.Less(t, time.Since(start), time.Second, "the hedge answered; the slow member was cancelled")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Hedges.WithLabelValues("g")))
}

func TestGroupHealthChecks(t *testing.T) {
	var up atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" || !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s := `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: g
    type: group
    retry: { attempts: 1 }
    health_check: { path: /healthz, interval: 20ms, unhealthy_after: 2 }
    members:
      - { name: m1, type: http, url: "` + srv.URL + `/hook" }
      - { name: m2, type: fault }
routes: [{ name: r, from: [in], to: [g] }]
`
	m1, m2 := &scripted{name: "m1"}, &scripted{name: "m2"}
	setSinks(t, m1, m2)
	e := build(t, s, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.healthLoop(ctx, e.dests["g"])
	require.Eventually(t, func() bool { return !e.dests["g"].endpoints[0].healthy.Load() }, 2*time.Second, 10*time.Millisecond)
	e.Inject(context.Background(), msg("in", `{}`))
	assert.Equal(t, 0, m1.count(), "the unhealthy member is skipped")
	assert.Equal(t, 1, m2.count())
	up.Store(true)
	require.Eventually(t, func() bool { return e.dests["g"].endpoints[0].healthy.Load() }, 2*time.Second, 10*time.Millisecond)
	e.Inject(context.Background(), msg("in", `{}`))
	assert.Equal(t, 1, m1.count(), "recovered members are used again")
}

func TestRunDrainsAndStops(t *testing.T) {
	const s = `
version: 1
name: relay
server: { shutdown_grace: 5s, workers: 4 }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`
	out := &scripted{name: "out", delay: 20 * time.Millisecond}
	setSinks(t, out)
	var msgs []*envelope.Envelope
	var mu sync.Mutex
	acks := map[envelope.AckStatus]int{}
	for i := 0; i < 20; i++ {
		m := msg("in", `{}`)
		m.OnAck(func(_ context.Context, o envelope.Outcome) { mu.Lock(); acks[o.Status]++; mu.Unlock() })
		msgs = append(msgs, m)
	}
	setSources(t, &listSource{name: "in", msgs: msgs})
	e, err := New(context.Background(), spec(t, s), Options{Metrics: metrics.New()})
	require.NoError(t, err)
	require.NoError(t, e.Run(context.Background()), "a finite source ends the run once everything is delivered")
	assert.Equal(t, 20, out.count())
	assert.Equal(t, 20, acks[envelope.Delivered])
	assert.True(t, out.closed, "sinks are closed on the way out")
}

func TestRunShutdownFinishesInFlight(t *testing.T) {
	const s = `
version: 1
name: relay
server: { shutdown_grace: 5s }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`
	out := &scripted{name: "out", delay: 200 * time.Millisecond}
	setSinks(t, out)
	var acked atomic.Int32
	m := msg("in", `{}`)
	m.OnAck(func(_ context.Context, o envelope.Outcome) {
		if o.Status == envelope.Delivered {
			acked.Add(1)
		}
	})
	setSources(t, &listSource{name: "in", msgs: []*envelope.Envelope{m}, hold: true})
	e, err := New(context.Background(), spec(t, s), Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- e.Run(ctx) }()
	require.Eventually(t, func() bool { return out.count() == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, int32(1), acked.Load(), "the in-flight message finished within the grace period")
}

func TestRunSourceErrorStops(t *testing.T) {
	setSinks(t, &scripted{name: "out"})
	setSources(t, &listSource{name: "in", err: errors.New("bind: address in use")})
	e, err := New(context.Background(), spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`), Options{})
	require.NoError(t, err)
	assert.ErrorContains(t, e.Run(context.Background()), "source in: bind: address in use")
}

func TestFilteredUnroutedAndRoutingFailure(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }, { name: orphan, type: stdin }]
destinations: [{ name: out, type: fault }]
dead_letter: { type: fault }
routes:
  - { name: r, from: [in], to: [out], filter: 'msg.json.type != "ping"' }
  - name: rules
    from: [in]
    router: { type: rules, rules: [{ when: 'msg.json.type == "never"', to: [out] }] }
`
	dlq := &scripted{name: "dead_letter"}
	setSinks(t, &scripted{name: "out"}, dlq)
	e := build(t, s, Options{})
	var acked envelope.Outcome
	ping := msg("in", `{"type":"ping"}`)
	ping.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep := e.Inject(context.Background(), ping)
	assert.True(t, rep.Routes[0].Filtered)
	assert.Equal(t, "dead_lettered", rep.Status, "the rules route could not decide: its message is dead-lettered")
	assert.Contains(t, plugin.Tags(acked.Err), plugin.TagNoRoute, "a source can tell no route from a failed delivery")
	require.Equal(t, 1, dlq.count())
	info, _ := InfoOf(dlq.call(0))
	assert.Equal(t, ReasonNoRoute, info.Reason)
	assert.Equal(t, "rules", info.Route)

	rep = e.Inject(context.Background(), msg("orphan", `{}`))
	assert.Equal(t, "unrouted", rep.Status)
}

// A body a route's validate step refuses is dead-lettered with reason
// schema, and a source can tell it from a transform that broke. Replay runs
// the route again, so a fixed message goes through.
func TestSchemaFailureDeadLetters(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }, { name: in2, type: stdin }]
destinations:
  - { name: out, type: fault }
  - name: strict
    type: fault
    transforms: [{ validate: { schema: { type: object, required: [sku] } } }]
dead_letter: { type: fault }
routes:
  - name: orders
    from: [in]
    transforms:
      - validate:
          schema: { type: object, required: [id], properties: { id: { type: string } } }
    to: [out]
  - { name: picky, from: [in2], to: [strict] }
`
	out, strict, dlq := &scripted{name: "out"}, &scripted{name: "strict"}, &scripted{name: "dead_letter"}
	setSinks(t, out, strict, dlq)
	reg := metrics.New()
	e := build(t, s, Options{Metrics: reg})

	rep := e.Inject(context.Background(), msg("in", `{"id":"o-1"}`))
	assert.Equal(t, "delivered", rep.Status)
	require.Equal(t, 1, out.count())

	var acked envelope.Outcome
	bad := msg("in", `{"id":7}`)
	bad.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep = e.Inject(context.Background(), bad)
	assert.Equal(t, "dead_lettered", rep.Status)
	assert.Equal(t, "schema: id: got number, want string", rep.Routes[0].Error)
	assert.Contains(t, plugin.Tags(acked.Err), plugin.TagSchema)
	assert.Equal(t, 1, out.count(), "the refused message never reaches the destination")
	require.Equal(t, 1, dlq.count())
	info, _ := InfoOf(dlq.call(0))
	assert.Equal(t, ReasonSchema, info.Reason)
	assert.Equal(t, "orders", info.Route)
	assert.Empty(t, info.Destination)
	assert.InDelta(t, 1, testutil.ToFloat64(reg.SchemaInvalid.WithLabelValues("orders")), 0)

	plans, err := e.DryRun(context.Background(), "in", msg("in", `[]`))
	require.NoError(t, err)
	assert.Equal(t, "schema: got array, want object", plans[0].Error)

	// On a destination, validate fails that hop as permanent: the dead
	// letter is a delivery one, and it names the destination.
	rep = e.Inject(context.Background(), msg("in2", `{"id":"o-2"}`))
	assert.Equal(t, "dead_lettered", rep.Status)
	assert.Equal(t, 0, strict.count())
	require.Equal(t, 2, dlq.count())
	info, _ = InfoOf(dlq.call(1))
	assert.Equal(t, ReasonDelivery, info.Reason)
	assert.Equal(t, "strict", info.Destination)
	assert.Contains(t, info.Error, "schema: missing property 'sku'")
}

// on_invalid makes a refused body a routing choice: it goes to the named
// destinations with what is wrong, and nothing is dead-lettered.
func TestSchemaOnInvalidDelivers(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: fault }
  - { name: quarantine, type: fault, retry: { attempts: 1 } }
dead_letter: { type: fault }
routes:
  - name: orders
    from: [in]
    transforms:
      - headers: { set: { X-Seen: "yes" } }
      - validate:
          schema: { type: object, required: [id] }
          on_invalid: { to: [quarantine] }
    to: [out]
`
	out, quarantine, dlq := &scripted{name: "out"}, &scripted{name: "quarantine"}, &scripted{name: "dead_letter"}
	setSinks(t, out, quarantine, dlq)
	reg := metrics.New()
	e := build(t, s, Options{Metrics: reg})

	var acked envelope.Outcome
	bad := msg("in", `{"sku":"x"}`)
	bad.OnAck(func(_ context.Context, o envelope.Outcome) { acked = o })
	rep := e.Inject(context.Background(), bad)
	assert.Equal(t, "delivered", rep.Status)
	assert.Equal(t, envelope.Delivered, acked.Status)
	assert.Equal(t, 0, out.count())
	assert.Equal(t, 0, dlq.count())
	require.Equal(t, 1, quarantine.count())
	got := quarantine.call(0)
	assert.JSONEq(t, `{"sku":"x"}`, got.BodyText())
	assert.Equal(t, "yes", got.Header("X-Seen"), "the transforms before validate ran")
	why, _ := got.Attribute(SchemaErrorAttribute)
	assert.Equal(t, "schema: missing property 'id'", why)
	require.NotNil(t, rep.Routes[0].Decision)
	assert.Equal(t, "validate", rep.Routes[0].Decision.Router)
	assert.Equal(t, []string{"quarantine"}, rep.Routes[0].Decision.Destinations)
	assert.InDelta(t, 1, testutil.ToFloat64(reg.SchemaInvalid.WithLabelValues("orders")), 0)

	plans, err := e.DryRun(context.Background(), "in", msg("in", `[]`))
	require.NoError(t, err)
	assert.Empty(t, plans[0].Error)
	require.Len(t, plans[0].Plans, 1)
	assert.Equal(t, "quarantine", plans[0].Plans[0].Destination)

	// A quarantine that fails is a failed delivery like any other.
	quarantine.final = err503
	rep = e.Inject(context.Background(), msg("in", `{}`))
	assert.Equal(t, "dead_lettered", rep.Status)
	require.Equal(t, 1, dlq.count())
	info, _ := InfoOf(dlq.call(0))
	assert.Equal(t, ReasonDelivery, info.Reason)
	assert.Equal(t, "quarantine", info.Destination)

	rep = e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	assert.Equal(t, 1, out.count())
}

func TestCheckSchema(t *testing.T) {
	l := spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`)
	l.Routes[0].Transforms = []config.Transform{{Validate: &config.SchemaCheck{Schema: map[string]any{"type": "strng"}}}}
	assert.Contains(t, Check(l).Error(), `route "r".transforms[0].validate: `)
}

type historyRouter struct{ mem *store.Memory }

func (h historyRouter) Route(_ context.Context, env *envelope.Envelope) (plugin.Decision, error) {
	return plugin.Decision{
		Destinations: []string{"billing"}, Router: "ai",
		History: &plugin.HistoryNote{Store: h.mem, Route: "r", Key: "cust-1", Options: store.AppendOptions{TTL: time.Hour},
			Record: store.CallRecord{MsgID: env.ID, Route: "r", Key: "cust-1", Decision: []store.Answer{{Candidate: "billing", Probability: 0.9, Chosen: true}}}},
	}, nil
}

func TestHistoryRecordedAfterDelivery(t *testing.T) {
	billing := &scripted{name: "billing", final: err503}
	queue := &scripted{name: "billing-queue", result: &plugin.Result{Status: 202, Reroute: []string{"support"}}}
	setSinks(t, billing, &scripted{name: "billing-dr", final: err503}, queue, &scripted{name: "dashboard"}, &scripted{name: "dead_letter"})
	mem := store.NewMemory("m")
	e := build(t, fallbackSpec, Options{Routers: map[string]plugin.Router{"r": historyRouter{mem: mem}}})
	env := msg("in", `{}`)
	e.Inject(context.Background(), env)
	e.Close(context.Background()) // flushes the recorder
	recs, err := mem.Recent(context.Background(), "r", "cust-1", store.Window{})
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, env.ID, recs[0].MsgID)
	assert.Equal(t, store.OutcomeFallback, recs[0].Outcome)
	assert.Equal(t, []string{"billing-queue"}, recs[0].DeliveredTo)
	require.NotNil(t, recs[0].Feedback, "X-Hikyaku-Reroute became a correction")
	assert.Equal(t, []string{"support"}, recs[0].Feedback.Correct)
	assert.Equal(t, "destination", recs[0].Feedback.Source)
}

func TestDryRunAndDeliverTo(t *testing.T) {
	setSinks(t, &scripted{name: "billing", final: err503}, &scripted{name: "billing-dr", final: err503},
		&scripted{name: "billing-queue", final: err503}, &scripted{name: "dashboard"}, &scripted{name: "dead_letter"})
	e := build(t, fallbackSpec, Options{})
	reps, err := e.DryRun(context.Background(), "in", msg("", `{}`))
	require.NoError(t, err)
	require.Len(t, reps, 1)
	assert.Equal(t, []string{"billing", "dashboard"}, reps[0].Decision.Destinations)
	assert.Equal(t, []string{"billing", "billing-dr", "billing-queue"}, reps[0].Plans[0].Chain)
	assert.Equal(t, "closed", reps[0].Plans[0].Breakers["billing"])
	assert.Equal(t, 0, sinks["billing"].count(), "a dry run delivers nothing")
	_, err = e.DryRun(context.Background(), "nope", msg("", `{}`))
	assert.Error(t, err)

	rep, err := e.DeliverTo(context.Background(), "billing", msg("in", `{}`))
	require.NoError(t, err)
	assert.False(t, rep.OK())
	assert.True(t, rep.DeadLettered)
	_, err = e.DeliverTo(context.Background(), "ghost", msg("in", `{}`))
	assert.Error(t, err)
	_, err = e.InjectRoute(context.Background(), "ghost", msg("in", `{}`))
	assert.Error(t, err)
	assert.Equal(t, []string{"billing", "billing-dr", "billing-queue", "dashboard"}, e.DestinationNames())
	assert.Len(t, e.CheckSinks(context.Background()), 4)
}

func TestFileDLQRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dlq", "dlq.ndjson")
	s := `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault, retry: { attempts: 1 } }]
dead_letter: { type: file, path: "` + path + `" }
routes: [{ name: r, from: [in], to: [out] }]
`
	setSinks(t, &scripted{name: "out", final: err503})
	e := build(t, s, Options{})
	env := msg("in", `{"order":42}`)
	env.SetAttribute("ai.decision", map[string]any{"model": "m"})
	e.Inject(context.Background(), env)
	e.Close(context.Background())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	got, err := ReadDLQ(bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, env.ID, got[0].ID)
	assert.JSONEq(t, `{"order":42}`, got[0].BodyText())
	info, ok := InfoOf(got[0])
	require.True(t, ok)
	assert.Equal(t, "out", info.Destination)
	assert.Equal(t, 1, info.Attempts)
	_, err = ReadDLQ(bytes.NewReader([]byte("{not json")))
	assert.Error(t, err)
}

// Health reports every source; a listen source that is down makes the
// relay not ready (it cannot take the traffic sent to it), others do not.
func TestHealthReportsSources(t *testing.T) {
	setSinks(t, &scripted{name: "out"})
	setSources(t, &listSource{name: "in", health: errors.New("not listening")}, &listSource{name: "feed"})
	sp := spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }, { name: feed, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in, feed], to: [out] }]
`)
	e, err := New(context.Background(), sp, Options{})
	require.NoError(t, err)
	defer e.Close(context.Background())
	h := e.Health(context.Background())
	assert.Equal(t, map[string]string{"in": "not listening", "feed": "ok"}, h.Sources)
	assert.True(t, h.ListenersUp, "a source that does not listen does not gate readiness")
	sp.Sources[0].Mode = "listen"
	assert.False(t, e.Health(context.Background()).ListenersUp, "a listen source that is down does")
}

func TestCheckDeadLetter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.ndjson")
	setSinks(t, &scripted{name: "out"}, &scripted{name: "spare", health: errors.New("spare is down")})
	e := build(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }, { name: spare, type: fault }]
dead_letter: { type: file, path: "`+path+`", fallback: [spare, stderr] }
routes: [{ name: r, from: [in], to: [out] }]
`, Options{})
	defer e.Close(context.Background())
	got := e.CheckDeadLetter(context.Background())
	require.Len(t, got, 2, "the DLQ and the stderr hop; spare is a destination, checked by CheckSinks")
	assert.Equal(t, SinkHealth{Name: "dead_letter"}, got[0])
	assert.Equal(t, SinkHealth{Name: "stderr"}, got[1])
	assert.ErrorContains(t, e.CheckSinks(context.Background())["spare"], "spare is down")
}

func TestCheckCompileErrors(t *testing.T) {
	l := spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`)
	l.Routes[0].Filter = "msg.(("
	l.Destinations[0].Body = "{{ .json.x "
	l.Routes = append(l.Routes, l.Routes[0])
	l.Routes[1].Router = nil
	errs := Check(l)
	text := errs.Error()
	assert.Contains(t, text, `route "r".filter`)
	assert.Contains(t, text, `destination "out".body`)
}

func TestNewErrors(t *testing.T) {
	setSinks(t)
	_, err := New(context.Background(), spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: q, type: kafka, brokers: [k:9092], topic: t }]
routes: [{ name: r, from: [in], to: [q] }]
`), Options{NoSources: true})
	assert.ErrorContains(t, err, "protocol kafka is not in this build: hikyaku-lite leaves out")

	_, err = New(context.Background(), spec(t, `
version: 1
name: relay
stores: [{ name: s, type: redis, addr: "x:1" }]
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`), Options{NoSources: true})
	assert.ErrorContains(t, err, `type "redis" is not available`)
}

// The engine reports each message queued as it takes it from the source,
// and closes a source that holds a connection only after the messages in
// flight are acked (a broker consumer needs it for the acks).
func TestRunMarksQueuedAndClosesSourcesLast(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`
	setSinks(t, &scripted{name: "out", delay: 20 * time.Millisecond})
	var queued, acked, ackedAtClose atomic.Bool
	m := msg("in", `{}`)
	m.OnQueued(func() { queued.Store(true) })
	m.OnAck(func(context.Context, envelope.Outcome) { acked.Store(true) })
	closed := make(chan struct{})
	setSources(t, &listSource{name: "in", msgs: []*envelope.Envelope{m}, onClose: func() {
		ackedAtClose.Store(acked.Load())
		close(closed)
	}})
	e, err := New(context.Background(), spec(t, s), Options{})
	require.NoError(t, err)
	require.NoError(t, e.Run(context.Background()))
	<-closed
	assert.True(t, queued.Load(), "OnQueued runs when the engine takes the message")
	assert.True(t, ackedAtClose.Load(), "the source is closed after its in-flight message was acked")
}

// s3 and gcs take split: lines, which is not jq; every other split is.
func TestCheckSplitLines(t *testing.T) {
	ok := spec(t, `
version: 1
name: relay
sources: [{ name: in, type: s3, bucket: b, split: lines }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`)
	assert.Empty(t, Check(ok))
	bad, err := config.LoadBytes(context.Background(), "bad.yaml", []byte(`
version: 1
name: relay
sources: [{ name: in, type: http, url: "http://feed", split: lines }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`), config.LoadOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, Check(bad.Spec), "lines is not jq for an http poll")
}

// validate compiles a router's when like any CEL in the spec.
func TestCheckRouterWhen(t *testing.T) {
	l, err := config.LoadBytes(context.Background(), "t.yaml", []byte(`
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes:
  - name: r
    from: [in]
    router: { type: static, to: [out], when: "msg.json.n >", fallback: [{ type: static, to: [out] }] }
`), config.LoadOptions{})
	require.NoError(t, err)
	assert.Contains(t, Check(l.Spec).Error(), `route "r".router.when`)
}
