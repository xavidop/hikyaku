// Package metrics holds the Prometheus instruments of a relay (design §7.7)
// and the OpenTelemetry tracer every delivery hop and attempt is a span of.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Metrics is one registry with every instrument. Each relay process has
// one; tests make their own.
type Metrics struct {
	Registry *prometheus.Registry

	Received         *prometheus.CounterVec   // hikyaku_messages_received_total{source}
	Acked            *prometheus.CounterVec   // hikyaku_messages_acked_total{source,status}
	Unrouted         *prometheus.CounterVec   // hikyaku_unrouted_total{source}
	QueueDepth       prometheus.Gauge         // hikyaku_queue_depth (the disk buffer's messages with buffer.type: disk)
	Rejected         *prometheus.CounterVec   // hikyaku_backpressure_total{source}
	SchemaInvalid    *prometheus.CounterVec   // hikyaku_schema_invalid_total{route}
	Routed           *prometheus.CounterVec   // hikyaku_route_decisions_total{route,router}
	DeliveryAttempts *prometheus.CounterVec   // hikyaku_delivery_attempts_total{destination,endpoint,outcome}
	DeliveryDuration *prometheus.HistogramVec // hikyaku_delivery_duration_seconds{destination,endpoint}
	RateLimitWait    *prometheus.HistogramVec // hikyaku_rate_limit_wait_seconds{destination}
	Delayed          *prometheus.GaugeVec     // hikyaku_delayed_messages{destination}
	BatchSize        *prometheus.HistogramVec // hikyaku_batch_size{destination}
	ModelTokens      *prometheus.CounterVec   // hikyaku_model_tokens_total{destination,type}
	Dropped          *prometheus.CounterVec   // hikyaku_delivery_dropped_total{destination}
	Fallback         *prometheus.CounterVec   // hikyaku_fallback_total{from,to,reason}
	BreakerState     *prometheus.GaugeVec     // hikyaku_breaker_state{endpoint}: 0 closed, 1 half-open, 2 open
	BudgetRemaining  *prometheus.GaugeVec     // hikyaku_retry_budget_remaining{destination}
	Hedges           *prometheus.CounterVec   // hikyaku_hedges_total{destination}
	DLQ              *prometheus.CounterVec   // hikyaku_dlq_total{destination,outcome}
	MemberHealthy    *prometheus.GaugeVec     // hikyaku_member_healthy{destination,member}

	AIDecisions     *prometheus.CounterVec   // hikyaku_ai_decisions_total{route,outcome}
	AILatency       *prometheus.HistogramVec // hikyaku_ai_latency_seconds{route,model}
	AIModelFallback *prometheus.CounterVec   // hikyaku_ai_model_fallback_total{route,from,to,reason}
	AITokens        *prometheus.CounterVec   // hikyaku_ai_tokens_total{route,model}
	RouterFallback  *prometheus.CounterVec   // hikyaku_router_fallback_total{route,to}
	RouterSkipped   *prometheus.CounterVec   // hikyaku_router_skipped_total{route,router}
	Deduped         *prometheus.CounterVec   // hikyaku_deduped_total{source}

	HistoryReads  *prometheus.CounterVec   // hikyaku_history_reads_total{route,outcome}
	HistoryErrors *prometheus.CounterVec   // hikyaku_history_errors_total{op}
	HistoryTurns  *prometheus.HistogramVec // hikyaku_history_turns{route}
	Sticky        *prometheus.CounterVec   // hikyaku_sticky_total{route,outcome}
	Feedback      *prometheus.CounterVec   // hikyaku_feedback_total{route}

	Reloads *prometheus.CounterVec // hikyaku_reloads_total{outcome}
}

// New returns a registry with every instrument, plus the Go and process
// collectors.
func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := factory{r}
	return &Metrics{
		Registry:         r,
		Received:         f.counter("hikyaku_messages_received_total", "Messages received, by source.", "source"),
		Acked:            f.counter("hikyaku_messages_acked_total", "Messages finished, by source and ack status (delivered, dead_lettered, unrouted, filtered, failed).", "source", "status"),
		Unrouted:         f.counter("hikyaku_unrouted_total", "Messages no route took.", "source"),
		QueueDepth:       f.gauge("hikyaku_queue_depth", "Messages waiting in the buffer."),
		Rejected:         f.counter("hikyaku_backpressure_total", "Messages a source refused because the relay's buffer was full: http and openai answer 503, grpc RESOURCE_EXHAUSTED, udp drops the datagram.", "source"),
		SchemaInvalid:    f.counter("hikyaku_schema_invalid_total", "Messages a route's validate step refused: the body did not match its JSON Schema.", "route"),
		Routed:           f.counter("hikyaku_route_decisions_total", "Routing decisions, by route and the router that decided.", "route", "router"),
		DeliveryAttempts: f.counter("hikyaku_delivery_attempts_total", "Delivery attempts, by destination, endpoint and outcome.", "destination", "endpoint", "outcome"),
		DeliveryDuration: f.histogram("hikyaku_delivery_duration_seconds", "Duration of one delivery attempt.", prometheus.DefBuckets, "destination", "endpoint"),
		RateLimitWait:    f.histogram("hikyaku_rate_limit_wait_seconds", "Time an attempt waited for its destination's rate_limit. Attempts that did not wait are not counted.", []float64{.005, .025, .1, .25, .5, 1, 2.5, 5, 10, 30}, "destination"),
		Delayed:          f.gaugeVec("hikyaku_delayed_messages", "Messages a destination's deliver_after or deliver_at holds right now.", "destination"),
		BatchSize:        f.histogram("hikyaku_batch_size", "Messages per delivered batch (destination batch:).", []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000}, "destination"),
		ModelTokens:      f.counter("hikyaku_model_tokens_total", "Tokens used by model destinations (the LLM-proxy mode), by type: input or output.", "destination", "type"),
		Dropped:          f.counter("hikyaku_delivery_dropped_total", "Messages a best-effort (required: false) destination failed to take.", "destination"),
		Fallback:         f.counter("hikyaku_fallback_total", "Destination fallback hops, by reason (exhausted, permanent, breaker_open, budget, deadline).", "from", "to", "reason"),
		BreakerState:     f.gaugeVec("hikyaku_breaker_state", "Circuit breaker state per endpoint: 0 closed, 1 half-open, 2 open.", "endpoint"),
		BudgetRemaining:  f.gaugeVec("hikyaku_retry_budget_remaining", "Retries the retry budget still allows in its window.", "destination"),
		Hedges:           f.counter("hikyaku_hedges_total", "Hedged requests started.", "destination"),
		DLQ:              f.counter("hikyaku_dlq_total", "Dead letters written (outcome stored) or lost to a failing DLQ chain (outcome failed).", "destination", "outcome"),
		MemberHealthy:    f.gaugeVec("hikyaku_member_healthy", "Group member health: 1 healthy, 0 unhealthy.", "destination", "member"),
		AIDecisions:      f.counter("hikyaku_ai_decisions_total", "AI routing decisions, by outcome (chosen, low_confidence, cached, sticky, shadow, error, over_budget).", "route", "outcome"),
		AILatency:        f.histogram("hikyaku_ai_latency_seconds", "Model call latency.", []float64{.05, .1, .25, .5, 1, 2, 4, 8}, "route", "model"),
		AIModelFallback:  f.counter("hikyaku_ai_model_fallback_total", "Model fallback hops.", "route", "from", "to", "reason"),
		AITokens:         f.counter("hikyaku_ai_tokens_total", "Model tokens used.", "route", "model"),
		RouterFallback:   f.counter("hikyaku_router_fallback_total", "Router fallback hops.", "route", "to"),
		RouterSkipped:    f.counter("hikyaku_router_skipped_total", "Messages a router's when condition left out: they went to the next router with no call (no model spend for an ai router).", "route", "router"),
		Deduped:          f.counter("hikyaku_deduped_total", "Messages a source's dedupe dropped: their key was seen within the ttl.", "source"),
		HistoryReads:     f.counter("hikyaku_history_reads_total", "History reads, by outcome (hit, empty, error).", "route", "outcome"),
		HistoryErrors:    f.counter("hikyaku_history_errors_total", "History store errors, by operation.", "op"),
		HistoryTurns:     f.histogram("hikyaku_history_turns", "Past calls sent to the model with a message.", []float64{0, 1, 2, 4, 8, 16, 32}, "route"),
		Sticky:           f.counter("hikyaku_sticky_total", "Sticky routing, by outcome (follow_up, new, no_history).", "route", "outcome"),
		Feedback:         f.counter("hikyaku_feedback_total", "Corrections recorded.", "route"),
		Reloads:          f.counter("hikyaku_reloads_total", "Spec reloads, by outcome.", "outcome"),
	}
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// Tracer is the tracer of every hikyaku span; it follows the global
// provider, which internal/cli sets up from observability.otel.
func Tracer() trace.Tracer { return otel.Tracer("github.com/xavidop/hikyaku") }

type factory struct{ r *prometheus.Registry }

func (f factory) counter(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(c)
	return c
}

func (f factory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	f.r.MustRegister(g)
	return g
}

func (f factory) gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.r.MustRegister(g)
	return g
}

func (f factory) histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.r.MustRegister(h)
	return h
}
