package metrics

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsServeEveryDesignInstrument(t *testing.T) {
	m := New()
	m.DeliveryAttempts.WithLabelValues("billing", "https://billing", "ok").Inc()
	m.Fallback.WithLabelValues("billing", "billing-dr", "exhausted").Inc()
	m.BreakerState.WithLabelValues("billing").Set(2)
	m.BudgetRemaining.WithLabelValues("billing").Set(3)
	m.Hedges.WithLabelValues("support").Inc()
	m.AIModelFallback.WithLabelValues("triage", "typesafe/jev", "ollama-decisions/clef", "unavailable").Inc()
	m.RouterFallback.WithLabelValues("triage", "rules").Inc()
	m.DLQ.WithLabelValues("billing", "stored").Inc()
	m.HistoryReads.WithLabelValues("triage", "hit").Inc()
	m.HistoryErrors.WithLabelValues("read").Inc()
	m.HistoryTurns.WithLabelValues("triage").Observe(4)
	m.Sticky.WithLabelValues("triage", "follow_up").Inc()
	m.Feedback.WithLabelValues("triage").Inc()

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, name := range []string{
		"hikyaku_delivery_attempts_total", "hikyaku_fallback_total", "hikyaku_breaker_state",
		"hikyaku_retry_budget_remaining", "hikyaku_hedges_total", "hikyaku_ai_model_fallback_total",
		"hikyaku_router_fallback_total", "hikyaku_dlq_total", "hikyaku_history_reads_total",
		"hikyaku_history_errors_total", "hikyaku_history_turns", "hikyaku_sticky_total", "hikyaku_feedback_total",
		"go_goroutines",
	} {
		assert.Contains(t, string(body), name)
	}
	assert.NotNil(t, Tracer())
}

func TestSetupTracingOffWithoutEndpoint(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), TracingOptions{})
	require.NoError(t, err)
	assert.NoError(t, shutdown(context.Background()))

	shutdown, err = SetupTracing(context.Background(), TracingOptions{Endpoint: "http://127.0.0.1:4318", ServiceName: "t", SampleRatio: 0.5})
	require.NoError(t, err)
	assert.NoError(t, shutdown(context.Background()))
}
