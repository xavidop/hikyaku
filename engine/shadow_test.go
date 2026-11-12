package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/metrics"
)

const shadowSpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: crm, type: fault, retry: { attempts: 1 } }
  - { name: crm-v2, type: fault, shadow: true, SAMPLE retry: { attempts: 1 }, circuit_breaker: { enabled: false } }
dead_letter: { type: fault }
routes: [{ name: orders, from: [in], to: [crm, crm-v2] }]
`

func shadowEngine(t *testing.T, sample string, sinks ...*scripted) (*Engine, *metrics.Metrics) {
	t.Helper()
	setSinks(t, sinks...)
	reg := metrics.New()
	if sample != "" {
		sample = "sample: " + sample + ","
	}
	return build(t, strings.Replace(shadowSpec, "SAMPLE", sample, 1), Options{Metrics: reg}), reg
}

func TestShadowGetsACopyOnTheSide(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2"}, &scripted{name: "dead_letter"}
	e, reg := shadowEngine(t, "", crm, v2, dlq)

	rep := e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	require.Len(t, rep.Routes, 1)
	assert.Equal(t, []string{"crm-v2"}, rep.Routes[0].Shadowed)
	require.Len(t, rep.Routes[0].Deliveries, 1, "the shadow is not one of the route's deliveries")
	assert.Equal(t, "crm", rep.Routes[0].Deliveries[0].Destination)

	eventually(t, func() bool { return v2.count() == 1 }, "the shadow gets its copy")
	cp := v2.call(0)
	assert.JSONEq(t, `{"id":1}`, string(cp.Body))
	assert.Equal(t, "true", cp.Header(ShadowHeader))
	shadow, _ := cp.Attribute(ShadowAttribute)
	assert.Equal(t, true, shadow)
	// The real destination's message says nothing of a shadow.
	assert.Empty(t, crm.call(0).Header(ShadowHeader))
	eventually(t, func() bool {
		return testutil.ToFloat64(reg.Shadow.WithLabelValues("crm-v2", shadowDelivered)) == 1
	}, "the copy is counted")
}

func TestShadowFailureChangesNothing(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2", final: err503}, &scripted{name: "dead_letter"}
	e, reg := shadowEngine(t, "", crm, v2, dlq)

	rep := e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	eventually(t, func() bool {
		return testutil.ToFloat64(reg.Shadow.WithLabelValues("crm-v2", shadowFailed)) == 1
	}, "the failure is counted")
	assert.Zero(t, dlq.count(), "a failed copy is not dead-lettered")
	assert.True(t, e.Health(context.Background()).Ready)
}

func TestShadowDoesNotHoldTheMessageUp(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2", delay: time.Minute}, &scripted{name: "dead_letter"}
	e, _ := shadowEngine(t, "", crm, v2, dlq)

	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	assert.Less(t, time.Since(start), 5*time.Second)

	// Closing ends the copy still in flight.
	e.shadows.stop(10 * time.Millisecond)
}

func TestShadowSample(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2"}, &scripted{name: "dead_letter"}
	e, _ := shadowEngine(t, "0.2", crm, v2, dlq)

	const n = 2000
	want := 0
	for i := range n {
		m := msg("in", `{}`)
		m.ID = fmt.Sprintf("msg-%d", i)
		if sampled(m.ID, "crm-v2", 0.2) {
			want++
		}
		rep := e.Inject(context.Background(), m)
		// The same message gets the same answer every time.
		assert.Equal(t, sampled(m.ID, "crm-v2", 0.2), len(rep.Routes[0].Shadowed) == 1)
	}
	assert.Equal(t, n, crm.count())
	assert.InDelta(t, 0.2*n, want, 0.05*n, "about a fifth of the messages")
	e.shadows.stop(5 * time.Second)
	got := 0
	for _, o := range []string{shadowDelivered, shadowDropped} {
		got += int(testutil.ToFloat64(e.m.Shadow.WithLabelValues("crm-v2", o)))
	}
	assert.Equal(t, want, got)
}

func TestSampled(t *testing.T) {
	assert.True(t, sampled("a", "d", 1))
	assert.Equal(t, sampled("a", "d", 0.5), sampled("a", "d", 0.5))
	in := 0
	for i := range 10000 {
		if sampled(fmt.Sprintf("id-%d", i), "d", 0.05) {
			in++
		}
	}
	assert.InDelta(t, 500, in, 150)
}

func TestShadowDropsWhenTooManyAreInFlight(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2", delay: time.Minute}, &scripted{name: "dead_letter"}
	e, reg := shadowEngine(t, "", crm, v2, dlq)
	for range maxShadows + 3 {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	assert.Equal(t, 3.0, testutil.ToFloat64(reg.Shadow.WithLabelValues("crm-v2", shadowDropped)))
	e.shadows.stop(10 * time.Millisecond)
}

func TestShadowInADryRun(t *testing.T) {
	crm, v2, dlq := &scripted{name: "crm"}, &scripted{name: "crm-v2"}, &scripted{name: "dead_letter"}
	e, _ := shadowEngine(t, "0.25", crm, v2, dlq)
	reps, err := e.DryRun(context.Background(), "in", msg("in", `{}`))
	require.NoError(t, err)
	require.Len(t, reps, 1)
	require.Len(t, reps[0].Plans, 2)
	assert.False(t, reps[0].Plans[0].Shadow)
	assert.True(t, reps[0].Plans[1].Shadow)
	assert.Equal(t, 0.25, reps[0].Plans[1].Sample)
	assert.False(t, reps[0].Plans[1].Required)
	assert.Zero(t, v2.count())
}
