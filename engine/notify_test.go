package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/metrics"
)

func alertOf(t *testing.T, s *scripted, i int) Alert {
	t.Helper()
	var a Alert
	require.NoError(t, json.Unmarshal(s.call(i).Body, &a))
	return a
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 2*time.Millisecond, msg)
}

const notifySpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: crm
    type: fault
    retry: { attempts: 1 }
    circuit_breaker: { enabled: false }
  - { name: ops, type: fault, retry: { attempts: 1 } }
dead_letter: { type: fault }
notify:
  - { to: [ops], every: EVERY }
routes: [{ name: orders, from: [in], to: [crm] }]
`

func notifyEngine(t *testing.T, yaml, every string, sinks ...*scripted) (*Engine, *metrics.Metrics) {
	t.Helper()
	setSinks(t, sinks...)
	reg := metrics.New()
	e := build(t, strings.ReplaceAll(yaml, "EVERY", every), Options{Metrics: reg})
	e.notifier.start() // Run does this; these tests inject
	return e, reg
}

func TestNotifyDeadLetter(t *testing.T) {
	crm, ops, dlq := &scripted{name: "crm", final: err503}, &scripted{name: "ops"}, &scripted{name: "dead_letter"}
	e, reg := notifyEngine(t, notifySpec, "1h", crm, ops, dlq)

	first := msg("in", `{"id":1}`)
	e.Inject(context.Background(), first)
	eventually(t, func() bool { return ops.count() == 1 }, "the first dead letter alerts at once")
	env := ops.call(0)
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, NotifySource, env.Source)
	ev, _ := env.Attribute(NotifyEventAttribute)
	assert.Equal(t, "dead_letter", ev)
	a := alertOf(t, ops, 0)
	assert.Equal(t, "dead_letter", a.Event)
	assert.Equal(t, "relay", a.Relay)
	assert.Equal(t, "crm", a.Destination)
	assert.Equal(t, "orders", a.Route)
	assert.Equal(t, ReasonDelivery, a.Reason)
	assert.Contains(t, a.Error, "503")
	assert.Equal(t, first.ID, a.MessageID)
	assert.Equal(t, 1, a.Count)
	assert.Nil(t, a.Since)
	assert.False(t, a.At.IsZero())

	// The next ones are counted, and sent as one alert when the relay stops.
	for i := 0; i < 3; i++ {
		e.Inject(context.Background(), msg("in", `{"id":2}`))
	}
	assert.Equal(t, 4, dlq.count())
	e.Close(context.Background())
	require.Equal(t, 2, ops.count())
	sum := alertOf(t, ops, 1)
	assert.Equal(t, 3, sum.Count)
	require.NotNil(t, sum.Since)
	assert.InDelta(t, 2, testutil.ToFloat64(reg.Notifications.WithLabelValues("dead_letter", "sent")), 0)
}

func TestNotifySendsOneAlertPerWindow(t *testing.T) {
	crm, ops, dlq := &scripted{name: "crm", final: err503}, &scripted{name: "ops"}, &scripted{name: "dead_letter"}
	e, _ := notifyEngine(t, notifySpec, "80ms", crm, ops, dlq)

	for i := 0; i < 5; i++ {
		e.Inject(context.Background(), msg("in", `{}`))
	}
	eventually(t, func() bool { return ops.count() == 2 }, "the first alert, then the window's summary")
	assert.Equal(t, 1, alertOf(t, ops, 0).Count)
	assert.Equal(t, 4, alertOf(t, ops, 1).Count)

	// A quiet window ends the hold: the next dead letter alerts at once.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 2, ops.count(), "nothing happened, nothing is sent")
	e.Inject(context.Background(), msg("in", `{}`))
	eventually(t, func() bool { return ops.count() == 3 }, "a new first alert")
	assert.Equal(t, 1, alertOf(t, ops, 2).Count)
}

func TestNotifyCircuit(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: crm
    type: fault
    retry: { attempts: 1 }
    circuit_breaker: { min_requests: 2, failure_ratio: 0.5, open_for: 20ms, half_open_max: 1 }
  - { name: ops, type: fault }
  - { name: audit, type: fault }
dead_letter: { type: fault }
notify:
  - { on: [circuit_open, circuit_closed], to: [ops], every: EVERY }
  - { on: [dead_letter], to: [audit, crm], every: EVERY }
routes: [{ name: orders, from: [in], to: [crm] }]
`
	crm := &scripted{name: "crm", script: []error{err503, err503}}
	ops, audit, dlq := &scripted{name: "ops"}, &scripted{name: "audit"}, &scripted{name: "dead_letter"}
	e, _ := notifyEngine(t, s, "1h", crm, ops, audit, dlq)

	e.Inject(context.Background(), msg("in", `{}`))
	e.Inject(context.Background(), msg("in", `{}`))
	eventually(t, func() bool { return ops.count() == 1 }, "the circuit opened")
	open := alertOf(t, ops, 0)
	assert.Equal(t, "circuit_open", open.Event)
	assert.Equal(t, "crm", open.Destination)
	assert.Equal(t, "crm", open.Endpoint)

	// Dead letters go to the second rule, and never to the destination
	// they are about.
	eventually(t, func() bool { return audit.count() == 1 }, "the dead letter alert")
	assert.Equal(t, "dead_letter", alertOf(t, audit, 0).Event)
	assert.Equal(t, 2, crm.count(), "crm got its two messages and no alert")

	// Once open_for has passed, a message that goes through closes it.
	time.Sleep(40 * time.Millisecond)
	e.Inject(context.Background(), msg("in", `{}`))
	eventually(t, func() bool { return ops.count() == 2 }, "the circuit closed")
	assert.Equal(t, "circuit_closed", alertOf(t, ops, 1).Event)
}

func TestNotifyFailureIsNotDeadLettered(t *testing.T) {
	crm, ops, dlq := &scripted{name: "crm", final: err503}, &scripted{name: "ops", final: err503}, &scripted{name: "dead_letter"}
	e, reg := notifyEngine(t, notifySpec, "1h", crm, ops, dlq)
	e.Inject(context.Background(), msg("in", `{}`))
	eventually(t, func() bool {
		return testutil.ToFloat64(reg.Notifications.WithLabelValues("dead_letter", "failed")) == 1
	}, "the failed alert is counted")
	assert.Equal(t, 1, dlq.count(), "only the message: an alert never becomes a dead letter")
}

func TestNotifyOnlyWhileRunning(t *testing.T) {
	crm, ops, dlq := &scripted{name: "crm", final: err503}, &scripted{name: "ops"}, &scripted{name: "dead_letter"}
	setSinks(t, crm, ops, dlq)
	e := build(t, strings.ReplaceAll(notifySpec, "EVERY", "1h"), Options{})
	e.Inject(context.Background(), msg("in", `{}`))
	e.Close(context.Background())
	assert.Equal(t, 1, dlq.count())
	assert.Equal(t, 0, ops.count(), "a one-shot command alerts nobody")
}
