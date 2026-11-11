package engine

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

func TestDueAt(t *testing.T) {
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	received := now.Add(-10 * time.Minute)
	at := func(body string) *envelope.Envelope {
		e := msg("in", body)
		e.ReceivedAt = received
		return e
	}
	both := &destination{delay: time.Hour, deliverAt: transform.MustTemplate("t", "{{ .json.send_at }}")}

	due, err := both.dueAt(at(`{"send_at":"2026-12-24T09:00:00+01:00"}`), now)
	require.NoError(t, err)
	assert.True(t, due.Equal(time.Date(2026, 12, 24, 8, 0, 0, 0, time.UTC)), "deliver_at wins when it renders a time")

	due, err = both.dueAt(at(`{}`), now)
	require.NoError(t, err)
	assert.Equal(t, received.Add(time.Hour), due, "without one, deliver_after counts from when the message was received")

	_, err = both.dueAt(at(`{"send_at":"tomorrow"}`), now)
	assert.ErrorContains(t, err, `deliver_at "tomorrow" is not an RFC 3339 time`)

	only := &destination{deliverAt: both.deliverAt}
	due, err = only.dueAt(at(`{}`), now)
	require.NoError(t, err)
	assert.Equal(t, now, due, "no time and no delay: it goes at once")

	assert.False(t, (&destination{}).delayed())
	assert.True(t, only.delayed())
}

// A route's message is held for the delay, without holding a worker: one
// worker serves other messages meanwhile.
func TestDelayHoldsWithoutAWorker(t *testing.T) {
	const s = `
version: 1
name: relay
server: { workers: 1 }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: later, type: fault, deliver_after: 150ms }
  - { name: now, type: fault }
routes:
  - { name: slow, from: [in], filter: 'msg.json.kind == "slow"', to: [later] }
  - { name: fast, from: [in], filter: 'msg.json.kind == "fast"', to: [now] }
`
	later, now := &scripted{name: "later"}, &scripted{name: "now"}
	setSinks(t, later, now)
	reg := metrics.New()
	var a acks
	slow := msg("in", `{"kind":"slow"}`)
	a.watch(slow)
	fast := msg("in", `{"kind":"fast"}`)
	a.watch(fast)
	setSources(t, &listSource{name: "in", msgs: []*envelope.Envelope{slow, fast}, hold: true})
	e, err := New(context.Background(), spec(t, s), Options{Metrics: reg})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- e.Run(ctx) }()

	require.Eventually(t, func() bool { return now.count() == 1 }, time.Second, 2*time.Millisecond)
	assert.Less(t, time.Since(start), 120*time.Millisecond, "the fast message did not wait behind the held one")
	assert.Equal(t, 0, later.count())
	assert.InDelta(t, 1, testutil.ToFloat64(reg.Delayed.WithLabelValues("later")), 0)

	require.Eventually(t, func() bool { return later.count() == 1 }, time.Second, 2*time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(slow.ReceivedAt), 150*time.Millisecond)
	require.Eventually(t, func() bool { return a.get(envelope.Delivered) == 2 }, time.Second, 2*time.Millisecond)
	assert.InDelta(t, 0, testutil.ToFloat64(reg.Delayed.WithLabelValues("later")), 0)
	cancel()
	require.NoError(t, <-done)
}

// A message a delay still holds at shutdown is not dead-lettered: it stays
// in the disk buffer, and the next run delivers it when it is due, without
// starting its wait over.
func TestDelaySurvivesARestartOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	yaml := `
version: 1
name: relay
buffer: { type: disk, path: "` + path + `" }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault, deliver_after: 800ms }]
dead_letter: { type: fault }
routes: [{ name: r, from: [in], to: [out] }]
`
	out, dlq := &scripted{name: "out"}, &scripted{name: "dead_letter"}
	setSinks(t, out, dlq)
	setSources(t, &listSource{name: "in", hold: true})
	reg := metrics.New()
	e, err := New(context.Background(), spec(t, yaml), Options{Metrics: reg})
	require.NoError(t, err)
	m := msg("in", `{"keep":"me"}`)
	received := m.ReceivedAt
	var nacked atomic.Bool
	m.OnAck(func(_ context.Context, o envelope.Outcome) { nacked.Store(o.Status == envelope.Failed) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	e.Buffer() <- m
	require.Eventually(t, func() bool { return testutil.ToFloat64(reg.Delayed.WithLabelValues("out")) == 1 }, time.Second, 2*time.Millisecond)

	stop := time.Now()
	cancel()
	require.NoError(t, <-done)
	assert.Less(t, time.Since(stop), 300*time.Millisecond, "a shutdown does not wait for the delay")
	assert.True(t, nacked.Load())
	assert.Equal(t, 0, out.count())
	assert.Equal(t, 0, dlq.count(), "a held message is not a dead letter")

	time.Sleep(time.Until(received.Add(500 * time.Millisecond)))
	out2 := &scripted{name: "out"}
	setSinks(t, out2, &scripted{name: "dead_letter"})
	restart := time.Now()
	run(t, yaml, &listSource{name: "in"})
	require.Equal(t, 1, out2.count(), "the next run delivers it")
	assert.Equal(t, m.ID, out2.call(0).ID)
	assert.GreaterOrEqual(t, time.Since(received), 800*time.Millisecond, "not before it is due")
	assert.Less(t, time.Since(restart), 700*time.Millisecond, "the wait did not start over: 500ms of it were already spent")
}

// deliver_at that is not a time is the message's fault: a permanent
// failure, dead-lettered like one.
func TestDeliverAtBadTimeDeadLetters(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault, deliver_at: "{{ .json.send_at }}" }]
dead_letter: { type: fault }
routes: [{ name: r, from: [in], to: [out] }]
`
	out, dlq := &scripted{name: "out"}, &scripted{name: "dead_letter"}
	setSinks(t, out, dlq)
	e := build(t, s, Options{})

	rep := e.Inject(context.Background(), msg("in", `{"send_at":"soon"}`))
	assert.Equal(t, "dead_lettered", rep.Status)
	assert.Contains(t, plugin.Tags(rep.Routes[0].Deliveries[0].Err), TagDeliverAt)
	require.Equal(t, 1, dlq.count())
	info, _ := InfoOf(dlq.call(0))
	assert.Equal(t, ReasonDelivery, info.Reason)
	assert.Contains(t, info.Error, `deliver_at "soon" is not an RFC 3339 time`)

	past := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	assert.Equal(t, "delivered", e.Inject(context.Background(), msg("in", `{"send_at":"`+past+`"}`)).Status)
	assert.Equal(t, "delivered", e.Inject(context.Background(), msg("in", `{}`)).Status, "no time: it goes at once")
	assert.Equal(t, 2, out.count())

	// hikyaku send delivers at once, whatever the time says.
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	d, err := e.DeliverTo(context.Background(), "out", msg("in", `{"send_at":"`+future+`"}`))
	require.NoError(t, err)
	assert.True(t, d.OK())
	assert.Equal(t, 3, out.count())
}

func TestDelayLintAndCheck(t *testing.T) {
	l := spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault, deliver_after: 5m }]
routes: [{ name: r, from: [in], to: [out] }]
`)
	var found bool
	for _, w := range config.Lint(l) {
		found = found || w.Path == `destination "out".deliver_after`
	}
	assert.True(t, found, "a delay with the memory buffer is worth a warning")
	l.Destinations[0].DeliverAt = "{{ .json.send_at "
	assert.Contains(t, Check(l).Error(), `destination "out".deliver_at`)
}
