package engine

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

const filterErrSpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: crm
    type: http
    url: https://crm.example.com/hook
    transforms: [{ filter: 'msg.json.amount > 10' }]
  - { name: audit, type: http, url: https://audit.example.com }
routes:
  - { name: orders, from: [in], filter: 'msg.json.kind == "order"', to: [crm] }
  - name: paid
    from: [in]
    transforms: [{ filter: 'msg.json.paid == true' }]
    to: [audit]
`

func TestFilterErrorIsCountedAndReported(t *testing.T) {
	var logs bytes.Buffer
	m := metrics.New()
	sinks := map[string]*scripted{}
	e := build(t, filterErrSpec, Options{
		Metrics: m,
		Logger:  slog.New(slog.NewTextHandler(&logs, nil)),
		Sinks: func(dest string, spec plugin.Spec) plugin.Sink {
			sinks[dest] = &scripted{name: spec.Name}
			return sinks[dest]
		},
	})
	count := func(scope, name string) float64 {
		return testutil.ToFloat64(m.FilterErrors.WithLabelValues(scope, name))
	}

	// No key any filter reads: every filter fails, and the message is kept.
	rep := e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	require.Len(t, rep.Routes, 2)
	orders, paid := rep.Routes[0], rep.Routes[1]
	assert.Contains(t, orders.FilterError, "msg.json.kind", "the route's filter")
	assert.Contains(t, paid.FilterError, "msg.json.paid", "a filter step of the route")
	require.Len(t, orders.Deliveries, 1)
	assert.Contains(t, orders.Deliveries[0].FilterError, "msg.json.amount", "a filter step of the destination")
	require.Len(t, paid.Deliveries, 1)
	assert.Empty(t, paid.Deliveries[0].FilterError, "the route's error is not the destination's")
	assert.Equal(t, 1, sinks["crm"].count())
	assert.Equal(t, 1, sinks["audit"].count())

	assert.Equal(t, 1.0, count("route", "orders"))
	assert.Equal(t, 1.0, count("route", "paid"))
	assert.Equal(t, 1.0, count("destination", "crm"))
	assert.Zero(t, count("destination", "audit"))

	// The same failure again is counted, and warned about once only.
	e.Inject(context.Background(), msg("in", `{"id":2}`))
	assert.Equal(t, 2.0, count("route", "orders"))
	assert.Equal(t, 3, strings.Count(logs.String(), "a filter could not be evaluated"), logs.String())

	// A dry run reports it too, and counts nothing.
	routes, err := e.DryRun(context.Background(), "in", msg("in", `{"id":3}`))
	require.NoError(t, err)
	require.Len(t, routes, 2)
	assert.Contains(t, routes[0].FilterError, "msg.json.kind")
	assert.Contains(t, routes[1].FilterError, "msg.json.paid")
	assert.Equal(t, 2.0, count("route", "orders"))

	// A message every filter can read has no error.
	rep = e.Inject(context.Background(), msg("in", `{"kind":"order","paid":true,"amount":50}`))
	for _, rr := range rep.Routes {
		assert.Empty(t, rr.FilterError)
		for _, d := range rr.Deliveries {
			assert.Empty(t, d.FilterError)
		}
	}
	assert.Equal(t, 2.0, count("route", "orders"))
}
