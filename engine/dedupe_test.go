package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

const dedupeSpec = `
version: 1
name: relay
server: { workers: 1 }
sources: [{ name: in, type: stdin, dedupe: { key: "{{ .json.id }}" } }]
destinations: [{ name: out, type: fault, retry: { attempts: 1 } }]
routes: [{ name: r, from: [in], to: [out] }]
`

// Any source's dedupe: the engine drops a repeated key before routing,
// acks it as filtered and counts it.
func TestDedupeOnAnySource(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	var a acks
	ids := []string{"1", "1", "2", "1"}
	e := run(t, dedupeSpec, &listSource{name: "in", msgs: msgs(t, &a, len(ids), func(i int) string { return fmt.Sprintf(`{"id":%q}`, ids[i]) })})
	assert.Equal(t, 2, out.count())
	assert.Equal(t, 2, a.get(envelope.Delivered))
	assert.Equal(t, 2, a.get(envelope.Filtered), "a duplicate is acked as filtered, so a broker forgets it")
	assert.Equal(t, 2.0, testutil.ToFloat64(e.m.Deduped.WithLabelValues("in")))
}

// The engine counts what sources report through their Env hooks.
func TestPluginEnvCounts(t *testing.T) {
	setSinks(t, &scripted{name: "out"})
	m := metrics.New()
	e := build(t, dedupeSpec, Options{Metrics: m})
	defer e.Close(context.Background())
	env := e.pluginEnv()
	env.Refused("in")
	env.Deduped("in")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Rejected.WithLabelValues("in")), "hikyaku_backpressure_total")
	assert.Equal(t, 1.0, testutil.ToFloat64(m.Deduped.WithLabelValues("in")), "hikyaku_deduped_total")
}

// The same message coming back (a retry from the disk buffer, a replay) is
// not a duplicate of itself, and the key of a message that failed is
// forgotten, so its redelivery goes through.
func TestDedupeNeverDropsByAccident(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	setSources(t, &listSource{name: "in", hold: true})
	e, err := New(context.Background(), spec(t, dedupeSpec), Options{})
	require.NoError(t, err)
	defer e.Close(context.Background())
	ctx := context.Background()

	first := msg("in", `{"id":"1"}`)
	e.process(ctx, first)
	again := msg("in", `{"id":"1"}`)
	again.ID = first.ID
	e.process(ctx, again)
	assert.Equal(t, 2, out.count(), "the same message again is delivered again")
	assert.Equal(t, "filtered", e.process(ctx, msg("in", `{"id":"1"}`)).Status, "another message with the key is a duplicate")
	assert.NotEqual(t, "filtered", e.process(ctx, msg("in", `{}`)).Status, "a message without a key is never one")

	down := plugin.Errorf(plugin.Transient, nil, "down")
	setSinks(t, &scripted{name: "out", final: down}, &scripted{name: "dead_letter", final: down})
	failing, err := New(context.Background(), spec(t, dedupeSpec+"dead_letter: { type: fault }\n"), Options{})
	require.NoError(t, err)
	defer failing.Close(context.Background())
	assert.Equal(t, "failed", failing.process(ctx, msg("in", `{"id":"9"}`)).Status)
	assert.Equal(t, "failed", failing.process(ctx, msg("in", `{"id":"9"}`)).Status, "the failed message's key was forgotten: its redelivery is not a duplicate")
}
