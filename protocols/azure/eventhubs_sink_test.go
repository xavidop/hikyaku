package azure

import (
	"context"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestEHSink(t *testing.T, ep config.Endpoint) (*ehSink, *fakeProducer) {
	t.Helper()
	if ep.Azure == nil {
		ep.Azure = connStr()
	}
	if ep.Topic == "" {
		ep.Topic = "telemetry"
	}
	spec := plugin.Spec{Name: "out", Kind: kindEventHubs, Config: &ep}
	opts, err := parseEHSink(spec)
	require.NoError(t, err)
	p := &fakeProducer{maxPerBatch: 1000}
	return buildEHSink(spec, opts, p), p
}

func TestEHSinkDeliver(t *testing.T) {
	s, p := newTestEHSink(t, config.Endpoint{Key: "{{ .json.device }}", Headers: map[string]string{"X-Site": "lab"}})
	env := envelope.New("in", "http", []byte(`{"device":"d-1"}`))
	env.ID = "ev-1"
	env.ContentType = "application/json"
	env.Headers = map[string][]string{"Idempotency-Key": {"ev-1"}, "Cookie": {"c"}, "X-Trace": {"t"}}
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "ns.servicebus.windows.net/telemetry", res.Endpoint)
	require.Len(t, p.batches, 1, "one batch of one")
	b := p.batches[0]
	assert.Equal(t, "d-1", *b.opts.PartitionKey)
	require.Len(t, b.events, 1)
	ev := b.events[0]
	assert.Equal(t, "ev-1", *ev.MessageID)
	assert.Equal(t, "application/json", *ev.ContentType)
	assert.Equal(t, env.Body, ev.Body)
	assert.Equal(t, map[string]any{"Idempotency-Key": "ev-1", "X-Trace": "t", "X-Site": "lab"}, ev.Properties)

	// No key: the service picks the partition.
	s, p = newTestEHSink(t, config.Endpoint{})
	_, err = s.Deliver(context.Background(), envelope.New("in", "http", []byte("x")))
	require.NoError(t, err)
	assert.Nil(t, p.batches[0].opts.PartitionKey)
	assert.Nil(t, p.batches[0].events[0].ContentType)
}

func TestEHSinkBatches(t *testing.T) {
	s, p := newTestEHSink(t, config.Endpoint{Key: "{{ .json.k }}"})
	p.maxPerBatch = 2
	var envs []*envelope.Envelope
	for i, k := range []string{"x", "y", "x", "x", "y", "", "x"} {
		env := envelope.New("in", "http", []byte(`{"k":"`+k+`"}`))
		env.ID = k + string(rune('0'+i))
		envs = append(envs, env)
	}
	_, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	type sent struct {
		key string
		ids []string
	}
	var got []sent
	for _, b := range p.batches {
		var g sent
		if b.opts.PartitionKey != nil {
			g.key = *b.opts.PartitionKey
		}
		for _, ev := range b.events {
			g.ids = append(g.ids, *ev.MessageID)
		}
		got = append(got, g)
	}
	assert.Equal(t, []sent{{"x", []string{"x0", "x2"}}, {"x", []string{"x3", "x6"}}, {"y", []string{"y1", "y4"}}, {"", []string{"5"}}}, got,
		"one partition key per batch, in order; a full batch is sent and a new one started")

	big := envelope.New("in", "http", []byte(strings.Repeat("b", 2000)))
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{big})
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"eventhubs:message_too_large"}, pe.Tags)
	assert.ErrorIs(t, err, azeventhubs.ErrEventDataTooLarge)

	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{envelope.New("in", "http", []byte(`{"k":{"a":1}}`))})
	require.NoError(t, err, "a key renders whatever the template gives")

	s, _ = newTestEHSink(t, config.Endpoint{Key: `{{ index .json "a" "b" }}`})
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{envelope.New("in", "http", []byte(`{}`))})
	assert.Equal(t, []string{"template"}, plugin.Tags(err))
}

func TestEHSinkErrorsHealthClose(t *testing.T) {
	s, p := newTestEHSink(t, config.Endpoint{})
	ctx := context.Background()
	env := envelope.New("in", "http", []byte("x"))

	p.sendErr = &amqp.Error{Condition: "com.microsoft:server-busy", Description: "throttled"}
	_, err := s.Deliver(ctx, env)
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.RateLimited, pe.Class)
	assert.Equal(t, throttleWait, pe.RetryAfter)
	assert.Equal(t, []string{"eventhubs:server-busy", "429"}, plugin.Tags(err))

	p.sendErr = &azeventhubs.Error{Code: azeventhubs.ErrorCodeConnectionLost}
	_, err = s.Deliver(ctx, env)
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Transient, pe.Class)
	assert.Equal(t, []string{"eventhubs:connlost", "connect"}, pe.Tags)

	p.sendErr = &azeventhubs.Error{Code: azeventhubs.ErrorCodeUnauthorizedAccess}
	_, err = s.Deliver(ctx, env)
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, plugin.Permanent, pe.Class)
	p.sendErr = nil

	require.NoError(t, s.Health(ctx))
	p.propsErr = &amqp.Error{Condition: amqp.ErrCondNotFound}
	assert.ErrorContains(t, s.Health(ctx), "ns.servicebus.windows.net/telemetry")
	p.propsErr = nil

	require.NoError(t, s.Close(ctx))
	require.NoError(t, s.Close(ctx))
	assert.Equal(t, 1, p.closed)
	assert.ErrorIs(t, s.Health(ctx), errClosed)
	_, err = s.Deliver(ctx, env)
	assert.ErrorIs(t, err, errClosed)
}
