//go:build integration

package nats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// image is the server the integration suite runs against (started with
// -js by the testcontainers module).
const image = "nats:2"

// startNATS runs a NATS server with JetStream for the test and returns its
// URL.
func startNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcnats.Run(ctx, image)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)
	return url
}

func TestIntegrationJetStreamRoundTrip(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(ctxT(t), jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}, Storage: jetstream.FileStorage})
	require.NoError(t, err)

	src := newTestStreamSource(t, config.Source{URL: url, Stream: "ORDERS", Subject: "orders.>", Start: "all", Reconnect: fast()})
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	require.NoError(t, src.Health(ctxT(t)))

	sink := newTestStreamSink(t, config.Endpoint{URL: url, Subject: "orders.{{ .json.region }}", Stream: "ORDERS"})
	require.NoError(t, sink.Health(ctxT(t)))
	sent := jsonEnv(`{"region":"eu","n":1}`)
	res, err := sink.Deliver(ctxT(t), sent)
	require.NoError(t, err)
	assert.Equal(t, []string{"ORDERS"}, res.Headers[resultStream])
	_, err = sink.Deliver(ctxT(t), sent) // a retry: dropped by the stream
	require.NoError(t, err)

	batch := []*envelope.Envelope{jsonEnv(`{"region":"us","n":2}`), jsonEnv(`{"region":"eu","n":3}`)}
	res, err = sink.DeliverBatch(ctxT(t), batch)
	require.NoError(t, err)
	assert.Len(t, res.Headers[resultSequence], 2)

	got := recv(t, out)
	assert.Equal(t, sent.ID, got.ID, "the envelope ID survives the hop as Nats-Msg-Id")
	assert.Equal(t, "orders.eu", got.Subject)
	assert.Equal(t, contentJSON, got.ContentType)
	assert.Equal(t, "ORDERS", got.Attributes["jetstream.stream"])
	assert.Equal(t, int64(1), got.Attributes["jetstream.sequence"])
	ack(got, envelope.Failed) // nak: it comes back
	for _, want := range []*envelope.Envelope{batch[0], batch[1]} {
		env := recv(t, out)
		assert.Equal(t, want.ID, env.ID)
		ack(env, envelope.Delivered)
	}
	again := recv(t, out)
	assert.Equal(t, sent.ID, again.ID)
	assert.Equal(t, int64(2), again.Attributes["jetstream.delivered"])
	ack(again, envelope.Delivered)
	require.Eventually(t, func() bool {
		return consumerInfo(t, js, "ORDERS", "in").NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond)
	none(t, out, 200*time.Millisecond)
}

func TestIntegrationNATSRoundTrip(t *testing.T) {
	url := startNATS(t)
	src := newTestSource(t, config.Source{URL: url, Subject: "orders.*", Queue: "relays", Reconnect: fast()})
	out, _ := run(t, src)
	require.Eventually(t, func() bool { return src.Health(context.Background()) == nil }, 10*time.Second, 10*time.Millisecond)

	sink := newTestSink(t, config.Endpoint{URL: url, Subject: "orders.{{ .json.region }}",
		Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}"}})
	require.NoError(t, sink.Health(ctxT(t)))
	sent := jsonEnv(`{"region":"eu","tenant":"acme"}`)
	// The subscription reaches the server shortly after the source
	// connects: deliver until the source gets one.
	var got *envelope.Envelope
	require.Eventually(t, func() bool {
		_, err := sink.Deliver(ctxT(t), sent)
		require.NoError(t, err)
		select {
		case got = <-out:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, sent.ID, got.ID, "the envelope ID survives the hop as Idempotency-Key")
	assert.Equal(t, "orders.eu", got.Subject)
	assert.Equal(t, "acme", got.Header("X-Tenant"))
	assert.Equal(t, "orders.eu", got.Attributes["nats.subject"])
	ack(got, envelope.Delivered)
}
