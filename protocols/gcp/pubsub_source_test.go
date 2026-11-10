package gcp

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestPubSubSource(t *testing.T, cfg config.Source) *PubSubSource {
	t.Helper()
	cfg.Name, cfg.Type = "in", kindPubSub
	s, err := newPubSubSource(context.Background(), plugin.Spec{Name: "in", Kind: kindPubSub, Config: &cfg})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// pubsubFixture is a fake server with topic t and subscription s.
func pubsubFixture(t *testing.T, mod func(*pubsubpb.Subscription)) (*pstest.Server, string, string) {
	t.Helper()
	srv := newPubSubServer(t)
	admin := psAdmin(t, srv)
	topic := createTopic(t, admin, "t")
	sub := createSub(t, admin, "s", topic, mod)
	return srv, topic, sub
}

func acks(srv *pstest.Server, id string) int {
	if m := srv.Message(id); m != nil {
		return m.Acks
	}
	return -1
}

// nacked reports whether the server got a nack (a zero deadline) for id.
func nacked(srv *pstest.Server, id string) bool {
	m := srv.Message(id)
	if m == nil {
		return false
	}
	for _, ma := range m.Modacks {
		if ma.AckDeadline == 0 {
			return true
		}
	}
	return false
}

func TestPubSubSourceReceivesAndAcksWhenFinal(t *testing.T) {
	srv, topic, sub := pubsubFixture(t, nil)
	id := srv.Publish(topic, []byte(`{"order":1}`), map[string]string{
		"Idempotency-Key": "key-1", "subject": "orders", "content-type": "application/vnd.order+json",
		"X-Trace": "t1", "hikyaku-internal": "x",
	})
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	out, _ := run(t, s)

	env := recv(t, out)
	assert.Equal(t, "key-1", env.ID, "the Idempotency-Key attribute is the envelope ID")
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, kindPubSub, env.Protocol)
	assert.Equal(t, "orders", env.Subject)
	assert.Equal(t, "application/vnd.order+json", env.ContentType)
	assert.JSONEq(t, `{"order":1}`, string(env.Body))
	assert.Equal(t, []string{"t1"}, env.Headers["X-Trace"])
	assert.Equal(t, []string{"orders"}, env.Headers["subject"])
	assert.NotContains(t, env.Headers, "Idempotency-Key")
	assert.NotContains(t, env.Headers, "hikyaku-internal")
	assert.Equal(t, id, env.Attributes["pubsub.message_id"])
	assert.Equal(t, sub, env.Attributes["pubsub.subscription"])
	_, err := time.Parse(time.RFC3339Nano, env.Attributes["pubsub.publish_time"].(string))
	assert.NoError(t, err)
	assert.NotContains(t, env.Attributes, "ordering_key", "a message without an ordering key is not ordered")
	assert.NotContains(t, env.Attributes, "pubsub.delivery_attempt")

	assert.Equal(t, 0, acks(srv, id), "not acked before the relay is done")
	settle(env, envelope.Delivered)
	require.Eventually(t, func() bool { return acks(srv, id) == 1 }, wait, tick)
}

func TestPubSubSourceDefaults(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "projects/" + testProject + "/subscriptions/s",
		GCP: &config.GCP{Endpoint: srv.Addr}})
	out, _ := run(t, s)

	cases := map[string]string{`{"a":1}`: contentJSON, "plain text": contentText, "\xff\xfe": contentBinary}
	for body, ct := range cases {
		id := srv.Publish(topic, []byte(body), nil)
		env := recv(t, out)
		assert.Equal(t, id, env.ID, "without Idempotency-Key the message ID is the envelope ID")
		assert.Equal(t, "s", env.Subject, "the subscription ID is the subject")
		assert.Equal(t, ct, env.ContentType, body)
		settle(env, envelope.Unrouted)
		require.Eventually(t, func() bool { return acks(srv, id) == 1 }, wait, tick, "unrouted is final")
	}
}

func TestPubSubSourceNacksFailedForRedelivery(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	id := srv.Publish(topic, []byte("retry me"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	out, _ := run(t, s)

	first := recv(t, out)
	settle(first, envelope.Failed)
	again := recv(t, out)
	assert.Equal(t, first.ID, again.ID, "Pub/Sub redelivers the nacked message")
	assert.Equal(t, 0, acks(srv, id))
	assert.GreaterOrEqual(t, srv.Message(id).Deliveries, 2)

	settle(again, envelope.DeadLettered)
	require.Eventually(t, func() bool { return acks(srv, id) == 1 }, wait, tick, "dead-lettered is final")
}

func TestPubSubSourceOrderingKeys(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, func(s *pubsubpb.Subscription) { s.EnableMessageOrdering = true })
	for _, b := range []string{"1", "2", "3"} {
		srv.PublishOrdered(topic, []byte(b), nil, "customer-7")
	}
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv), Prefetch: 10})
	out, _ := run(t, s)

	for _, want := range []string{"1", "2", "3"} {
		env := recv(t, out)
		assert.Equal(t, want, string(env.Body))
		assert.Equal(t, "customer-7", env.Attributes["ordering_key"])
		assert.Equal(t, "customer-7", env.Attributes["pubsub.ordering_key"])
		quiet(t, out, 150*time.Millisecond) // the next one waits for this one to settle
		settle(env, envelope.Delivered)
	}
}

func TestPubSubSourceDeliveryAttempt(t *testing.T) {
	srv := newPubSubServer(t)
	admin := psAdmin(t, srv)
	topic := createTopic(t, admin, "t")
	dead := createTopic(t, admin, "dead")
	createSub(t, admin, "s", topic, func(s *pubsubpb.Subscription) {
		s.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dead, MaxDeliveryAttempts: 5}
	})
	srv.Publish(topic, []byte("x"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	out, _ := run(t, s)

	first := recv(t, out)
	assert.Equal(t, 1, first.Attributes["pubsub.delivery_attempt"])
	settle(first, envelope.Failed)
	second := recv(t, out)
	assert.Equal(t, 2, second.Attributes["pubsub.delivery_attempt"])
	settle(second, envelope.Delivered)
}

func TestPubSubSourceAcksAndDropsOversizeMessages(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	big := srv.Publish(topic, []byte("far too long"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv), MaxBodyBytes: 4})
	out, _ := run(t, s)

	require.Eventually(t, func() bool { return acks(srv, big) == 1 }, wait, tick, "acked, or it would come back forever")
	small := srv.Publish(topic, []byte("ok"), nil)
	env := recv(t, out)
	assert.Equal(t, small, env.ID)
	settle(env, envelope.Delivered)
}

func TestPubSubSourceAckAcceptedAcksWhenQueued(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	id := srv.Publish(topic, []byte("x"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv), Ack: "accepted"})
	out, _ := run(t, s)

	env := recv(t, out)
	env.Queued()
	require.Eventually(t, func() bool { return acks(srv, id) == 1 }, wait, tick)
	settle(env, envelope.Failed) // too late: the message is acked
	quiet(t, out, 200*time.Millisecond)
}

func TestPubSubSourceShutdownKeepsAckingInFlight(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	good := srv.Publish(topic, []byte("a"), nil)
	bad := srv.Publish(topic, []byte("b"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	out, stop := run(t, s)

	inflight := map[string]*envelope.Envelope{}
	for range 2 {
		env := recv(t, out)
		inflight[env.ID] = env
	}
	stop() // Start returns although two messages are in flight

	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the messages in flight were settled: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	settle(inflight[good], envelope.Delivered)
	settle(inflight[bad], envelope.Failed)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("Close did not return once the messages were settled")
	}
	// Both reached the server before Close returned. (Pub/Sub redelivers the
	// nacked one; how soon depends on whether it pushed it back onto the
	// closing stream, which TestPubSubSourceNacksFailedForRedelivery covers
	// with a live consumer.)
	assert.Equal(t, 1, acks(srv, good), "the ack after shutdown reached the server")
	assert.True(t, nacked(srv, bad), "the nack after shutdown reached the server")
	assert.Equal(t, 0, acks(srv, bad))
}

func TestPubSubSourceShutdownNacksWhatTheRelayNeverTook(t *testing.T) {
	srv, topic, _ := pubsubFixture(t, nil)
	id := srv.Publish(topic, []byte("x"), nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	stop := runOut(t, s, make(chan *envelope.Envelope)) // nobody reads
	// The client's receipt (a modack) says it read the message off the
	// stream: it now holds it, waiting to hand it over.
	require.Eventually(t, func() bool { m := srv.Message(id); return m != nil && len(m.Modacks) > 0 }, wait, tick)
	stop()
	closeWithin(t, s, wait)
	assert.True(t, nacked(srv, id), "nacked at shutdown, so Pub/Sub redelivers it")
	assert.Equal(t, 0, acks(srv, id))
}

func TestPubSubSourceGivesUpAfterMaxAttempts(t *testing.T) {
	srv := newPubSubServer(t)
	s := newTestPubSubSource(t, config.Source{Subscription: "missing", GCP: psGCP(srv),
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}})
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 2 failed receives in a row")
	assert.Contains(t, err.Error(), "NotFound")
	assert.Error(t, s.Health(context.Background()))
}

func TestPubSubSourceHealth(t *testing.T) {
	srv, _, _ := pubsubFixture(t, nil)
	ok := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	assert.NoError(t, ok.Health(context.Background()))

	missing := newTestPubSubSource(t, config.Source{Subscription: "nope", GCP: psGCP(srv)})
	err := missing.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestPubSubSourceCloseWithoutStart(t *testing.T) {
	srv, _, _ := pubsubFixture(t, nil)
	s := newTestPubSubSource(t, config.Source{Subscription: "s", GCP: psGCP(srv)})
	closeWithin(t, s, time.Second)
	closeWithin(t, s, time.Second)
	assert.Error(t, s.Start(context.Background(), make(chan *envelope.Envelope)), "a closed source does not start")
}

func TestPubSubSourceRegistered(t *testing.T) {
	Register()
	srv, _, _ := pubsubFixture(t, nil)
	cfg := config.Source{Name: "in", Type: kindPubSub, Subscription: "s", GCP: psGCP(srv)}
	src, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: kindPubSub, Config: &cfg})
	require.NoError(t, err)
	assert.Equal(t, "in", src.Name())
	require.Implements(t, (*plugin.Closer)(nil), src)
	closeWithin(t, src.(plugin.Closer), time.Second)
}
