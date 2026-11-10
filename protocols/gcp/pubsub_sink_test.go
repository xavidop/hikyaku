package gcp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestPubSubSink(t *testing.T, ep config.Endpoint) *PubSubSink {
	t.Helper()
	ep.Type = kindPubSub
	s, err := newPubSubSink(context.Background(), plugin.Spec{Name: "out", Kind: kindPubSub, Config: &ep})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func jsonEnvelope(body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.ContentType = contentJSON
	return env
}

func messageID(t *testing.T, res *plugin.Result) string {
	t.Helper()
	require.NotNil(t, res)
	require.Len(t, res.Headers[resultHeader], 1)
	return res.Headers[resultHeader][0]
}

func TestPubSubSinkPublishesAttributes(t *testing.T) {
	srv := newPubSubServer(t)
	topic := createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv),
		Headers: map[string]string{"X-Static": "v", "X-Kind": "{{ .json.kind }}"}})

	env := jsonEnvelope(`{"kind":"order"}`)
	env.Subject = "/orders"
	env.Headers = map[string][]string{
		"X-Trace": {"a", "b"}, "Authorization": {"Bearer secret"}, "Content-Type": {contentJSON},
		"Idempotency-Key": {"idem-1"}, "googclient_traceparent": {"x"},
	}
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, topic, res.Endpoint)

	m := srv.Message(messageID(t, res))
	require.NotNil(t, m)
	assert.Equal(t, topic, m.Topic)
	assert.JSONEq(t, `{"kind":"order"}`, string(m.Data))
	assert.Equal(t, map[string]string{
		"X-Trace": "a, b", "Idempotency-Key": "idem-1", "content-type": contentJSON, "subject": "/orders",
		"X-Static": "v", "X-Kind": "order",
	}, m.Attributes, "credentials, reserved goog* keys and the headers written explicitly are not copied")
	assert.Empty(t, m.OrderingKey)
}

func TestPubSubSinkIdempotencyKeyDefaultsToTheID(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "projects/" + testProject + "/topics/t", GCP: &config.GCP{Endpoint: srv.Addr}})
	env := envelope.New("in", "http", []byte("x"))
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, env.ID, srv.Message(messageID(t, res)).Attributes["Idempotency-Key"])
}

func TestPubSubSinkOrderingKeyAndBody(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv), Key: "{{ .json.customer }}", Body: `{"c":"{{ .json.customer }}"}`})
	for _, c := range []string{"c1", "c1", "c2"} {
		res, err := s.Deliver(context.Background(), jsonEnvelope(`{"customer":"`+c+`"}`))
		require.NoError(t, err)
		m := srv.Message(messageID(t, res))
		assert.Equal(t, c, m.OrderingKey)
		assert.JSONEq(t, `{"c":"`+c+`"}`, string(m.Data))
	}
}

func TestPubSubSinkTemplatedTopic(t *testing.T) {
	srv := newPubSubServer(t)
	admin := psAdmin(t, srv)
	eu := createTopic(t, admin, "eu-events")
	us := createTopic(t, admin, "us-events")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "{{ .json.region }}-events", GCP: psGCP(srv)})
	assert.NoError(t, s.Health(context.Background()), "a templated topic is not checked")

	for region, topic := range map[string]string{"eu": eu, "us": us} {
		res, err := s.Deliver(context.Background(), jsonEnvelope(`{"region":"`+region+`"}`))
		require.NoError(t, err)
		assert.Equal(t, topic, srv.Message(messageID(t, res)).Topic)
	}
	_, err := s.Deliver(context.Background(), jsonEnvelope(`{"region":"mars"}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Permanent, class, "a missing topic is permanent")
	assert.Contains(t, tags, "pubsub:NOT_FOUND")
	assert.Contains(t, tags, "grpc:NOT_FOUND")
}

func TestPubSubSinkBatch(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv), Key: "k"})
	envs := []*envelope.Envelope{jsonEnvelope(`{"n":1}`), jsonEnvelope(`{"n":2}`), jsonEnvelope(`{"n":3}`)}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	ids := res.Headers[resultHeader]
	require.Len(t, ids, 3)
	for i, id := range ids {
		m := srv.Message(id)
		require.NotNil(t, m)
		assert.Equal(t, envs[i].ID, m.Attributes["Idempotency-Key"])
		assert.Equal(t, "k", m.OrderingKey)
	}

	empty, err := s.DeliverBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, empty.Headers)
}

func TestPubSubSinkBatchWithABadMessagePublishesNothing(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv), Headers: map[string]string{"X": "{{ index .json.items 0 }}"}})
	envs := []*envelope.Envelope{jsonEnvelope(`{"items":["a"]}`), jsonEnvelope(`{"items":[]}`)}
	_, err := s.DeliverBatch(context.Background(), envs)
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, tagTemplate)
	assert.Empty(t, srv.Messages(), "the first message was not published either")
}

// failingPublish fails the first n Publish calls with code and counts them
// all.
type failingPublish struct {
	n     int64
	code  codes.Code
	calls atomic.Int64
}

func (f *failingPublish) React(any) (bool, any, error) {
	if f.calls.Add(1) <= f.n {
		return true, nil, status.Error(f.code, "injected")
	}
	return false, nil, nil
}

func TestPubSubSinkFailureIsOneAttempt(t *testing.T) {
	r := &failingPublish{n: 1 << 30, code: codes.Unavailable}
	srv := newPubSubServer(t, pstest.ServerReactorOption{FuncName: "Publish", Reactor: r})
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv)})

	_, err := s.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Transient, class)
	assert.Subset(t, tags, []string{"pubsub:UNAVAILABLE", "grpc:UNAVAILABLE", plugin.Tag5xx})
	assert.Equal(t, int64(1), r.calls.Load(), "the client does not retry: the relay does")
}

func TestPubSubSinkResumesAPausedOrderingKey(t *testing.T) {
	r := &failingPublish{n: 1, code: codes.Unavailable}
	srv := newPubSubServer(t, pstest.ServerReactorOption{FuncName: "Publish", Reactor: r})
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv), Key: "{{ .json.k }}"})

	env := jsonEnvelope(`{"k":"a"}`)
	_, err := s.Deliver(context.Background(), env)
	class, _ := classOf(t, err)
	assert.Equal(t, plugin.Transient, class)

	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err, "the retry resumes the paused ordering key")
	assert.Equal(t, "a", srv.Message(messageID(t, res)).OrderingKey)
}

func TestPubSubSinkTemplateErrorsArePermanent(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv), Headers: map[string]string{"X": "{{ index .json.items 5 }}"}})
	_, err := s.Deliver(context.Background(), jsonEnvelope(`{"items":[]}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, tagTemplate)
}

func TestPubSubSinkHealthAndClose(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	ok := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv)})
	assert.NoError(t, ok.Health(context.Background()))

	missing := newTestPubSubSink(t, config.Endpoint{Topic: "nope", GCP: psGCP(srv)})
	err := missing.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	_, err = ok.Deliver(context.Background(), jsonEnvelope(`{}`))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	require.NoError(t, ok.Close(ctx))
	require.NoError(t, ok.Close(ctx), "closing twice is fine")
	_, err = ok.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, _ := classOf(t, err)
	assert.Equal(t, plugin.Transient, class)
	assert.ErrorIs(t, err, errClosed)
	assert.ErrorIs(t, ok.Health(context.Background()), errClosed)
}

func TestPubSubSinkTimeout(t *testing.T) {
	srv := newPubSubServer(t)
	createTopic(t, psAdmin(t, srv), "t")
	s := newTestPubSubSink(t, config.Endpoint{Topic: "t", GCP: psGCP(srv)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	_, err := s.Deliver(ctx, jsonEnvelope(`{}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Contains(t, tags, plugin.TagTimeout)
	_, err = s.DeliverBatch(ctx, []*envelope.Envelope{jsonEnvelope(`{}`)})
	class, _ = classOf(t, err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Empty(t, srv.Messages(), "an attempt already out of time publishes nothing that could land later")
}

func TestExistence(t *testing.T) {
	assert.NoError(t, existence("topic", "t", nil))
	assert.NoError(t, existence("topic", "t", status.Error(codes.PermissionDenied, "no get")),
		"may publish without reading the topic")
	assert.ErrorContains(t, existence("topic", "t", status.Error(codes.NotFound, "x")), "topic t does not exist")
	assert.ErrorContains(t, existence("subscription", "s", status.Error(codes.Unavailable, "down")), "subscription s: ")
}

func TestPubSubSinkRegistered(t *testing.T) {
	Register()
	srv := newPubSubServer(t)
	ep := config.Endpoint{Type: kindPubSub, Topic: "t", GCP: psGCP(srv)}
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: kindPubSub, Config: &ep})
	require.NoError(t, err)
	defer func() { _ = sink.Close(context.Background()) }()
	assert.Implements(t, (*plugin.BatchSink)(nil), sink)
}
