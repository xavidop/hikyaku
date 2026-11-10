package gcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestGCSSink(t *testing.T, srv *fakestorage.Server, ep config.Endpoint) *GCSSink {
	t.Helper()
	ep.Type = kindGCS
	if ep.Bucket == "" {
		ep.Bucket = testBucket
	}
	ep.GCP = gcsGCP(srv)
	s, err := newGCSSink(context.Background(), plugin.Spec{Name: "out", Kind: kindGCS, Config: &ep, Env: plugin.Env{Relay: "relay"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func receivedOn(env *envelope.Envelope, day string) *envelope.Envelope {
	t, _ := time.Parse(time.DateOnly, day)
	env.ReceivedAt = t.Add(13 * time.Hour)
	return env
}

func TestGCSSinkWritesOneObjectPerMessage(t *testing.T) {
	srv := newGCSServer(t)
	s := newTestGCSSink(t, srv, config.Endpoint{Headers: map[string]string{"kind": "{{ .json.kind }}"}})
	env := receivedOn(jsonEnvelope(`{"kind":"order"}`), "2026-11-09")
	env.Headers = map[string][]string{"X-Trace": {"t1"}, "Authorization": {"Bearer secret"}, "Content-Type": {contentJSON}}

	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	key := "relay/2026/11/09/" + env.ID + ".json"
	assert.Equal(t, "gs://"+testBucket+"/"+key, res.Endpoint)
	assert.NotEmpty(t, res.Headers[headerGeneration])

	o, err := srv.GetObject(testBucket, key)
	require.NoError(t, err)
	assert.JSONEq(t, `{"kind":"order"}`, string(o.Content))
	assert.Equal(t, contentJSON, o.ContentType)
	assert.Equal(t, map[string]string{"X-Trace": "t1", "kind": "order"}, o.Metadata,
		"headers become metadata, but credentials and Content-Type")
}

func TestGCSSinkRetryOfTheSameMessageIsIdempotent(t *testing.T) {
	srv := newGCSServer(t)
	s := newTestGCSSink(t, srv, config.Endpoint{Key: "events/{{ .id }}.txt", ContentType: "text/plain"})
	env := envelope.New("in", "http", []byte("first"))
	_, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)

	env.Body = []byte("changed")
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err, "the object exists: an earlier attempt wrote it")
	assert.Equal(t, []string{"true"}, res.Headers[headerExisting])
	o, err := srv.GetObject(testBucket, "events/"+env.ID+".txt")
	require.NoError(t, err)
	assert.Equal(t, "first", string(o.Content), "the precondition kept the first write")
	assert.Equal(t, "text/plain", o.ContentType, "content_type wins")
}

func TestGCSSinkBatchIsOneNDJSONObject(t *testing.T) {
	srv := newGCSServer(t)
	s := newTestGCSSink(t, srv, config.Endpoint{})
	envs := []*envelope.Envelope{
		receivedOn(jsonEnvelope("{\n  \"n\": 1\n}\n"), "2026-01-02"),
		receivedOn(envelope.New("in", "http", []byte("plain text")), "2026-01-02"),
		receivedOn(jsonEnvelope(`{"n":3}`), "2026-01-02"),
	}
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	key := strings.TrimPrefix(res.Endpoint, "gs://"+testBucket+"/")
	assert.True(t, strings.HasPrefix(key, "relay/2026/01/02/"+envs[0].ID+"-"), key)
	assert.True(t, strings.HasSuffix(key, ".ndjson"), key)

	o, err := srv.GetObject(testBucket, key)
	require.NoError(t, err)
	assert.Equal(t, "{\"n\":1}\nplain text\n{\"n\":3}\n", string(o.Content), "one line per message, JSON compacted")
	assert.Equal(t, contentNDJSON, o.ContentType)

	again, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Equal(t, res.Endpoint, again.Endpoint, "a retried batch writes the same key")
	assert.Equal(t, []string{"true"}, again.Headers[headerExisting])

	other, err := s.DeliverBatch(context.Background(), envs[:2])
	require.NoError(t, err)
	assert.NotEqual(t, res.Endpoint, other.Endpoint, "another batch, even with the same first message, gets its own key")

	empty, err := s.DeliverBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, empty.Endpoint)
}

func TestGCSSinkErrors(t *testing.T) {
	srv := newGCSServer(t)
	missing := newTestGCSSink(t, srv, config.Endpoint{Bucket: "missing"})
	_, err := missing.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "gcs:404")
	assert.Contains(t, tags, plugin.Tag4xx)

	bad := newTestGCSSink(t, srv, config.Endpoint{Key: "{{ .json.k }}"})
	_, err = bad.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, tags = classOf(t, err)
	assert.Equal(t, plugin.Permanent, class, "an empty key is a bad template")
	assert.Contains(t, tags, tagTemplate)

	ep := config.Endpoint{Type: kindGCS, Bucket: testBucket, GCP: &config.GCP{Endpoint: "http://" + closedAddr(t) + "/storage/v1/"}}
	unreachable, err := newGCSSink(context.Background(), plugin.Spec{Name: "out", Kind: kindGCS, Config: &ep})
	require.NoError(t, err)
	defer func() { _ = unreachable.Close(context.Background()) }()
	_, err = unreachable.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, tags = classOf(t, err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, tags, plugin.TagConnect)
}

func TestGCSSinkHealthAndClose(t *testing.T) {
	srv := newGCSServer(t)
	ok := newTestGCSSink(t, srv, config.Endpoint{})
	assert.NoError(t, ok.Health(context.Background()))
	missing := newTestGCSSink(t, srv, config.Endpoint{Bucket: "missing"})
	assert.ErrorContains(t, missing.Health(context.Background()), "does not exist")

	require.NoError(t, ok.Close(context.Background()))
	require.NoError(t, ok.Close(context.Background()))
	_, err := ok.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, _ := classOf(t, err)
	assert.Equal(t, plugin.Transient, class)
	assert.ErrorIs(t, err, errClosed)
	assert.ErrorIs(t, ok.Health(context.Background()), errClosed)
}

func TestGCSSinkRegistered(t *testing.T) {
	Register()
	srv := newGCSServer(t)
	ep := config.Endpoint{Type: kindGCS, Bucket: testBucket, GCP: gcsGCP(srv)}
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: kindGCS, Config: &ep})
	require.NoError(t, err)
	defer func() { _ = sink.Close(context.Background()) }()
	assert.Implements(t, (*plugin.BatchSink)(nil), sink)
	res, err := sink.Deliver(context.Background(), jsonEnvelope(`{}`))
	require.NoError(t, err)
	assert.Contains(t, res.Endpoint, "gs://"+testBucket+"/hikyaku/", "without a relay name the key starts with hikyaku")
}
