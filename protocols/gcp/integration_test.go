//go:build integration

package gcp

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpubsub "github.com/testcontainers/testcontainers-go/modules/gcloud/pubsub"
	tcwait "github.com/testcontainers/testcontainers-go/wait"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

// The emulators the integration suite runs against.
const (
	pubsubImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli:592.0.0-emulators"
	gcsImage    = "fsouza/fake-gcs-server:1.56.1"
)

// startPubSub runs the Pub/Sub emulator and returns the gcp: block of a
// source or destination on it, and an admin client.
func startPubSub(t *testing.T) (*config.GCP, *pubsub.Client) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcpubsub.Run(ctx, pubsubImage, tcpubsub.WithProjectID(testProject))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	gcp := &config.GCP{Project: testProject, Endpoint: ctr.URI()}
	c, err := parseGCP(gcp, kindPubSub)
	require.NoError(t, err)
	admin, err := pubsub.NewClient(ctx, testProject, c.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	return gcp, admin
}

// startGCS runs fake-gcs-server and returns the gcp: block of a source or
// destination on it, with the bucket created.
func startGCS(t *testing.T) *config.GCP {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, gcsImage,
		testcontainers.WithCmd("-scheme", "http", "-port", "4443"),
		testcontainers.WithExposedPorts("4443/tcp"),
		testcontainers.WithWaitStrategy(tcwait.ForHTTP("/_internal/healthcheck").WithPort("4443/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	endpoint, err := ctr.PortEndpoint(ctx, "4443/tcp", "http")
	require.NoError(t, err)
	gcp := &config.GCP{Endpoint: endpoint + "/storage/v1/"}

	client := gcsClient(t, gcp)
	require.NoError(t, client.Bucket(testBucket).Create(ctx, testProject, nil))
	return gcp
}

func gcsClient(t *testing.T, gcp *config.GCP) *storage.Client {
	t.Helper()
	c, err := parseGCP(gcp, kindGCS)
	require.NoError(t, err)
	client, err := newStorageClient(context.Background(), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestIntegrationPubSub(t *testing.T) {
	gcp, admin := startPubSub(t) // one emulator: it takes a while to start
	t.Run("round trip", func(t *testing.T) { pubsubRoundTrip(t, gcp, admin) })
	t.Run("missing topic is permanent", func(t *testing.T) { pubsubMissingTopic(t, gcp) })
}

func pubsubRoundTrip(t *testing.T, gcp *config.GCP, admin *pubsub.Client) {
	ctx := context.Background()
	topic := createTopic(t, admin, "orders")
	createSub(t, admin, "orders-sub", topic, func(s *pubsubpb.Subscription) { s.EnableMessageOrdering = true })

	sinkCfg := config.Endpoint{Type: kindPubSub, Topic: "orders", GCP: gcp, Key: "{{ .json.customer }}",
		Headers: map[string]string{"X-Region": "{{ .json.region }}"}}
	sink, err := newPubSubSink(ctx, plugin.Spec{Name: "out", Kind: kindPubSub, Config: &sinkCfg})
	require.NoError(t, err)
	srcCfg := config.Source{Name: "in", Type: kindPubSub, Subscription: "orders-sub", GCP: gcp}
	src, err := newPubSubSource(ctx, plugin.Spec{Name: "in", Kind: kindPubSub, Config: &srcCfg})
	require.NoError(t, err)
	require.NoError(t, sink.Health(ctx))
	require.NoError(t, src.Health(ctx))
	out, stop := run(t, src)

	// Published in order with one ordering key, received in that order with
	// the envelope's ID, subject, content type and headers.
	var sent []*envelope.Envelope
	for n := range 3 {
		env := jsonEnvelope(fmt.Sprintf(`{"customer":"c1","region":"eu","n":%d}`, n))
		env.Subject = "orders/new"
		res, err := sink.Deliver(ctx, env)
		require.NoError(t, err)
		require.NotEmpty(t, res.Headers[resultHeader])
		sent = append(sent, env)
	}
	for _, want := range sent {
		got := recv(t, out)
		assert.Equal(t, want.ID, got.ID)
		assert.JSONEq(t, string(want.Body), string(got.Body))
		assert.Equal(t, "orders/new", got.Subject)
		assert.Equal(t, contentJSON, got.ContentType)
		assert.Equal(t, []string{"eu"}, got.Headers["X-Region"])
		assert.Equal(t, "c1", got.Attributes["ordering_key"])
		settle(got, envelope.Delivered)
	}

	// A failed message is redelivered.
	_, err = sink.Deliver(ctx, jsonEnvelope(`{"customer":"c2","region":"us"}`))
	require.NoError(t, err)
	first := recv(t, out)
	settle(first, envelope.Failed)
	again := recv(t, out)
	assert.Equal(t, first.ID, again.ID)
	settle(again, envelope.Delivered)

	// A batch is published whole.
	batch := []*envelope.Envelope{jsonEnvelope(`{"customer":"c3"}`), jsonEnvelope(`{"customer":"c4"}`)}
	res, err := sink.DeliverBatch(ctx, batch)
	require.NoError(t, err)
	assert.Len(t, res.Headers[resultHeader], 2)
	ids := map[string]bool{}
	for range batch {
		got := recv(t, out)
		ids[got.ID] = true
		settle(got, envelope.Delivered)
	}
	assert.Equal(t, map[string]bool{batch[0].ID: true, batch[1].ID: true}, ids)

	quiet(t, out, 500*time.Millisecond) // everything was acked
	stop()
	closeWithin(t, src, wait)
	require.NoError(t, sink.Close(ctx))
}

func pubsubMissingTopic(t *testing.T, gcp *config.GCP) {
	cfg := config.Endpoint{Type: kindPubSub, Topic: "nope", GCP: gcp}
	sink, err := newPubSubSink(context.Background(), plugin.Spec{Name: "out", Kind: kindPubSub, Config: &cfg})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	assert.ErrorContains(t, sink.Health(context.Background()), "does not exist")
	_, err = sink.Deliver(context.Background(), jsonEnvelope(`{}`))
	class, tags := classOf(t, err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "pubsub:NOT_FOUND")
}

func TestIntegrationGCSRoundTrip(t *testing.T) {
	ctx := context.Background()
	gcp := startGCS(t)
	client := gcsClient(t, gcp)
	bkt := client.Bucket(testBucket)

	sinkCfg := config.Endpoint{Type: kindGCS, Bucket: testBucket, GCP: gcp, Key: "in/{{ .id }}.json"}
	sink, err := newGCSSink(ctx, plugin.Spec{Name: "out", Kind: kindGCS, Config: &sinkCfg, Env: plugin.Env{Relay: "relay"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close(ctx) })
	require.NoError(t, sink.Health(ctx))

	// One object per message, written once however often it is retried.
	env := jsonEnvelope(`{"n":1}`)
	env.Headers = map[string][]string{"X-Trace": {"t1"}}
	_, err = sink.Deliver(ctx, env)
	require.NoError(t, err)
	res, err := sink.Deliver(ctx, env)
	require.NoError(t, err)
	assert.Equal(t, []string{"true"}, res.Headers[headerExisting])

	// The source reads it, then moves it under move_to.
	srcCfg := config.Source{Name: "in", Type: kindGCS, Bucket: testBucket, Prefix: "in/", After: afterMove, MoveTo: "done/",
		GCP: gcp, Interval: config.Duration(50 * time.Millisecond)}
	src, err := newGCSSource(ctx, plugin.Spec{Name: "in", Kind: kindGCS, Config: &srcCfg})
	require.NoError(t, err)
	out, stop := run(t, src)
	got := recv(t, out)
	assert.JSONEq(t, `{"n":1}`, string(got.Body))
	assert.Equal(t, []string{"t1"}, got.Headers["X-Trace"], "headers travel as metadata")
	assert.Equal(t, "in/"+env.ID+".json", got.Subject)
	settle(got, envelope.Delivered)
	require.Eventually(t, func() bool {
		_, errIn := bkt.Object("in/" + env.ID + ".json").Attrs(ctx)
		_, errDone := bkt.Object("done/in/" + env.ID + ".json").Attrs(ctx)
		return isNotFound(errIn) && errDone == nil
	}, wait, 20*time.Millisecond)
	stop()
	require.NoError(t, src.Close(ctx))
}

func TestIntegrationGCSBatchSplitAndKeep(t *testing.T) {
	ctx := context.Background()
	gcp := startGCS(t)
	client := gcsClient(t, gcp)

	sinkCfg := config.Endpoint{Type: kindGCS, Bucket: testBucket, GCP: gcp}
	sink, err := newGCSSink(ctx, plugin.Spec{Name: "out", Kind: kindGCS, Config: &sinkCfg, Env: plugin.Env{Relay: "relay"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close(ctx) })
	batch := []*envelope.Envelope{jsonEnvelope(`{"n":1}`), jsonEnvelope(`{"n":2}`), jsonEnvelope(`{"n":3}`)}
	res, err := sink.DeliverBatch(ctx, batch)
	require.NoError(t, err)
	key := res.Endpoint[len("gs://"+testBucket+"/"):]

	r, err := client.Bucket(testBucket).Object(key).NewReader(ctx)
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n", string(content))
	attrs, err := client.Bucket(testBucket).Object(key).Attrs(ctx)
	require.NoError(t, err)
	assert.Equal(t, contentNDJSON, attrs.ContentType)

	// Read back line by line and kept: remembered in the store, not read
	// twice.
	st := store.NewMemory("state")
	srcCfg := config.Source{Name: "in", Type: kindGCS, Bucket: testBucket, Prefix: "relay/", Split: splitLines, After: afterKeep,
		GCP: gcp, Interval: config.Duration(50 * time.Millisecond)}
	src, err := newGCSSource(ctx, plugin.Spec{Name: "in", Kind: kindGCS, Config: &srcCfg, Env: testEnv(st)})
	require.NoError(t, err)
	out, stop := run(t, src)
	for i := range batch {
		got := recv(t, out)
		assert.Equal(t, fmt.Sprintf("%d-%d", attrs.Generation, i+1), got.ID)
		assert.Equal(t, i+1, got.Attributes["gcs.line"])
		settle(got, envelope.Delivered)
	}
	kept := "gcs:" + testBucket + ":" + key + ":" + strconv.FormatInt(attrs.Generation, 10)
	require.Eventually(t, func() bool { _, ok, _ := st.Get(ctx, kept); return ok }, wait, 20*time.Millisecond)
	quiet(t, out, 300*time.Millisecond)
	_, err = client.Bucket(testBucket).Object(key).Attrs(ctx)
	assert.NoError(t, err, "kept")
	stop()
	require.NoError(t, src.Close(ctx))
}
