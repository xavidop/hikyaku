package gcp

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const (
	testProject = "hikyaku-test"
	testBucket  = "bkt"
	wait        = 5 * time.Second
	tick        = 5 * time.Millisecond
)

// newPubSubServer starts an in-process Pub/Sub fake.
func newPubSubServer(t *testing.T, opts ...pstest.ServerReactorOption) *pstest.Server {
	t.Helper()
	srv := pstest.NewServer(opts...)
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// psGCP is the gcp: block of a pubsub source or destination on srv.
func psGCP(srv *pstest.Server) *config.GCP {
	return &config.GCP{Project: testProject, Endpoint: srv.Addr}
}

// psAdmin is a client of the fake, for topics and subscriptions.
func psAdmin(t *testing.T, srv *pstest.Server) *pubsub.Client {
	t.Helper()
	c, err := parseGCP(psGCP(srv), kindPubSub)
	require.NoError(t, err)
	client, err := pubsub.NewClient(context.Background(), testProject, c.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func createTopic(t *testing.T, c *pubsub.Client, id string) string {
	t.Helper()
	name := "projects/" + testProject + "/topics/" + id
	_, err := c.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{Name: name})
	require.NoError(t, err)
	return name
}

func createSub(t *testing.T, c *pubsub.Client, id, topic string, mod func(*pubsubpb.Subscription)) string {
	t.Helper()
	name := "projects/" + testProject + "/subscriptions/" + id
	sub := &pubsubpb.Subscription{Name: name, Topic: topic, AckDeadlineSeconds: 10}
	if mod != nil {
		mod(sub)
	}
	_, err := c.SubscriptionAdminClient.CreateSubscription(context.Background(), sub)
	require.NoError(t, err)
	return name
}

// newGCSServer starts an in-process Cloud Storage fake with a bucket.
func newGCSServer(t *testing.T) *fakestorage.Server {
	t.Helper()
	srv, err := fakestorage.NewServerWithOptions(fakestorage.Options{Scheme: "http", Host: "127.0.0.1", PublicHost: "127.0.0.1"})
	require.NoError(t, err)
	t.Cleanup(srv.Stop)
	srv.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: testBucket})
	return srv
}

// gcsGCP is the gcp: block of a gcs source or destination on srv.
func gcsGCP(srv *fakestorage.Server) *config.GCP {
	return &config.GCP{Endpoint: srv.URL() + "/storage/v1/"}
}

func putObject(srv *fakestorage.Server, name, contentType, content string, meta map[string]string) {
	srv.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: testBucket, Name: name, ContentType: contentType, Metadata: meta},
		Content:     []byte(content),
	})
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// testEnv is a plugin environment with an in-memory store.
func testEnv(st store.Store) plugin.Env {
	return plugin.Env{Relay: "relay", Store: func(string) (store.Store, error) { return st, nil }}
}

// run starts src. The returned stop (also run at cleanup) cancels it and
// checks that Start returned nil promptly.
func run(t *testing.T, src plugin.Source) (chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 64)
	return out, runOut(t, src, out)
}

func runOut(t *testing.T, src plugin.Source, out chan *envelope.Envelope) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- src.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(wait):
				t.Error("Start did not return after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(wait):
		t.Fatal("no envelope arrived")
		return nil
	}
}

// quiet checks that nothing arrives on out for d.
func quiet(t *testing.T, out <-chan *envelope.Envelope, d time.Duration) {
	t.Helper()
	select {
	case env := <-out:
		t.Fatalf("unexpected envelope %s: %s", env.ID, env.Body)
	case <-time.After(d):
	}
}

// settle finishes a message the way the engine does.
func settle(env *envelope.Envelope, st envelope.AckStatus) {
	env.Ack(context.Background(), envelope.Outcome{Status: st})
}

// closeWithin closes c and checks that it returns nil within d.
func closeWithin(t *testing.T, c plugin.Closer, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	require.NoError(t, c.Close(ctx))
}

// classOf returns the class and tags of a delivery error.
func classOf(t *testing.T, err error) (plugin.Class, []string) {
	t.Helper()
	require.Error(t, err)
	var pe *plugin.Error
	require.ErrorAs(t, err, &pe, "a sink error is a *plugin.Error: %v", err)
	return pe.Class, plugin.Tags(err)
}
