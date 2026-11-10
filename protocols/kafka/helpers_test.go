package kafka

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// wait bounds every wait of the suite on the in-process cluster.
const wait = 15 * time.Second

// testLogger discards logs unless KAFKA_TEST_LOG is set.
func testLogger() *slog.Logger {
	if os.Getenv("KAFKA_TEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// cluster starts an in-process Kafka cluster with topics of partitions
// partitions each.
func cluster(t *testing.T, partitions int32, topics ...string) *kfake.Cluster {
	t.Helper()
	return clusterWith(t, kfake.SeedTopics(partitions, topics...))
}

func clusterWith(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(append([]kfake.Opt{kfake.NumBrokers(1)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// client is a plain client of the cluster: it produces the test records to
// the partition each names, and reads commits and topics back.
func client(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	return cl
}

func produce(t *testing.T, cl *kgo.Client, recs ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	require.NoError(t, cl.ProduceSync(ctx, recs...).FirstErr())
}

func record(topic string, partition int32, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
}

// committed is the group's committed offset of a partition, -1 for none.
func committed(t *testing.T, cl *kgo.Client, group, topic string, partition int32) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	resp, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		return -1
	}
	o, ok := resp.Lookup(topic, partition)
	if !ok || o.Err != nil {
		return -1
	}
	return o.At
}

// eventuallyCommitted waits for the group to commit want on a partition.
func eventuallyCommitted(t *testing.T, cl *kgo.Client, group, topic string, partition int32, want int64) {
	t.Helper()
	require.Eventually(t, func() bool { return committed(t, cl, group, topic, partition) == want },
		wait, 20*time.Millisecond, "committed offset of %s/%d is %d, want %d", topic, partition, committed(t, cl, group, topic, partition), want)
}

// consumed reads n records of topic from the start, outside any group.
func consumed(t *testing.T, c *kfake.Cluster, topic string, n int) []*kgo.Record {
	t.Helper()
	cl := client(t, c, kgo.ConsumeTopics(topic), kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "read %d of %d records", len(out), n)
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// sourceRun runs a source the way the relay does: Start feeds out, and the
// test settles each envelope as the engine would.
type sourceRun struct {
	src    *Source
	out    chan *envelope.Envelope
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

// newTestSource builds a source on the cluster: start earliest, commits
// every 20ms, heartbeats every 100ms (rebalances settle fast).
func newTestSource(t *testing.T, c *kfake.Cluster, cfg config.Source) *Source {
	t.Helper()
	if cfg.Brokers == nil {
		cfg.Brokers = c.ListenAddrs()
	}
	if cfg.Start == "" {
		cfg.Start = "earliest"
	}
	if cfg.Group == "" {
		cfg.Group = "g"
	}
	src, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: Kind, Config: &cfg, Env: plugin.Env{Relay: "test", Logger: testLogger()}})
	require.NoError(t, err)
	src.commitEvery = 20 * time.Millisecond
	src.kopts = append(src.kopts, kgo.HeartbeatInterval(100*time.Millisecond), kgo.FetchMaxWait(200*time.Millisecond))
	return src
}

func startSource(t *testing.T, c *kfake.Cluster, cfg config.Source) *sourceRun {
	t.Helper()
	return run(t, newTestSource(t, c, cfg))
}

func run(t *testing.T, src *Source) *sourceRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &sourceRun{src: src, out: make(chan *envelope.Envelope, 1000), cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- src.Start(ctx, r.out) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

// next is the next envelope the source hands over.
func (r *sourceRun) next(t *testing.T) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-r.out:
		return env
	case err := <-r.done:
		r.done <- err
		t.Fatalf("the source stopped: %v", err)
	case <-time.After(wait):
		t.Fatal("no message from the source")
	}
	return nil
}

// none checks that nothing comes within d.
func (r *sourceRun) none(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case env := <-r.out:
		t.Fatalf("unexpected message %s", env.ID)
	case <-time.After(d):
	}
}

// stop ends Start, then closes the source, as the relay does at shutdown.
func (r *sourceRun) stop(t *testing.T) {
	r.once.Do(func() {
		r.cancel()
		select {
		case err := <-r.done:
			if err != nil {
				t.Logf("Start returned %v", err)
			}
		case <-time.After(wait):
			t.Error("Start did not return after its context ended")
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		_ = r.src.Close(ctx)
	})
}

// refuseSASL makes the cluster answer every SASL authentication with
// SASL_AUTHENTICATION_FAILED, as a broker answers a wrong password (kfake
// itself just drops the connection).
func refuseSASL(c *kfake.Cluster) {
	c.ControlKey(int16(kmsg.SASLAuthenticate), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		msg := "Authentication failed: invalid credentials"
		resp.ErrorMessage = &msg
		return resp, nil, true
	})
}

// saslMech is the mechanism a source or destination builds from a sasl
// block, for the test's own client.
func saslMech(t *testing.T, mechanism, user, pass string) sasl.Mechanism {
	t.Helper()
	o, err := parseClient(&config.SASL{Mechanism: mechanism, Username: user, Password: pass}, nil, "test")
	require.NoError(t, err)
	return o.sasl
}

func ack(env *envelope.Envelope, status envelope.AckStatus) {
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}

func offsetOf(t *testing.T, env *envelope.Envelope) int64 {
	t.Helper()
	v, ok := env.Attribute("kafka.offset")
	require.True(t, ok)
	return v.(int64)
}

func partitionOf(t *testing.T, env *envelope.Envelope) int32 {
	t.Helper()
	v, ok := env.Attribute("kafka.partition")
	require.True(t, ok)
	return int32(v.(int64))
}
