//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// image is the server the integration suite runs against.
const image = "redis:7"

// startRedis runs a server container for the test and returns its address
// and an admin client to it.
func startRedis(t *testing.T) (string, *goredis.Client) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcredis.Run(ctx, image)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	uri, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)
	opt, err := goredis.ParseURL(uri)
	require.NoError(t, err)
	return opt.Addr, adminClient(t, opt.Addr)
}

func integrationStream(addr, consumer string) config.Source {
	cfg := streamConfig(addr)
	cfg.Stream, cfg.Group, cfg.Consumer, cfg.Start = "jobs", "workers", consumer, "earliest"
	return cfg
}

func TestIntegrationStreamConsumersShareAGroup(t *testing.T) {
	addr, admin := startRedis(t)
	ctx := context.Background()
	merged := make(chan *envelope.Envelope, 64)
	for _, consumer := range []string{"a", "b"} {
		cfg := integrationStream(addr, consumer)
		cfg.Prefetch = 1
		runOut(t, newStreamSourceT(t, cfg), merged)
	}
	waitGroup(t, admin, "jobs")

	sink := newTestSink(t, config.Endpoint{URL: redisURL(addr), Stream: "jobs"})
	sent := map[string]bool{}
	for range 20 {
		env := msg(`{"job":true}`)
		sent[env.ID] = true
		_, err := sink.Deliver(ctx, env)
		require.NoError(t, err)
	}
	got := map[string]bool{}
	per := map[any]int{}
	for range 20 {
		env := recv(t, merged)
		assert.False(t, got[env.ID], "%s delivered twice", env.ID)
		got[env.ID] = true
		per[env.Attributes["redis.consumer"]]++
		go func() { // slow work: the other consumer takes the next entry
			time.Sleep(20 * time.Millisecond)
			settle(env, envelope.Delivered)
		}()
	}
	assert.Equal(t, sent, got)
	assert.Positive(t, per["a"], "both consumers work: %v", per)
	assert.Positive(t, per["b"], "both consumers work: %v", per)
	eventuallyPending(t, admin, "jobs", "workers", 0)
	noRecv(t, merged, 300*time.Millisecond)
}

func TestIntegrationStreamFailedEntryIsReclaimed(t *testing.T) {
	addr, admin := startRedis(t)
	cfg := integrationStream(addr, "a")
	cfg.AckWait = config.Duration(500 * time.Millisecond)
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "jobs")
	xadd(t, admin, "jobs", "body", "flaky")

	first := recv(t, out)
	assert.Equal(t, int64(1), first.Attributes["redis.deliveries"])
	settle(first, envelope.Failed)
	again := recv(t, out)
	assert.Equal(t, first.ID, again.ID)
	assert.Equal(t, int64(2), again.Attributes["redis.deliveries"], "XAUTOCLAIM counts; the XCLAIM refresh does not")
	settle(again, envelope.Delivered)
	eventuallyPending(t, admin, "jobs", "workers", 0)
}

func TestIntegrationStreamResumeSkipsDeletedEntries(t *testing.T) {
	addr, admin := startRedis(t)
	ctx := context.Background()
	cfg := integrationStream(addr, "a")
	cfg.AckWait = config.Duration(time.Hour)
	first := newStreamSourceT(t, cfg)
	out, stop := run(t, first)
	waitGroup(t, admin, "jobs")
	gone := xadd(t, admin, "jobs", "body", "gone")
	kept := xadd(t, admin, "jobs", "body", "kept")
	recv(t, out)
	recv(t, out)
	stop()
	require.NoError(t, first.Close(ctx))
	require.NoError(t, admin.XDel(ctx, "jobs", gone).Err())

	out, _ = run(t, newStreamSourceT(t, cfg))
	env := recv(t, out)
	assert.Equal(t, kept, env.Attributes["redis.entry_id"], "a pending entry deleted since is acknowledged, not delivered")
	assert.Equal(t, int64(2), env.Attributes["redis.deliveries"])
	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "jobs", "workers", 0)
}

func TestIntegrationSinkStreams(t *testing.T) {
	addr, admin := startRedis(t)
	ctx := context.Background()

	capped := newTestSink(t, config.Endpoint{URL: redisURL(addr), Stream: "capped", MaxLen: 100})
	for range 3 {
		envs := make([]*envelope.Envelope, 100)
		for i := range envs {
			envs[i] = msg(`{}`)
		}
		res, err := capped.DeliverBatch(ctx, envs)
		require.NoError(t, err)
		assert.Len(t, res.Headers[HeaderEntryID], 100)
	}
	n, err := admin.XLen(ctx, "capped").Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(100))
	assert.Less(t, n, int64(300), "MAXLEN ~ trims whole nodes")

	require.NoError(t, admin.Set(ctx, "scalar", "x", 0).Err())
	wrong := newTestSink(t, config.Endpoint{URL: redisURL(addr), Stream: "scalar"})
	_, err = wrong.Deliver(ctx, msg(`{}`))
	class, tags := classOf(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"redis:WRONGTYPE"}, tags)

	require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", "limited", "on", ">pw", "~*", "+@all", "-xadd").Err())
	denied := newTestSink(t, config.Endpoint{URL: "redis://limited:pw@" + addr, Stream: "events"})
	_, err = denied.Deliver(ctx, msg(`{}`))
	class, tags = classOf(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"redis:NOPERM"}, tags)
}

func TestIntegrationPubSubRoundTrip(t *testing.T) {
	addr, admin := startRedis(t)
	ctx := context.Background()
	out, _ := run(t, newPubSubSourceT(t, channelConfig(addr, "alerts.*")))
	require.Eventually(t, func() bool {
		n, err := admin.PubSubNumPat(ctx).Result()
		return err == nil && n == 1
	}, waitFor, 10*time.Millisecond)

	sink := newTestSink(t, config.Endpoint{URL: redisURL(addr), Channel: "alerts.{{ .json.level }}"})
	res, err := sink.Deliver(ctx, msg(`{"level":"high"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, res.Headers[HeaderReceivers])
	env := recv(t, out)
	assert.Equal(t, `{"level":"high"}`, env.BodyText())
	assert.Equal(t, "alerts.high", env.Subject)
	assert.Equal(t, "alerts.*", env.Attributes["redis.pattern"])
}
