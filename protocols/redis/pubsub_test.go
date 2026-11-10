package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

func channelConfig(addr, channel string) config.Source {
	return config.Source{URL: redisURL(addr), Channel: channel,
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxBackoff: config.Duration(50 * time.Millisecond)}}
}

// subscribed waits until channel has n subscribers.
func subscribed(t *testing.T, mr *miniredis.Miniredis, channel string, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return mr.PubSubNumSub(channel)[channel] == n }, waitFor, 5*time.Millisecond,
		"%s never had %d subscribers", channel, n)
}

// patterns waits until the server has n pattern subscriptions.
func patterns(t *testing.T, mr *miniredis.Miniredis, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return mr.PubSubNumPat() == n }, waitFor, 5*time.Millisecond)
}

func TestPubSubSubscribe(t *testing.T) {
	mr, _ := startMini(t)
	out, _ := run(t, newPubSubSourceT(t, channelConfig(mr.Addr(), "alerts")))
	subscribed(t, mr, "alerts", 1)

	mr.Publish("alerts", `{"level":"high"}`)
	env := recv(t, out)
	assert.Equal(t, `{"level":"high"}`, env.BodyText())
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, "redis", env.Protocol)
	assert.Equal(t, "alerts", env.Subject)
	assert.Equal(t, "application/json", env.ContentType)
	assert.Empty(t, env.Headers)
	assert.Equal(t, map[string]any{"redis.channel": "alerts", "ordering_key": "alerts"}, env.Attributes)
	assert.Len(t, env.ID, 26, "a new ULID: pub/sub messages have no ID")
	settle(env, envelope.Failed) // at most once: nothing to nack

	mr.Publish("alerts", "plain words")
	env = recv(t, out)
	assert.Equal(t, "text/plain; charset=utf-8", env.ContentType)
	noRecv(t, out, 100*time.Millisecond)
}

func TestPubSubPattern(t *testing.T) {
	mr, _ := startMini(t)
	out, _ := run(t, newPubSubSourceT(t, channelConfig(mr.Addr(), "events.*")))
	patterns(t, mr, 1)

	mr.Publish("other", "x")
	mr.Publish("events.created", `{"id":1}`)
	env := recv(t, out)
	assert.Equal(t, `{"id":1}`, env.BodyText())
	assert.Equal(t, "events.created", env.Subject)
	assert.Equal(t, map[string]any{"redis.channel": "events.created", "redis.pattern": "events.*", "ordering_key": "events.created"}, env.Attributes)
	noRecv(t, out, 100*time.Millisecond)
}

func TestPubSubResubscribesAfterTheServerRestarts(t *testing.T) {
	mr, _ := startMini(t)
	src := newPubSubSourceT(t, channelConfig(mr.Addr(), "alerts"))
	out, _ := run(t, src)
	subscribed(t, mr, "alerts", 1)

	mr.Close()
	require.Eventually(t, func() bool { return src.Health(context.Background()) != nil }, waitFor, 5*time.Millisecond)
	require.NoError(t, mr.Restart())
	subscribed(t, mr, "alerts", 1)
	mr.Publish("alerts", "back")
	assert.Equal(t, "back", recv(t, out).BodyText())
	require.NoError(t, src.Health(context.Background()))
}

func TestPubSubDropsAnUnresponsiveSubscription(t *testing.T) {
	mr, _ := startMini(t)
	p := newProxy(t, mr.Addr())
	logs := &records{}
	src := newTestSourceLog(t, channelConfig(p.Addr(), "alerts"), logs.logger()).(*PubSubSource)
	src.pingEvery, src.pingTimeout, src.subscribeTimeout = 50*time.Millisecond, 50*time.Millisecond, 200*time.Millisecond
	out, _ := run(t, src)
	subscribed(t, mr, "alerts", 1)

	p.SetBlackhole(true) // a half-open connection: no error, no data
	require.Eventually(t, func() bool { return logs.count("is unresponsive") > 0 }, waitFor, 5*time.Millisecond)
	subscribed(t, mr, "alerts", 0)
	p.SetBlackhole(false)
	subscribed(t, mr, "alerts", 1)
	mr.Publish("alerts", "after")
	assert.Equal(t, "after", recv(t, out).BodyText())
}

func TestPubSubBackpressureKeepsTheSubscription(t *testing.T) {
	mr, _ := startMini(t)
	logs := &records{}
	src := newTestSourceLog(t, channelConfig(mr.Addr(), "alerts"), logs.logger()).(*PubSubSource)
	src.pingEvery, src.pingTimeout = 30*time.Millisecond, 30*time.Millisecond
	out := make(chan *envelope.Envelope) // nobody reads for a while
	runOut(t, src, out)
	subscribed(t, mr, "alerts", 1)
	for _, m := range []string{"1", "2", "3"} {
		mr.Publish("alerts", m)
	}
	time.Sleep(400 * time.Millisecond)
	for _, want := range []string{"1", "2", "3"} {
		assert.Equal(t, want, recv(t, out).BodyText())
	}
	assert.Equal(t, 1, logs.count("redis source subscribed"), "waiting for the relay is not a dead connection")
	assert.Zero(t, logs.count("is unresponsive"))
}

func TestPubSubOversizeIsDropped(t *testing.T) {
	mr, _ := startMini(t)
	logs := &records{}
	cfg := channelConfig(mr.Addr(), "alerts")
	cfg.MaxBodyBytes = 4
	out, _ := run(t, newTestSourceLog(t, cfg, logs.logger()))
	subscribed(t, mr, "alerts", 1)
	mr.Publish("alerts", "too long")
	mr.Publish("alerts", "ok")
	assert.Equal(t, "ok", recv(t, out).BodyText())
	assert.Equal(t, 1, logs.count("over max_body_bytes"))
}

func TestPubSubGivesUpAfterMaxAttempts(t *testing.T) {
	cfg := channelConfig(closedAddr(t), "alerts")
	cfg.Reconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}
	src := newPubSubSourceT(t, cfg)
	err := src.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 2 failed subscribes in a row")
	assert.Contains(t, err.Error(), "SUBSCRIBE alerts")
	assert.Equal(t, err, src.Health(context.Background()))
}

func TestPubSubFailsOverToTheNextURL(t *testing.T) {
	mr, _ := startMini(t)
	cfg := channelConfig(closedAddr(t), "alerts")
	cfg.FailoverURLs = []string{redisURL(mr.Addr())}
	out, _ := run(t, newPubSubSourceT(t, cfg))
	subscribed(t, mr, "alerts", 1)
	mr.Publish("alerts", "via standby")
	assert.Equal(t, "via standby", recv(t, out).BodyText())
}

func TestCloseStopsARunningSource(t *testing.T) {
	mr, admin := startMini(t)
	for name, cfg := range map[string]config.Source{
		"stream":  streamConfig(mr.Addr()),
		"channel": channelConfig(mr.Addr(), "alerts"),
	} {
		t.Run(name, func(t *testing.T) {
			src := newTestSource(t, cfg)
			errc := make(chan error, 1)
			go func() { errc <- src.Start(context.Background(), make(chan *envelope.Envelope)) }()
			if name == "stream" {
				waitGroup(t, admin, "orders")
			} else {
				subscribed(t, mr, "alerts", 1)
			}
			require.NoError(t, src.(interface{ Close(context.Context) error }).Close(context.Background()))
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(waitFor):
				t.Fatal("Start kept running after Close")
			}
		})
	}
}

func TestPubSubStopAndClose(t *testing.T) {
	mr, _ := startMini(t)
	ctx := context.Background()
	src := newPubSubSourceT(t, channelConfig(mr.Addr(), "alerts"))
	require.NoError(t, src.Health(ctx), "a PING before it starts")
	_, stop := run(t, src)
	subscribed(t, mr, "alerts", 1)
	start := time.Now()
	stop()
	assert.Less(t, time.Since(start), 2*time.Second)
	subscribed(t, mr, "alerts", 0)
	assert.ErrorIs(t, src.Health(ctx), errStopped)
	require.NoError(t, src.Close(ctx))
	require.NoError(t, src.Close(ctx))
	assert.ErrorIs(t, src.Health(ctx), errClosed)
	assert.Error(t, src.Start(ctx, make(chan *envelope.Envelope)))
}
