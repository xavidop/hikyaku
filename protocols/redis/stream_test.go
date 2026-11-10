package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

func TestStreamCreatesGroupAndStream(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	src := newStreamSourceT(t, streamConfig(mr.Addr()))
	out, _ := run(t, src)
	require.Eventually(t, func() bool {
		groups, err := admin.XInfoGroups(ctx, "orders").Result()
		return err == nil && len(groups) == 1 && groups[0].Name == "relay"
	}, waitFor, 10*time.Millisecond, "XGROUP CREATE ... MKSTREAM")
	xadd(t, admin, "orders", "body", "hi")
	assert.Equal(t, "hi", recv(t, out).BodyText())
}

func TestStreamStart(t *testing.T) {
	mr, admin := startMini(t)
	xadd(t, admin, "orders", "body", "old")

	earliest := streamConfig(mr.Addr())
	earliest.Group, earliest.Start = "from-start", "earliest"
	out, _ := run(t, newStreamSourceT(t, earliest))
	assert.Equal(t, "old", recv(t, out).BodyText(), "earliest reads what is already there")

	latest := streamConfig(mr.Addr())
	latest.Group = "from-now"
	out, _ = run(t, newStreamSourceT(t, latest))
	require.Eventually(t, func() bool {
		groups, err := admin.XInfoGroups(context.Background(), "orders").Result()
		return err == nil && len(groups) == 2
	}, waitFor, 10*time.Millisecond)
	noRecv(t, out, 300*time.Millisecond)
	xadd(t, admin, "orders", "body", "new")
	assert.Equal(t, "new", recv(t, out).BodyText(), "latest reads what comes next")
}

func TestStreamReadAndAck(t *testing.T) {
	mr, admin := startMini(t)
	out, _ := run(t, newStreamSourceT(t, streamConfig(mr.Addr())))
	waitGroup(t, admin, "orders")
	id := xadd(t, admin, "orders", "id", "01J0ABC", "body", `{"total":42}`, "content_type", "application/json",
		"subject", "/checkout", "h:X-Tenant", "acme")

	env := recv(t, out)
	assert.Equal(t, "01J0ABC", env.ID)
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, "redis", env.Protocol)
	assert.Equal(t, "/checkout", env.Subject)
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, `{"total":42}`, env.BodyText())
	assert.Equal(t, map[string][]string{"X-Tenant": {"acme"}}, env.Headers)
	assert.Equal(t, map[string]any{
		"redis.stream": "orders", "redis.entry_id": id, "redis.group": "relay", "redis.consumer": "c1",
		"redis.deliveries": int64(1), "ordering_key": "orders",
	}, env.Attributes)
	assert.Equal(t, int64(1), pendingCount(t, admin, "orders", "relay"), "pending until settled")

	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamFinalOutcomesAck(t *testing.T) {
	for _, status := range []envelope.AckStatus{envelope.Delivered, envelope.DeadLettered, envelope.Unrouted, envelope.Filtered} {
		t.Run(status.String(), func(t *testing.T) {
			mr, admin := startMini(t)
			out, _ := run(t, newStreamSourceT(t, streamConfig(mr.Addr())))
			waitGroup(t, admin, "orders")
			xadd(t, admin, "orders", "body", "x")
			settle(recv(t, out), status)
			eventuallyPending(t, admin, "orders", "relay", 0)
		})
	}
}

func TestStreamFailedIsRedeliveredAfterAckWait(t *testing.T) {
	mr, admin := startMini(t)
	cfg := streamConfig(mr.Addr())
	cfg.AckWait = config.Duration(200 * time.Millisecond)
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "retry me")

	first := recv(t, out)
	read := time.Now()
	settle(first, envelope.Failed)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(1), pendingCount(t, admin, "orders", "relay"), "a failed entry stays pending")

	again := recv(t, out)
	assert.GreaterOrEqual(t, time.Since(read), 200*time.Millisecond, "redelivered once idle for ack_wait")
	assert.Equal(t, first.ID, again.ID)
	assert.Equal(t, "retry me", again.BodyText())
	assert.GreaterOrEqual(t, again.Attributes["redis.deliveries"], int64(2))
	settle(again, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
	noRecv(t, out, 400*time.Millisecond)
}

func TestStreamResumesOwnPendingEntriesFirst(t *testing.T) {
	mr, admin := startMini(t)
	cfg := streamConfig(mr.Addr())
	cfg.AckWait = config.Duration(time.Hour) // no reclaim: the restart alone resumes

	first := newStreamSourceT(t, cfg)
	out, stop := run(t, first)
	waitGroup(t, admin, "orders")
	p1 := xadd(t, admin, "orders", "body", "p1")
	p2 := xadd(t, admin, "orders", "body", "p2")
	recv(t, out)
	recv(t, out)
	stop() // a crash: neither entry is settled
	require.NoError(t, first.Close(context.Background()))
	n1 := xadd(t, admin, "orders", "body", "n1")

	out, _ = run(t, newStreamSourceT(t, cfg)) // the same consumer, c1
	for _, want := range []string{p1, p2} {
		env := recv(t, out)
		assert.Equal(t, want, env.Attributes["redis.entry_id"], "own pending entries first, in order")
		assert.GreaterOrEqual(t, env.Attributes["redis.deliveries"], int64(2))
		settle(env, envelope.Delivered)
	}
	env := recv(t, out)
	assert.Equal(t, n1, env.Attributes["redis.entry_id"])
	assert.Equal(t, int64(1), env.Attributes["redis.deliveries"])
	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamDoesNotReclaimEntriesInFlight(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	cfg := streamConfig(mr.Addr())
	cfg.AckWait = config.Duration(400 * time.Millisecond)
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "slow")

	env := recv(t, out) // still being delivered, for well over ack_wait
	noRecv(t, out, 1200*time.Millisecond)
	// Its idle time is refreshed, so another consumer cannot take it either.
	stolen, _, err := admin.XAutoClaim(ctx, &goredis.XAutoClaimArgs{Stream: "orders", Group: "relay", Consumer: "thief",
		MinIdle: 400 * time.Millisecond, Start: "0-0", Count: 10}).Result()
	require.NoError(t, err)
	assert.Empty(t, stolen)

	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamRefreshesEntriesWaitingForTheRelay(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	for _, b := range []string{"1", "2", "3"} {
		xadd(t, admin, "orders", "body", b)
	}
	cfg := streamConfig(mr.Addr())
	cfg.AckWait = config.Duration(400 * time.Millisecond)
	cfg.Start = "earliest"               // one read takes all three
	out := make(chan *envelope.Envelope) // backpressure: nobody reads yet
	runOut(t, newStreamSourceT(t, cfg), out)
	require.Eventually(t, func() bool {
		p, err := admin.XPending(ctx, "orders", "relay").Result()
		return err == nil && p.Count == 3
	}, waitFor, 5*time.Millisecond)
	time.Sleep(1200 * time.Millisecond)
	stolen, _, err := admin.XAutoClaim(ctx, &goredis.XAutoClaimArgs{Stream: "orders", Group: "relay", Consumer: "thief",
		MinIdle: 400 * time.Millisecond, Start: "0-0", Count: 10}).Result()
	require.NoError(t, err)
	assert.Empty(t, stolen, "read but not yet handed over is still in flight")
	for _, want := range []string{"1", "2", "3"} {
		env := recv(t, out)
		assert.Equal(t, want, env.BodyText())
		settle(env, envelope.Delivered)
	}
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamReclaimsDeadConsumersEntries(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	require.NoError(t, admin.XGroupCreateMkStream(ctx, "orders", "relay", "0").Err())
	id := xadd(t, admin, "orders", "body", "orphan")
	_, err := admin.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "relay", Consumer: "dead", Streams: []string{"orders", ">"}}).Result()
	require.NoError(t, err)

	cfg := streamConfig(mr.Addr())
	cfg.AckWait = config.Duration(200 * time.Millisecond)
	out, _ := run(t, newStreamSourceT(t, cfg))
	env := recv(t, out)
	assert.Equal(t, id, env.Attributes["redis.entry_id"])
	assert.Equal(t, "c1", env.Attributes["redis.consumer"], "claimed by this consumer")
	assert.Equal(t, int64(2), env.Attributes["redis.deliveries"])
	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamForeignEntry(t *testing.T) {
	mr, admin := startMini(t)
	out, _ := run(t, newStreamSourceT(t, streamConfig(mr.Addr())))
	waitGroup(t, admin, "orders")
	id := xadd(t, admin, "orders", "user", "ada", "n", "1", "id", "42")
	env := recv(t, out)
	assert.Equal(t, "orders-"+id, env.ID)
	assert.Equal(t, "orders", env.Subject)
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, `{"id":"42","n":"1","user":"ada"}`, env.BodyText())
	assert.Empty(t, env.Headers)
}

func TestStreamRoundTripsTheSinksEntries(t *testing.T) {
	mr, admin := startMini(t)
	out, _ := run(t, newStreamSourceT(t, streamConfig(mr.Addr())))
	waitGroup(t, admin, "orders")
	sink := newTestSink(t, config.Endpoint{URL: redisURL(mr.Addr()), Stream: "orders"})

	sent := msg(`{"order":1}`)
	sent.Subject = "/hooks/shop"
	sent.Headers = map[string][]string{
		"X-Tenant":    {"acme"},
		"X-Multi":     {"a", "b"},
		"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	binary := envelope.New("test", "file", []byte{0xff, 0x00, 0x01})
	for _, env := range []*envelope.Envelope{sent, binary} {
		_, err := sink.Deliver(context.Background(), env)
		require.NoError(t, err)
	}

	got := recv(t, out)
	assert.Equal(t, sent.ID, got.ID)
	assert.Equal(t, "/hooks/shop", got.Subject)
	assert.Equal(t, "application/json", got.ContentType)
	assert.Equal(t, sent.Body, got.Body)
	assert.Equal(t, map[string][]string{
		"X-Tenant":    {"acme"},
		"X-Multi":     {"a, b"}, // repeated values travel joined
		"Traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}, got.Headers)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", got.Trace.TraceID().String(), "the trace continues")

	got = recv(t, out)
	assert.Equal(t, binary.ID, got.ID)
	assert.Equal(t, []byte{0xff, 0x00, 0x01}, got.Body, "binary-safe")
	assert.Equal(t, "application/octet-stream", got.ContentType)
	assert.Equal(t, "orders", got.Subject)
}

func TestStreamOversizeEntryIsAcked(t *testing.T) {
	mr, admin := startMini(t)
	logs := &records{}
	cfg := streamConfig(mr.Addr())
	cfg.MaxBodyBytes = 8
	s := newTestSourceLog(t, cfg, logs.logger()).(*StreamSource)
	out, _ := run(t, s)
	waitGroup(t, admin, "orders")
	big := xadd(t, admin, "orders", "body", "0123456789abcdef")
	xadd(t, admin, "orders", "body", "small")
	assert.Equal(t, "small", recv(t, out).BodyText())
	require.Eventually(t, func() bool {
		p, err := admin.XPendingExt(context.Background(), &goredis.XPendingExtArgs{Stream: "orders", Group: "relay", Start: big, End: big, Count: 1}).Result()
		return err == nil && len(p) == 0
	}, waitFor, 10*time.Millisecond, "acknowledged without delivery")
	assert.Equal(t, 1, logs.count("over max_body_bytes"))
}

func TestStreamAckAccepted(t *testing.T) {
	mr, admin := startMini(t)
	cfg := streamConfig(mr.Addr())
	cfg.Ack = "accepted"
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "x")
	env := recv(t, out)
	env.Queued() // the engine holds it
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamPrefetch(t *testing.T) {
	mr, admin := startMini(t)
	cfg := streamConfig(mr.Addr())
	cfg.Prefetch = 2
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	for _, b := range []string{"1", "2", "3", "4"} {
		xadd(t, admin, "orders", "body", b)
	}
	first := recv(t, out)
	recv(t, out)
	noRecv(t, out, 300*time.Millisecond)
	settle(first, envelope.Delivered)
	assert.Equal(t, "3", recv(t, out).BodyText(), "a settled entry frees a slot")
	noRecv(t, out, 200*time.Millisecond)
}

func TestStreamReconnects(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	p := newProxy(t, mr.Addr())
	src := newStreamSourceT(t, streamConfig(p.Addr()))
	out, _ := run(t, src)
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "before")
	settle(recv(t, out), envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)

	p.SetRefuse(true)
	p.Drop()
	require.Eventually(t, func() bool { return src.Health(ctx) != nil }, waitFor, 10*time.Millisecond)
	xadd(t, admin, "orders", "body", "during")
	p.SetRefuse(false)

	env := recv(t, out)
	assert.Equal(t, "during", env.BodyText(), "nothing is lost while the connection is down")
	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
	require.Eventually(t, func() bool { return src.Health(ctx) == nil }, waitFor, 10*time.Millisecond)
}

func TestStreamRecreatesALostGroup(t *testing.T) {
	mr, admin := startMini(t)
	ctx := context.Background()
	cfg := streamConfig(mr.Addr())
	cfg.Start = "earliest"
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "one")
	settle(recv(t, out), envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)

	require.NoError(t, admin.Del(ctx, "orders").Err()) // a restart that lost the data
	xadd(t, admin, "orders", "body", "two")
	env := recv(t, out)
	assert.Equal(t, "two", env.BodyText())
	settle(env, envelope.Delivered)
	eventuallyPending(t, admin, "orders", "relay", 0)
}

func TestStreamGivesUpAfterMaxAttempts(t *testing.T) {
	cfg := streamConfig(closedAddr(t))
	cfg.Reconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}
	src := newStreamSourceT(t, cfg)
	err := src.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 2 failed attempts in a row")
	assert.Contains(t, err.Error(), "connection refused")
	assert.Equal(t, err, src.Health(context.Background()))
}

func TestStreamFailsOverToTheNextURL(t *testing.T) {
	mr, admin := startMini(t)
	cfg := streamConfig(closedAddr(t))
	cfg.FailoverURLs = []string{redisURL(mr.Addr())}
	out, _ := run(t, newStreamSourceT(t, cfg))
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "via standby")
	assert.Equal(t, "via standby", recv(t, out).BodyText())
}

func TestStreamStopsWithoutWaitingForBlock(t *testing.T) {
	mr, admin := startMini(t)
	src := newStreamSourceT(t, streamConfig(mr.Addr()))
	src.block = time.Minute
	_, stop := run(t, src)
	waitGroup(t, admin, "orders")
	time.Sleep(50 * time.Millisecond) // blocked in XREADGROUP
	start := time.Now()
	stop()
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.ErrorIs(t, src.Health(context.Background()), errStopped)
}

func TestStreamCloseFlushesAcks(t *testing.T) {
	mr, admin := startMini(t)
	src := newStreamSourceT(t, streamConfig(mr.Addr()))
	out, stop := run(t, src)
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "x")
	env := recv(t, out)
	stop() // the engine stops the source, then settles what is in flight...
	settle(env, envelope.Delivered)
	require.NoError(t, src.Close(context.Background())) // ...then closes it
	assert.Equal(t, int64(0), pendingCount(t, admin, "orders", "relay"), "Close waited for the XACK")
	assert.ErrorIs(t, src.Health(context.Background()), errClosed)
	require.NoError(t, src.Close(context.Background()), "closing twice is fine")
	assert.Error(t, src.Start(context.Background(), make(chan *envelope.Envelope)), "a closed source does not start")
}

func TestStreamCloseGivesUpWhenRedisIsDown(t *testing.T) {
	mr, admin := startMini(t)
	p := newProxy(t, mr.Addr())
	src := newStreamSourceT(t, streamConfig(p.Addr()))
	out, stop := run(t, src)
	waitGroup(t, admin, "orders")
	xadd(t, admin, "orders", "body", "x")
	env := recv(t, out)
	stop()
	p.SetRefuse(true)
	p.Drop()
	settle(env, envelope.Delivered)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := src.Close(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 acknowledgements not sent")
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, int64(1), pendingCount(t, admin, "orders", "relay"), "it stays pending, to be redelivered")
}

func TestStreamHealth(t *testing.T) {
	mr, _ := startMini(t)
	src := newStreamSourceT(t, streamConfig(mr.Addr()))
	require.NoError(t, src.Health(context.Background()), "a PING before it starts")
	mr.Close()
	err := src.Health(context.Background())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "EOF"), err.Error())
}

// waitGroup waits until the source has created its group, so entries added
// next are new to it.
func waitGroup(t *testing.T, admin *goredis.Client, stream string) {
	t.Helper()
	require.Eventually(t, func() bool {
		groups, err := admin.XInfoGroups(context.Background(), stream).Result()
		return err == nil && len(groups) > 0
	}, waitFor, 5*time.Millisecond)
}
