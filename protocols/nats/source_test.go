package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

const connIn = "hikyaku-test-in"

func TestSourceSubscribesAndMapsTheMessage(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders.*", Reconnect: fast()})
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders.*")
	require.NoError(t, src.Health(context.Background()))

	pub := ts.client()
	msg := nats.NewMsg("orders.eu")
	msg.Data = []byte(`{"id":1}`)
	msg.Header.Set("X-Tenant", "acme")
	msg.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	msg.Header.Set(headerMsgID, "order-1")
	msg.Reply = "_INBOX.reply"
	require.NoError(t, pub.PublishMsg(msg))

	env := recv(t, out)
	assert.Equal(t, "order-1", env.ID, "Nats-Msg-Id is the envelope ID")
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, KindNATS, env.Protocol)
	assert.Equal(t, "orders.eu", env.Subject)
	assert.Equal(t, `{"id":1}`, string(env.Body))
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, []string{"acme"}, env.Headers["X-Tenant"])
	assert.Equal(t, "acme", env.Header("x-tenant"))
	assert.Equal(t, "orders.eu", env.Attributes["nats.subject"])
	assert.Equal(t, "_INBOX.reply", env.Attributes["nats.reply"])
	assert.Equal(t, "orders.eu", env.Attributes["ordering_key"])
	assert.True(t, env.Trace.IsValid(), "the traceparent header continues the trace")
	ack(env, envelope.Delivered)

	// Idempotency-Key when there is no Nats-Msg-Id; else a ULID. No reply,
	// no nats.reply.
	msg = nats.NewMsg("orders.us")
	msg.Data = []byte("plain text")
	msg.Header.Set("Idempotency-Key", "key-2")
	require.NoError(t, pub.PublishMsg(msg))
	env = recv(t, out)
	assert.Equal(t, "key-2", env.ID)
	assert.Equal(t, contentText, env.ContentType)
	assert.NotContains(t, env.Attributes, "nats.reply")
	ack(env, envelope.Delivered)

	require.NoError(t, pub.Publish("orders.asia", []byte{0xff, 0x00}))
	env = recv(t, out)
	assert.Len(t, env.ID, 26, "a ULID")
	assert.Equal(t, contentBinary, env.ContentType)
	ack(env, envelope.Delivered)

	// The wildcard is one token: orders.eu.x is not delivered.
	require.NoError(t, pub.Publish("orders.eu.x", []byte("no")))
	require.NoError(t, pub.Flush())
	none(t, out, 100*time.Millisecond)
}

func TestSourceQueueGroupShares(t *testing.T) {
	ts := startServer(t)
	// A burst over prefetch would be dropped (see the slow consumer test):
	// this test is about sharing.
	a := newTestSource(t, config.Source{URL: ts.url(), Subject: "jobs", Queue: "workers", Prefetch: 1000})
	b := newTestSourceNamed(t, "in2", config.Source{URL: ts.url(), Subject: "jobs", Queue: "workers", Prefetch: 1000})
	all := newTestSourceNamed(t, "all", config.Source{URL: ts.url(), Subject: "jobs", Prefetch: 1000})

	outA, _ := run(t, a)
	outB, _ := run(t, b)
	outAll, _ := run(t, all)
	ts.subscribed(connIn, "jobs")
	ts.subscribed("hikyaku-test-in2", "jobs")
	ts.subscribed("hikyaku-test-all", "jobs")

	pub := ts.client()
	const n = 200
	for i := range n {
		require.NoError(t, pub.Publish("jobs", []byte(fmt.Sprint(i))))
	}
	seen := map[string]int{}
	got := 0
	deadline := time.After(10 * time.Second)
	for got < n {
		select {
		case env := <-outA:
			seen[string(env.Body)]++
			got++
			ack(env, envelope.Delivered)
		case env := <-outB:
			seen[string(env.Body)]++
			got++
			ack(env, envelope.Delivered)
		case <-deadline:
			t.Fatalf("the queue group delivered %d of %d messages", got, n)
		}
	}
	assert.Len(t, seen, n)
	for body, count := range seen {
		assert.Equal(t, 1, count, "message %s went to more than one member", body)
	}
	none(t, outA, 50*time.Millisecond)
	none(t, outB, 50*time.Millisecond)
	// A subscriber outside the group gets every message.
	for range n {
		ack(recv(t, outAll), envelope.Delivered)
	}
}

func newTestSourceNamed(t *testing.T, name string, cfg config.Source) *Source {
	t.Helper()
	cfg.Name, cfg.Type = name, KindNATS
	sp := spec(t, KindNATS, &cfg)
	sp.Name = name
	s, err := newSource(sp)
	require.NoError(t, err)
	closeAtCleanup(t, s)
	return s
}

func TestSourceSettleCannotRedeliver(t *testing.T) {
	ts := startServer(t)
	logs := &logBuffer{}
	cfg := config.Source{Name: "in", Type: KindNATS, URL: ts.url(), Subject: "orders"}
	sp := spec(t, KindNATS, &cfg)
	sp.Env.Logger = testLogger(t, logs)
	src, err := newSource(sp)
	require.NoError(t, err)
	out, stop := run(t, src)
	ts.subscribed(connIn, "orders")

	pub := ts.client()
	require.NoError(t, pub.Publish("orders", []byte("1")))
	require.NoError(t, pub.Publish("orders", []byte("2")))
	ack(recv(t, out), envelope.Delivered)
	failed := recv(t, out)
	ack(failed, envelope.Failed)
	assert.Equal(t, int64(1), src.Lost())
	assert.Contains(t, logs.String(), "core NATS cannot redeliver")
	assert.Contains(t, logs.String(), failed.ID)
	stop()
	require.NoError(t, src.Close(ctxT(t)))
}

func TestSourceAckAcceptedSettlesOnQueued(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", Ack: "accepted"})
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders")
	require.NoError(t, ts.client().Publish("orders", []byte("1")))
	env := recv(t, out)
	assert.Equal(t, 1, src.flights.len())
	env.Queued()
	assert.Equal(t, 0, src.flights.len())
	ack(env, envelope.Failed) // too late: accepted already settled it
	assert.Equal(t, int64(0), src.Lost())
}

func TestSourceDropsOversizeMessages(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", MaxBodyBytes: 4})
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders")
	pub := ts.client()
	require.NoError(t, pub.Publish("orders", []byte("too large")))
	require.NoError(t, pub.Publish("orders", []byte("ok")))
	env := recv(t, out)
	assert.Equal(t, "ok", string(env.Body))
	ack(env, envelope.Delivered)
	assert.Equal(t, int64(1), src.Lost())
}

func TestSourceSlowConsumerDrops(t *testing.T) {
	ts := startServer(t)
	logs := &logBuffer{}
	cfg := config.Source{Name: "in", Type: KindNATS, URL: ts.url(), Subject: "flood", Prefetch: 2}
	sp := spec(t, KindNATS, &cfg)
	sp.Env.Logger = testLogger(t, logs)
	src, err := newSource(sp)
	require.NoError(t, err)
	closeAtCleanup(t, src)
	out := make(chan *envelope.Envelope) // nobody reads: the relay is stuck
	runOut(t, src, out)
	ts.subscribed(connIn, "flood")

	pub := ts.client()
	for i := range 50 {
		require.NoError(t, pub.Publish("flood", []byte(fmt.Sprint(i))))
	}
	require.NoError(t, pub.Flush())
	require.Eventually(t, func() bool { return src.Dropped() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "too slow") }, 5*time.Second, 10*time.Millisecond)
	// The relay takes them again: the source goes on with what it kept.
	ack(recv(t, out), envelope.Delivered)
	ack(recv(t, out), envelope.Delivered)
}

func TestSourceReconnectsAfterServerRestart(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", Reconnect: fast()})
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders")
	require.NoError(t, ts.client().Publish("orders", []byte("before")))
	env := recv(t, out)
	assert.Equal(t, "before", string(env.Body))
	ack(env, envelope.Delivered)

	ts.stop()
	require.Eventually(t, func() bool { return src.Health(context.Background()) != nil }, 5*time.Second, 10*time.Millisecond)
	ts.start()
	ts.subscribed(connIn, "orders") // the subscription is sent again
	require.Eventually(t, func() bool { return src.Health(context.Background()) == nil }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, ts.client().Publish("orders", []byte("after")))
	env = recv(t, out)
	assert.Equal(t, "after", string(env.Body))
	ack(env, envelope.Delivered)
}

func TestSourceStartsWhileTheServerIsDown(t *testing.T) {
	ts := startServer(t)
	ts.stop()
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", Reconnect: fast()})
	out, _ := run(t, src)
	require.Eventually(t, func() bool {
		err := src.Health(context.Background())
		return err != nil && strings.Contains(err.Error(), "not connected")
	}, 5*time.Second, 10*time.Millisecond)
	ts.start()
	ts.subscribed(connIn, "orders")
	require.NoError(t, ts.client().Publish("orders", []byte("late")))
	env := recv(t, out)
	assert.Equal(t, "late", string(env.Body))
	ack(env, envelope.Delivered)
}

func TestSourceGivesUpAfterMaxAttempts(t *testing.T) {
	src := newTestSource(t, config.Source{URL: "nats://" + closedAddr(t), Subject: "orders",
		Reconnect: &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond), MaxBackoff: config.Duration(10 * time.Millisecond), MaxAttempts: 2}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := src.Start(ctx, make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gave up after reconnect.max_attempts (2) reconnects to each server")
	assert.ErrorIs(t, src.Health(ctx), errStopped)
}

func TestSourceFailsWhenTheSubscriptionIsRefused(t *testing.T) {
	ts := startServer(t, func(o *server.Options) {
		o.Users = []*server.User{{Username: "app", Password: "pw", Permissions: &server.Permissions{
			Subscribe: &server.SubjectPermission{Allow: []string{"allowed.>", "_INBOX.>"}},
		}}}
	})
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "secret.orders", Username: "app", Password: "pw"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := src.Start(ctx, make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subscribe to secret.orders")
	assert.ErrorIs(t, err, nats.ErrPermissionViolation)
}

func TestSourceHealthAndClose(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders"})
	assert.Equal(t, "in", src.Name())
	assert.ErrorIs(t, src.Health(context.Background()), errNotStarted)
	require.NoError(t, src.Close(context.Background()), "closing a source that never started")

	src = newTestSource(t, config.Source{URL: ts.url(), Subject: "orders"})
	out, stop := run(t, src)
	ts.subscribed(connIn, "orders")
	require.NoError(t, src.Health(context.Background()))
	assert.ErrorContains(t, src.Start(context.Background(), out), "already started")

	require.NoError(t, ts.client().Publish("orders", []byte("1")))
	env := recv(t, out)
	stop()
	assert.ErrorIs(t, src.Health(context.Background()), errStopped)

	// Close waits for the message the relay still holds.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		assert.NoError(t, src.Close(ctxT(t)))
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the relay settled its message")
	case <-time.After(100 * time.Millisecond):
	}
	ack(env, envelope.Delivered)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	src.mu.Lock()
	c := src.conn
	src.mu.Unlock()
	assert.True(t, c.nc.IsClosed())
	require.NoError(t, src.Close(context.Background()), "closing twice")
}

func TestSourceCloseGivesUpWaitingWithItsContext(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders"})
	out, stop := run(t, src)
	ts.subscribed(connIn, "orders")
	require.NoError(t, ts.client().Publish("orders", []byte("1")))
	recv(t, out) // never settled
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, src.Close(ctx))
}

func TestSourceStopDropsWhatTheRelayDidNotTake(t *testing.T) {
	ts := startServer(t)
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders"})
	out := make(chan *envelope.Envelope) // nobody reads
	stop := runOut(t, src, out)
	ts.subscribed(connIn, "orders")
	pub := ts.client()
	for i := range 3 {
		require.NoError(t, pub.Publish("orders", []byte(fmt.Sprint(i))))
	}
	require.NoError(t, pub.Flush())
	require.Eventually(t, func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return src.conn != nil && src.conn.nc.Stats().InMsgs >= 3
	}, 5*time.Second, 5*time.Millisecond)
	stop()
	assert.Equal(t, int64(3), src.Lost(), "one cut while handed over, two pending")
}

func TestSourceLogsHideCredentials(t *testing.T) {
	ts := startServer(t, func(o *server.Options) { o.Username, o.Password = "app", "hunter2" })
	logs := &logBuffer{}
	cfg := config.Source{Name: "in", Type: KindNATS, Subject: "orders", Reconnect: fast(),
		URL: strings.Replace(ts.url(), "nats://", "nats://app:hunter2@", 1)}
	sp := spec(t, KindNATS, &cfg)
	sp.Env.Logger = testLogger(t, logs)
	src, err := newSource(sp)
	require.NoError(t, err)
	closeAtCleanup(t, src)
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders")
	ts.restart() // logs a disconnect and a reconnect
	ts.subscribed(connIn, "orders")
	require.NoError(t, ts.client(nats.UserInfo("app", "hunter2")).Publish("orders", []byte("x")))
	ack(recv(t, out), envelope.Delivered)
	text := logs.String()
	assert.Contains(t, text, "nats reconnected")
	assert.Contains(t, text, "app:xxxxx@")
	assert.NotContains(t, text, "hunter2")
}
