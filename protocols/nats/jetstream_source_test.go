package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// streamCfg is a jetstream source on ORDERS (durable "in").
func streamCfg(ts *testServer) config.Source {
	return config.Source{URL: ts.url(), Stream: "ORDERS", Reconnect: fast()}
}

func publish(t *testing.T, js jetstream.JetStream, subject string, bodies ...string) {
	t.Helper()
	for _, b := range bodies {
		_, err := js.Publish(ctxT(t), subject, []byte(b))
		require.NoError(t, err)
	}
}

// settled waits until the consumer has nothing unacknowledged.
func settled(t *testing.T, js jetstream.JetStream, durable string) {
	t.Helper()
	require.Eventually(t, func() bool {
		info := consumerInfo(t, js, "ORDERS", durable)
		return info.NumAckPending == 0
	}, 10*time.Second, 10*time.Millisecond)
}

func TestStreamSourceAcksOnSuccess(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.Subject = "orders.eu"
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	require.NoError(t, src.Health(ctxT(t)))

	msg := nats.NewMsg("orders.eu")
	msg.Data = []byte(`{"n":1}`)
	msg.Header.Set(headerMsgID, "order-1")
	msg.Header.Set("X-Tenant", "acme")
	_, err := js.PublishMsg(ctxT(t), msg)
	require.NoError(t, err)
	publish(t, js, "orders.us", "not for this consumer")
	publish(t, js, "orders.eu", "two")

	env := recv(t, out)
	assert.Equal(t, "order-1", env.ID, "Nats-Msg-Id is the envelope ID")
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, KindJetStream, env.Protocol)
	assert.Equal(t, "orders.eu", env.Subject)
	assert.Equal(t, `{"n":1}`, string(env.Body))
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, "acme", env.Header("X-Tenant"))
	assert.Equal(t, "ORDERS", env.Attributes["jetstream.stream"])
	assert.Equal(t, int64(1), env.Attributes["jetstream.sequence"])
	assert.Equal(t, "in", env.Attributes["jetstream.consumer"])
	assert.Equal(t, int64(1), env.Attributes["jetstream.delivered"])
	assert.Equal(t, "orders.eu", env.Attributes["jetstream.subject"])
	assert.Equal(t, "orders.eu", env.Attributes["ordering_key"])
	ack(env, envelope.Delivered)

	env = recv(t, out)
	assert.Equal(t, "ORDERS-3", env.ID, "without Nats-Msg-Id: <stream>-<sequence>")
	assert.Equal(t, "two", string(env.Body))
	ack(env, envelope.DeadLettered) // final: acked too

	settled(t, js, "in")
	info := consumerInfo(t, js, "ORDERS", "in")
	assert.Equal(t, 0, info.NumRedelivered)
	assert.Equal(t, uint64(2), info.Delivered.Consumer)
	assert.Equal(t, jetstream.AckExplicitPolicy, info.Config.AckPolicy)
	assert.Equal(t, 30*time.Second, info.Config.AckWait)
	assert.Equal(t, 64, info.Config.MaxAckPending)
	assert.Equal(t, "orders.eu", info.Config.FilterSubject)
	assert.Equal(t, jetstream.DeliverNewPolicy, info.Config.DeliverPolicy)
	none(t, out, 100*time.Millisecond)
}

func TestStreamSourceNaksAndRedeliversFailed(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	src := newTestStreamSource(t, streamCfg(ts))
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "flaky")

	for want := int64(1); want <= 3; want++ {
		env := recv(t, out)
		assert.Equal(t, "ORDERS-1", env.ID, "the same ID on every delivery")
		assert.Equal(t, want, env.Attributes["jetstream.delivered"])
		if want < 3 {
			ack(env, envelope.Failed) // nak: redelivered after a short delay
		} else {
			ack(env, envelope.Delivered)
		}
	}
	settled(t, js, "in")
	none(t, out, 100*time.Millisecond)
	assert.Equal(t, 0, src.flights.len())
}

func TestStreamSourceResumesWhereItStopped(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	first := newTestStreamSource(t, streamCfg(ts))
	out, stop := run(t, first)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "1", "2")
	ack(recv(t, out), envelope.Delivered)
	ack(recv(t, out), envelope.Delivered)
	settled(t, js, "in")
	stop()
	require.NoError(t, first.Close(ctxT(t)))

	publish(t, js, "orders.new", "3", "4") // while no relay runs
	second := newTestStreamSource(t, streamCfg(ts))
	out, _ = run(t, second)
	for _, want := range []string{"3", "4"} {
		env := recv(t, out)
		assert.Equal(t, want, string(env.Body))
		assert.Equal(t, int64(1), env.Attributes["jetstream.delivered"])
		ack(env, envelope.Delivered)
	}
	none(t, out, 200*time.Millisecond) // 1 and 2 were acked: never again
}

func TestStreamSourceReportsSlowMessagesInProgress(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.AckWait = config.Duration(600 * time.Millisecond)
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "slow")

	env := recv(t, out)
	// The relay works on it for three times ack_wait: it is not
	// redelivered, because the source reports it in progress.
	none(t, out, 1800*time.Millisecond)
	assert.Equal(t, 0, consumerInfo(t, js, "ORDERS", "in").NumRedelivered)
	ack(env, envelope.Delivered)
	settled(t, js, "in")
	none(t, out, 700*time.Millisecond)
}

func TestStreamSourceWithoutProgressReportsIsRedelivered(t *testing.T) {
	// The control for the test above: the same message, not reported in
	// progress, comes back after ack_wait.
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.AckWait = config.Duration(600 * time.Millisecond)
	src := newTestStreamSource(t, cfg)
	src.progressEvery = time.Hour
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "slow")
	first := recv(t, out)
	again := recv(t, out)
	assert.Equal(t, first.ID, again.ID)
	assert.Equal(t, int64(2), again.Attributes["jetstream.delivered"])
	ack(again, envelope.Delivered)
	ack(first, envelope.Delivered)
}

func TestStreamSourceStartPolicies(t *testing.T) {
	cases := map[string][]string{
		"":     {"4"}, // new
		"new":  {"4"},
		"all":  {"1", "2", "3", "4"},
		"last": {"3", "4"},
	}
	for start, want := range cases {
		t.Run("start="+start, func(t *testing.T) {
			ts := startServer(t)
			js := ts.stream("ORDERS", "orders.>")
			publish(t, js, "orders.new", "1", "2", "3")
			cfg := streamCfg(ts)
			cfg.Start = start
			src := newTestStreamSource(t, cfg)
			out, _ := run(t, src)
			pulling(t, js, "ORDERS", "in")
			publish(t, js, "orders.new", "4")
			var got []string
			for len(got) < len(want) {
				env := recv(t, out)
				got = append(got, string(env.Body))
				ack(env, envelope.Delivered)
			}
			assert.Equal(t, want, got)
			none(t, out, 100*time.Millisecond)
		})
	}
}

func TestStreamSourceUpdatesAnExistingConsumer(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	publish(t, js, "orders.new", "old")
	// An operator made the durable: from the start, with a description and
	// a delivery limit.
	_, err := js.CreateOrUpdateConsumer(ctxT(t), "ORDERS", jetstream.ConsumerConfig{
		Durable: "in", DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: time.Minute, MaxAckPending: 1000, Description: "kept", MaxDeliver: 7, FilterSubject: "orders.old",
	})
	require.NoError(t, err)
	cfg := streamCfg(ts)
	cfg.Start, cfg.Prefetch, cfg.AckWait, cfg.Subject = "new", 8, config.Duration(5*time.Second), "orders.>"
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")

	info := consumerInfo(t, js, "ORDERS", "in")
	assert.Equal(t, jetstream.DeliverAllPolicy, info.Config.DeliverPolicy, "where it started is kept")
	assert.Equal(t, "kept", info.Config.Description)
	assert.Equal(t, 7, info.Config.MaxDeliver)
	assert.Equal(t, 8, info.Config.MaxAckPending)
	assert.Equal(t, 5*time.Second, info.Config.AckWait)
	assert.Equal(t, "orders.>", info.Config.FilterSubject)
	env := recv(t, out)
	assert.Equal(t, "old", string(env.Body), "the kept policy delivers from the start")
	ack(env, envelope.Delivered)
}

func TestStreamSourceStopGivesBackWhatTheRelayDidNotTake(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.AckWait = config.Duration(time.Minute) // a redelivery in this test can only come from a nak
	src := newTestStreamSource(t, cfg)
	out := make(chan *envelope.Envelope) // the relay never takes anything
	stop := runOut(t, src, out)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "1", "2", "3", "4", "5")
	require.Eventually(t, func() bool { return consumerInfo(t, js, "ORDERS", "in").NumAckPending == 5 },
		10*time.Second, 10*time.Millisecond)
	stop()
	require.NoError(t, src.Close(ctxT(t)))

	next := newTestStreamSource(t, cfg)
	out2, _ := run(t, next)
	var got []string
	for range 5 {
		env := recv(t, out2)
		assert.Equal(t, int64(2), env.Attributes["jetstream.delivered"])
		got = append(got, string(env.Body))
		ack(env, envelope.Delivered)
	}
	assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5"}, got)
}

func TestStreamSourceCloseWaitsForAcks(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	src := newTestStreamSource(t, streamCfg(ts))
	out, stop := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "1")
	env := recv(t, out)
	stop() // the relay still holds the message

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
	ack(env, envelope.Delivered) // the ack goes out on the open connection
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
	settled(t, js, "in")
	assert.Equal(t, 0, consumerInfo(t, js, "ORDERS", "in").NumRedelivered)
	require.NoError(t, src.Close(context.Background()), "closing twice")
}

func TestStreamSourceCloseGivesBackUnsettledMessages(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.AckWait = config.Duration(time.Minute)
	src := newTestStreamSource(t, cfg)
	out, stop := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "held")
	recv(t, out) // the relay never settles it
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, src.Close(ctx))

	next := newTestStreamSource(t, cfg)
	out2, _ := run(t, next)
	env := recv(t, out2)
	assert.Equal(t, "held", string(env.Body))
	assert.Equal(t, int64(2), env.Attributes["jetstream.delivered"])
	ack(env, envelope.Delivered)
}

func TestStreamSourceAckAccepted(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.Ack = "accepted"
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "1")
	env := recv(t, out)
	assert.Equal(t, 1, consumerInfo(t, js, "ORDERS", "in").NumAckPending)
	env.Queued() // the relay holds it: acked now
	settled(t, js, "in")
	ack(env, envelope.Failed) // too late to nak
	none(t, out, 200*time.Millisecond)
}

func TestStreamSourceTerminatesOversizeMessages(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.MaxBodyBytes = 4
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "far too large", "ok")
	env := recv(t, out)
	assert.Equal(t, "ok", string(env.Body))
	ack(env, envelope.Delivered)
	settled(t, js, "in")
	assert.Equal(t, 0, consumerInfo(t, js, "ORDERS", "in").NumRedelivered)
	none(t, out, 200*time.Millisecond)
}

func TestStreamSourceRecreatesADeletedConsumer(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	src := newTestStreamSource(t, streamCfg(ts))
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "1")
	ack(recv(t, out), envelope.Delivered)
	settled(t, js, "in")

	require.NoError(t, js.DeleteConsumer(ctxT(t), "ORDERS", "in"))
	pulling(t, js, "ORDERS", "in") // set up again
	publish(t, js, "orders.new", "2")
	env := recv(t, out)
	assert.Equal(t, "2", string(env.Body))
	ack(env, envelope.Delivered)
}

func TestStreamSourceWaitsForTheStream(t *testing.T) {
	ts := startServer(t)
	cfg := streamCfg(ts)
	cfg.Stream = "LATER"
	src := newTestStreamSource(t, cfg)
	out, _ := run(t, src)
	require.Eventually(t, func() bool {
		err := src.Health(context.Background())
		return err != nil && strings.Contains(err.Error(), "stream not found")
	}, 10*time.Second, 10*time.Millisecond)

	js := ts.js()
	_, err := js.CreateStream(ctxT(t), jetstream.StreamConfig{Name: "LATER", Subjects: []string{"later.>"}})
	require.NoError(t, err)
	pulling(t, js, "LATER", "in")
	require.NoError(t, src.Health(context.Background()))
	_, err = js.Publish(ctxT(t), "later.x", []byte("finally"))
	require.NoError(t, err)
	env := recv(t, out)
	assert.Equal(t, "finally", string(env.Body))
	ack(env, envelope.Delivered)
}

func TestStreamSourceGivesUpAfterMaxAttempts(t *testing.T) {
	ts := startServer(t)
	cfg := streamCfg(ts)
	cfg.Stream = "MISSING"
	cfg.Reconnect = &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond), MaxBackoff: config.Duration(10 * time.Millisecond), MaxAttempts: 2}
	src := newTestStreamSource(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := src.Start(ctx, make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 2 failed attempts to set up consumer in on stream MISSING")
	assert.ErrorIs(t, err, jetstream.ErrStreamNotFound)

	down := newTestStreamSource(t, config.Source{URL: "nats://" + closedAddr(t), Stream: "ORDERS",
		Reconnect: &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond), MaxBackoff: config.Duration(10 * time.Millisecond), MaxAttempts: 1}})
	err = down.Start(ctx, make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconnect.max_attempts")
}

func TestStreamSourceReconnectsAfterServerRestart(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	src := newTestStreamSource(t, streamCfg(ts))
	out, _ := run(t, src)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "before")
	ack(recv(t, out), envelope.Delivered)
	settled(t, js, "in")

	ts.restart()
	js = ts.js()
	require.Eventually(t, func() bool { return src.Health(context.Background()) == nil }, 10*time.Second, 10*time.Millisecond)
	pulling(t, js, "ORDERS", "in")
	publish(t, js, "orders.new", "after")
	env := recv(t, out)
	assert.Equal(t, "after", string(env.Body))
	ack(env, envelope.Delivered)
	settled(t, js, "in")
}

func TestStreamSourceHealth(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	src := newTestStreamSource(t, streamCfg(ts))
	assert.Equal(t, "in", src.Name())
	assert.ErrorIs(t, src.Health(context.Background()), errNotStarted)
	out, stop := run(t, src)
	pulling(t, js, "ORDERS", "in")
	require.NoError(t, src.Health(context.Background()))
	assert.ErrorContains(t, src.Start(context.Background(), out), "already started")

	ts.stop()
	require.Eventually(t, func() bool {
		err := src.Health(context.Background())
		return err != nil && strings.Contains(err.Error(), "not connected")
	}, 10*time.Second, 10*time.Millisecond)
	stop()
	assert.ErrorIs(t, src.Health(context.Background()), errStopped)
	require.NoError(t, src.Close(ctxT(t)))

	never := newTestStreamSource(t, streamCfg(ts))
	require.NoError(t, never.Close(context.Background()), "closing a source that never started")
	assert.ErrorIs(t, never.Health(context.Background()), errNotStarted)
}

func TestStreamSourceKeepsOrderUnderBackpressure(t *testing.T) {
	ts := startServer(t)
	js := ts.stream("ORDERS", "orders.>")
	cfg := streamCfg(ts)
	cfg.Prefetch = 4
	src := newTestStreamSource(t, cfg)
	out := make(chan *envelope.Envelope) // the relay takes one at a time
	runOut(t, src, out)
	pulling(t, js, "ORDERS", "in")
	const n = 20
	for i := range n {
		publish(t, js, "orders.new", fmt.Sprint(i))
	}
	for i := range n {
		env := recv(t, out)
		assert.Equal(t, fmt.Sprint(i), string(env.Body))
		ack(env, envelope.Delivered)
	}
	settled(t, js, "in")
}
