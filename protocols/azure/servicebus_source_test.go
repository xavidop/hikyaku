package azure

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

const waitFor, tick = 3 * time.Second, 5 * time.Millisecond

func newTestSBSource(t *testing.T, src config.Source) (*sbSource, *fakeSB) {
	t.Helper()
	if src.Azure == nil {
		src.Azure = connStr()
	}
	if src.Queue == "" && src.Topic == "" {
		src.Queue = "orders"
	}
	spec := plugin.Spec{Name: "in", Kind: kindServiceBus, Config: &src}
	opts, err := parseSBSource(spec)
	require.NoError(t, err)
	fake := newFakeSB()
	s := buildSBSource(spec, opts, fake)
	s.renewEvery, s.sessionIdle = 10*time.Millisecond, 50*time.Millisecond
	return s, fake
}

// run starts the source and returns its envelopes and a stop function that
// waits for Start to return and gives its error.
func run(t *testing.T, s plugin.Source, buffer int) (<-chan *envelope.Envelope, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *envelope.Envelope, buffer)
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	stop := func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(waitFor):
			t.Fatal("Start did not return")
			return nil
		}
	}
	t.Cleanup(func() { cancel() })
	return out, stop
}

func next(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(waitFor):
		t.Fatal("no envelope")
		return nil
	}
}

func sbMessage(id string, body string) *azservicebus.ReceivedMessage {
	until := time.Now().Add(time.Minute)
	return &azservicebus.ReceivedMessage{MessageID: id, Body: []byte(body), LockedUntil: &until, DeliveryCount: 1}
}

func TestSBSourceCompletesAndAbandons(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	out, stop := run(t, s, 10)
	fake.recv.msgs <- sbMessage("m1", `{"a":1}`)
	fake.recv.msgs <- sbMessage("m2", "plain")
	fake.recv.msgs <- sbMessage("m3", "x")
	fake.recv.msgs <- sbMessage("m4", "x")

	ctx := context.Background()
	next(t, out).Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	next(t, out).Ack(ctx, envelope.Outcome{Status: envelope.Failed})
	next(t, out).Ack(ctx, envelope.Outcome{Status: envelope.DeadLettered})
	next(t, out).Ack(ctx, envelope.Outcome{Status: envelope.Filtered})
	require.Eventually(t, func() bool {
		c, a := fake.recv.done()
		return len(c) == 3 && len(a) == 1
	}, waitFor, tick)
	c, a := fake.recv.done()
	assert.ElementsMatch(t, []string{"m1", "m3", "m4"}, c, "final outcomes complete")
	assert.Equal(t, []string{"m2"}, a, "a failed message is abandoned: Service Bus redelivers it")

	require.NoError(t, stop())
	require.NoError(t, s.Close(ctx))
	assert.True(t, fake.recv.isClosed())
	assert.True(t, fake.closed.Load())
	assert.ErrorIs(t, s.Health(ctx), errStopped)
}

func TestSBSourceEnvelope(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{Topic: "events", Subscription: "audit"})
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()

	enq := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	m := sbMessage("mid-1", `{"n":1}`)
	m.ApplicationProperties = map[string]any{"tenant": "acme", "attempt": int64(2)}
	m.SequenceNumber = ptr(int64(77))
	m.EnqueuedTime = &enq
	m.DeliveryCount = 3
	m.CorrelationID = ptr("corr")
	fake.recv.msgs <- m
	env := next(t, out)
	assert.Equal(t, "mid-1", env.ID)
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, kindServiceBus, env.Protocol)
	assert.Equal(t, "events", env.Subject, "no Subject: the topic")
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, map[string][]string{"tenant": {"acme"}, "attempt": {"2"}}, env.Headers)
	assert.Equal(t, map[string]any{
		"servicebus.message_id": "mid-1", "servicebus.sequence_number": int64(77), "servicebus.delivery_count": int64(3),
		"servicebus.enqueued_time": "2026-11-09T08:00:00Z", "servicebus.entity": "events/subscriptions/audit",
		"servicebus.correlation_id": "corr", "ordering_key": "events/subscriptions/audit",
	}, env.Attributes)

	m = sbMessage("mid-2", "hi")
	m.Subject = ptr("orders.created")
	m.ContentType = ptr("text/x-custom")
	m.SessionID = ptr("s-9")
	m.ApplicationProperties = map[string]any{"Idempotency-Key": "idem-7"}
	fake.recv.msgs <- m
	env = next(t, out)
	assert.Equal(t, "idem-7", env.ID, "Idempotency-Key wins over MessageId")
	attr, _ := env.Attribute("idempotency_key")
	assert.Equal(t, "idem-7", attr)
	assert.Equal(t, "orders.created", env.Subject)
	assert.Equal(t, "text/x-custom", env.ContentType)
	attr, _ = env.Attribute("ordering_key")
	assert.Equal(t, "s-9", attr, "the session is the order unit")
	attr, _ = env.Attribute("servicebus.session_id")
	assert.Equal(t, "s-9", attr)

	m = sbMessage("", "")
	m.RawAMQPMessage = &azservicebus.AMQPAnnotatedMessage{Body: azservicebus.AMQPAnnotatedMessageBody{Value: "from value"}}
	m.Body = nil
	fake.recv.msgs <- m
	env = next(t, out)
	assert.Equal(t, "from value", string(env.Body), "an amqp-value body")
	assert.NotEmpty(t, env.ID, "no MessageId: a new ID")
}

func TestSBSourceAcceptedAndOversize(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{Ack: "accepted", MaxBodyBytes: 4})
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()

	fake.recv.msgs <- sbMessage("big", "too large")
	fake.recv.msgs <- sbMessage("ok", "fine")
	env := next(t, out)
	assert.Equal(t, "ok", env.ID, "an oversized message is not delivered")
	require.Eventually(t, func() bool { c, _ := fake.recv.done(); return len(c) == 1 }, waitFor, tick)
	c, _ := fake.recv.done()
	assert.Equal(t, []string{"big"}, c, "it is completed, or it would come back forever")

	env.Queued()
	require.Eventually(t, func() bool { c, _ := fake.recv.done(); return len(c) == 2 }, waitFor, tick)
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	time.Sleep(20 * time.Millisecond)
	c, a := fake.recv.done()
	assert.Equal(t, []string{"big", "ok"}, c, "ack: accepted completes once the relay holds the message")
	assert.Empty(t, a, "settled once")
}

func TestSBSourcePrefetchBoundsInFlight(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{Prefetch: 2})
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()
	for i := range 5 {
		fake.recv.msgs <- sbMessage(fmt.Sprint("m", i), "x")
	}
	first, second := next(t, out), next(t, out)
	select {
	case env := <-out:
		t.Fatalf("a third message %s while two are in flight", env.ID)
	case <-time.After(50 * time.Millisecond):
	}
	for _, n := range fake.recv.askedFor() {
		assert.LessOrEqual(t, n, 2)
	}
	first.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	third := next(t, out)
	assert.Equal(t, "m2", third.ID)
	second.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	third.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	assert.Equal(t, "m3", next(t, out).ID)
}

func TestSBSourceRenewsLocks(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	fake.recv.lockFor = 60 * time.Millisecond
	out, stop := run(t, s, 10)
	defer func() { _ = stop() }()

	m := sbMessage("slow", "x")
	until := time.Now().Add(60 * time.Millisecond)
	m.LockedUntil = &until
	fake.recv.msgs <- m
	env := next(t, out)
	require.Eventually(t, func() bool { return fake.recv.renewed("slow") >= 3 }, waitFor, tick,
		"a message in flight past half its lock is renewed, again and again")
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	require.Eventually(t, func() bool { c, _ := fake.recv.done(); return len(c) == 1 }, waitFor, tick)
	n := fake.recv.renewed("slow")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, n, fake.recv.renewed("slow"), "a settled message is not renewed")

	// A lost lock stops the renewals of that message.
	fake.recv.mu.Lock()
	fake.recv.renewErr = &azservicebus.Error{Code: azservicebus.CodeLockLost}
	fake.recv.mu.Unlock()
	m = sbMessage("lost", "x")
	until = time.Now().Add(20 * time.Millisecond)
	m.LockedUntil = &until
	fake.recv.msgs <- m
	env = next(t, out)
	require.Eventually(t, func() bool { return fake.recv.renewed("lost") == 1 }, waitFor, tick)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, fake.recv.renewed("lost"))
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
}

func TestSBSourceShutdownAbandonsWhatItHeld(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	_, stop := run(t, s, 0) // nobody reads: the source blocks handing over
	fake.recv.msgs <- sbMessage("held", "x")
	require.Eventually(t, func() bool { return len(fake.recv.askedFor()) > 0 && len(fake.recv.msgs) == 0 }, waitFor, tick)
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, stop())
	require.Eventually(t, func() bool { _, a := fake.recv.done(); return len(a) == 1 }, waitFor, tick)
	_, a := fake.recv.done()
	assert.Equal(t, []string{"held"}, a, "a message never handed over goes back at once")
	require.NoError(t, s.Close(context.Background()))
}

func TestSBSourceCloseWaitsForInFlight(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	out, stop := run(t, s, 10)
	fake.recv.msgs <- sbMessage("m1", "x")
	env := next(t, out)
	require.NoError(t, stop())

	closed := make(chan struct{})
	go func() { _ = s.Close(context.Background()); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned with a message in flight")
	case <-time.After(50 * time.Millisecond):
	}
	assert.False(t, fake.recv.isClosed(), "the receiver stays open to settle")
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	select {
	case <-closed:
	case <-time.After(waitFor):
		t.Fatal("Close did not return")
	}
	c, _ := fake.recv.done()
	assert.Equal(t, []string{"m1"}, c)
	assert.True(t, fake.recv.isClosed())
	assert.True(t, fake.closed.Load())
	require.NoError(t, s.Close(context.Background()), "twice is fine")

	// Close gives up when ctx ends.
	s, fake = newTestSBSource(t, config.Source{})
	out, stop = run(t, s, 10)
	fake.recv.msgs <- sbMessage("stuck", "x")
	next(t, out)
	require.NoError(t, stop())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Close(ctx))
	assert.True(t, fake.recv.isClosed())
}

func TestSBSourceReconnectsAndGivesUp(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond),
		MaxBackoff: config.Duration(5 * time.Millisecond), MaxAttempts: 3}})
	ctx := context.Background()
	assert.ErrorIs(t, s.Health(ctx), errNotStarted)

	fake.recv.errs <- &azservicebus.Error{Code: azservicebus.CodeConnectionLost}
	fake.recv.errs <- &azservicebus.Error{Code: azservicebus.CodeConnectionLost}
	fake.recv.msgs <- sbMessage("after", "x")
	out, stop := run(t, s, 10)
	env := next(t, out)
	assert.Equal(t, "after", env.ID, "a receive failure is retried")
	require.Eventually(t, func() bool { return s.Health(ctx) == nil }, waitFor, tick, "a good receive clears the failure")
	fake.recv.peekErr = errors.New("peek failed")
	assert.ErrorContains(t, s.Health(ctx), "peek failed")
	fake.recv.peekErr = nil
	require.NoError(t, stop())
	env.Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	require.NoError(t, s.Close(ctx))

	s, fake = newTestSBSource(t, config.Source{Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond),
		MaxBackoff: config.Duration(time.Millisecond), MaxAttempts: 2}})
	for range 3 {
		fake.recv.errs <- &azservicebus.Error{Code: azservicebus.CodeUnauthorizedAccess}
	}
	err := s.Start(ctx, make(chan *envelope.Envelope))
	assert.ErrorContains(t, err, "giving up after 2 failed receives")
	assert.ErrorContains(t, s.Health(ctx), "unauthorized")
	assert.ErrorContains(t, s.Start(ctx, nil), "already started")
}

func TestSBSourceSessions(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	s.maxSessions = 2
	fake.recv.errs <- &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondNotAllowed,
		Description: "It is not possible for an entity that requires sessions to create a non-sessionful message receiver."}}
	a := newFakeSession("a", time.Minute, sbMessage("a1", "x"), sbMessage("a2", "x"))
	b := newFakeSession("b", 40*time.Millisecond, sbMessage("b1", "x"))
	fake.sessions <- a
	fake.sessions <- b
	out, stop := run(t, s, 10)

	got := map[string]*envelope.Envelope{}
	for range 3 {
		env := next(t, out)
		got[env.ID] = env
	}
	require.Len(t, got, 3)
	sid, _ := got["a2"].Attribute("servicebus.session_id")
	assert.Equal(t, "a", sid)
	key, _ := got["b1"].Attribute("ordering_key")
	assert.Equal(t, "b", key)
	assert.True(t, fake.recv.isClosed(), "the receiver without sessions is closed")

	// b's lock is short: it is renewed while b1 is in flight.
	require.Eventually(t, func() bool { n, _ := b.state(); return n >= 2 }, waitFor, tick)

	ctx := context.Background()
	got["a1"].Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	got["a2"].Ack(ctx, envelope.Outcome{Status: envelope.Failed})
	got["b1"].Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	require.Eventually(t, func() bool {
		_, ca := a.state()
		_, cb := b.state()
		return ca && cb
	}, waitFor, tick, "a drained session is released once its messages are settled")
	c, ab := a.done()
	assert.Equal(t, []string{"a1"}, c)
	assert.Equal(t, []string{"a2"}, ab)
	c, _ = b.done()
	assert.Equal(t, []string{"b1"}, c)

	// A session accepted later is received too.
	fake.sessions <- newFakeSession("c", time.Minute, sbMessage("c1", "x"))
	env := next(t, out)
	assert.Equal(t, "c1", env.ID)
	require.NoError(t, s.Health(ctx))
	require.NoError(t, stop())

	closed := make(chan struct{})
	go func() { _ = s.Close(ctx); close(closed) }()
	env.Ack(ctx, envelope.Outcome{Status: envelope.Delivered})
	<-closed
	assert.True(t, fake.closed.Load())
}

func TestSBSourceSessionLockLost(t *testing.T) {
	s, fake := newTestSBSource(t, config.Source{})
	s.maxSessions = 1
	fake.recv.errs <- &amqp.Error{Condition: amqp.ErrCondNotAllowed, Description: "entity requires sessions"}
	lost := newFakeSession("x", 10*time.Millisecond, sbMessage("x1", "x"))
	lost.renewErr = &azservicebus.Error{Code: azservicebus.CodeLockLost}
	fake.sessions <- lost
	out, stop := run(t, s, 10)
	env := next(t, out)
	require.Eventually(t, func() bool { n, _ := lost.state(); return n == 1 }, waitFor, tick)
	time.Sleep(50 * time.Millisecond)
	n, _ := lost.state()
	assert.Equal(t, 1, n, "a lost session lock is not renewed again")
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	require.Eventually(t, func() bool { _, c := lost.state(); return c }, waitFor, tick)
	require.NoError(t, stop())
	require.NoError(t, s.Close(context.Background()))
}

func TestHalfway(t *testing.T) {
	now := time.Now()
	assert.Equal(t, now.Add(15*time.Second), halfway(now, now.Add(30*time.Second)))
	assert.Equal(t, now, halfway(now, now.Add(-time.Second)), "an expired lock is renewed at once")
	assert.Equal(t, now.Add(defaultLockDuration/2), halfway(now, time.Time{}))
}
