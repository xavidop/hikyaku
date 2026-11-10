package amqp

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

const (
	urlA = "amqp://guest:s3cret@a:5672/"
	urlB = "amqp://guest:s3cret@b:5672/"
)

// logBuffer collects log lines; safe for concurrent use.
type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func testLogger(logs *logBuffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newTestSource builds a source on a fake broker, with short timings. The
// URL defaults to urlA and the queue to jobs.
func newTestSource(t *testing.T, cfg config.Source) (*Source, *fakeBroker, *logBuffer) {
	t.Helper()
	if cfg.URL == "" {
		cfg.URL = urlA
	}
	if cfg.Queue == "" {
		cfg.Queue = "jobs"
	}
	if cfg.Reconnect == nil {
		cfg.Reconnect = &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond), MaxBackoff: config.Duration(20 * time.Millisecond)}
	}
	spec := sourceSpec(cfg)
	logs := &logBuffer{}
	spec.Env.Logger = testLogger(logs)
	s, err := newSource(context.Background(), spec)
	require.NoError(t, err)
	b := newFakeBroker()
	s.dial = b.dial
	s.rnd = func() float64 { return 0.5 }
	s.nackBackoff, s.nackMaxBackoff = 40*time.Millisecond, 160*time.Millisecond
	s.stopWait = time.Second
	return s, b, logs
}

// run starts s with an output buffer of size n. The returned stop (also
// run at cleanup) cancels it and checks that Start returned nil.
func run(t *testing.T, s *Source, n int) (chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, n)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
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
	t.Cleanup(func() {
		stop()
		_ = s.Close(context.Background())
	})
	return out, stop
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

// consuming starts s and waits for its first consuming channel.
func consuming(t *testing.T, s *Source, b *fakeBroker, n int) (chan *envelope.Envelope, func(), *fakeConn, *fakeChannel) {
	t.Helper()
	out, stop := run(t, s, n)
	c := b.nextConn(t)
	ch := c.nextChannel(t)
	ch.awaitConsume(t)
	return out, stop, c, ch
}

func settle(env *envelope.Envelope, status envelope.AckStatus) {
	env.Queued()
	env.Ack(context.Background(), envelope.Outcome{Status: status})
}

func TestConsumeMapsDelivery(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, c, ch := consuming(t, s, b, 8)
	assert.Equal(t, []string{"qos 64 global=false", "consume jobs hikyaku-relay-in autoAck=false"}, ch.callLog())
	assert.Equal(t, "hikyaku-relay-in", c.cfg.Properties["connection_name"])

	ts := time.Date(2026, 11, 9, 18, 30, 0, 0, time.UTC)
	tag := ch.deliver(t, amqp091.Delivery{
		Headers:     amqp091.Table{"x-tenant": "acme", "x-attempt": int32(2), "x-meta": amqp091.Table{"k": "v"}, "x-hops": []any{"a", "b"}},
		ContentType: "application/vnd.acme+json", ContentEncoding: "gzip", CorrelationId: "corr-1", ReplyTo: "replies",
		MessageId: "msg-1", Timestamp: ts, Exchange: "events", RoutingKey: "job.created", Redelivered: true,
		Body: []byte(`{"id":1}`),
	})
	env := recv(t, out)
	assert.Equal(t, "msg-1", env.ID)
	assert.Equal(t, "in", env.Source)
	assert.Equal(t, "amqp", env.Protocol)
	assert.Equal(t, "job.created", env.Subject)
	assert.Equal(t, "application/vnd.acme+json", env.ContentType)
	assert.Equal(t, `{"id":1}`, string(env.Body))
	assert.Equal(t, map[string][]string{
		"x-tenant": {"acme"}, "x-attempt": {"2"}, "x-meta": {`{"k":"v"}`}, "x-hops": {"a", "b"},
		"Content-Type": {"application/vnd.acme+json"}, "Content-Encoding": {"gzip"},
		"Correlation-Id": {"corr-1"}, "Reply-To": {"replies"},
	}, env.Headers)
	assert.Equal(t, map[string]any{
		"amqp.queue": "jobs", "amqp.exchange": "events", "amqp.routing_key": "job.created",
		"amqp.delivery_tag": int64(tag), "amqp.redelivered": true, "amqp.message_id": "msg-1",
		"amqp.timestamp": "2026-11-09T18:30:00Z", "ordering_key": "jobs",
	}, env.Attributes)
}

func TestEnvelopeIDsAndContentTypes(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, _, ch := consuming(t, s, b, 8)

	ch.deliver(t, amqp091.Delivery{Headers: amqp091.Table{"idempotency-key": "key-7"}, Body: []byte("hello")})
	env := recv(t, out)
	assert.Equal(t, "key-7", env.ID, "the Idempotency-Key header when there is no message ID")
	assert.Equal(t, contentText, env.ContentType)
	_, ok := env.Attributes["amqp.message_id"]
	assert.False(t, ok)
	_, ok = env.Attributes["amqp.timestamp"]
	assert.False(t, ok)

	ch.deliver(t, amqp091.Delivery{Headers: amqp091.Table{"Idempotency-Key": strings.Repeat("k", 300)}, Body: []byte(`[1]`)})
	env = recv(t, out)
	assert.Len(t, env.ID, 26, "an overlong key is not an ID: a new ULID")
	assert.Equal(t, contentJSON, env.ContentType)

	ch.deliver(t, amqp091.Delivery{Body: []byte{0xff}})
	env = recv(t, out)
	assert.Len(t, env.ID, 26)
	assert.Equal(t, contentBinary, env.ContentType)
	assert.Empty(t, env.Headers)
}

// Every final outcome acks the message's own delivery tag.
func TestAckWhenFinal(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, _, ch := consuming(t, s, b, 8)
	for _, status := range []envelope.AckStatus{envelope.Delivered, envelope.DeadLettered, envelope.Unrouted, envelope.Filtered} {
		tag := ch.deliver(t, amqp091.Delivery{Body: []byte("x")})
		settle(recv(t, out), status)
		assert.Equal(t, settlement{op: "ack", tag: tag}, ch.nextSettlement(t), status.String())
	}
}

// A failed message is nacked with requeue after a delay that grows with
// its failed attempts; a success forgets them.
func TestNackWithBackoff(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, _, ch := consuming(t, s, b, 8)
	// rnd is 0.5 and jitter equal: 3/4 of 40ms, 80ms, then the 160ms cap.
	for i, least := range []time.Duration{30 * time.Millisecond, 60 * time.Millisecond, 120 * time.Millisecond, 120 * time.Millisecond} {
		tag := ch.deliver(t, amqp091.Delivery{MessageId: "poison", Redelivered: i > 0, Body: []byte("x")})
		env := recv(t, out)
		start := time.Now()
		settle(env, envelope.Failed)
		got := ch.nextSettlement(t)
		assert.GreaterOrEqual(t, time.Since(start), least, "attempt %d", i+1)
		assert.Equal(t, settlement{op: "nack", tag: tag, requeue: true}, got)
	}
	tag := ch.deliver(t, amqp091.Delivery{MessageId: "poison", Redelivered: true, Body: []byte("x")})
	settle(recv(t, out), envelope.Delivered)
	assert.Equal(t, settlement{op: "ack", tag: tag}, ch.nextSettlement(t))
	n, _ := s.failed(tracked{id: "poison", fromMessage: true})
	assert.Equal(t, 1, n, "the success forgot the earlier failures")
}

func TestFailedAttemptCounts(t *testing.T) {
	s, _, _ := newTestSource(t, config.Source{})
	byID := tracked{id: "m", fromMessage: true}
	for want := 1; want <= 3; want++ {
		n, stopping := s.failed(byID)
		assert.Equal(t, want, n)
		assert.False(t, stopping)
	}
	// Without an ID, a message is known by where it was published and its
	// body.
	a := tracked{id: "01J...", exchange: "e", key: "k", body: []byte("x")}
	a2 := a
	a2.id = "01K..."
	other := a
	other.body = []byte("y")
	n, _ := s.failed(a)
	assert.Equal(t, 1, n)
	n, _ = s.failed(a2)
	assert.Equal(t, 2, n, "a redelivery with a new ULID is the same message")
	n, _ = s.failed(other)
	assert.Equal(t, 1, n)
	// A quorum queue's delivery count is the broker's own.
	n, _ = s.failed(tracked{id: "q", fromMessage: true, count: 5})
	assert.Equal(t, 5, n)
	s.forgetFailures(byID)
	n, _ = s.failed(byID)
	assert.Equal(t, 1, n)
}

// x-delivery-count (quorum queues) sets the attempt, so the first failure
// of a message the broker delivered three times waits the third delay.
func TestDeliveryCountHeader(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, _, ch := consuming(t, s, b, 8)
	tag := ch.deliver(t, amqp091.Delivery{Headers: amqp091.Table{"x-delivery-count": int64(2)}, Body: []byte("x")})
	start := time.Now()
	settle(recv(t, out), envelope.Failed)
	assert.Equal(t, settlement{op: "nack", tag: tag, requeue: true}, ch.nextSettlement(t))
	assert.GreaterOrEqual(t, time.Since(start), 120*time.Millisecond)
}

// Tags are acked one by one as messages finish, in whatever order.
func TestSettleInAnyOrder(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, _, _, ch := consuming(t, s, b, 8)
	var envs []*envelope.Envelope
	for range 3 {
		ch.deliver(t, amqp091.Delivery{Body: []byte("x")})
		envs = append(envs, recv(t, out))
	}
	for _, i := range []int{2, 0, 1} {
		settle(envs[i], envelope.Delivered)
		assert.Equal(t, settlement{op: "ack", tag: uint64(i + 1)}, ch.nextSettlement(t))
	}
	envs = envs[:0]
	for range 3 {
		ch.deliver(t, amqp091.Delivery{Body: []byte("x")})
		envs = append(envs, recv(t, out))
	}
	for i, env := range envs {
		settle(env, envelope.Delivered)
		assert.Equal(t, settlement{op: "ack", tag: uint64(i + 4)}, ch.nextSettlement(t))
	}
}

func TestOversizeIsAcked(t *testing.T) {
	s, b, logs := newTestSource(t, config.Source{MaxBodyBytes: 8})
	out, _, _, ch := consuming(t, s, b, 8)
	big := ch.deliver(t, amqp091.Delivery{MessageId: "big", Body: []byte(strings.Repeat("x", 9))})
	assert.Equal(t, settlement{op: "ack", tag: big}, ch.nextSettlement(t))
	small := ch.deliver(t, amqp091.Delivery{Body: []byte("12345678")})
	env := recv(t, out)
	assert.Equal(t, int64(small), env.Attributes["amqp.delivery_tag"])
	assert.Contains(t, logs.String(), "over max_body_bytes")
	assert.Contains(t, logs.String(), "message_id=big")
}

func TestAckAccepted(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{Ack: "accepted"})
	out, _, _, ch := consuming(t, s, b, 8)
	tag := ch.deliver(t, amqp091.Delivery{Body: []byte("x")})
	env := recv(t, out)
	env.Queued()
	assert.Equal(t, settlement{op: "ack", tag: tag}, ch.nextSettlement(t), "acked once the relay holds it")
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	ch.noSettlement(t, 100*time.Millisecond)
}

// The source reconnects when the broker drops the connection: a connection
// that delivered goes back to the same URL first, a failed connect moves
// on to the next one. An ack for a message of the dead channel is dropped
// (the broker redelivers it), never sent on the new channel.
func TestReconnectRotatesThroughFailoverURLs(t *testing.T) {
	s, b, logs := newTestSource(t, config.Source{URL: urlA, FailoverURLs: []string{urlB}})
	b.setDown(urlA, true)
	out, _ := run(t, s, 8)
	c1 := b.nextConn(t)
	assert.Equal(t, urlB, c1.url)
	ch1 := c1.nextChannel(t)
	ch1.deliver(t, amqp091.Delivery{Body: []byte("one")})
	inflight := recv(t, out)

	c1.drop(exception(amqp091.ConnectionForced, "CONNECTION_FORCED - broker forced connection closure"))
	c2 := b.nextConn(t)
	assert.Equal(t, urlB, c2.url, "a connection that delivered is retried first")
	ch2 := c2.nextChannel(t)
	ch2.awaitConsume(t)

	settle(inflight, envelope.Delivered)
	ch2.noSettlement(t, 50*time.Millisecond)
	assert.Eventually(t, func() bool { return strings.Contains(logs.String(), "seen twice") }, wait, 5*time.Millisecond)

	tag := ch2.deliver(t, amqp091.Delivery{Body: []byte("two")})
	settle(recv(t, out), envelope.Delivered)
	assert.Equal(t, settlement{op: "ack", tag: tag}, ch2.nextSettlement(t))

	b.setDown(urlA, false)
	b.setDown(urlB, true)
	c2.drop(exception(amqp091.ConnectionForced, "CONNECTION_FORCED"))
	c3 := b.nextConn(t)
	assert.Equal(t, urlA, c3.url, "the failed connect to b moved on to a")
	assert.Equal(t, []string{urlA, urlB, urlB, urlB, urlA}, b.dialed())
	c3.nextChannel(t).awaitConsume(t)
	require.NoError(t, s.Health(context.Background()))

	assert.Contains(t, logs.String(), "amqp source reconnecting")
	assert.Contains(t, logs.String(), "CONNECTION_FORCED")
	assert.NotContains(t, logs.String(), "s3cret", "URLs are logged without their password")
}

// A channel exception (here a declare that conflicts) ends the session;
// the source reconnects and consumes once the broker accepts it.
func TestChannelErrorReconnects(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{Declare: true})
	first := true
	b.setup = func(c *fakeConn) {
		if first {
			first = false
			c.setup = func(ch *fakeChannel) {
				ch.fail("queue.declare", exception(amqp091.PreconditionFailed, "PRECONDITION_FAILED - inequivalent arg 'durable'"))
			}
		}
	}
	_, _ = run(t, s, 8)
	c1 := b.nextConn(t)
	c1.nextChannel(t)
	c2 := b.nextConn(t)
	c2.nextChannel(t).awaitConsume(t)
	assert.True(t, c1.IsClosed(), "the failed session's connection is closed")
}

// A broker cancelling the consumer (its queue was deleted) ends the
// session too.
func TestBrokerCancelReconnects(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	_, _, c1, ch1 := consuming(t, s, b, 8)
	require.NoError(t, ch1.Cancel("broker-side", false))
	c2 := b.nextConn(t)
	c2.nextChannel(t).awaitConsume(t)
	assert.True(t, c1.IsClosed())
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{URL: urlA, FailoverURLs: []string{urlB},
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 3}})
	b.setDown(urlA, true)
	b.setDown(urlB, true)
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 3 failed connects in a row")
	assert.Contains(t, err.Error(), "connection refused")
	assert.NotContains(t, err.Error(), "s3cret")
	assert.Equal(t, []string{urlA, urlB, urlA}, b.dialed())
	assert.ErrorContains(t, s.Start(context.Background(), nil), "already started")
}

func TestDeclareAndBind(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  config.Source
		want []string
	}{
		{"exchange", config.Source{Declare: true, Exchange: "events", RoutingKey: "job.*"}, []string{
			"qos 64 global=false",
			"queue.declare jobs durable=true autoDelete=false exclusive=false",
			"exchange.declare events topic durable=true autoDelete=false internal=false",
			"queue.bind jobs events job.*",
			"consume jobs hikyaku-relay-in autoAck=false",
		}},
		{"default key", config.Source{Declare: true, Exchange: "events", Prefetch: 5}, []string{
			"qos 5 global=false",
			"queue.declare jobs durable=true autoDelete=false exclusive=false",
			"exchange.declare events topic durable=true autoDelete=false internal=false",
			"queue.bind jobs events #",
			"consume jobs hikyaku-relay-in autoAck=false",
		}},
		{"queue only", config.Source{Declare: true}, []string{
			"qos 64 global=false",
			"queue.declare jobs durable=true autoDelete=false exclusive=false",
			"consume jobs hikyaku-relay-in autoAck=false",
		}},
		{"no declare", config.Source{}, []string{"qos 64 global=false", "consume jobs hikyaku-relay-in autoAck=false"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, b, _ := newTestSource(t, c.cfg)
			_, _, _, ch := consuming(t, s, b, 8)
			assert.Equal(t, c.want, ch.callLog())
		})
	}
}

func TestSourceHealth(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	assert.ErrorIs(t, s.Health(context.Background()), errNotStarted)
	_, stop, c, consumer := consuming(t, s, b, 8)

	require.NoError(t, s.Health(context.Background()))
	probe := c.nextChannel(t)
	assert.Equal(t, []string{"queue.declare.passive jobs"}, probe.callLog())
	assert.True(t, probe.IsClosed(), "the probe's channel is closed after")

	c.mu.Lock()
	c.setup = func(ch *fakeChannel) {
		ch.fail("queue.declare.passive", exception(amqp091.NotFound, "NOT_FOUND - no queue 'jobs' in vhost '/'"))
	}
	c.mu.Unlock()
	err := s.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queue jobs")
	assert.Contains(t, err.Error(), "NOT_FOUND")
	assert.False(t, consumer.IsClosed(), "a failed probe leaves the consumer alone")

	stop()
	assert.ErrorIs(t, s.Health(context.Background()), errStopped)
}

// At shutdown the consumer is cancelled and what was not handed to the
// relay goes back to the queue; the messages in flight still settle on the
// open channel (a failure without delay), and Close closes the connection.
func TestShutdownCancelsAndSettlesInFlight(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	out, stop, c, ch := consuming(t, s, b, 2)
	ch.deliver(t, amqp091.Delivery{Body: []byte("1")})
	ch.deliver(t, amqp091.Delivery{Body: []byte("2")})
	ch.deliver(t, amqp091.Delivery{Body: []byte("3")}) // waits for room in out
	ch.deliver(t, amqp091.Delivery{Body: []byte("4")}) // still with the client
	require.Eventually(t, func() bool { return len(out) == 2 }, wait, time.Millisecond)

	stop()
	assert.Contains(t, ch.callLog(), "cancel hikyaku-relay-in")
	assert.Equal(t, settlement{op: "nack", tag: 3, requeue: true}, ch.nextSettlement(t))
	assert.Equal(t, settlement{op: "nack", tag: 4, requeue: true}, ch.nextSettlement(t))

	settle(<-out, envelope.Delivered)
	assert.Equal(t, settlement{op: "ack", tag: 1}, ch.nextSettlement(t))
	start := time.Now()
	settle(<-out, envelope.Failed)
	assert.Equal(t, settlement{op: "nack", tag: 2, requeue: true}, ch.nextSettlement(t))
	assert.Less(t, time.Since(start), 30*time.Millisecond, "no delay while stopping")

	assert.False(t, c.IsClosed(), "the acks need the connection until Close")
	require.NoError(t, s.Close(context.Background()))
	assert.True(t, c.IsClosed())
	require.NoError(t, s.Close(context.Background()), "Close twice is fine")
}

// Close sends the delayed nacks at once before it closes the connection,
// and a running source then stops without reconnecting.
func TestCloseFlushesDelayedNacks(t *testing.T) {
	s, b, _ := newTestSource(t, config.Source{})
	s.nackBackoff, s.nackMaxBackoff = time.Minute, time.Minute
	out, _, c, ch := consuming(t, s, b, 8)
	tag := ch.deliver(t, amqp091.Delivery{Body: []byte("x")})
	settle(recv(t, out), envelope.Failed)
	ch.noSettlement(t, 50*time.Millisecond)
	require.NoError(t, s.Close(context.Background()))
	assert.Equal(t, settlement{op: "nack", tag: tag, requeue: true}, ch.nextSettlement(t))
	assert.True(t, c.IsClosed())
	select {
	case extra := <-b.conns:
		t.Fatalf("reconnected to %s after Close", extra.url)
	case <-time.After(100 * time.Millisecond):
	}
}
