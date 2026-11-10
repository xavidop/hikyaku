//go:build integration

package amqp

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// rabbitImage is the broker the container suite runs against.
const rabbitImage = "rabbitmq:4.1-management-alpine"

// itPassword is the broker password; it must never show in logs.
const itPassword = "it-s3cret"

var (
	rabbitOnce sync.Once
	rabbitCtr  *tcrabbit.RabbitMQContainer
	rabbitURL  string
	rabbitErr  error
)

// TestMain stops the shared broker after the suite.
func TestMain(m *testing.M) {
	code := m.Run()
	if rabbitCtr != nil {
		_ = testcontainers.TerminateContainer(rabbitCtr)
	}
	os.Exit(code)
}

// rabbit starts the suite's broker on first use: one container for every
// test, which run one at a time.
func rabbit(t *testing.T) (*tcrabbit.RabbitMQContainer, string) {
	t.Helper()
	rabbitOnce.Do(func() {
		ctx := context.Background()
		rabbitCtr, rabbitErr = tcrabbit.Run(ctx, rabbitImage,
			tcrabbit.WithAdminUsername("hikyaku"), tcrabbit.WithAdminPassword(itPassword))
		if rabbitErr == nil {
			rabbitURL, rabbitErr = rabbitCtr.AmqpURL(ctx)
		}
	})
	require.NoError(t, rabbitErr)
	return rabbitCtr, rabbitURL + "/"
}

// admin is a plain client channel for setting up and inspecting queues.
func admin(t *testing.T, url string) *amqp091.Channel {
	t.Helper()
	c, err := amqp091.Dial(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	ch, err := c.Channel()
	require.NoError(t, err)
	return ch
}

// depth is how many messages of a queue are ready (neither delivered nor
// waiting for an acknowledgement).
func depth(t *testing.T, ch *amqp091.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

// rabbitctl runs rabbitmqctl in the broker container.
func rabbitctl(t *testing.T, ctr *tcrabbit.RabbitMQContainer, args ...string) string {
	t.Helper()
	code, r, err := ctr.Exec(context.Background(), append([]string{"rabbitmqctl"}, args...), tcexec.Multiplexed())
	require.NoError(t, err)
	out, _ := io.ReadAll(r)
	require.Equal(t, 0, code, string(out))
	return string(out)
}

func itSource(t *testing.T, cfg config.Source) (*Source, *logBuffer) {
	t.Helper()
	spec := sourceSpec(cfg)
	logs := &logBuffer{}
	spec.Env.Logger = testLogger(logs)
	s, err := newSource(context.Background(), spec)
	require.NoError(t, err)
	s.nackBackoff, s.nackMaxBackoff = 20*time.Millisecond, 100*time.Millisecond
	return s, logs
}

func itSink(t *testing.T, cfg config.Endpoint) *Sink {
	t.Helper()
	s, err := newSink(context.Background(), sinkSpec(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// healthy waits until the source consumes.
func healthy(t *testing.T, s *Source) {
	t.Helper()
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 30*time.Second, 20*time.Millisecond)
}

func jsonMessage(body string) *envelope.Envelope {
	env := envelope.New("http-in", "http", []byte(body))
	env.ContentType = "application/json"
	return env
}

// declare: true declares the queue, the exchange and the binding; a message
// published by a destination arrives with its properties, headers and
// attributes, and the ack removes it from the queue.
func TestIntegrationDeclareBindRoundTrip(t *testing.T) {
	_, url := rabbit(t)
	ctx := context.Background()
	src, _ := itSource(t, config.Source{URL: url, Queue: "it.jobs", Declare: true, Exchange: "it.events", RoutingKey: "job.*", Prefetch: 10})
	out, stop := run(t, src, 8)
	healthy(t, src)

	sink := itSink(t, config.Endpoint{URL: url, Exchange: "it.events", Declare: true, Mandatory: true,
		RoutingKey: "{{ .json.type }}", Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}"}})
	env := jsonMessage(`{"type":"job.created","tenant":"acme"}`)
	env.SetHeader("Correlation-Id", "corr-1")
	_, err := sink.Deliver(ctx, env)
	require.NoError(t, err)

	got := recv(t, out)
	assert.Equal(t, env.ID, got.ID, "the message ID is the envelope ID")
	assert.Equal(t, "job.created", got.Subject)
	assert.Equal(t, "application/json", got.ContentType)
	assert.JSONEq(t, `{"type":"job.created","tenant":"acme"}`, string(got.Body))
	assert.Equal(t, "acme", got.Header("X-Tenant"))
	assert.Equal(t, env.ID, got.Header("Idempotency-Key"))
	assert.Equal(t, "corr-1", got.Header("Correlation-Id"))
	assert.Equal(t, "it.events", got.Attributes["amqp.exchange"])
	assert.Equal(t, "it.jobs", got.Attributes["amqp.queue"])
	assert.Equal(t, "job.created", got.Attributes["amqp.routing_key"])
	assert.Equal(t, false, got.Attributes["amqp.redelivered"])
	assert.Equal(t, env.ID, got.Attributes["amqp.message_id"])
	assert.NotEmpty(t, got.Attributes["amqp.timestamp"])
	assert.Equal(t, "it.jobs", got.Attributes["ordering_key"])

	// The binding takes job.* only.
	_, err = sink.Deliver(ctx, jsonMessage(`{"type":"invoice.paid"}`))
	require.Error(t, err)
	assert.Equal(t, []string{"amqp:312"}, plugin.Tags(err))

	settle(got, envelope.Delivered)
	stop()
	require.NoError(t, src.Close(ctx))
	assert.Equal(t, 0, depth(t, admin(t, url), "it.jobs"), "acked")
}

// A failed message goes back to the queue and comes again, redelivered.
func TestIntegrationNackRedelivers(t *testing.T) {
	_, url := rabbit(t)
	ctx := context.Background()
	src, _ := itSource(t, config.Source{URL: url, Queue: "it.redeliver", Declare: true})
	out, stop := run(t, src, 8)
	healthy(t, src)
	sink := itSink(t, config.Endpoint{URL: url, RoutingKey: "it.redeliver", Mandatory: true})
	_, err := sink.Deliver(ctx, jsonMessage(`{"n":1}`))
	require.NoError(t, err)

	first := recv(t, out)
	assert.Equal(t, false, first.Attributes["amqp.redelivered"])
	settle(first, envelope.Failed)
	again := recv(t, out)
	assert.Equal(t, true, again.Attributes["amqp.redelivered"])
	assert.Equal(t, first.ID, again.ID)
	settle(again, envelope.Delivered)

	stop()
	require.NoError(t, src.Close(ctx))
	assert.Equal(t, 0, depth(t, admin(t, url), "it.redeliver"))
}

// At shutdown the deliveries not handed to the relay go back to the queue
// at once; the ones in flight follow when the connection closes, and the
// next consumer gets them all, redelivered.
func TestIntegrationRestartRedeliversUnacked(t *testing.T) {
	_, url := rabbit(t)
	ctx := context.Background()
	cfg := config.Source{URL: url, Queue: "it.restart", Declare: true, Prefetch: 10}
	a, _ := itSource(t, cfg)
	outA, stopA := run(t, a, 1)
	healthy(t, a)

	sink := itSink(t, config.Endpoint{URL: url, RoutingKey: "it.restart", Mandatory: true})
	var sent []string
	var batch []*envelope.Envelope
	for i := range 5 {
		env := jsonMessage(fmt.Sprintf(`{"n":%d}`, i))
		batch = append(batch, env)
		sent = append(sent, env.ID)
	}
	_, err := sink.DeliverBatch(ctx, batch)
	require.NoError(t, err)

	recv(t, outA) // in flight, never settled; the next one waits in outA
	require.Eventually(t, func() bool { return len(outA) == 1 }, 10*time.Second, 10*time.Millisecond)
	stopA()
	ch := admin(t, url)
	assert.Eventually(t, func() bool { return depth(t, ch, "it.restart") == 3 }, 10*time.Second, 50*time.Millisecond,
		"the three never handed over are requeued while the connection stays open")
	require.NoError(t, a.Close(ctx))
	assert.Eventually(t, func() bool { return depth(t, ch, "it.restart") == 5 }, 10*time.Second, 50*time.Millisecond,
		"the two in flight are requeued when it closes")

	b, _ := itSource(t, cfg)
	outB, stopB := run(t, b, 8)
	var got []string
	for range 5 {
		env := recv(t, outB)
		assert.Equal(t, true, env.Attributes["amqp.redelivered"])
		got = append(got, env.ID)
		settle(env, envelope.Delivered)
	}
	sort.Strings(sent)
	sort.Strings(got)
	assert.Equal(t, sent, got)
	stopB()
	require.NoError(t, b.Close(ctx))
	assert.Equal(t, 0, depth(t, ch, "it.restart"))
}

// A mandatory message no queue is bound to receive fails as permanent
// (amqp:312); without mandatory the broker drops it and confirms. An
// exchange that does not exist is amqp:404, and the channel it closed is
// reopened for the next message.
func TestIntegrationMandatoryAndMissingExchange(t *testing.T) {
	_, url := rabbit(t)
	ctx := context.Background()
	mandatory := itSink(t, config.Endpoint{URL: url, Exchange: "it.unrouted", Declare: true, Mandatory: true, RoutingKey: "nobody.home"})
	_, err := mandatory.Deliver(ctx, jsonMessage(`{}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"amqp:312"}, plugin.Tags(err))
	assert.ErrorContains(t, err, "NO_ROUTE")

	dropped := itSink(t, config.Endpoint{URL: url, Exchange: "it.unrouted", RoutingKey: "nobody.home"})
	_, err = dropped.Deliver(ctx, jsonMessage(`{}`))
	require.NoError(t, err)

	missing := itSink(t, config.Endpoint{URL: url, Exchange: "it.missing", RoutingKey: "k"})
	for range 2 {
		_, err = missing.Deliver(ctx, jsonMessage(`{}`))
		require.Error(t, err)
		class, _ = plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class)
		assert.Equal(t, []string{"amqp:404"}, plugin.Tags(err))
	}
	require.NoError(t, missing.Health(ctx), "the connection survived the channel exceptions")
}

// The broker dropping every connection (CONNECTION_FORCED): the source
// reconnects and gets the message that was in flight again, redelivered
// (the ack on the dead channel is dropped); the destination reconnects on
// its next delivery.
func TestIntegrationConnectionDropRecovers(t *testing.T) {
	ctr, url := rabbit(t)
	ctx := context.Background()
	src, logs := itSource(t, config.Source{URL: url, Queue: "it.drop", Declare: true,
		Reconnect: &config.Reconnect{Backoff: config.Duration(100 * time.Millisecond), MaxBackoff: config.Duration(time.Second)}})
	out, stop := run(t, src, 8)
	healthy(t, src)
	sink := itSink(t, config.Endpoint{URL: url, RoutingKey: "it.drop", Mandatory: true})

	_, err := sink.Deliver(ctx, jsonMessage(`{"n":1}`))
	require.NoError(t, err)
	settle(recv(t, out), envelope.Delivered)
	_, err = sink.Deliver(ctx, jsonMessage(`{"n":2}`))
	require.NoError(t, err)
	inflight := recv(t, out)

	rabbitctl(t, ctr, "close_all_connections", "hikyaku integration test")

	again := recv(t, out)
	assert.Equal(t, inflight.ID, again.ID)
	assert.Equal(t, true, again.Attributes["amqp.redelivered"])
	settle(inflight, envelope.Delivered)
	assert.Eventually(t, func() bool { return strings.Contains(logs.String(), "seen twice") }, 5*time.Second, 10*time.Millisecond)
	settle(again, envelope.Delivered)

	require.Eventually(t, func() bool {
		_, err := sink.Deliver(ctx, jsonMessage(`{"n":3}`))
		return err == nil
	}, 30*time.Second, 100*time.Millisecond, "the destination reconnects")
	settle(recv(t, out), envelope.Delivered)

	healthy(t, src)
	stop()
	require.NoError(t, src.Close(ctx))
	assert.Equal(t, 0, depth(t, admin(t, url), "it.drop"))
	assert.Contains(t, logs.String(), "amqp source reconnecting")
	assert.Contains(t, logs.String(), "CONNECTION_FORCED")
	assert.NotContains(t, logs.String(), itPassword)
}

// A resource alarm blocks the destination's connection: deliveries fail as
// rate limited until the alarm clears.
func TestIntegrationBlockedConnection(t *testing.T) {
	ctr, url := rabbit(t)
	ctx := context.Background()
	_, err := admin(t, url).QueueDeclare("it.blocked", true, false, false, false, nil)
	require.NoError(t, err)
	sink := itSink(t, config.Endpoint{URL: url, RoutingKey: "it.blocked"})
	_, err = sink.Deliver(ctx, jsonMessage(`{}`))
	require.NoError(t, err)

	const alarm = "{resource_limit, memory, node()}"
	rabbitctl(t, ctr, "eval", "rabbit_alarm:set_alarm({"+alarm+", []}).")
	t.Cleanup(func() { rabbitctl(t, ctr, "eval", "rabbit_alarm:clear_alarm("+alarm+").") })

	require.Eventually(t, func() bool {
		actx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := sink.Deliver(actx, jsonMessage(`{}`))
		class, _ := plugin.Classify(err)
		return err != nil && class == plugin.RateLimited
	}, 30*time.Second, 50*time.Millisecond, "the broker blocks the connection once it publishes")
	_, err = sink.Deliver(ctx, jsonMessage(`{}`))
	require.Error(t, err)
	assert.Equal(t, []string{"amqp:blocked", "429"}, plugin.Tags(err))
	assert.Error(t, sink.Health(ctx))

	rabbitctl(t, ctr, "eval", "rabbit_alarm:clear_alarm("+alarm+").")
	require.Eventually(t, func() bool {
		_, err := sink.Deliver(ctx, jsonMessage(`{}`))
		return err == nil
	}, 30*time.Second, 50*time.Millisecond, "deliveries resume once the alarm clears")
	require.NoError(t, sink.Health(ctx))
}
