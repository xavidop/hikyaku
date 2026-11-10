package amqp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// newTestSink builds a destination on a fake broker. The URL defaults to
// urlA. onPublish, when set, decides what the broker does with every
// publish.
func newTestSink(t *testing.T, cfg config.Endpoint, onPublish func(publishRecord) publishAction) (*Sink, *fakeBroker, *logBuffer) {
	t.Helper()
	if cfg.URL == "" {
		cfg.URL = urlA
	}
	spec := sinkSpec(cfg)
	logs := &logBuffer{}
	spec.Env.Logger = testLogger(logs)
	s, err := newSink(context.Background(), spec)
	require.NoError(t, err)
	b := newFakeBroker()
	b.setup = func(c *fakeConn) {
		c.setup = func(ch *fakeChannel) { ch.onPublish = onPublish }
	}
	s.dial = b.dial
	s.blockedRetry = 3 * time.Second
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, b, logs
}

func message(body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.ContentType = "application/json"
	env.ReceivedAt = time.Date(2026, 11, 9, 18, 30, 0, 0, time.UTC)
	return env
}

// failure checks a delivery error's class and tags.
func failure(t *testing.T, err error, class plugin.Class, tags ...string) {
	t.Helper()
	require.Error(t, err)
	got, _ := plugin.Classify(err)
	assert.Equal(t, class, got, err.Error())
	assert.Equal(t, tags, plugin.Tags(err), err.Error())
}

func TestPublishProperties(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{
		Exchange: "events", RoutingKey: "{{ .json.type }}",
		Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}", "x-static": "s", "x-trace": "overridden"},
	}, nil)
	env := message(`{"type":"job.created","tenant":"acme"}`)
	env.Headers = map[string][]string{
		"X-Trace": {"t1"}, "X-Many": {"a", "b"}, "Authorization": {"Bearer secret"},
		"Correlation-Id": {"corr-1"}, "Reply-To": {"replies"}, "Content-Encoding": {"gzip"},
		"x-death": {`{"count":1}`}, "Idempotency-Key": {"idem-1"},
	}
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "amqp://guest:xxxxx@a:5672/", res.Endpoint)

	c := b.nextConn(t)
	assert.Equal(t, "hikyaku-relay-out", c.cfg.Properties["connection_name"])
	ch := c.nextChannel(t)
	assert.Equal(t, []string{"confirm"}, ch.callLog())
	p := ch.nextPublish(t)
	assert.Equal(t, "events", p.exchange)
	assert.Equal(t, "job.created", p.key)
	assert.False(t, p.mandatory)
	assert.Equal(t, amqp091.Publishing{
		Headers: amqp091.Table{
			"x-trace": "overridden", "X-Many": []any{"a", "b"}, "X-Tenant": "acme", "x-static": "s", "Idempotency-Key": "idem-1",
		},
		ContentType: "application/json", ContentEncoding: "gzip", DeliveryMode: amqp091.Persistent,
		CorrelationId: "corr-1", ReplyTo: "replies", MessageId: env.ID, Timestamp: env.ReceivedAt,
		Body: env.Body,
	}, p.msg)
}

func TestPublishDefaults(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs", Persistent: config.Ptr(false), Body: `{"wrapped":{{ .bodyText }}}`}, nil)
	env := message(`{"a":1}`)
	env.ID = strings.Repeat("i", 300)
	env.ReceivedAt = time.Time{}
	env.SetHeader("Correlation-Id", strings.Repeat("c", 300))
	before := time.Now()
	_, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	p := b.nextConn(t).nextChannel(t).nextPublish(t)
	assert.Equal(t, "", p.exchange, "the default exchange")
	assert.Equal(t, "jobs", p.key)
	assert.Equal(t, amqp091.Transient, p.msg.DeliveryMode)
	sum := sha256.Sum256([]byte(env.ID))
	assert.Equal(t, hex.EncodeToString(sum[:]), p.msg.MessageId, "an ID over 255 bytes is hashed")
	assert.Equal(t, env.ID, p.msg.Headers["Idempotency-Key"], "the full ID stays in the Idempotency-Key header")
	assert.Empty(t, p.msg.CorrelationId)
	assert.Equal(t, strings.Repeat("c", 300), p.msg.Headers["Correlation-Id"], "an overlong property travels as a header")
	assert.False(t, p.msg.Timestamp.Before(before.Truncate(time.Second)))
	assert.Equal(t, `{"wrapped":{"a":1}}`, string(p.msg.Body))
}

func TestDeclareExchange(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{Exchange: "events", RoutingKey: "k", Declare: true}, nil)
	_, err := s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err)
	ch := b.nextConn(t).nextChannel(t)
	assert.Equal(t, []string{"confirm", "exchange.declare events topic durable=true autoDelete=false internal=false"}, ch.callLog())
}

func TestNackIsTransient(t *testing.T) {
	s, _, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, func(publishRecord) publishAction { return publishAction{nack: true} })
	_, err := s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Transient, "amqp:nack", "5xx")
	assert.ErrorContains(t, err, "nacked")
}

func TestMandatoryReturnIsPermanent(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{Exchange: "events", RoutingKey: "nowhere", Mandatory: true},
		func(publishRecord) publishAction {
			return publishAction{ret: &amqp091.Return{ReplyCode: amqp091.NoRoute, ReplyText: "NO_ROUTE"}}
		})
	_, err := s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Permanent, "amqp:312")
	assert.ErrorContains(t, err, "312 NO_ROUTE")
	assert.True(t, b.nextConn(t).nextChannel(t).nextPublish(t).mandatory)
}

// A return left over from an attempt that timed out is not held against
// the next message.
func TestStaleReturnsAreDropped(t *testing.T) {
	hold := true
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "k", Mandatory: true}, func(publishRecord) publishAction {
		if hold {
			return publishAction{hold: true}
		}
		return publishAction{}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	first := message(`{}`)
	_, err := s.Deliver(ctx, first)
	failure(t, err, plugin.Timeout, "timeout")
	ch := b.nextConn(t).nextChannel(t)
	ch.mu.Lock()
	returns := append([]chan amqp091.Return(nil), ch.returns...)
	ch.mu.Unlock()
	for _, l := range returns {
		l <- amqp091.Return{ReplyCode: amqp091.NoRoute, ReplyText: "NO_ROUTE", MessageId: first.ID}
	}
	hold = false
	_, err = s.Deliver(context.Background(), first)
	require.NoError(t, err, "the same message, routed this time")
}

// A channel exception fails the message by its code and leaves the
// connection open: the next delivery opens a new channel on it.
func TestChannelExceptionReopensChannel(t *testing.T) {
	first := true
	s, b, _ := newTestSink(t, config.Endpoint{Exchange: "missing", RoutingKey: "k"}, func(publishRecord) publishAction {
		if first {
			first = false
			return publishAction{except: exception(amqp091.NotFound, "NOT_FOUND - no exchange 'missing' in vhost '/'")}
		}
		return publishAction{}
	})
	_, err := s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Permanent, "amqp:404")
	assert.ErrorContains(t, err, "no exchange 'missing'")
	_, err = s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err)
	c := b.nextConn(t)
	ch1, ch2 := c.nextChannel(t), c.nextChannel(t)
	assert.True(t, ch1.IsClosed())
	assert.False(t, ch2.IsClosed())
	assert.Len(t, b.dialed(), 1, "no new connection")
}

// A connection that drops while a message waits for its confirm fails it
// as transient (connect): the default policy retries on a new connection.
func TestConnectionDropDuringConfirm(t *testing.T) {
	drop := true
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, func(publishRecord) publishAction {
		if drop {
			drop = false
			return publishAction{hold: true, fn: func(ch *fakeChannel) {
				ch.conn.drop(exception(amqp091.ConnectionForced, "CONNECTION_FORCED - broker forced connection closure"))
			}}
		}
		return publishAction{}
	})
	_, err := s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Transient, "amqp:320", "connect")
	_, err = s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err)
	assert.Equal(t, []string{urlA, urlA}, b.dialed(), "redialed")
}

func TestConfirmTimeout(t *testing.T) {
	hold := true
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, func(publishRecord) publishAction {
		return publishAction{hold: hold}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := s.Deliver(ctx, message(`{}`))
	failure(t, err, plugin.Timeout, "timeout")
	assert.ErrorContains(t, err, "to confirm the message")

	hold = false
	_, err = s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err, "the channel is still usable")
	assert.Len(t, b.dialed(), 1)
}

// While the broker blocks the connection (a resource alarm), deliveries
// fail fast as rate limited, with a retry hint, until it unblocks.
func TestBlockedIsRateLimited(t *testing.T) {
	s, b, logs := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, nil)
	_, err := s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err)
	c := b.nextConn(t)
	ch := c.nextChannel(t)
	ch.nextPublish(t)

	c.block(t, true, "low on memory")
	require.Eventually(t, func() bool { return s.Health(context.Background()) != nil }, wait, time.Millisecond)
	assert.ErrorContains(t, s.Health(context.Background()), "low on memory")
	_, err = s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.RateLimited, "amqp:blocked", "429")
	_, retry := plugin.Classify(err)
	assert.Equal(t, 3*time.Second, retry)
	assert.Empty(t, ch.published, "nothing is published while blocked")
	// The watcher logs right after it records the block, which Health
	// may already have seen.
	assert.Eventually(t, func() bool { return strings.Contains(logs.String(), "blocked") }, wait, time.Millisecond)

	c.block(t, false, "")
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, wait, time.Millisecond)
	_, err = s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err)
}

func TestDialErrors(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, nil)
	b.failDials = 1
	_, err := s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Transient, "connect")
	assert.NotContains(t, err.Error(), "s3cret")

	b.failDials, b.dialErr = 1, amqp091.ErrCredentials
	_, err = s.Deliver(context.Background(), message(`{}`))
	failure(t, err, plugin.Permanent, "amqp:403")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = s.Deliver(ctx, message(`{}`))
	failure(t, err, plugin.Timeout, "timeout")

	_, err = s.Deliver(context.Background(), message(`{}`))
	require.NoError(t, err, "connects once the broker takes it")
}

func TestTemplateErrorsArePermanent(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "{{ b64dec .bodyText }}"}, nil)
	_, err := s.Deliver(context.Background(), message(`!!`))
	failure(t, err, plugin.Permanent, "template")
	_, err = s.Deliver(context.Background(), message(strings.Repeat("a", 400)))
	require.Error(t, err)
	assert.Empty(t, b.dialed(), "nothing is dialed for a message that cannot be sent")

	s, _, _ = newTestSink(t, config.Endpoint{RoutingKey: "{{ .bodyText }}"}, nil)
	_, err = s.Deliver(context.Background(), message(strings.Repeat("k", 256)))
	failure(t, err, plugin.Permanent)
	assert.ErrorContains(t, err, "over the AMQP limit of 255")

	s, _, _ = newTestSink(t, config.Endpoint{RoutingKey: "k", Headers: map[string]string{"X": "{{ b64dec .bodyText }}"}}, nil)
	_, err = s.Deliver(context.Background(), message(`!!`))
	failure(t, err, plugin.Permanent, "template")
}

// A batch is published whole before the sink waits for any confirm.
func TestDeliverBatch(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, func(publishRecord) publishAction { return publishAction{hold: true} })
	envs := []*envelope.Envelope{message(`1`), message(`2`), message(`3`)}
	done := make(chan error, 1)
	go func() {
		_, err := s.DeliverBatch(context.Background(), envs)
		done <- err
	}()
	ch := b.nextConn(t).nextChannel(t)
	for i := range envs {
		assert.Equal(t, envs[i].ID, ch.nextPublish(t).msg.MessageId)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		ch.confirm(seq, true)
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("the batch never finished")
	}

	res, err := s.DeliverBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "amqp://guest:xxxxx@a:5672/", res.Endpoint)
}

// A batch fails whole, with its most severe failure.
func TestDeliverBatchFailure(t *testing.T) {
	n := 0
	s, _, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs", Mandatory: true}, func(publishRecord) publishAction {
		n++
		switch n {
		case 1:
			return publishAction{nack: true}
		case 2:
			return publishAction{ret: &amqp091.Return{ReplyCode: amqp091.NoRoute, ReplyText: "NO_ROUTE"}}
		}
		return publishAction{}
	})
	_, err := s.DeliverBatch(context.Background(), []*envelope.Envelope{message(`1`), message(`2`), message(`3`)})
	failure(t, err, plugin.Permanent, "amqp:312")
	assert.ErrorContains(t, err, "2 of 3 messages failed")

	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "{{ b64dec .bodyText }}"}, nil)
	_, err = s.DeliverBatch(context.Background(), []*envelope.Envelope{message(`YQ==`), message(`!!`)})
	failure(t, err, plugin.Permanent, "template")
	assert.Empty(t, b.dialed(), "nothing is published when one message cannot be")
}

func TestSinkHealth(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, nil)
	b.failDials = 1
	assert.Error(t, s.Health(context.Background()), "cannot connect")
	require.NoError(t, s.Health(context.Background()), "connects")
	c := b.nextConn(t)
	require.NoError(t, s.Health(context.Background()), "connected")
	assert.Len(t, b.dialed(), 2)

	c.drop(exception(amqp091.ConnectionForced, "CONNECTION_FORCED"))
	require.NoError(t, s.Health(context.Background()), "reconnects")
	assert.Len(t, b.dialed(), 3)

	require.NoError(t, s.Close(context.Background()))
	assert.ErrorIs(t, s.Health(context.Background()), errClosed)
}

// Close closes the connection; it waits for the delivery in flight only
// as long as its context allows.
func TestSinkClose(t *testing.T) {
	s, b, _ := newTestSink(t, config.Endpoint{RoutingKey: "jobs"}, func(publishRecord) publishAction { return publishAction{hold: true} })
	done := make(chan error, 1)
	go func() {
		_, err := s.Deliver(context.Background(), message(`{}`))
		done <- err
	}()
	c := b.nextConn(t)
	c.nextChannel(t).nextPublish(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Close(ctx))
	assert.True(t, c.IsClosed())
	select {
	case err := <-done:
		failure(t, err, plugin.Transient, "connect")
	case <-time.After(wait):
		t.Fatal("the delivery in flight never ended")
	}
	_, err := s.Deliver(context.Background(), message(`{}`))
	require.ErrorIs(t, err, errClosed)
	require.NoError(t, s.Close(context.Background()), "Close twice is fine")
}
