package amqp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	"github.com/xavidop/hikyaku/plugin"
)

// exception is a broker exception, as the client reports it.
func exception(code int, reason string) *amqp091.Error {
	return &amqp091.Error{Code: code, Reason: reason, Server: true}
}

func TestBrokerErrorCodes(t *testing.T) {
	for _, c := range []struct {
		code  int
		class plugin.Class
		tags  []string
	}{
		{amqp091.NotFound, plugin.Permanent, []string{"amqp:404"}},
		{amqp091.AccessRefused, plugin.Permanent, []string{"amqp:403"}},
		{amqp091.PreconditionFailed, plugin.Permanent, []string{"amqp:406"}},
		{amqp091.NotAllowed, plugin.Permanent, []string{"amqp:530"}},
		{amqp091.NoRoute, plugin.Permanent, []string{"amqp:312"}},
		{amqp091.ContentTooLarge, plugin.Permanent, []string{"amqp:311"}},
		{amqp091.ConnectionForced, plugin.Transient, []string{"amqp:320", "connect"}},
		{amqp091.ChannelError, plugin.Transient, []string{"amqp:504", "connect"}},
		{amqp091.ResourceError, plugin.Transient, []string{"amqp:506", "connect"}},
		{amqp091.InternalError, plugin.Transient, []string{"amqp:541", "connect"}},
		{amqp091.ResourceLocked, plugin.Transient, []string{"amqp:405", "connect"}},
	} {
		err := brokerError("publish", exception(c.code, "WHY"))
		class, _ := plugin.Classify(err)
		assert.Equal(t, c.class, class, "%d", c.code)
		assert.Equal(t, c.tags, plugin.Tags(err), "%d", c.code)
		assert.ErrorContains(t, err, "WHY")
		var ae *amqp091.Error
		assert.True(t, errors.As(err, &ae), "the exception stays reachable")
	}
}

func TestClassify(t *testing.T) {
	ctx := context.Background()
	check := func(err error, class plugin.Class, tags ...string) {
		t.Helper()
		got, _ := plugin.Classify(err)
		assert.Equal(t, class, got, err.Error())
		assert.Equal(t, tags, plugin.Tags(err), err.Error())
	}
	check(classify(ctx, "x", amqp091.ErrClosed), plugin.Transient, "connect")
	check(classify(ctx, "x", fmt.Errorf("wrapped: %w", exception(404, "NOT_FOUND"))), plugin.Permanent, "amqp:404")
	check(classify(ctx, "x", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), plugin.Transient, "connect")
	check(classify(ctx, "x", &net.OpError{Op: "read", Err: timeoutErr{}}), plugin.Timeout, "timeout")
	check(classify(ctx, "x", plugin.Permanentf("kept")), plugin.Permanent)

	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	check(classify(expired, "x", errors.New("late")), plugin.Timeout, "timeout")
	stopped, stop := context.WithCancel(ctx)
	stop()
	got, _ := plugin.Classify(classify(stopped, "x", errors.New("cut")))
	assert.Equal(t, plugin.Transient, got)

	// Bad credentials at the handshake are permanent; a refused dial is not.
	check(dialError(ctx, "amqp://b/", amqp091.ErrCredentials), plugin.Permanent, "amqp:403")
	check(dialError(ctx, "amqp://b/", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), plugin.Transient, "connect")
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestProtocolErrors(t *testing.T) {
	nack := nackError("events", "k")
	class, _ := plugin.Classify(nack)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{"amqp:nack", "5xx"}, plugin.Tags(nack))
	assert.ErrorContains(t, nack, `exchange "events"`)

	ret := returnError(amqp091.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", RoutingKey: "nowhere"})
	class, _ = plugin.Classify(ret)
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{"amqp:312"}, plugin.Tags(ret))
	assert.ErrorContains(t, ret, "the default exchange")
	assert.ErrorContains(t, ret, "312 NO_ROUTE")

	blocked := blockedError("low on memory", 5*time.Second)
	class, retry := plugin.Classify(blocked)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Equal(t, 5*time.Second, retry)
	assert.Equal(t, []string{"amqp:blocked", "429"}, plugin.Tags(blocked))
	assert.ErrorContains(t, blocked, "low on memory")
}

func TestBatchError(t *testing.T) {
	assert.NoError(t, batchError(nil, 3))
	nack := nackError("", "q")
	assert.Same(t, nack, batchError([]error{nack}, 1), "a single message keeps its error")

	ret := returnError(amqp091.Return{ReplyCode: 312, ReplyText: "NO_ROUTE"})
	err := batchError([]error{nack, ret}, 3)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "the most severe failure wins")
	assert.Equal(t, []string{"amqp:312"}, plugin.Tags(err))
	assert.ErrorContains(t, err, "2 of 3 messages failed")

	blocked := blockedError("alarm", time.Second)
	err = batchError([]error{nack, blocked}, 2)
	class, retry := plugin.Classify(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Equal(t, time.Second, retry)
}
