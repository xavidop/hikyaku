package amqp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/xavidop/hikyaku/plugin"
)

// Error tags of the protocol, next to amqp:<reply code>.
const (
	tagNack     = "amqp:nack"
	tagBlocked  = "amqp:blocked"
	tagTemplate = "template"
)

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("amqp destination is closed")

// codeTag is the error tag of an AMQP reply code, such as amqp:404.
func codeTag(code int) string { return "amqp:" + strconv.Itoa(code) }

// permanentCodes are the reply codes a retry cannot fix: the message, the
// credentials, the permissions or the topology are wrong, and stay wrong.
var permanentCodes = map[int]bool{
	amqp091.ContentTooLarge:    true, // 311
	amqp091.NoRoute:            true, // 312
	amqp091.NoConsumers:        true, // 313
	amqp091.InvalidPath:        true, // 402
	amqp091.AccessRefused:      true, // 403
	amqp091.NotFound:           true, // 404
	amqp091.PreconditionFailed: true, // 406
	amqp091.NotAllowed:         true, // 530
	amqp091.NotImplemented:     true, // 540
}

// brokerError classifies an AMQP exception. The reply code is the tag
// (amqp:404) and decides the class: permanentCodes are permanent; every
// other code (320 CONNECTION_FORCED, 504 CHANNEL_ERROR, 506 RESOURCE_ERROR,
// 541 INTERNAL_ERROR, ...) closed the channel or the connection for a
// reason that can pass, so it is transient and also tagged connect: the
// default policy retries it, on a new channel.
func brokerError(what string, e *amqp091.Error) error {
	tags := []string{codeTag(e.Code)}
	class := plugin.Permanent
	if !permanentCodes[e.Code] {
		class = plugin.Transient
		tags = append(tags, plugin.TagConnect)
	}
	return &plugin.Error{Class: class, Tags: tags, Err: fmt.Errorf("%s: %w", what, e)}
}

// classify classifies a failed call: the attempt's context first, then an
// AMQP exception by its code, then a closed channel or a broken connection.
func classify(ctx context.Context, what string, err error) error {
	if ctx.Err() != nil {
		return ctxError(ctx, fmt.Errorf("%s: %w", what, err))
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	if errors.Is(err, amqp091.ErrClosed) {
		return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("%s: the channel or the connection is closed", what))
	}
	var ae *amqp091.Error
	if errors.As(err, &ae) {
		return brokerError(what, ae)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, fmt.Errorf("%s: %w", what, err))
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("%s: %w", what, err))
}

// ctxError classifies a failure caused by the attempt's context: a deadline
// is a timeout; a cancellation (shutdown) is transient.
func ctxError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// dialError classifies a failed dial: the handshake's AMQP exception (bad
// credentials are 403, a vhost the user may not open 530: permanent), else
// a connection that could not be made (transient, connect).
func dialError(ctx context.Context, endpoint string, err error) error {
	return classify(ctx, "connect to "+endpoint, err)
}

// nackError is a message the broker nacked: it could not take
// responsibility for it (an internal error, or a full queue that rejects
// publishes). Transient, tagged amqp:nack, and 5xx like a server's failure,
// so the default policy retries it.
func nackError(exchange, key string) error {
	return plugin.Errorf(plugin.Transient, []string{tagNack, plugin.Tag5xx},
		"the broker nacked the message published to %s with routing key %q", exchangeName(exchange), key)
}

// returnError is a mandatory message the broker returned: 312 NO_ROUTE when
// no queue is bound to receive it. Permanent, tagged amqp:<code>.
func returnError(r amqp091.Return) error {
	class := plugin.Permanent
	if !permanentCodes[int(r.ReplyCode)] {
		class = plugin.Transient
	}
	return plugin.Errorf(class, []string{codeTag(int(r.ReplyCode))},
		"the broker returned the message published to %s with routing key %q: %d %s", exchangeName(r.Exchange), r.RoutingKey, r.ReplyCode, r.ReplyText)
}

// blockedError is a delivery refused while the broker blocks the
// connection: it raised a resource alarm (memory or disk) and reads no
// publishes until it clears. Rate limited, with retryAfter.
func blockedError(reason string, retryAfter time.Duration) error {
	return &plugin.Error{Class: plugin.RateLimited, RetryAfter: retryAfter, Tags: []string{tagBlocked},
		Err: fmt.Errorf("the broker blocked the connection (%s): it accepts no publishes until its resource alarm clears", reason)}
}

// exchangeName names an exchange in messages.
func exchangeName(exchange string) string {
	if exchange == "" {
		return "the default exchange"
	}
	return "exchange " + strconv.Quote(exchange)
}

// severity orders failures for a batch: the batch fails with its most
// severe one, since a retry cannot fix a permanent failure of one message.
func severity(err error) int {
	switch c, _ := plugin.Classify(err); c {
	case plugin.Permanent:
		return 3
	case plugin.Timeout:
		return 2
	case plugin.RateLimited:
		return 1
	}
	return 0
}

// batchError is the failure of a batch whose messages failed with errs
// (one per failed message, in batch order): the most severe failure, which
// keeps its class, tags and retry hint, saying how many messages failed.
func batchError(errs []error, total int) error {
	if len(errs) == 0 {
		return nil
	}
	worst := errs[0]
	for _, e := range errs[1:] {
		if severity(e) > severity(worst) {
			worst = e
		}
	}
	if total == 1 {
		return worst
	}
	class, retryAfter := plugin.Classify(worst)
	return &plugin.Error{Class: class, RetryAfter: retryAfter, Tags: plugin.Tags(worst),
		Err: fmt.Errorf("%d of %d messages failed: %w", len(errs), total, worst)}
}
