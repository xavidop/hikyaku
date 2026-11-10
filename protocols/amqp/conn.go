package amqp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// The source and the sink use the client through these interfaces, so the
// unit tests can run them against an in-process fake broker; the real
// client is adapted by amqpConn and amqpChannel.

// conn is the part of an AMQP connection the protocol uses.
type conn interface {
	Channel() (channel, error)
	NotifyClose(chan *amqp091.Error) chan *amqp091.Error
	NotifyBlocked(chan amqp091.Blocking) chan amqp091.Blocking
	IsClosed() bool
	// CloseDeadline closes the connection, giving the broker until deadline
	// to answer the close handshake.
	CloseDeadline(deadline time.Time) error
}

// channel is the part of an AMQP channel the protocol uses.
type channel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp091.Table) (<-chan amqp091.Delivery, error)
	Cancel(consumer string, noWait bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Confirm(noWait bool) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) (confirmation, error)
	NotifyClose(chan *amqp091.Error) chan *amqp091.Error
	NotifyReturn(chan amqp091.Return) chan amqp091.Return
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp091.Table) (amqp091.Queue, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp091.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp091.Table) error
	IsClosed() bool
	Close() error
}

// confirmation is a publisher confirm to wait for: Done closes when the
// broker acked or nacked the message, or the channel closed (then Acked is
// false).
type confirmation interface {
	Done() <-chan struct{}
	Acked() bool
}

// dialFunc opens a connection to rawURL; ctx bounds the dial and the
// handshake.
type dialFunc func(ctx context.Context, rawURL string, cfg amqp091.Config) (conn, error)

// amqpConn adapts *amqp091.Connection to conn.
type amqpConn struct{ *amqp091.Connection }

func (c amqpConn) Channel() (channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return amqpChannel{ch}, nil
}

// amqpChannel adapts *amqp091.Channel to channel.
type amqpChannel struct{ *amqp091.Channel }

// errNotConfirming is returned by a publish on a channel that is not in
// confirm mode, which the protocol never does.
var errNotConfirming = errors.New("the channel is not in confirm mode")

func (c amqpChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp091.Publishing) (confirmation, error) {
	dc, err := c.Channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	if dc == nil {
		return nil, errNotConfirming
	}
	return dc, nil
}

// handshakeTimeout bounds the TCP connect, the TLS handshake and the AMQP
// handshake of a dial when ctx has no earlier deadline.
const handshakeTimeout = 30 * time.Second

// dialBroker dials rawURL with the real client. The client's dial is not
// context-aware, so the socket's deadline carries ctx's: the handshake gives
// up at ctx's deadline, or at once when ctx is cancelled.
func dialBroker(ctx context.Context, rawURL string, cfg amqp091.Config) (conn, error) {
	var (
		mu sync.Mutex
		nc net.Conn
	)
	cfg.Dial = func(network, addr string) (net.Conn, error) {
		d := net.Dialer{KeepAlive: 30 * time.Second}
		c, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		deadline := time.Now().Add(handshakeTimeout)
		if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
			deadline = dl
		}
		if err := c.SetDeadline(deadline); err != nil {
			_ = c.Close()
			return nil, err
		}
		mu.Lock()
		nc = c
		mu.Unlock()
		return c, nil
	}
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if nc != nil {
			_ = nc.SetDeadline(time.Unix(1, 0)) // fails the handshake's pending read
		}
	})
	c, err := amqp091.DialConfig(rawURL, cfg)
	if !stop() {
		// ctx ended during the dial: whatever it opened is not used.
		if err == nil {
			_ = c.CloseDeadline(time.Now())
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
	}
	if err != nil {
		return nil, err
	}
	return amqpConn{c}, nil
}
