package mqtt

import (
	"context"
	"errors"
	"net"
	"sync"
)

// client is one connection to a broker, of either protocol version: the
// source and the destination are written once against it. A client does not
// reconnect; once its connection ends, Lost and then Done close, and the
// caller connects a new one.
type client interface {
	// SessionPresent reports whether the broker resumed a session.
	SessionPresent() bool
	// Subscribe subscribes to filters at qos. It fails when the broker
	// refuses any of them.
	Subscribe(ctx context.Context, filters []string, qos byte) error
	// Publish sends p and waits for its PUBACK (QoS 1) or PUBCOMP (QoS 2),
	// or until it is written (QoS 0). A broker's refusal is a *reasonError.
	Publish(ctx context.Context, p *publication) error
	// Lost is closed as soon as the network connection is gone.
	Lost() <-chan struct{}
	// Done is closed once the client has stopped, after Lost; Err then says
	// why.
	Done() <-chan struct{}
	Err() error
	// Disconnect sends DISCONNECT (the broker keeps a persistent session),
	// closes the connection and waits, within ctx, for the client to stop.
	Disconnect(ctx context.Context) error
}

// connectFunc dials srv and connects one client. onMessage (nil for a
// destination) receives every PUBLISH, one at a time and in arrival order;
// it may block, as backpressure, but must return once the message's
// connection is lost.
type connectFunc func(ctx context.Context, srv server, o connOptions, onMessage func(*message)) (client, error)

// connectorFor picks the client library of a protocol version.
func connectorFor(version string) connectFunc {
	if version == version311 {
		return connectV3
	}
	return connectV5
}

// message is one PUBLISH received.
type message struct {
	topic     string
	payload   []byte
	qos       byte
	retained  bool
	duplicate bool
	packetID  uint16
	// MQTT 5 properties; empty over 3.1.1.
	userProps   []userProp
	contentType string
	correlation []byte
	// ack sends the PUBACK (QoS 1) or PUBREC (QoS 2); nil for QoS 0. Acks
	// must go out in arrival order (see ackQueue).
	ack func() error
	// lost is closed once the connection the message came on is gone: its
	// acknowledgement can no longer be sent.
	lost <-chan struct{}
}

// userProp is one MQTT 5 user property.
type userProp struct{ key, value string }

// publication is one PUBLISH to send.
type publication struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	// MQTT 5 only: 3.1.1 has no properties.
	userProps   []userProp
	contentType string
}

// errNotConnected: the client's connection is gone.
var errNotConnected = errors.New("not connected")

// errDisconnected is why a client this process disconnected stopped.
var errDisconnected = errors.New("disconnected")

// ending records why a client stopped and closes its done channel, once.
type ending struct {
	once sync.Once
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func newEnding() *ending { return &ending{done: make(chan struct{})} }

func (e *ending) finish(err error) {
	e.once.Do(func() {
		if err == nil {
			err = errNotConnected
		}
		e.mu.Lock()
		e.err = err
		e.mu.Unlock()
		close(e.done)
	})
}

func (e *ending) Done() <-chan struct{} { return e.done }

func (e *ending) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// watchedConn is a network connection that reports when it closes. Both
// client libraries close their connection on every failure, before they
// wait for the message handler to return, so lost tells a blocked handler
// to give up.
type watchedConn struct {
	net.Conn
	lost chan struct{}
	once sync.Once
}

func watch(c net.Conn) *watchedConn { return &watchedConn{Conn: c, lost: make(chan struct{})} }

func (c *watchedConn) Close() error {
	c.once.Do(func() { close(c.lost) })
	return c.Conn.Close()
}

// whileConnected derives a context that also ends, with errNotConnected as
// its cause, once lost closes: a wait for an acknowledgement then fails at
// once instead of at its deadline.
func whileConnected(ctx context.Context, lost <-chan struct{}) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancelCause(ctx)
	go func() {
		select {
		case <-lost:
			cancel(errNotConnected)
		case <-cctx.Done():
		}
	}()
	return cctx, func() { cancel(nil) }
}

// lostWhileWaiting reports whether a wait under cctx (from whileConnected)
// ended because the connection was lost rather than because ctx ended.
func lostWhileWaiting(ctx, cctx context.Context) bool {
	return ctx.Err() == nil && errors.Is(context.Cause(cctx), errNotConnected)
}

// closed reports whether ch is closed.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
