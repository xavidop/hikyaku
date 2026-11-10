package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/xavidop/hikyaku/internal/backoff"
)

// dialTimeout bounds one connect to one server.
const dialTimeout = 5 * time.Second

// conn is a NATS connection and what happened to it: why it is not
// connected, for Health, and when it closed for good.
type conn struct {
	nc   *nats.Conn
	log  *slog.Logger
	role string // "source" or "destination", for logs
	name string

	// onPermission is told of the server's permission violations (set
	// before the connection is made).
	onPermission func(error)

	mu      sync.Mutex
	lastErr error  // why the connection is down, or the last async error
	server  string // the server connected to, redacted

	dropped   atomic.Int64 // messages a slow subscription dropped
	closed    chan struct{}
	closeOnce sync.Once
}

// dial connects. A server that is down does not fail it: the connection
// keeps trying in the background (RetryOnFailedConnect) and Health says
// why it is not connected. It fails only on settings nats.go refuses.
func dial(o connOptions, log *slog.Logger, role, name string, onPermission func(error), extra ...nats.Option) (*conn, error) {
	c := &conn{log: log, role: role, name: name, onPermission: onPermission, closed: make(chan struct{})}
	maxReconnects := -1
	if o.reconnect.maxAttempts > 0 {
		maxReconnects = o.reconnect.maxAttempts
	}
	rc := o.reconnect
	opts := nats.GetDefaultOptions()
	opts.Servers = o.servers
	options := []nats.Option{
		nats.Name(o.name),
		nats.DontRandomize(), // url first, then failover_urls in order
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(maxReconnects),
		// Called once per pass over the servers that all failed: the wait
		// grows with the passes in a row.
		nats.CustomReconnectDelay(func(attempts int) time.Duration {
			return backoff.Delay(rc.backoff, rc.maxBackoff, rc.jitter, attempts, rand.Float64)
		}),
		// Bad credentials are retried like any other failure, until
		// reconnect.max_attempts: they may be fixed on the server.
		nats.IgnoreAuthErrorAbort(),
		nats.Timeout(dialTimeout),
		nats.ConnectHandler(c.connected),
		nats.ReconnectHandler(c.reconnected),
		nats.DisconnectErrHandler(c.disconnected),
		nats.ReconnectErrHandler(c.connectFailed),
		nats.ClosedHandler(c.closedNow),
		nats.ErrorHandler(c.asyncError),
	}
	switch {
	case o.credsFile != "":
		options = append(options, nats.UserCredentials(o.credsFile))
	case o.token != "":
		options = append(options, nats.Token(o.token))
	case o.username != "":
		options = append(options, nats.UserInfo(o.username, o.password))
	}
	if o.tls != nil {
		options = append(options, nats.Secure(o.tls))
	}
	options = append(options, extra...)
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, err
		}
	}
	nc, err := opts.Connect()
	if err != nil {
		return nil, err
	}
	c.nc = nc
	return c, nil
}

func (c *conn) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastErr = err
}

// reason is why the connection is not connected, as far as it knows.
func (c *conn) reason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastErr != nil {
		return c.lastErr
	}
	return c.nc.LastError()
}

// url is the server the connection is (or was last) connected to, without
// credentials.
func (c *conn) url() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server
}

func (c *conn) connected(nc *nats.Conn) {
	shown := redactURL(nc.ConnectedUrl())
	c.mu.Lock()
	c.lastErr, c.server = nil, shown
	c.mu.Unlock()
	c.log.Info("nats connected", c.role, c.name, "url", shown)
}

func (c *conn) reconnected(nc *nats.Conn) {
	shown := redactURL(nc.ConnectedUrl())
	c.mu.Lock()
	c.lastErr, c.server = nil, shown
	c.mu.Unlock()
	c.log.Info("nats reconnected", c.role, c.name, "url", shown)
}

func (c *conn) disconnected(nc *nats.Conn, err error) {
	if nc.IsClosed() {
		// Closed on purpose (Close, or it gave up): no reconnect follows.
		c.log.Debug("nats disconnected", c.role, c.name, "url", c.url())
		return
	}
	if err == nil {
		err = errors.New("disconnected")
	}
	c.setErr(err)
	c.log.Warn("nats disconnected; reconnecting", c.role, c.name, "url", c.url(), "err", err)
}

func (c *conn) connectFailed(_ *nats.Conn, err error) {
	c.setErr(err)
	c.log.Warn("nats connect failed", c.role, c.name, "err", err)
}

func (c *conn) closedNow(*nats.Conn) {
	// Log first: once closed is closed, this connection writes nothing more.
	c.log.Debug("nats connection closed", c.role, c.name)
	c.closeOnce.Do(func() { close(c.closed) })
}

// waitClosed waits (up to a second) for the client's closed handler, which
// runs on its callback goroutine after the status turned closed, so nothing
// of this connection runs after its source or sink returned.
func (c *conn) waitClosed() {
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-c.closed:
	case <-t.C:
	}
}

// asyncError handles what the server or client reports out of band: a
// subscription too slow to keep up (it drops messages), a permission
// violation, an auth error.
func (c *conn) asyncError(_ *nats.Conn, sub *nats.Subscription, err error) {
	switch {
	case errors.Is(err, nats.ErrSlowConsumer):
		var total int
		if sub != nil {
			total, _ = sub.Dropped()
		}
		if total > 0 {
			c.dropped.Store(int64(total))
		}
		c.log.Warn("nats subscription too slow: messages dropped (raise prefetch, or use jetstream)",
			c.role, c.name, "dropped_total", total)
	case errors.Is(err, nats.ErrPermissionViolation):
		c.setErr(err)
		c.log.Error("nats permission violation", c.role, c.name, "err", err)
		if c.onPermission != nil {
			c.onPermission(err)
		}
	default:
		c.setErr(err)
		c.log.Warn("nats error", c.role, c.name, "err", err)
	}
}

// health is nil while connected, else why not.
func (c *conn) health() error {
	st := c.nc.Status()
	if st == nats.CONNECTED {
		return nil
	}
	state := strings.ToLower(st.String())
	if err := c.reason(); err != nil {
		return fmt.Errorf("nats not connected (%s): %w", state, err)
	}
	return fmt.Errorf("nats not connected (%s)", state)
}

// isClosed reports whether the connection closed for good: it gave up
// reconnecting, or it was closed. (The status, not the closed handler,
// which runs later.)
func (c *conn) isClosed() bool { return c.nc.IsClosed() }

// close drains the connection, so what it holds (acks, publishes) reaches
// the server, and waits for it to close, or closes it at once when ctx
// ends first.
func (c *conn) close(ctx context.Context) {
	if c.nc.IsClosed() {
		c.waitClosed()
		return
	}
	if err := c.nc.Drain(); err != nil {
		c.nc.Close()
		return
	}
	select {
	case <-c.closed:
	case <-ctx.Done():
		c.nc.Close()
	}
}

// deniedSubscription reports whether a permission violation is about
// subscribing to subject (with queue): the server refused the
// subscription.
func deniedSubscription(err error, subject, queue string) bool {
	if !errors.Is(err, nats.ErrPermissionViolation) {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, strings.ToLower(`subscription to "`+subject+`"`)) {
		return false
	}
	return queue == "" || strings.Contains(msg, strings.ToLower(`using queue "`+queue+`"`))
}

// deniedPublish turns the server's refusal of a publish to subject into a
// permanent error. The refusal comes as an asynchronous -ERR, which the
// connection records as its last error before it reads the PONG that ends
// a flush; before is the last error from before the publish, so an older
// refusal does not count.
func deniedPublish(now, before error, subject string) bool {
	if now == nil || now == before || !errors.Is(now, nats.ErrPermissionViolation) {
		return false
	}
	return strings.Contains(strings.ToLower(now.Error()), strings.ToLower(`publish to "`+subject+`"`))
}
