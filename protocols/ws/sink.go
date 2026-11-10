package ws

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// defaultReaderWait bounds how long a failed write waits for the
// connection's reader to report the peer's close status.
const defaultReaderWait = 250 * time.Millisecond

// Sink is a ws or wss destination: one persistent connection to its URL.
type Sink struct {
	name     string
	endpoint string // the URL without its password
	opts     sinkOptions
	dialOpts *websocket.DialOptions
	log      *slog.Logger

	pingEvery   time.Duration
	pingTimeout time.Duration
	readerWait  time.Duration

	// sem serializes dials and writes; unlike a mutex, waiting for it
	// honours the caller's context.
	sem chan struct{}

	mu     sync.Mutex
	conn   *sinkConn
	closed bool
}

// sinkConn is one connection and the goroutine reading from it, which
// answers pings and notices when the peer goes away.
type sinkConn struct {
	c    *websocket.Conn
	done chan struct{} // closed once the reader stopped
	err  error         // why it stopped; read only after done is closed
}

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	return &Sink{
		name: spec.Name, endpoint: redactURL(opts.url), opts: opts,
		dialOpts: opts.dial.options(httpClient(opts.clientTLS)), log: spec.Env.Log(),
		pingEvery: defaultPingEvery, pingTimeout: defaultPingTimeout, readerWait: defaultReaderWait,
		sem: make(chan struct{}, 1),
	}, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver writes the body as one frame on the persistent connection,
// dialing it first when there is none. It makes one attempt: a failed write
// drops the connection, and the next delivery redials.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if err := s.acquire(ctx); err != nil {
		return nil, ctxError(ctx, err)
	}
	defer s.release()
	sc, err := s.connection(ctx, true)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, ctxError(ctx, err) // a write would kill the connection
	}
	if err := sc.c.Write(ctx, s.opts.frame, env.Body); err != nil {
		return nil, s.writeFailed(ctx, sc, err)
	}
	return &plugin.Result{Endpoint: s.endpoint}, nil
}

// Health is nil when the destination is connected or can connect now.
func (s *Sink) Health(ctx context.Context) error {
	if err := s.acquire(ctx); err != nil {
		return ctxError(ctx, err)
	}
	defer s.release()
	_, err := s.connection(ctx, false)
	return err
}

// Close says goodbye (1000) and releases the connection.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	sc := s.conn
	s.conn = nil
	s.mu.Unlock()
	if sc == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sc.c.Close(websocket.StatusNormalClosure, "")
	}()
	select {
	case <-done:
	case <-ctx.Done(): // the close handshake gives up on its own
	}
	return nil
}

func (s *Sink) acquire(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) release() { <-s.sem }

// connection returns the live connection, dialing one when there is none.
// A connection that ended since the last delivery is replaced; when the
// peer closed it with an error status and report is set, that status is
// this call's failure instead (ws:close_<code>), and the next call
// redials.
func (s *Sink) connection(ctx context.Context, report bool) (*sinkConn, error) {
	s.mu.Lock()
	closed, sc := s.closed, s.conn
	s.mu.Unlock()
	if closed {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	if sc != nil {
		select {
		case <-sc.done:
			s.forget(sc)
			if code := websocket.CloseStatus(sc.err); report && code >= 0 && !normalClose(code) {
				return nil, peerClosed(s.endpoint, code, sc.err)
			}
		default:
			return sc, nil
		}
	}
	return s.dial(ctx)
}

func (s *Sink) dial(ctx context.Context) (*sinkConn, error) {
	c, resp, err := websocket.Dial(ctx, s.opts.url, s.dialOpts)
	if err != nil {
		return nil, dialError(ctx, s.endpoint, err, resp)
	}
	// What the peer sends is read and dropped, whatever its size.
	c.SetReadLimit(-1)
	sc := &sinkConn{c: c, done: make(chan struct{})}
	go sc.read()
	go keepalive(c, s.pingEvery, s.pingTimeout, sc.done, nil)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = c.CloseNow()
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	s.conn = sc
	s.log.Debug("ws destination connected", "destination", s.name, "url", s.endpoint)
	return sc, nil
}

// read discards what the peer sends until the connection ends. Reading is
// what answers the peer's pings and receives its close frame.
func (sc *sinkConn) read() {
	defer close(sc.done)
	for {
		_, r, err := sc.c.Reader(context.Background())
		if err == nil {
			_, err = io.Copy(io.Discard, r)
		}
		if err != nil {
			sc.err = err
			_ = sc.c.CloseNow()
			return
		}
	}
}

// forget drops sc if it is still the current connection.
func (s *Sink) forget(sc *sinkConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == sc {
		s.conn = nil
	}
}

// writeFailed drops the connection after a failed write and classifies the
// failure, waiting briefly for the reader to learn the peer's close status.
func (s *Sink) writeFailed(ctx context.Context, sc *sinkConn, err error) error {
	_ = sc.c.CloseNow()
	s.forget(sc)
	var why error
	t := time.NewTimer(s.readerWait)
	defer t.Stop()
	select {
	case <-sc.done:
		why = sc.err
	case <-t.C:
	}
	return writeError(ctx, s.endpoint, err, why)
}
