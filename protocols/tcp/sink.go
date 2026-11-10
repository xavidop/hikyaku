package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/dtls/v4"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings of a destination; tests shorten them.
const (
	// defaultWriteTimeout bounds a write when the attempt has no deadline.
	defaultWriteTimeout = 30 * time.Second
	// defaultHealthTimeout bounds a health check's dial or lookup.
	defaultHealthTimeout = 2 * time.Second
	// defaultVerdictWait is how long a TLS 1.3 dial that was asked for a
	// client certificate waits for the server to refuse it.
	defaultVerdictWait = 200 * time.Millisecond
)

// Sink is a tcp or udp destination.
type Sink struct {
	name string
	kind string
	opts sinkOptions
	log  *slog.Logger

	dialer        net.Dialer
	resolver      *net.Resolver
	healthTimeout time.Duration
	verdictWait   time.Duration
	redialIdle    time.Duration // DTLS only: see defaultRedialIdle

	// lock serializes the dials and writes of the persistent connection, so
	// frames never interleave. Unlike a mutex, waiting for it honours the
	// caller's context.
	lock chan struct{}

	mu     sync.Mutex
	conn   *sinkConn // the persistent connection (tcp) or socket (udp)
	closed bool
}

// sinkConn is one connection. A tcp connection, or a DTLS association, has
// a reader that discards what the peer sends, and so notices when the peer
// goes away.
type sinkConn struct {
	c    net.Conn
	done chan struct{} // closed once the reader stopped; nil for plain udp
	err  error         // why the reader stopped; read only after done is closed
	// DTLS only: when the association was dialed, and how long after that
	// it was last written to.
	born time.Time
	used atomic.Int64
}

func newSink(_ context.Context, spec plugin.Spec) (*Sink, error) {
	opts, err := parseSink(spec)
	if err != nil {
		return nil, err
	}
	s := &Sink{
		name: spec.Name, kind: spec.Kind, opts: opts, log: spec.Env.Log(),
		resolver: net.DefaultResolver, healthTimeout: defaultHealthTimeout, verdictWait: defaultVerdictWait,
		lock: make(chan struct{}, 1),
	}
	if s.kind == kindUDP && opts.tls != nil {
		s.redialIdle = defaultRedialIdle
	}
	return s, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver writes the body (or the rendered body template) as one frame. It
// makes one attempt: a failed write drops the connection, and the next
// delivery redials. A write that the kernel took counts as delivered: tcp,
// udp and DTLS have no acknowledgement.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	frame, err := s.encode(env)
	if err != nil {
		return nil, err
	}
	if s.kind == kindTCP && s.opts.frame.mode == frameRaw {
		err = s.sendOnce(ctx, frame)
	} else {
		err = s.send(ctx, frame)
	}
	if err != nil {
		return nil, err
	}
	return &plugin.Result{Endpoint: s.opts.endpoint}, nil
}

func (s *Sink) encode(env *envelope.Envelope) ([]byte, error) {
	body := env.Body
	if s.opts.body != nil {
		b, err := s.opts.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		body = []byte(b)
	}
	frame, err := s.opts.frame.encode(body)
	if err != nil {
		return nil, encodeError(s.kind, s.opts.endpoint, err)
	}
	if s.kind == kindUDP && s.opts.tls != nil && len(frame) > dtlsMaxRecord {
		err := fmt.Errorf("%w: %d bytes do not fit one DTLS record (at most %d)", errFrameTooLarge, len(frame), dtlsMaxRecord)
		return nil, encodeError(s.kind, s.opts.endpoint, err)
	}
	return frame, nil
}

// send writes frame on the persistent connection (tcp), socket (udp) or
// association (DTLS), dialing it first when there is none.
func (s *Sink) send(ctx context.Context, frame []byte) error {
	if err := s.acquire(ctx); err != nil {
		return ctxError(ctx, err)
	}
	defer s.release()
	sc, err := s.connection(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return ctxError(ctx, err) // a write now would break the connection for nothing
	}
	if err := write(ctx, sc.c, frame); err != nil {
		// A datagram longer than the network carries never left, and leaves
		// the socket (and its association) as it was.
		if !errors.Is(err, syscall.EMSGSIZE) {
			s.drop(sc)
		}
		return writeError(ctx, s.kind, s.opts.endpoint, err)
	}
	if s.redialIdle > 0 {
		sc.touch()
	}
	return nil
}

// sendOnce writes frame on a connection of its own and closes it: tcp with
// frame: raw, where the end of the stream ends the message.
func (s *Sink) sendOnce(ctx context.Context, frame []byte) error {
	if s.isClosed() {
		return plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	sc, err := s.dial(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = sc.c.Close()
		return ctxError(ctx, err)
	}
	if err := write(ctx, sc.c, frame); err != nil {
		abort(sc.c)
		return writeError(ctx, s.kind, s.opts.endpoint, err)
	}
	// Over TLS, closing sends close_notify; when that fails, whether the
	// message arrived is unknown, and a retry is the safe answer.
	if err := sc.c.Close(); err != nil {
		return writeError(ctx, s.kind, s.opts.endpoint, err)
	}
	return nil
}

// abort closes a raw connection whose message was cut short with a reset
// rather than an orderly close (and, over TLS, without close_notify): with
// raw framing the end of the stream ends the message, so an orderly close
// would deliver the part written as if it were whole. The connection
// carries this one message, so the reset discards nothing else.
func abort(c net.Conn) {
	c = netConn(c)
	if t, ok := c.(*net.TCPConn); ok {
		_ = t.SetLinger(0)
	}
	_ = c.Close()
}

// netConn is the socket under c, a TLS connection's own.
func netConn(c net.Conn) net.Conn {
	if tc, ok := c.(*tls.Conn); ok {
		return tc.NetConn()
	}
	return c
}

// connection returns the live connection, dialing one when there is none.
// A tcp connection or a DTLS association the peer closed since the last
// delivery is replaced, so this delivery is not written into one that is
// gone, and so is an association unused for redialIdle.
func (s *Sink) connection(ctx context.Context) (*sinkConn, error) {
	s.mu.Lock()
	closed, sc := s.closed, s.conn
	s.mu.Unlock()
	if closed {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	if sc != nil {
		select {
		case <-sc.done: // never, for plain udp
			s.drop(sc)
			s.log.Debug(s.kind+" destination connection ended; redialing", "destination", s.name, "url", s.opts.endpoint, "err", sc.err)
		default:
			if !s.stale(sc) {
				return sc, nil
			}
			s.drop(sc)
			s.log.Debug("udp destination association unused for a while; redialing", "destination", s.name, "url", s.opts.endpoint, "idle", s.redialIdle.String())
		}
	}
	var err error
	sc, err = s.dial(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = sc.c.Close()
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	s.conn = sc
	return sc, nil
}

// dial opens a connection of the destination's kind: tcp (and its TLS
// session), a udp socket, or a udp socket and its DTLS association.
func (s *Sink) dial(ctx context.Context) (*sinkConn, error) {
	switch {
	case s.kind == kindTCP:
		return s.dialTCP(ctx)
	case s.opts.tls != nil:
		return s.dialDTLS(ctx)
	}
	return s.dialUDP(ctx)
}

// stale reports whether sc is a DTLS association unused for redialIdle.
func (s *Sink) stale(sc *sinkConn) bool {
	return s.redialIdle > 0 && time.Since(sc.born)-time.Duration(sc.used.Load()) > s.redialIdle
}

// touch records that sc was just written to.
func (sc *sinkConn) touch() { sc.used.Store(int64(time.Since(sc.born))) }

// dialTCP opens a tcp connection (and its TLS session) and starts its
// reader.
func (s *Sink) dialTCP(ctx context.Context) (*sinkConn, error) {
	var c net.Conn
	var err error
	var asked atomic.Bool // the server asked for a client certificate
	if s.opts.tls == nil {
		c, err = s.dialer.DialContext(ctx, "tcp", s.opts.addr)
	} else {
		cfg := s.opts.tls.Clone()
		certs := cfg.Certificates
		// The same choice crypto/tls makes, noting that the server asked.
		cfg.GetClientCertificate = func(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			asked.Store(true)
			for i := range certs {
				if cri.SupportsCertificate(&certs[i]) == nil {
					return &certs[i], nil
				}
			}
			return new(tls.Certificate), nil
		}
		d := tls.Dialer{NetDialer: &s.dialer, Config: cfg}
		c, err = d.DialContext(ctx, "tcp", s.opts.addr)
	}
	if err != nil {
		return nil, dialError(ctx, s.kind, s.opts.endpoint, err)
	}
	sc := &sinkConn{c: c, done: make(chan struct{})}
	go sc.read()
	if tc, ok := c.(*tls.Conn); ok && asked.Load() && tc.ConnectionState().Version >= tls.VersionTLS13 {
		if err := s.verdict(ctx, sc); err != nil {
			_ = c.Close()
			return nil, err
		}
	}
	s.log.Debug("tcp destination connected", "destination", s.name, "url", s.opts.endpoint)
	return sc, nil
}

// verdict waits briefly for a TLS 1.3 (or DTLS 1.3) server to refuse the
// client certificate. Version 1.3 finishes the client's handshake before the
// server checks that certificate, so a refusal arrives as an alert after it,
// and a message written meanwhile would be lost though its write succeeded.
func (s *Sink) verdict(ctx context.Context, sc *sinkConn) error {
	wait := s.verdictWait
	if d, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(d))
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	proto := "TLS"
	if s.kind == kindUDP {
		proto = "DTLS"
	}
	select {
	case <-sc.done:
		return dialError(ctx, s.kind, s.opts.endpoint, fmt.Errorf("the server ended the connection after the %s handshake: %w", proto, sc.err))
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctxError(ctx, fmt.Errorf("dial %s: %w", s.opts.endpoint, ctx.Err()))
	}
}

// read discards what the peer sends until the connection ends. A DTLS
// association is read one record at a time, into a buffer that holds the
// longest.
func (sc *sinkConn) read() {
	defer close(sc.done)
	var err error
	if _, ok := sc.c.(*dtls.Conn); ok {
		buf := make([]byte, dtlsMaxRecord)
		for err == nil {
			_, err = sc.c.Read(buf)
		}
	} else if _, err = io.Copy(io.Discard, sc.c); err == nil {
		err = io.EOF
	}
	sc.err = err
}

func (s *Sink) dialUDP(ctx context.Context) (*sinkConn, error) {
	c, err := s.dialer.DialContext(ctx, "udp", s.opts.addr)
	if err != nil {
		return nil, dialError(ctx, s.kind, s.opts.endpoint, err)
	}
	return &sinkConn{c: c}, nil
}

// drop closes sc at once and forgets it if it is still the current
// connection.
func (s *Sink) drop(sc *sinkConn) {
	closeNow(sc.c)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == sc {
		s.conn = nil
	}
}

// closeNow closes a connection that failed or ended. Over TLS it skips
// close_notify: on a connection whose peer stopped reading, that alert
// would wait for its own 5s deadline, past the attempt's. Over DTLS it says
// close_notify, one datagram that never waits.
func closeNow(c net.Conn) { _ = netConn(c).Close() }

// write writes b in one call, bounded by ctx: its deadline (else
// defaultWriteTimeout), and its cancellation.
func write(ctx context.Context, c net.Conn, b []byte) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultWriteTimeout)
	}
	if err := c.SetWriteDeadline(deadline); err != nil {
		return err
	}
	var mu sync.Mutex
	finished := false
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if !finished {
			_ = c.SetWriteDeadline(time.Unix(1, 0)) // in the past: the write returns
		}
	})
	_, err := c.Write(b)
	mu.Lock()
	finished = true
	mu.Unlock()
	stop()
	return err
}

// Health is nil when the destination can take a message now. A tcp or
// DTLS destination has its persistent connection (or association) up, or
// dials one and keeps it; while a delivery is dialing, or for tcp with raw
// framing, it dials one of its own and closes it. A plain udp destination
// resolves its host. Health never waits for a delivery.
func (s *Sink) Health(ctx context.Context) error {
	if s.isClosed() {
		return plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	ctx, cancel := context.WithTimeout(ctx, s.healthTimeout)
	defer cancel()
	switch {
	case s.kind == kindUDP && s.opts.tls == nil:
		return s.resolve(ctx)
	case s.kind == kindTCP && s.opts.frame.mode == frameRaw:
		return s.probe(ctx)
	case s.live() != nil:
		return nil
	}
	select {
	case s.lock <- struct{}{}:
		defer s.release()
		_, err := s.connection(ctx)
		return err
	default:
		return s.probe(ctx) // a delivery is dialing
	}
}

// live returns the persistent connection while it is up (and, for DTLS, in
// use: the next delivery redials a stale association).
func (s *Sink) live() *sinkConn {
	s.mu.Lock()
	sc := s.conn
	s.mu.Unlock()
	if sc == nil || s.stale(sc) {
		return nil
	}
	select {
	case <-sc.done:
		return nil
	default:
		return sc
	}
}

// probe dials a connection and closes it.
func (s *Sink) probe(ctx context.Context) error {
	sc, err := s.dial(ctx)
	if err != nil {
		return err
	}
	_ = sc.c.Close()
	return nil
}

// resolve looks the destination's host up; an IP needs no lookup.
func (s *Sink) resolve(ctx context.Context) error {
	host, _, err := net.SplitHostPort(s.opts.addr)
	if err != nil {
		return err
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return nil
	}
	if _, err := s.resolver.LookupHost(ctx, host); err != nil {
		return plugin.Wrap(plugin.Transient, connectTags(s.kind, err), fmt.Errorf("resolve %s: %w", host, err))
	}
	return nil
}

// Close releases the connection, once a delivery in progress has written
// its frame. Over TLS it says close_notify. ctx bounds both: when it ends,
// the connection is cut. Deliveries after Close fail.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.acquire(ctx) == nil {
		defer s.release()
	}
	s.mu.Lock()
	sc := s.conn
	s.conn = nil
	s.mu.Unlock()
	if sc == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sc.c.Close()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		closeNow(sc.c) // ends the close_notify write that waits
		<-done
	}
	return nil
}

func (s *Sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Sink) acquire(ctx context.Context) error {
	select {
	case s.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) release() { <-s.lock }
