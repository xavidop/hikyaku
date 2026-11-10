package tcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/plugin"
)

// Timings and sizes of a source; tests shorten the timings.
const (
	// defaultIdle closes a tcp connection, or a DTLS association, that sent
	// nothing for this long.
	defaultIdle = 5 * time.Minute
	// defaultHandshake bounds a TLS or DTLS handshake.
	defaultHandshake = 10 * time.Second
	// defaultLogEvery is the most often a udp source repeats a warning
	// (a full queue, a bad datagram), so a flood does not flood the logs.
	defaultLogEvery = 10 * time.Second
	// readBufferSize is a tcp connection's read buffer. Lines and frames
	// longer than it are read in several reads.
	readBufferSize = 16 << 10
	// maxDatagram holds the largest udp payload (65,507 bytes over IPv4,
	// 65,527 over IPv6).
	maxDatagram = 64 << 10
	// acceptBackoff and acceptMaxBackoff space out retries of a failing
	// accept or read (too many open files, ...).
	acceptBackoff    = 5 * time.Millisecond
	acceptMaxBackoff = time.Second
)

var (
	errNotStarted = errors.New("not listening yet")
	errStopped    = errors.New("stopped")
)

// Source is a tcp or udp listen source.
type Source struct {
	refused func(source string) // hikyaku_backpressure_total
	name    string
	kind    string
	opts    sourceOptions
	log     *slog.Logger

	idle      time.Duration
	handshake time.Duration
	logEvery  time.Duration

	started atomic.Bool
	dropped atomic.Int64 // udp messages the full queue could not take

	mu     sync.Mutex
	addr   string
	status error
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup

	// When a udp source last logged each warning. logMu guards them: every
	// DTLS association logs through them.
	logMu                   sync.Mutex
	lastDropLog, lastBadLog time.Time
}

func newSource(_ context.Context, spec plugin.Spec) (*Source, error) {
	opts, err := parseSource(spec)
	if err != nil {
		return nil, err
	}
	return &Source{
		refused: spec.Env.Refused,
		name:    spec.Name, kind: spec.Kind, opts: opts, log: spec.Env.Log(),
		idle: defaultIdle, handshake: defaultHandshake, logEvery: defaultLogEvery,
		status: errNotStarted, conns: map[net.Conn]struct{}{},
	}, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is nil while the source listens, else why it does not.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Addr is the address the source is bound to, "" before it listens.
func (s *Source) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Dropped counts the udp messages dropped because the relay's queue was
// full.
func (s *Source) Dropped() int64 { return s.dropped.Load() }

func (s *Source) setStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = err
}

func (s *Source) bound(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addr, s.status = addr, nil
}

// Start listens until ctx ends, then closes the listener and every
// connection and returns nil. It fails only when it cannot listen.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	if s.kind == kindUDP {
		return s.serveUDP(ctx, out)
	}
	return s.serveTCP(ctx, out)
}

func (s *Source) serveTCP(ctx context.Context, out chan<- *envelope.Envelope) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.opts.listen)
	if err != nil {
		err = fmt.Errorf("listen %s: %w", s.opts.listen, err)
		s.setStatus(err)
		return err
	}
	s.bound(ln.Addr().String())
	s.log.Info("tcp source listening", "source", s.name, "addr", ln.Addr().String(),
		"frame", s.opts.frame.String(), "tls", s.opts.tls != nil)
	// Connections stop with cctx: when ctx ends, or when the listener fails.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(cctx, func() { _ = ln.Close() })
	defer stop()

	var runErr error
	for failures := 0; ; {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, net.ErrClosed) {
				runErr = fmt.Errorf("listen %s: %w", s.opts.listen, err)
				break
			}
			failures++
			wait := backoff.Base(acceptBackoff, acceptMaxBackoff, failures)
			s.log.Warn("tcp accept failed; retrying", "source", s.name, "err", err, "wait", wait.String())
			if !backoff.Sleep(ctx, wait) {
				break
			}
			continue
		}
		failures = 0
		s.track(c)
		go s.serveConn(cctx, c, out)
	}
	cancel()
	// No connection is tracked after this: the loop that tracks them is done.
	s.mu.Lock()
	s.status = errStopped
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	if runErr != nil {
		s.setStatus(runErr)
	}
	return runErr
}

func (s *Source) track(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[c] = struct{}{}
	s.wg.Add(1)
}

func (s *Source) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	_ = c.Close()
	s.wg.Done()
}

// serveConn reads one client's frames until it goes away, idles out, or
// sends a frame the source cannot take.
func (s *Source) serveConn(ctx context.Context, raw net.Conn, out chan<- *envelope.Envelope) {
	defer s.untrack(raw)
	remote := raw.RemoteAddr().String()
	if !s.opts.allow.allows(addrOf(raw.RemoteAddr())) {
		s.log.Debug("tcp connection refused: the client is not in auth.ip_allow", "source", s.name, "remote_addr", remote)
		return
	}
	c := raw
	if s.opts.tls != nil {
		tc := tls.Server(raw, s.opts.tls)
		hctx, cancel := context.WithTimeout(ctx, s.handshake)
		err := tc.HandshakeContext(hctx)
		cancel()
		if err != nil {
			s.log.Debug("tcp tls handshake failed", "source", s.name, "remote_addr", remote, "err", err)
			return
		}
		defer func() { _ = tc.Close() }() // says close_notify before untrack closes raw
		c = tc
	}
	id := envelope.NewID()
	log := s.log.With("source", s.name, "remote_addr", remote, "conn_id", id)
	log.Debug("tcp client connected")
	frames := s.opts.frame.reader(bufio.NewReaderSize(idleReader{c: c, idle: s.idle}, readBufferSize), s.opts.maxBytes)
	n := 0
	for {
		body, err := frames.next()
		if err != nil {
			s.ended(ctx, log, n, err)
			return
		}
		env := s.newEnvelope(body, remote)
		env.Attributes["tcp.conn_id"] = id
		env.Attributes["ordering_key"] = id
		if !handOver(ctx, out, env) {
			log.Warn("tcp source stopped before the relay took a message; it is lost", "messages", n)
			return
		}
		n++
	}
}

// ended logs why a connection ended.
func (s *Source) ended(ctx context.Context, log *slog.Logger, n int, err error) {
	switch {
	case errors.Is(err, io.EOF):
		log.Debug("tcp client gone", "messages", n)
	case errors.Is(err, errFrameTooLarge):
		log.Warn("tcp frame over max_body_bytes; closing the connection", "messages", n, "max_body_bytes", s.opts.maxBytes, "err", err)
	case errors.Is(err, errTruncated):
		log.Warn("tcp connection ended inside a frame; the partial frame is dropped", "messages", n, "err", err)
	case ctx.Err() != nil:
		log.Debug("tcp connection closed at shutdown", "messages", n)
	case isTimeout(err):
		log.Debug("tcp connection idle; closing it", "messages", n, "idle", s.idle.String())
	default:
		log.Debug("tcp connection failed", "messages", n, "err", err)
	}
}

// handOver gives env to the relay, waiting while its queue is full: the
// connection is not read meanwhile, so TCP flow control holds the client
// back. When ctx ends first, env is kept only if the queue has room.
func handOver(ctx context.Context, out chan<- *envelope.Envelope, env *envelope.Envelope) bool {
	select {
	case out <- env:
		return true
	case <-ctx.Done():
	}
	select {
	case out <- env:
	default:
		return false
	}
	return true
}

// newEnvelope turns one frame into an envelope.
func (s *Source) newEnvelope(body []byte, remote string) *envelope.Envelope {
	env := envelope.New(s.name, s.kind, body)
	env.ContentType = contentType(body)
	env.Attributes[s.kind+".remote_addr"] = remote
	return env
}

// idleReader reads c with a deadline of idle from each read, so a
// connection that sends nothing for that long fails with a timeout. It
// sets no deadline while the source waits for the relay.
type idleReader struct {
	c    net.Conn
	idle time.Duration
}

func (r idleReader) Read(p []byte) (int, error) {
	if r.idle > 0 {
		if err := r.c.SetReadDeadline(time.Now().Add(r.idle)); err != nil {
			return 0, err
		}
	}
	return r.c.Read(p)
}

func (s *Source) serveUDP(ctx context.Context, out chan<- *envelope.Envelope) error {
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, "udp", s.opts.listen)
	if err != nil {
		err = fmt.Errorf("listen %s: %w", s.opts.listen, err)
		s.setStatus(err)
		return err
	}
	conn, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		err := fmt.Errorf("listen %s: not a udp socket (%T)", s.opts.listen, pc)
		s.setStatus(err)
		return err
	}
	if s.opts.tls != nil {
		return s.serveDTLS(ctx, conn, out)
	}
	s.bound(conn.LocalAddr().String())
	s.log.Info("udp source listening", "source", s.name, "addr", conn.LocalAddr().String(), "frame", s.opts.frame.String())
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	buf := make([]byte, maxDatagram)
	var datagram bytes.Reader
	br := bufio.NewReaderSize(&datagram, readBufferSize)
	for failures := 0; ; {
		n, from, err := conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if errors.Is(err, net.ErrClosed) {
				err = fmt.Errorf("listen %s: %w", s.opts.listen, err)
				s.setStatus(err)
				return err
			}
			// Some systems report an ICMP error for an earlier send here.
			failures++
			wait := backoff.Base(acceptBackoff, acceptMaxBackoff, failures)
			s.log.Debug("udp read failed; retrying", "source", s.name, "err", err, "wait", wait.String())
			if !backoff.Sleep(ctx, wait) {
				break
			}
			continue
		}
		failures = 0
		if !s.opts.allow.allows(from.Addr()) {
			s.log.Debug("udp datagram refused: the sender is not in auth.ip_allow", "source", s.name, "remote_addr", from.String())
			continue
		}
		datagram.Reset(buf[:n])
		br.Reset(&datagram)
		s.datagram(out, s.opts.frame.datagramReader(br, s.opts.maxBytes), from.String())
	}
	s.setStatus(errStopped)
	return nil
}

// datagram hands one datagram's frames (or one DTLS record's) to the relay.
// A frame its full queue cannot take is dropped: udp cannot hold its senders
// back.
func (s *Source) datagram(out chan<- *envelope.Envelope, frames *frameReader, remote string) {
	for {
		body, err := frames.next()
		if err != nil {
			if !errors.Is(err, io.EOF) && s.sample(&s.lastBadLog) {
				s.log.Warn("udp datagram dropped: it does not hold whole frames within max_body_bytes",
					"source", s.name, "remote_addr", remote, "max_body_bytes", s.opts.maxBytes, "err", err)
			}
			return
		}
		env := s.newEnvelope(body, remote)
		env.Attributes["ordering_key"] = remote
		select {
		case out <- env:
		default:
			if s.refused != nil {
				s.refused(s.name)
			}
			total := s.dropped.Add(1)
			if s.sample(&s.lastDropLog) {
				s.log.Warn("udp source dropped a message: the relay's queue is full",
					"source", s.name, "remote_addr", remote, "dropped_total", total)
			}
		}
	}
}

// sample reports whether a repeated warning may be logged now: once per
// logEvery.
func (s *Source) sample(last *time.Time) bool {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	now := time.Now()
	if !last.IsZero() && now.Sub(*last) < s.logEvery {
		return false
	}
	*last = now
	return true
}
