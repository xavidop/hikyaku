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
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v4"
	"github.com/pion/dtls/v4/pkg/protocol"
	"github.com/pion/logging"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
)

// DTLS is TLS for udp, through pion/dtls (DTLS 1.2 and 1.3). A udp source
// with tls is a DTLS server on its port: each client's association is a
// peer, and each record it sends is read the way plain udp reads a
// datagram. A udp destination with tls, or a dtls:// URL, keeps one
// association and writes each message as one record.

const (
	// dtlsMaxRecord is the most one DTLS record carries, in either version:
	// 2^14 bytes. A destination refuses a longer frame.
	dtlsMaxRecord = 1 << 14
	// dtlsMaxDatagram holds one record of that size, with its header and
	// any cipher suite's overhead (at most 2,048 bytes).
	dtlsMaxDatagram = dtlsMaxRecord + 2048 + 512
	// defaultRedialIdle: a destination redials an association it has not
	// written to for this long. A server, or a NAT on the way, may have
	// dropped it meanwhile, and DTLS does not tell the client: what it
	// wrote into it would be lost.
	defaultRedialIdle = 30 * time.Second
)

// quietLogs keeps pion/dtls from writing logs of its own to stderr. The
// source and the destination log what matters through the relay's logger.
var quietLogs = &logging.DefaultLoggerFactory{Writer: io.Discard, DefaultLogLevel: logging.LogLevelDisabled}

// dtlsServerOptions turns a source's server TLS settings into DTLS ones: its
// certificate, and client certificates verified against client_ca_file
// (mTLS) when that is set.
func dtlsServerOptions(cfg *tls.Config) []dtls.ServerOption {
	opts := []dtls.ServerOption{
		dtls.WithCertificates(cfg.Certificates...),
		dtls.WithReceiveBufferSize(dtlsMaxDatagram),
		dtls.WithLoggerFactory(quietLogs),
	}
	if cfg.ClientCAs != nil {
		opts = append(opts, dtls.WithClientCAs(cfg.ClientCAs), dtls.WithClientAuth(dtls.RequireAndVerifyClientCert))
	}
	return opts
}

// dtlsClientOptions turns a destination's client TLS settings into DTLS
// ones: the CAs that verify the server (the system's without ca_file), the
// name it is verified against, and a client certificate. asked records that
// the server asked for a client certificate.
func dtlsClientOptions(cfg *tls.Config, asked *atomic.Bool) []dtls.ClientOption {
	certs := cfg.Certificates
	opts := []dtls.ClientOption{
		dtls.WithServerName(cfg.ServerName),
		dtls.WithInsecureSkipVerify(cfg.InsecureSkipVerify),
		dtls.WithLoggerFactory(quietLogs),
		// The same choice pion/dtls makes, noting that the server asked.
		dtls.WithGetClientCertificate(func(cri *dtls.CertificateRequestInfo) (*tls.Certificate, error) {
			asked.Store(true)
			for i := range certs {
				if cri.SupportsCertificate(&certs[i]) == nil {
					return &certs[i], nil
				}
			}
			return new(tls.Certificate), nil
		}),
	}
	if cfg.RootCAs != nil {
		opts = append(opts, dtls.WithRootCAs(cfg.RootCAs))
	}
	if len(certs) > 0 {
		opts = append(opts, dtls.WithCertificates(certs...))
	}
	return opts
}

// dtlsSocket is a source's udp socket as pion/dtls reads it. A datagram
// from a sender that auth.ip_allow keeps out is dropped before DTLS sees
// it, so it costs no handshake. A failed read is retried, as plain udp
// does, rather than ending the listener.
type dtlsSocket struct {
	*net.UDPConn
	ctx context.Context
	s   *Source
}

func (d dtlsSocket) ReadFrom(p []byte) (int, net.Addr, error) {
	for failures := 0; ; {
		n, from, err := d.ReadFromUDPAddrPort(p)
		if err != nil {
			if d.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return 0, nil, err
			}
			// Some systems report an ICMP error for an earlier send here.
			failures++
			wait := backoff.Base(acceptBackoff, acceptMaxBackoff, failures)
			d.s.log.Debug("udp read failed; retrying", "source", d.s.name, "err", err, "wait", wait.String())
			if !backoff.Sleep(d.ctx, wait) {
				return 0, nil, err
			}
			continue
		}
		failures = 0
		if !d.s.opts.allow.allows(from.Addr()) {
			d.s.log.Debug("udp datagram refused: the sender is not in auth.ip_allow", "source", d.s.name, "remote_addr", from.String())
			continue
		}
		return n, net.UDPAddrFromAddrPort(from), nil
	}
}

// serveDTLS runs a DTLS server on the source's udp socket until ctx ends.
// Then it closes every association, each with a close_notify, and the
// socket.
func (s *Source) serveDTLS(ctx context.Context, conn *net.UDPConn, out chan<- *envelope.Envelope) error {
	ln, err := dtls.Listen(dtlsSocket{UDPConn: conn, ctx: ctx, s: s}, dtlsServerOptions(s.opts.tls)...)
	if err != nil {
		_ = conn.Close()
		err = fmt.Errorf("listen %s: dtls: %w", s.opts.listen, err)
		s.setStatus(err)
		return err
	}
	s.bound(conn.LocalAddr().String())
	s.log.Info("udp source listening", "source", s.name, "addr", conn.LocalAddr().String(),
		"frame", s.opts.frame.String(), "dtls", true)
	// Associations stop with cctx: when ctx ends, or when the listener fails.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(cctx, func() { _ = ln.Close() })
	defer stop()

	var runErr error
	for {
		c, err := ln.Accept()
		if err != nil {
			// The socket reads until it is closed, so only the end of ctx
			// or a closed socket ends the listener.
			if ctx.Err() == nil {
				runErr = fmt.Errorf("listen %s: %w", s.opts.listen, err)
			}
			break
		}
		s.track(c)
		go s.serveAssociation(cctx, c, out)
	}
	cancel()
	// No association is tracked after this: the loop that tracks them is done.
	s.mu.Lock()
	s.status = errStopped
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	_ = conn.Close() // pion/dtls closes it with the last association, if there was one
	if runErr != nil {
		s.setStatus(runErr)
	}
	return runErr
}

// handshaker is a connection whose handshake runs on its own: a *dtls.Conn.
type handshaker interface {
	HandshakeContext(ctx context.Context) error
}

// serveAssociation runs one client's association: the handshake, then each
// record the client sends, read the way plain udp reads a datagram. It ends
// when the client closes the association, when the association idles out,
// or when the source stops.
func (s *Source) serveAssociation(ctx context.Context, c net.Conn, out chan<- *envelope.Envelope) {
	defer s.untrack(c) // says close_notify
	remote := remoteOf(c.RemoteAddr())
	log := s.log.With("source", s.name, "remote_addr", remote)
	if h, ok := c.(handshaker); ok {
		hctx, cancel := context.WithTimeout(ctx, s.handshake)
		err := h.HandshakeContext(hctx)
		cancel()
		if err != nil {
			log.Debug("udp dtls handshake failed", "err", err)
			return
		}
	}
	log.Debug("udp dtls client connected")
	buf := make([]byte, dtlsMaxRecord)
	var record bytes.Reader
	br := bufio.NewReaderSize(&record, readBufferSize)
	for {
		if s.idle > 0 {
			if err := c.SetReadDeadline(time.Now().Add(s.idle)); err != nil {
				s.associationEnded(ctx, log, err)
				return
			}
		}
		n, err := c.Read(buf)
		if err != nil {
			s.associationEnded(ctx, log, err)
			return
		}
		record.Reset(buf[:n])
		br.Reset(&record)
		s.datagram(out, s.opts.frame.datagramReader(br, s.opts.maxBytes), remote)
	}
}

// associationEnded logs why an association ended.
func (s *Source) associationEnded(ctx context.Context, log *slog.Logger, err error) {
	switch {
	case ctx.Err() != nil:
		log.Debug("udp dtls association closed at shutdown")
	case errors.Is(err, io.EOF):
		log.Debug("udp dtls client gone")
	case isTimeout(err):
		log.Debug("udp dtls association idle; closing it", "idle", s.idle.String())
	default:
		log.Debug("udp dtls association failed", "err", err)
	}
}

// remoteOf is a peer's address the way plain udp writes udp.remote_addr: an
// IPv4 client of an IPv6 socket stays ::ffff:a.b.c.d.
func remoteOf(a net.Addr) string {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.AddrPort().String()
	}
	return a.String()
}

// connectedSocket is a udp socket dialed to the server, as the
// net.PacketConn pion/dtls writes through. Only a connected socket hears
// that the server's port is closed (ICMP), so a dial to a server that is
// down fails at once with udp:ECONNREFUSED, rather than waiting out the
// handshake.
type connectedSocket struct{ *net.UDPConn }

func (c connectedSocket) WriteTo(p []byte, _ net.Addr) (int, error) { return c.Write(p) }

func (c connectedSocket) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.Read(p)
	return n, c.RemoteAddr(), err
}

// dialDTLS opens a udp socket to the server and runs the DTLS handshake on
// it, within the attempt (or defaultHandshake when the attempt has no
// deadline). Then it starts the association's reader, so that a server
// that closes the association (close_notify, or a port no longer open) is
// noticed before the next delivery.
func (s *Sink) dialDTLS(ctx context.Context) (*sinkConn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultHandshake)
		defer cancel()
	}
	raw, err := s.dialer.DialContext(ctx, "udp", s.opts.addr)
	if err != nil {
		return nil, dialError(ctx, s.kind, s.opts.endpoint, err)
	}
	socket, ok := raw.(*net.UDPConn)
	if !ok {
		_ = raw.Close()
		return nil, dialError(ctx, s.kind, s.opts.endpoint, fmt.Errorf("not a udp socket (%T)", raw))
	}
	var asked atomic.Bool // the server asked for a client certificate
	c, err := dtls.Client(connectedSocket{socket}, socket.RemoteAddr(), dtlsClientOptions(s.opts.tls, &asked)...)
	if err != nil {
		_ = socket.Close()
		return nil, dialError(ctx, s.kind, s.opts.endpoint, fmt.Errorf("dtls: %w", err))
	}
	if err := c.HandshakeContext(ctx); err != nil {
		_ = c.Close()
		_ = socket.Close() // c leaves it open when the handshake stopped before failing
		return nil, dialError(ctx, s.kind, s.opts.endpoint, err)
	}
	sc := &sinkConn{c: c, done: make(chan struct{}), born: time.Now()}
	go sc.read()
	if state, ok := c.ConnectionState(); ok && asked.Load() && state.NegotiatedVersion() == protocol.Version1_3 {
		if err := s.verdict(ctx, sc); err != nil {
			closeNow(c)
			return nil, err
		}
	}
	s.log.Debug("udp destination connected over dtls", "destination", s.name, "url", s.opts.endpoint)
	return sc, nil
}
