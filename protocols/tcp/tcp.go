// Package tcp is hikyaku's raw socket protocol: the tcp and udp source and
// destination types.
//
// A source listens. Clients connect (tcp, optionally over TLS or mTLS) or
// send datagrams (udp, optionally over DTLS or DTLS with client
// certificates), and every frame they send becomes one envelope. The frame
// setting says how bytes become messages: newline (one message per line,
// the default), length (each message after a big-endian length_bytes
// prefix) or raw (a tcp connection's whole stream, or one udp datagram). A
// udp datagram, or a DTLS record, may hold several newline or length
// frames. A frame that the end of a tcp stream cuts short is dropped, never
// passed on as a whole message.
//
// Neither protocol acknowledges messages, so ack does not apply. A tcp
// connection reads its next frame only once the relay has taken the one
// before: while the relay's queue is full, TCP flow control holds the
// client back. A udp source cannot hold its senders back, so a message the
// full queue cannot take is dropped, counted and logged.
//
// Every envelope has Protocol tcp or udp, a content type sniffed from its
// bytes (application/json, text/plain; charset=utf-8 or
// application/octet-stream) and the attributes tcp.remote_addr and
// tcp.conn_id (a ULID per connection), or udp.remote_addr. Its
// ordering_key is the connection (tcp) or the sender's address (udp), so
// one client's messages keep their order.
//
// A destination writes each message as one frame of the same framing. A tcp
// destination keeps one persistent connection for newline and length
// framing and opens one connection per message for raw. A udp destination
// sends one datagram per message; over DTLS (dtls://), one record per
// message on one association.
package tcp

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/plugin"
)

// The protocol kinds.
const (
	kindTCP = "tcp"
	kindUDP = "udp"
)

// Register makes the tcp and udp source and destination types available.
func Register() {
	for _, kind := range []string{kindTCP, kindUDP} {
		plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
			s, err := newSource(ctx, spec)
			if err != nil {
				return nil, err
			}
			return s, nil
		})
		plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
			s, err := newSink(ctx, spec)
			if err != nil {
				return nil, err
			}
			return s, nil
		})
	}
}

// Content types of received messages.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// contentType sniffs a message: JSON when it parses as JSON, text when it is
// valid UTF-8, else binary.
func contentType(b []byte) string {
	switch {
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}

// addrOf is the IP of a socket address, the zero Addr when it has none.
func addrOf(a net.Addr) netip.Addr {
	switch a := a.(type) {
	case *net.TCPAddr:
		return a.AddrPort().Addr()
	case *net.UDPAddr:
		return a.AddrPort().Addr()
	case nil:
		return netip.Addr{}
	}
	ap, err := netip.ParseAddrPort(a.String())
	if err != nil {
		return netip.Addr{}
	}
	return ap.Addr()
}

// isTimeout reports whether err is a deadline: a context's, or a socket's.
func isTimeout(err error) bool {
	class, _ := plugin.Classify(err)
	return class == plugin.Timeout
}
