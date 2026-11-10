package tcp

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/dtls/v4"
	"github.com/pion/dtls/v4/pkg/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
)

// The log lines a DTLS source writes per association.
const (
	logConnected = "udp dtls client connected"
	logGone      = "udp dtls client gone"
)

// closedUDPAddr is a udp address nothing listens on.
func closedUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pc.LocalAddr().String()
	require.NoError(t, pc.Close())
	return addr
}

// dtlsDial is a DTLS client of addr with the client settings c, built the
// way a destination builds its own. It closes at cleanup.
func dtlsDial(t *testing.T, addr string, c *config.TLS, extra ...dtls.ClientOption) *dtls.Conn {
	t.Helper()
	conn, _ := dtlsDialOn(t, addr, c, func(s *net.UDPConn) net.PacketConn { return connectedSocket{s} }, extra...)
	return conn
}

func dtlsDialOn(t *testing.T, addr string, c *config.TLS, wrap func(*net.UDPConn) net.PacketConn, extra ...dtls.ClientOption) (*dtls.Conn, *net.UDPConn) {
	t.Helper()
	cfg, err := httpmux.ClientTLS(c)
	require.NoError(t, err)
	cfg.ServerName = "127.0.0.1"
	raw, err := net.Dial("udp", addr)
	require.NoError(t, err)
	socket, ok := raw.(*net.UDPConn)
	require.True(t, ok)
	var asked atomic.Bool
	conn, err := dtls.Client(wrap(socket), socket.RemoteAddr(), append(dtlsClientOptions(cfg, &asked), extra...)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = socket.Close()
	})
	return conn, socket
}

func dtlsHandshake(t *testing.T, c *dtls.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.HandshakeContext(ctx))
}

func writeRecords(t *testing.T, c *dtls.Conn, records ...[]byte) {
	t.Helper()
	for _, r := range records {
		_, err := c.Write(r)
		require.NoError(t, err)
	}
}

// waitEOF waits for the server to close c with close_notify.
func waitEOF(t *testing.T, c *dtls.Conn) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err := c.Read(make([]byte, dtlsMaxRecord))
	assert.ErrorIs(t, err, io.EOF, "the server said close_notify")
}

// mute lets a DTLS client's first datagram through and drops the rest, so
// that its handshake stops after the first flight.
type mute struct {
	connectedSocket
	sent atomic.Int64
}

func (m *mute) WriteTo(p []byte, a net.Addr) (int, error) {
	if m.sent.Add(1) > 1 {
		return len(p), nil
	}
	return m.connectedSocket.WriteTo(p, a)
}

func TestDTLSSourceFramesPerRecord(t *testing.T) {
	p := newPKI(t)
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		records     func(t *testing.T) [][]byte
		want        []string
	}{
		{
			name:    "newline: several lines per record",
			frame:   "newline",
			records: func(*testing.T) [][]byte { return [][]byte{[]byte("a\nb\r\n\nc"), []byte(`{"d":4}` + "\n")} },
			want:    []string{"a", "b", "c", `{"d":4}`},
		},
		{
			name:  "length: several frames per record",
			frame: "length", lengthBytes: 2,
			records: func(t *testing.T) [][]byte {
				return [][]byte{wire(t, mustFraming(t, "length", 2), "x", `{"y":1}`), wire(t, mustFraming(t, "length", 2), "z")}
			},
			want: []string{"x", `{"y":1}`, "z"},
		},
		{
			name:    "raw: one message per record",
			frame:   "raw",
			records: func(*testing.T) [][]byte { return [][]byte{[]byte("a\nb"), []byte("c")} },
			want:    []string{"a\nb", "c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS, Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, s)
			c := dtlsDial(t, listening(t, s), p.anonymous)
			dtlsHandshake(t, c)
			writeRecords(t, c, tc.records(t)...)
			for _, want := range tc.want {
				env := recv(t, out)
				assert.Equal(t, want, env.BodyText())
				assert.Equal(t, kindUDP, env.Protocol)
				assert.Equal(t, c.LocalAddr().String(), env.Attributes["udp.remote_addr"])
				assert.Equal(t, c.LocalAddr().String(), env.Attributes["ordering_key"])
				assert.NotContains(t, env.Attributes, "tcp.conn_id")
			}
			noEnvelope(t, out, 50*time.Millisecond)
		})
	}
}

func TestDTLSSourceContentTypes(t *testing.T) {
	p := newPKI(t)
	s := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS, Frame: "raw"})
	out, _ := run(t, s)
	c := dtlsDial(t, listening(t, s), p.anonymous)
	dtlsHandshake(t, c)
	writeRecords(t, c, []byte(`{"a":1}`), []byte("text"), []byte{0xff, 0x00})
	assert.Equal(t, contentJSON, recv(t, out).ContentType)
	assert.Equal(t, contentText, recv(t, out).ContentType)
	assert.Equal(t, contentBinary, recv(t, out).ContentType)
}

// A datagram that is not DTLS never reaches the relay.
func TestDTLSSourceDropsPlainDatagrams(t *testing.T) {
	p := newPKI(t)
	s := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, s)
	send(t, udpClient(t, listening(t, s)), []byte("plain text\n"), []byte{0x16, 0xfe, 0xfd, 0x00})
	noEnvelope(t, out, 100*time.Millisecond)
}

func TestDTLSSourceAndSink(t *testing.T) {
	p := newPKI(t)
	for _, tc := range []struct {
		frame       string
		lengthBytes int
	}{{"newline", 0}, {"length", 2}, {"raw", 0}} {
		t.Run(tc.frame, func(t *testing.T) {
			src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS, Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, src)
			url := "dtls://" + listening(t, src)
			sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous, Frame: tc.frame, LengthBytes: tc.lengthBytes})
			for _, body := range []string{"one", `{"two":2}`, strings.Repeat("big ", 2000)} {
				res, err := sink.Deliver(context.Background(), msg(body))
				require.NoError(t, err)
				assert.Equal(t, url, res.Endpoint)
				env := recv(t, out)
				assert.Equal(t, body, env.BodyText())
				assert.Equal(t, kindUDP, env.Protocol)
			}
		})
	}

	// Any tls key on a udp:// URL turns DTLS on too.
	src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	url := "udp://" + listening(t, src)
	sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous})
	res, err := sink.Deliver(context.Background(), msg("over udp://"))
	require.NoError(t, err)
	assert.Equal(t, url, res.Endpoint)
	assert.Equal(t, "over udp://", recv(t, out).BodyText())
}

func TestDTLSSinkRefusesAnUntrustedServer(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + listening(t, src)}) // the system's CAs
	err := deliver(sink, "x")
	assertClass(t, err, plugin.Transient, plugin.TagConnect)
	assert.Contains(t, err.Error(), "certificate")
	noEnvelope(t, out, 50*time.Millisecond)
}

func TestDTLSMutualAuth(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.mutualTLS})
	out, _ := run(t, src)
	addr := listening(t, src)
	url := "dtls://" + addr

	sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.clientTLS})
	require.NoError(t, sink.Health(context.Background()))
	require.NoError(t, deliver(sink, "signed"))
	assert.Equal(t, "signed", recv(t, out).BodyText())

	// DTLS 1.3 refuses a certificate after the client's handshake is done:
	// the destination waits for that verdict instead of reporting delivered
	// a message the server drops.
	for _, tc := range []struct {
		name string
		tls  *config.TLS
	}{
		{"no client certificate", p.anonymous},
		{"a client certificate from another CA", &config.TLS{CAFile: p.ca, CertFile: p.otherCert, KeyFile: p.otherKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refused := newTestSink(t, config.Endpoint{URL: url, TLS: tc.tls})
			refused.verdictWait = 3 * time.Second // a busy machine answers late; the refusal ends the wait
			err := deliver(refused, "forged")
			assertClass(t, err, plugin.Transient, plugin.TagConnect)
			assert.Contains(t, err.Error(), "the server ended the connection after the DTLS handshake")
			assert.Error(t, refused.Health(context.Background()))
			noEnvelope(t, out, 50*time.Millisecond)
		})
	}

	// DTLS 1.2 refuses it inside the handshake.
	t.Run("a DTLS 1.2 client without a certificate", func(t *testing.T) {
		before := logs.count(slog.LevelDebug, "udp dtls handshake failed")
		c := dtlsDial(t, addr, p.anonymous, dtls.WithMaxVersion(protocol.Version1_2))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.Error(t, c.HandshakeContext(ctx))
		require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, "udp dtls handshake failed") > before },
			2*time.Second, time.Millisecond)
		noEnvelope(t, out, 50*time.Millisecond)
	})
}

// dtlsServer is a pion/dtls server that keeps the records it receives.
type dtlsServer struct {
	addr    string
	records chan string
}

func newDTLSServer(t *testing.T, opts ...dtls.ServerOption) *dtlsServer {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	ln, err := dtls.Listen(pc, opts...)
	require.NoError(t, err)
	srv := &dtlsServer{addr: pc.LocalAddr().String(), records: make(chan string, 64)}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go func() {
				buf := make([]byte, dtlsMaxRecord)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					srv.records <- string(buf[:n])
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		_ = pc.Close()
	})
	return srv
}

// A DTLS 1.2 server refuses a missing client certificate inside the
// handshake, so the destination needs no verdict to fail.
func TestDTLSSinkMutualAuthOverDTLS12(t *testing.T) {
	p := newPKI(t)
	cfg, _, err := httpmux.ServerTLS(p.mutualTLS)
	require.NoError(t, err)
	srv := newDTLSServer(t, append(dtlsServerOptions(cfg), dtls.WithMaxVersion(protocol.Version1_2))...)
	url := "dtls://" + srv.addr

	refused := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous})
	refused.verdictWait = time.Hour // never waited for
	err = deliver(refused, "forged")
	assertClass(t, err, plugin.Transient, plugin.TagConnect)
	assert.NotContains(t, err.Error(), "after the DTLS handshake")

	sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.clientTLS})
	require.NoError(t, deliver(sink, "signed"))
	select {
	case got := <-srv.records:
		assert.Equal(t, "signed\n", got)
	case <-time.After(5 * time.Second):
		t.Fatal("the server got no record")
	}
}

func TestDTLSSinkDialRefused(t *testing.T) {
	p := newPKI(t)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + closedUDPAddr(t), TLS: p.anonymous})
	start := time.Now()
	err := deliver(sink, "x")
	assertClass(t, err, plugin.Transient, plugin.TagConnect, "udp:ECONNREFUSED")
	assert.Less(t, time.Since(start), 4*time.Second, "a closed port answers at once")
	assert.Contains(t, err.Error(), "dial dtls://127.0.0.1:")
}

// A server that never answers holds the handshake for the whole attempt.
func TestDTLSSinkHandshakeTimeout(t *testing.T) {
	p := newPKI(t)
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + silent.LocalAddr().String(), TLS: p.anonymous})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = sink.Deliver(ctx, msg("x"))
	assertClass(t, err, plugin.Timeout, plugin.TagTimeout)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestDTLSSinkFrameTooLarge(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS, Frame: "raw"})
	out, _ := run(t, src)
	url := "dtls://" + listening(t, src)

	raw := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous, Frame: "raw"})
	err := deliver(raw, strings.Repeat("x", dtlsMaxRecord+1))
	assertClass(t, err, plugin.Permanent, "udp:frame_too_large")
	assert.Contains(t, err.Error(), "16385 bytes do not fit one DTLS record (at most 16384)")
	// With newline framing, the line ending counts.
	lines := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous})
	assertClass(t, deliver(lines, strings.Repeat("x", dtlsMaxRecord)), plugin.Permanent, "udp:frame_too_large")
	assert.Zero(t, logs.count(slog.LevelDebug, logConnected), "refused before any dial")

	// A record over what the network carries in one datagram (macOS sends
	// at most 9,216 bytes by default) never leaves, and the association
	// carries the next message.
	big := strings.Repeat("y", 12000)
	if err := deliver(raw, big); err != nil {
		assertClass(t, err, plugin.Permanent, "udp:frame_too_large")
	} else {
		assert.Equal(t, big, recv(t, out).BodyText())
	}
	require.NoError(t, deliver(raw, "after"))
	assert.Equal(t, "after", recv(t, out).BodyText())
	assert.Equal(t, 1, logs.count(slog.LevelDebug, logConnected))
}

// The source's shutdown says close_notify: the destination notices, fails
// while the port is closed, and redials once a server is back.
func TestDTLSSinkRedialsAfterTheServerCloses(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, stop := run(t, src)
	addr := listening(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + addr, TLS: p.anonymous})
	require.NoError(t, deliver(sink, "one"))
	assert.Equal(t, "one", recv(t, out).BodyText())

	stop()
	require.Eventually(t, func() bool { return sink.live() == nil }, 2*time.Second, time.Millisecond,
		"the destination heard the close_notify")
	err := deliver(sink, "while down")
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Contains(t, []plugin.Class{plugin.Transient, plugin.Timeout}, class, "refused, or no answer: %v", err)

	again := newTestSource(t, kindUDP, config.Source{Listen: addr, TLS: p.serverTLS})
	out, _ = run(t, again)
	listening(t, again)
	require.NoError(t, deliver(sink, "two"))
	assert.Equal(t, "two", recv(t, out).BodyText())
}

// An association unused for redialIdle is closed and redialed: the server,
// or a NAT, may have dropped it.
func TestDTLSSinkRedialsAnIdleAssociation(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + listening(t, src), TLS: p.anonymous})
	assert.Equal(t, defaultRedialIdle, sink.redialIdle)
	sink.redialIdle = 20 * time.Millisecond

	require.NoError(t, deliver(sink, "one"))
	assert.Equal(t, "one", recv(t, out).BodyText())
	time.Sleep(50 * time.Millisecond)
	assert.Nil(t, sink.live(), "an idle association is not live")
	require.NoError(t, deliver(sink, "two"))
	assert.Equal(t, "two", recv(t, out).BodyText())
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, logConnected) == 2 }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, logGone) == 1 }, 2*time.Second, time.Millisecond,
		"the destination closed the idle association with close_notify")

	plain := newTestSink(t, config.Endpoint{URL: "udp://127.0.0.1:9"})
	assert.Zero(t, plain.redialIdle, "plain udp has no association")
}

func TestDTLSSinkHealth(t *testing.T) {
	ctx := context.Background()
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + listening(t, src), TLS: p.anonymous})

	// While a delivery is dialing, Health dials an association of its own,
	// and closes it.
	require.NoError(t, sink.acquire(ctx))
	assert.NoError(t, sink.Health(ctx))
	assert.Nil(t, sink.conn)
	sink.release()
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, logGone) == 1 }, 2*time.Second, time.Millisecond,
		"the probe's association is closed with close_notify")

	// Otherwise it dials one and keeps it for the deliveries.
	require.NoError(t, sink.Health(ctx))
	require.NoError(t, deliver(sink, "a"))
	assert.Equal(t, "a", recv(t, out).BodyText())
	require.NoError(t, sink.Health(ctx))
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, logConnected) == 2 }, 2*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 2, logs.count(slog.LevelDebug, logConnected), "the health check's association carries the deliveries")

	down := newTestSink(t, config.Endpoint{URL: "dtls://" + closedUDPAddr(t), TLS: p.anonymous})
	assert.Error(t, down.Health(ctx))
}

func TestDTLSSinkClose(t *testing.T) {
	ctx := context.Background()
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + listening(t, src), TLS: p.anonymous})
	require.NoError(t, deliver(sink, "last words"))
	assert.Equal(t, "last words", recv(t, out).BodyText())
	require.NoError(t, sink.Close(ctx))
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, logGone) == 1 }, 2*time.Second, time.Millisecond,
		"Close says close_notify")

	assertClass(t, deliver(sink, "too late"), plugin.Transient)
	assert.Error(t, sink.Health(ctx))
	assert.NoError(t, sink.Close(ctx), "closing twice is fine")
}

// A client that stops after its first flight holds an association for the
// handshake timeout only.
func TestDTLSSourceHandshakeTimeout(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	src.handshake = 100 * time.Millisecond
	run(t, src)
	c, _ := dtlsDialOn(t, listening(t, src), p.anonymous, func(s *net.UDPConn) net.PacketConn { return &mute{connectedSocket: connectedSocket{s}} })
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.Error(t, c.HandshakeContext(ctx))
	require.Eventually(t, func() bool { return logs.count(slog.LevelDebug, "udp dtls handshake failed") == 1 }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return len(src.conns) == 0
	}, 2*time.Second, time.Millisecond, "the association is gone")
}

// The source closes an association that sent nothing for idle, with a
// close_notify.
func TestDTLSSourceClosesIdleAssociations(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	src.idle = 100 * time.Millisecond
	out, _ := run(t, src)
	c := dtlsDial(t, listening(t, src), p.anonymous)
	dtlsHandshake(t, c)
	writeRecords(t, c, []byte("hello\n"))
	assert.Equal(t, "hello", recv(t, out).BodyText())
	start := time.Now()
	waitEOF(t, c)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, 1, logs.count(slog.LevelDebug, "udp dtls association idle; closing it"))
}

// DTLS cannot hold its senders back either: what the full queue cannot
// take is dropped and counted.
func TestDTLSSourceDropsWhenTheQueueIsFull(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	var refused atomic.Int64
	src.refused = func(string) { refused.Add(1) } // hikyaku_backpressure_total
	queue := make(chan *envelope.Envelope, 1)
	runOut(t, src, queue)
	c := dtlsDial(t, listening(t, src), p.anonymous)
	dtlsHandshake(t, c)

	writeRecords(t, c, []byte("kept\n"))
	require.Eventually(t, func() bool { return len(queue) == 1 }, 2*time.Second, time.Millisecond)
	writeRecords(t, c, []byte("dropped 1\ndropped 2\n"), []byte("dropped 3\n"))
	require.Eventually(t, func() bool { return src.Dropped() == 3 }, 2*time.Second, time.Millisecond)
	assert.Equal(t, int64(3), refused.Load())
	assert.Equal(t, 1, logs.count(slog.LevelWarn, "udp source dropped a message: the relay's queue is full"), "one warning per logEvery")
	assert.Equal(t, "kept", (<-queue).BodyText())

	writeRecords(t, c, []byte("after\n"))
	assert.Equal(t, "after", recv(t, queue).BodyText(), "the queue has room again")
}

// auth.ip_allow is checked before DTLS: a sender kept out gets no handshake.
func TestDTLSSourceIPAllow(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindUDP, config.Source{TLS: p.serverTLS, Auth: &config.Auth{IPAllow: []string{"192.0.2.0/24"}}})
	run(t, src)
	c := dtlsDial(t, listening(t, src), p.anonymous)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.Error(t, c.HandshakeContext(ctx))
	assert.Positive(t, logs.count(slog.LevelDebug, "udp datagram refused: the sender is not in auth.ip_allow"))
	assert.Zero(t, logs.count(slog.LevelDebug, logConnected))

	src = newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS, Auth: &config.Auth{IPAllow: []string{"127.0.0.0/8"}}})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "dtls://" + listening(t, src), TLS: p.anonymous})
	require.NoError(t, deliver(sink, "let in"))
	assert.Equal(t, "let in", recv(t, out).BodyText())
}

// Stopping closes every association with a close_notify, then the socket.
func TestDTLSSourceStops(t *testing.T) {
	ctx := context.Background()
	p := newPKI(t)
	src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	assert.ErrorIs(t, src.Health(ctx), errNotStarted)
	out, stop := run(t, src)
	addr := listening(t, src)
	require.NoError(t, src.Health(ctx))
	c := dtlsDial(t, addr, p.anonymous)
	dtlsHandshake(t, c)
	writeRecords(t, c, []byte("x\n"))
	assert.Equal(t, "x", recv(t, out).BodyText())
	// One more client in the middle of its handshake.
	mid, _ := dtlsDialOn(t, addr, p.anonymous, func(s *net.UDPConn) net.PacketConn { return &mute{connectedSocket: connectedSocket{s}} })
	hctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_ = mid.HandshakeContext(hctx)

	stop() // fails the test unless Start returns
	assert.ErrorIs(t, src.Health(ctx), errStopped)
	waitEOF(t, c)
	pc, err := net.ListenPacket("udp", addr)
	require.NoError(t, err, "the socket is closed")
	require.NoError(t, pc.Close())
}

// A source whose certificate pion/dtls cannot use fails to start, and says
// why.
func TestDTLSSourceFailsWithoutAUsableCertificate(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS})
	src.opts.tls = &tls.Config{Certificates: []tls.Certificate{{}}}
	err := src.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen 127.0.0.1:0: dtls:")
	assert.Equal(t, err, src.Health(context.Background()))
	assert.Empty(t, src.Addr(), "it never listened")
}
