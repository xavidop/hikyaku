package tcp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// udpClient sends datagrams to addr from a socket of its own.
func udpClient(t *testing.T, addr string) *net.UDPConn {
	t.Helper()
	c, err := net.Dial("udp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	uc, ok := c.(*net.UDPConn)
	require.True(t, ok)
	return uc
}

func send(t *testing.T, c *net.UDPConn, datagrams ...[]byte) {
	t.Helper()
	for _, d := range datagrams {
		_, err := c.Write(d)
		require.NoError(t, err)
	}
}

func TestUDPSourceFramesPerDatagram(t *testing.T) {
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		datagrams   func(t *testing.T) [][]byte
		want        []string
	}{
		{
			name:      "newline: several lines per datagram",
			frame:     "newline",
			datagrams: func(*testing.T) [][]byte { return [][]byte{[]byte("a\nb\r\n\nc"), []byte("d\n")} },
			want:      []string{"a", "b", "c", "d"},
		},
		{
			name:  "length: several frames per datagram",
			frame: "length", lengthBytes: 2,
			datagrams: func(t *testing.T) [][]byte {
				return [][]byte{wire(t, mustFraming(t, "length", 2), "x", `{"y":1}`), wire(t, mustFraming(t, "length", 2), "z")}
			},
			want: []string{"x", `{"y":1}`, "z"},
		},
		{
			name:      "raw: one message per datagram",
			frame:     "raw",
			datagrams: func(*testing.T) [][]byte { return [][]byte{[]byte("a\nb"), {}, []byte("c")} },
			want:      []string{"a\nb", "c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSource(t, kindUDP, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, s)
			c := udpClient(t, listening(t, s))
			send(t, c, tc.datagrams(t)...)
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

func TestUDPSourceContentTypes(t *testing.T) {
	s := newTestSource(t, kindUDP, config.Source{Frame: "raw"})
	out, _ := run(t, s)
	c := udpClient(t, listening(t, s))
	send(t, c, []byte(`{"a":1}`), []byte("text"), []byte{0xff, 0x00})
	assert.Equal(t, contentJSON, recv(t, out).ContentType)
	assert.Equal(t, contentText, recv(t, out).ContentType)
	assert.Equal(t, contentBinary, recv(t, out).ContentType)
}

// UDP cannot hold its senders back: what the full queue cannot take is
// dropped, counted, and logged at most once per logEvery.
func TestUDPSourceDropsWhenTheQueueIsFull(t *testing.T) {
	s, logs := newLoggedSource(t, kindUDP, config.Source{})
	queue := make(chan *envelope.Envelope, 1)
	runOut(t, s, queue)
	c := udpClient(t, listening(t, s))

	send(t, c, []byte("kept\n"))
	require.Eventually(t, func() bool { return len(queue) == 1 }, 2*time.Second, time.Millisecond)
	send(t, c, []byte("dropped 1\ndropped 2\n"), []byte("dropped 3\n"))
	require.Eventually(t, func() bool { return s.Dropped() == 3 }, 2*time.Second, time.Millisecond)

	const warning = "udp source dropped a message: the relay's queue is full"
	assert.Equal(t, 1, logs.count(slog.LevelWarn, warning), "one warning per logEvery")
	total, ok := logs.attr(warning, "dropped_total")
	require.True(t, ok)
	assert.Equal(t, int64(1), total.Int64())
	assert.Equal(t, "kept", (<-queue).BodyText())

	send(t, c, []byte("after\n"))
	assert.Equal(t, "after", recv(t, queue).BodyText(), "the queue has room again")
}

func TestUDPSourceDropsBadDatagrams(t *testing.T) {
	s, logs := newLoggedSource(t, kindUDP, config.Source{Frame: "length", LengthBytes: 1, MaxBodyBytes: 4})
	s.logEvery = 0 // every warning
	out, _ := run(t, s)
	c := udpClient(t, listening(t, s))
	send(t, c,
		[]byte("\x02ok\x05toolong\x01x"), // "ok", then a frame over max_body_bytes ends the datagram
		[]byte("\x01a\x03ab"),            // "a", then a truncated frame
		[]byte("\x04good"),
	)
	for _, want := range []string{"ok", "a", "good"} {
		assert.Equal(t, want, recv(t, out).BodyText())
	}
	noEnvelope(t, out, 50*time.Millisecond)
	assert.Equal(t, 2, logs.count(slog.LevelWarn, "udp datagram dropped: it does not hold whole frames within max_body_bytes"))
}

func TestUDPSourceIPAllow(t *testing.T) {
	s, logs := newLoggedSource(t, kindUDP, config.Source{Auth: &config.Auth{IPAllow: []string{"192.0.2.0/24"}}})
	out, _ := run(t, s)
	send(t, udpClient(t, listening(t, s)), []byte("kept out\n"))
	require.Eventually(t, func() bool {
		return logs.count(slog.LevelDebug, "udp datagram refused: the sender is not in auth.ip_allow") == 1
	}, 2*time.Second, time.Millisecond)
	noEnvelope(t, out, 50*time.Millisecond)

	s = newTestSource(t, kindUDP, config.Source{Auth: &config.Auth{IPAllow: []string{"127.0.0.0/8"}}})
	out, _ = run(t, s)
	send(t, udpClient(t, listening(t, s)), []byte("let in\n"))
	assert.Equal(t, "let in", recv(t, out).BodyText())
}

func TestUDPSourceStopsWhenTheContextEnds(t *testing.T) {
	s := newTestSource(t, kindUDP, config.Source{})
	ctx := context.Background()
	assert.ErrorIs(t, s.Health(ctx), errNotStarted)
	_, stop := run(t, s)
	listening(t, s)
	require.NoError(t, s.Health(ctx))
	stop()
	assert.ErrorIs(t, s.Health(ctx), errStopped)
}

func TestUDPSourceFailsWhenItCannotListen(t *testing.T) {
	taken, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = taken.Close() }()
	s := newTestSource(t, kindUDP, config.Source{Listen: taken.LocalAddr().String()})
	err = s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Equal(t, err, s.Health(context.Background()))
}

func TestUDPSinkToUDPSource(t *testing.T) {
	for _, tc := range []struct {
		frame       string
		lengthBytes int
	}{{"newline", 0}, {"length", 2}, {"raw", 0}} {
		t.Run(tc.frame, func(t *testing.T) {
			src := newTestSource(t, kindUDP, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, src)
			sink := newTestSink(t, config.Endpoint{URL: "udp://" + listening(t, src), Frame: tc.frame, LengthBytes: tc.lengthBytes})
			for _, body := range []string{"one", `{"two":2}`} {
				res, err := sink.Deliver(context.Background(), msg(body))
				require.NoError(t, err)
				assert.Equal(t, "udp://"+src.Addr(), res.Endpoint)
				assert.Equal(t, body, recv(t, out).BodyText())
			}
		})
	}
}

func TestUDPSinkErrors(t *testing.T) {
	sink := newTestSink(t, config.Endpoint{URL: "udp://127.0.0.1:9", Frame: "length", LengthBytes: 1})
	err := deliver(sink, strings.Repeat("x", 256))
	assertClass(t, err, plugin.Permanent, "udp:frame_too_large")

	raw := newTestSink(t, config.Endpoint{URL: "udp://127.0.0.1:9", Frame: "raw"})
	err = deliver(raw, strings.Repeat("x", 70000)) // over any datagram
	assertClass(t, err, plugin.Permanent, "udp:frame_too_large")
	assert.Contains(t, err.Error(), "write to udp://127.0.0.1:9")
}

func TestUDPSinkHealthResolves(t *testing.T) {
	ip := newTestSink(t, config.Endpoint{URL: "udp://127.0.0.1:9"})
	assert.NoError(t, ip.Health(context.Background()))

	named := newTestSink(t, config.Endpoint{URL: "udp://relay.example:9"})
	named.resolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("no DNS here")
	}}
	err := named.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve relay.example")

	require.NoError(t, ip.Close(context.Background()))
	assert.Error(t, ip.Health(context.Background()), "closed")
	assert.Error(t, deliver(ip, "x"), "closed")
}
