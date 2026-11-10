package tcp

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// wire encodes messages with a framing, back to back.
func wire(t *testing.T, f framing, messages ...string) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, m := range messages {
		frame, err := f.encode([]byte(m))
		require.NoError(t, err)
		b.Write(frame)
	}
	return b.Bytes()
}

func TestTCPSourceFraming(t *testing.T) {
	cases := []struct {
		name        string
		frame       string
		lengthBytes int
		send        func(t *testing.T) []byte
		want        []string
		types       []string
	}{
		{
			name:  "newline",
			frame: "newline",
			send:  func(*testing.T) []byte { return []byte("plain\r\n{\"b\":1}\n\n\xff\xfe\nlast\n") },
			want:  []string{"plain", `{"b":1}`, "\xff\xfe", "last"},
			types: []string{contentText, contentJSON, contentBinary, contentText},
		},
		{
			name:  "length 1",
			frame: "length", lengthBytes: 1,
			send: func(t *testing.T) []byte {
				return wire(t, mustFraming(t, "length", 1), "a\nb", `{"c":[1,2]}`, "\x00\x01")
			},
			want:  []string{"a\nb", `{"c":[1,2]}`, "\x00\x01"},
			types: []string{contentText, contentJSON, contentText},
		},
		{
			name:  "length 4",
			frame: "length",
			send: func(t *testing.T) []byte {
				return wire(t, mustFraming(t, "length", 4), strings.Repeat("x", 70000), "y")
			},
			want:  []string{strings.Repeat("x", 70000), "y"},
			types: []string{contentText, contentText},
		},
		{
			name:  "length 8",
			frame: "length", lengthBytes: 8,
			send:  func(t *testing.T) []byte { return wire(t, mustFraming(t, "length", 8), "42") },
			want:  []string{"42"},
			types: []string{contentJSON},
		},
		{
			name:  "raw",
			frame: "raw",
			send:  func(*testing.T) []byte { return []byte("the whole\nstream\n") },
			want:  []string{"the whole\nstream\n"},
			types: []string{contentText},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestSource(t, kindTCP, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, s)
			c := dial(t, listening(t, s))
			_, err := c.Write(tc.send(t))
			require.NoError(t, err)
			require.NoError(t, c.CloseWrite()) // raw: the end of the stream ends the message

			var connID any
			for i, want := range tc.want {
				env := recv(t, out)
				assert.Equal(t, want, env.BodyText())
				assert.Equal(t, tc.types[i], env.ContentType, "content type of %q", want)
				assert.Equal(t, "in", env.Source)
				assert.Equal(t, kindTCP, env.Protocol)
				assert.Empty(t, env.Subject)
				assert.Equal(t, c.LocalAddr().String(), env.Attributes["tcp.remote_addr"])
				assert.NotEmpty(t, env.Attributes["tcp.conn_id"])
				assert.Equal(t, env.Attributes["tcp.conn_id"], env.Attributes["ordering_key"])
				if i == 0 {
					connID = env.Attributes["tcp.conn_id"]
				}
				assert.Equal(t, connID, env.Attributes["tcp.conn_id"], "one id per connection")
			}
			waitClosed(t, c)
			noEnvelope(t, out, 50*time.Millisecond)
		})
	}
}

func TestTCPSourceConnectionsHaveTheirOwnID(t *testing.T) {
	s := newTestSource(t, kindTCP, config.Source{})
	out, _ := run(t, s)
	addr := listening(t, s)
	ids := map[any]bool{}
	for i := range 3 {
		c := dial(t, addr)
		_, err := fmt.Fprintf(c, "message %d\n", i)
		require.NoError(t, err)
		env := recv(t, out)
		assert.Equal(t, fmt.Sprintf("message %d", i), env.BodyText())
		ids[env.Attributes["ordering_key"]] = true
	}
	assert.Len(t, ids, 3)
}

func TestTCPSourceClosesTheConnectionOnAFrameOverMaxBodyBytes(t *testing.T) {
	for _, tc := range []struct {
		frame string
		send  []byte
	}{
		{"newline", []byte("ok\nthis line is too long\nnever\n")},
		{"length", append(wire(t, mustFraming(t, "length", 4), "ok"), wire(t, mustFraming(t, "length", 4), "this frame is too long", "never")...)},
	} {
		t.Run(tc.frame, func(t *testing.T) {
			s, logs := newLoggedSource(t, kindTCP, config.Source{Frame: tc.frame, MaxBodyBytes: 8})
			out, _ := run(t, s)
			c := dial(t, listening(t, s))
			_, err := c.Write(tc.send)
			require.NoError(t, err)

			assert.Equal(t, "ok", recv(t, out).BodyText())
			waitClosed(t, c)
			noEnvelope(t, out, 50*time.Millisecond)
			const warning = "tcp frame over max_body_bytes; closing the connection"
			require.Eventually(t, func() bool { return logs.count(slog.LevelWarn, warning) == 1 }, 2*time.Second, 5*time.Millisecond)
			remote, ok := logs.attr(warning, "remote_addr")
			require.True(t, ok)
			assert.Equal(t, c.LocalAddr().String(), remote.String())
		})
	}
}

func TestTCPSourceRawStreamOverMaxBodyBytes(t *testing.T) {
	s, logs := newLoggedSource(t, kindTCP, config.Source{Frame: "raw", MaxBodyBytes: 4})
	out, _ := run(t, s)
	c := dial(t, listening(t, s))
	_, err := c.Write([]byte("12345"))
	require.NoError(t, err)
	waitClosed(t, c)
	noEnvelope(t, out, 50*time.Millisecond)
	assert.Equal(t, 1, logs.count(slog.LevelWarn, "tcp frame over max_body_bytes; closing the connection"))
}

// A stream that ends inside a frame, such as a sender cut off in the middle
// of a write, loses that frame rather than passing it on as a whole message.
func TestTCPSourceDropsATruncatedFrame(t *testing.T) {
	for _, tc := range []struct {
		frame       string
		lengthBytes int
		send        string
	}{
		{"newline", 0, "a\n{\"cut\":"},
		{"length", 2, "\x00\x01a\x00\x09abc"},
	} {
		t.Run(tc.frame, func(t *testing.T) {
			s, logs := newLoggedSource(t, kindTCP, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes})
			out, _ := run(t, s)
			c := dial(t, listening(t, s))
			_, err := c.Write([]byte(tc.send))
			require.NoError(t, err)
			require.NoError(t, c.CloseWrite())
			assert.Equal(t, "a", recv(t, out).BodyText())
			waitClosed(t, c)
			noEnvelope(t, out, 50*time.Millisecond)
			assert.Equal(t, 1, logs.count(slog.LevelWarn, "tcp connection ended inside a frame; the partial frame is dropped"))
		})
	}
}

// A raw stream the sender reset, rather than closed, is not a message: the
// sender's message was cut short.
func TestTCPSourceRawIgnoresAResetStream(t *testing.T) {
	s := newTestSource(t, kindTCP, config.Source{Frame: "raw"})
	out, _ := run(t, s)
	c := dial(t, listening(t, s))
	_, err := c.Write([]byte("the first half of"))
	require.NoError(t, err)
	abort(c)
	noEnvelope(t, out, 100*time.Millisecond)
}

// While the relay does not take a message, the connection is not read, so
// the client's writes block: TCP flow control is the backpressure. Nothing
// is lost meanwhile.
func TestTCPSourceBackpressure(t *testing.T) {
	s := newTestSource(t, kindTCP, config.Source{})
	out := make(chan *envelope.Envelope) // nobody takes anything yet
	runOut(t, s, out)
	c := dial(t, listening(t, s))
	require.NoError(t, c.SetWriteBuffer(16<<10))

	line := func(i int) []byte { return fmt.Appendf(nil, "%08d %s\n", i, strings.Repeat("x", 1000)) }
	const most = 64 << 10 // 64MiB of lines: far more than any socket buffers
	written := 0
	for ; written < most; written++ {
		require.NoError(t, c.SetWriteDeadline(time.Now().Add(300*time.Millisecond)))
		if _, err := c.Write(line(written)); err != nil {
			require.True(t, isTimeout(err), "the write failed: %v", err)
			break
		}
	}
	require.Less(t, written, most, "the client was never held back")

	// Every line written in full arrives, in order.
	for i := range written {
		env := recv(t, out)
		require.Equal(t, string(bytes.TrimSuffix(line(i), []byte("\n"))), env.BodyText())
	}
}

func TestTCPSourceClosesIdleConnections(t *testing.T) {
	s := newTestSource(t, kindTCP, config.Source{})
	s.idle = 600 * time.Millisecond
	out, _ := run(t, s)
	c := dial(t, listening(t, s))

	// Sending keeps it open past the idle time.
	for i := range 10 {
		time.Sleep(s.idle / 6)
		_, err := fmt.Fprintf(c, "%d\n", i)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprint(i), recv(t, out).BodyText())
	}
	start := time.Now()
	waitClosed(t, c)
	assert.GreaterOrEqual(t, time.Since(start), s.idle/2, "not before it idles")
}

func TestTCPSourceIPAllow(t *testing.T) {
	allowed, logs := newLoggedSource(t, kindTCP, config.Source{Auth: &config.Auth{IPAllow: []string{"10.0.0.0/8", "127.0.0.1"}}})
	out, _ := run(t, allowed)
	c := dial(t, listening(t, allowed))
	_, err := c.Write([]byte("let in\n"))
	require.NoError(t, err)
	assert.Equal(t, "let in", recv(t, out).BodyText())
	assert.Zero(t, logs.count(slog.LevelDebug, "tcp connection refused: the client is not in auth.ip_allow"))

	refused, logs := newLoggedSource(t, kindTCP, config.Source{Auth: &config.Auth{IPAllow: []string{"10.0.0.0/8", "::1"}}})
	out, _ = run(t, refused)
	c = dial(t, listening(t, refused))
	_, _ = c.Write([]byte("kept out\n"))
	waitClosed(t, c)
	noEnvelope(t, out, 50*time.Millisecond)
	assert.Equal(t, 1, logs.count(slog.LevelDebug, "tcp connection refused: the client is not in auth.ip_allow"))
}

func TestTCPSourceStopsWhenTheContextEnds(t *testing.T) {
	s := newTestSource(t, kindTCP, config.Source{})
	ctx := context.Background()
	assert.ErrorIs(t, s.Health(ctx), errNotStarted)
	out, stop := run(t, s)
	addr := listening(t, s)
	require.NoError(t, s.Health(ctx))

	c := dial(t, addr)
	_, err := c.Write([]byte("before\n"))
	require.NoError(t, err)
	assert.Equal(t, "before", recv(t, out).BodyText())

	stop() // checks that Start returned nil
	waitClosed(t, c)
	assert.ErrorIs(t, s.Health(ctx), errStopped)
	_, err = net.DialTimeout("tcp", addr, time.Second)
	assert.Error(t, err, "the listener is closed")
}

// A message the relay could not take before shutdown is kept when the
// queue has room by then, and is lost (and logged) when it does not.
func TestHandOverAtShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	roomy := make(chan *envelope.Envelope, 1)
	assert.True(t, handOver(ctx, roomy, msg("kept")))
	assert.Len(t, roomy, 1)
	assert.False(t, handOver(ctx, roomy, msg("lost")), "the queue is full")

	s, logs := newLoggedSource(t, kindTCP, config.Source{})
	queue := make(chan *envelope.Envelope, 1)
	stop := runOut(t, s, queue)
	c := dial(t, listening(t, s))
	_, err := c.Write([]byte("first\nstuck\n"))
	require.NoError(t, err)
	// "first" fills the queue; "stuck", already read, waits for room.
	require.Eventually(t, func() bool { return len(queue) == 1 }, 2*time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	stop()
	assert.Equal(t, "first", (<-queue).BodyText())
	assert.Equal(t, 1, logs.count(slog.LevelWarn, "tcp source stopped before the relay took a message; it is lost"))
}

func TestTCPSourceFailsWhenItCannotListen(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = taken.Close() }()
	s := newTestSource(t, kindTCP, config.Source{Listen: taken.Addr().String()})
	err = s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen "+taken.Addr().String())
	assert.Equal(t, err, s.Health(context.Background()))
	assert.EqualError(t, s.Start(context.Background(), nil), "already started")
}
