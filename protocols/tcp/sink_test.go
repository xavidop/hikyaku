package tcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pion/dtls/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestTCPSinkWritesFrames(t *testing.T) {
	for _, tc := range []struct {
		frame       string
		lengthBytes int
		conns       int64 // connections the peer accepts for three messages
	}{
		{"newline", 0, 1},
		{"length", 2, 1},
		{"raw", 0, 3}, // one connection per message
	} {
		t.Run(tc.frame, func(t *testing.T) {
			p := newPeer(t, mustFraming(t, tc.frame, tc.lengthBytes))
			s := newTestSink(t, config.Endpoint{URL: p.url(), Frame: tc.frame, LengthBytes: tc.lengthBytes})
			for _, body := range []string{"one", `{"two":2}`, "three"} {
				res, err := s.Deliver(context.Background(), msg(body))
				require.NoError(t, err)
				assert.Equal(t, "tcp://"+p.addr(), res.Endpoint)
				assert.Equal(t, body, p.next(t))
			}
			assert.Equal(t, tc.conns, p.accepted.Load())
		})
	}
}

// wireBytes collects what a destination writes, until it closes.
func wireBytes(t *testing.T, p *peer) <-chan []byte {
	t.Helper()
	got := make(chan []byte, 1)
	p.serve(func(c net.Conn) {
		b, _ := io.ReadAll(c)
		got <- b
	})
	return got
}

func TestTCPSinkWireFormat(t *testing.T) {
	for _, tc := range []struct {
		name        string
		frame       string
		lengthBytes int
		bodies      []string
		want        string
	}{
		{"newline", "newline", 0, []string{"a", "b\n", "{\n  \"c\": 1\n}"}, "a\nb\n{\"c\":1}\n"},
		{"length 1", "length", 1, []string{"hi", "a\nb"}, "\x02hi\x03a\nb"},
		{"length 8", "length", 8, []string{"hi"}, "\x00\x00\x00\x00\x00\x00\x00\x02hi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPeer(t, framing{})
			got := wireBytes(t, p)
			s := newTestSink(t, config.Endpoint{URL: p.url(), Frame: tc.frame, LengthBytes: tc.lengthBytes})
			for _, b := range tc.bodies {
				require.NoError(t, deliver(s, b))
			}
			require.NoError(t, s.Close(context.Background()))
			assert.Equal(t, tc.want, string(<-got))
		})
	}
}

func TestTCPSinkBodyTemplate(t *testing.T) {
	p := newPeer(t, framing{})
	s := newTestSink(t, config.Endpoint{URL: p.url(), Body: "{{ .json.name }} via {{ .source }}"})
	_, err := s.Deliver(context.Background(), msg(`{"name":"ada"}`))
	require.NoError(t, err)
	assert.Equal(t, "ada via in", p.next(t))
}

// sinkConnEnded reports whether the destination saw its connection end.
func sinkConnEnded(s *Sink) bool {
	s.mu.Lock()
	sc := s.conn
	s.mu.Unlock()
	if sc == nil {
		return false
	}
	select {
	case <-sc.done:
		return true
	default:
		return false
	}
}

func TestTCPSinkRedialsAfterThePeerCloses(t *testing.T) {
	p := newPeer(t, framing{})
	p.serve(func(c net.Conn) { // one message per connection, then hang up
		p.read(&oneLine{Conn: c})
	})
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	require.NoError(t, deliver(s, "a"))
	assert.Equal(t, "a", p.next(t))
	require.Eventually(t, func() bool { return sinkConnEnded(s) }, 2*time.Second, time.Millisecond,
		"the destination notices the peer hung up")
	require.NoError(t, deliver(s, "b"), "the next delivery redials instead of writing into the closed connection")
	assert.Equal(t, "b", p.next(t))
	assert.Equal(t, int64(2), p.accepted.Load())
}

// oneLine reads up to the first newline, then reports EOF.
type oneLine struct {
	net.Conn
	done bool
}

func (o *oneLine) Read(b []byte) (int, error) {
	if o.done {
		return 0, io.EOF
	}
	n, err := o.Conn.Read(b[:1])
	if n == 1 && b[0] == '\n' {
		o.done = true
	}
	return n, err
}

func TestTCPSinkDropsAConnectionAWriteFailedOn(t *testing.T) {
	p := newPeer(t, framing{})
	reset := make(chan struct{})
	p.serve(func(c net.Conn) { // reset the connection when told
		<-reset
		abort(c)
	})
	s := newTestSink(t, config.Endpoint{URL: p.url()})

	// A connection the peer reset, whose reader has not noticed yet. The
	// reset waits for the dial: one that races it fails the dial instead.
	c, err := net.Dial("tcp", p.addr())
	require.NoError(t, err)
	close(reset)
	waitClosed(t, c)
	s.conn = &sinkConn{c: c, done: make(chan struct{})}
	p.serve(nil) // later connections are read

	err = deliver(s, "lost")
	assertClass(t, err, plugin.Transient, plugin.TagConnect)
	tags := plugin.Tags(err)
	assert.True(t, slices.Contains(tags, "tcp:EPIPE") || slices.Contains(tags, "tcp:ECONNRESET"), "tags %v", tags)
	assert.Nil(t, s.conn, "the broken connection is dropped")

	require.NoError(t, deliver(s, "redialed"))
	assert.Equal(t, "redialed", p.next(t))
}

func TestTCPSinkDialRefused(t *testing.T) {
	s := newTestSink(t, config.Endpoint{URL: "tcp://" + closedAddr(t)})
	err := deliver(s, "x")
	assertClass(t, err, plugin.Transient, plugin.TagConnect, "tcp:ECONNREFUSED")
	assert.Contains(t, err.Error(), "dial tcp://")

	raw := newTestSink(t, config.Endpoint{URL: "tcp://" + closedAddr(t), Frame: "raw"})
	assertClass(t, deliver(raw, "x"), plugin.Transient, plugin.TagConnect)
}

// A peer that stops reading makes a write run out of time: a timeout, and
// the connection, which may hold part of a frame, is dropped.
func TestTCPSinkWriteTimeout(t *testing.T) {
	p := newPeer(t, framing{})
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	p.serve(func(net.Conn) { <-hold }) // never reads
	s := newTestSink(t, config.Endpoint{URL: p.url()})

	big := strings.Repeat("x", 1<<20)
	var err error
	for range 128 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err = s.Deliver(ctx, msg(big))
		cancel()
		if err != nil {
			break
		}
	}
	assertClass(t, err, plugin.Timeout, plugin.TagTimeout)
	assert.Nil(t, s.conn, "a connection a write timed out on is dropped")

	p.serve(nil)
	require.NoError(t, deliver(s, "fresh"))
	assert.Equal(t, "fresh", p.next(t))
	assert.Equal(t, int64(2), p.accepted.Load())
}

// With raw framing the end of the stream ends the message, so a message cut
// short by a failed write is reset, never closed: the peer must not take
// the part written as the whole message.
func TestTCPSinkRawResetsAMessageCutShort(t *testing.T) {
	p := newPeer(t, framing{})
	release := make(chan struct{})
	read := make(chan error, 1)
	p.serve(func(c net.Conn) {
		<-release // read nothing until the write gave up
		_, err := io.ReadAll(c)
		read <- err
	})
	s := newTestSink(t, config.Endpoint{URL: p.url(), Frame: "raw"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := s.Deliver(ctx, envelope.New("in", "test", bytes.Repeat([]byte("x"), 32<<20)))
	assertClass(t, err, plugin.Timeout, plugin.TagTimeout)
	close(release)
	assert.Error(t, <-read, "the peer sees a reset, not the end of a whole stream")
}

func TestTCPSinkPermanentFailuresDoNotDial(t *testing.T) {
	p := newPeer(t, framing{})
	short := newTestSink(t, config.Endpoint{URL: p.url(), Frame: "length", LengthBytes: 1})
	err := deliver(short, strings.Repeat("x", 256))
	assertClass(t, err, plugin.Permanent, "tcp:frame_too_large")
	assert.Contains(t, err.Error(), "256 bytes do not fit a 1-byte length prefix")

	lines := newTestSink(t, config.Endpoint{URL: p.url()})
	assertClass(t, deliver(lines, "two\nlines"), plugin.Permanent, "tcp:newline_in_body")

	tmpl := newTestSink(t, config.Endpoint{URL: p.url(), Body: `{{ b64dec "%%%" }}`})
	assertClass(t, deliver(tmpl, "x"), plugin.Permanent, "template")

	// No framing carries an empty message: a source would skip it.
	for _, frame := range []string{"newline", "length", "raw"} {
		empty := newTestSink(t, config.Endpoint{URL: p.url(), Frame: frame})
		assertClass(t, deliver(empty, ""), plugin.Permanent, "tcp:empty_body")
	}
	assertClass(t, deliver(lines, "\r\n"), plugin.Permanent, "tcp:empty_body")

	assert.Zero(t, p.accepted.Load())
}

func TestTCPSinkKeepsItsConnectionOnACanceledAttempt(t *testing.T) {
	p := newPeer(t, framing{})
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	require.NoError(t, deliver(s, "a"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Deliver(ctx, msg("never"))
	assertClass(t, err, plugin.Transient)
	require.NoError(t, deliver(s, "b"))
	assert.Equal(t, "a", p.next(t))
	assert.Equal(t, "b", p.next(t))
	assert.Equal(t, int64(1), p.accepted.Load())
}

func TestTCPSinkWaitsForItsTurnWithinTheAttempt(t *testing.T) {
	p := newPeer(t, framing{})
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	require.NoError(t, s.acquire(context.Background())) // a delivery in progress
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.Deliver(ctx, msg("x"))
	s.release()
	assertClass(t, err, plugin.Timeout, plugin.TagTimeout)
}

func TestTCPSinkSerializesDeliveries(t *testing.T) {
	p := newPeer(t, mustFraming(t, "length", 4))
	s := newTestSink(t, config.Endpoint{URL: p.url(), Frame: "length"})
	const workers, each = 16, 25
	want := map[string]bool{}
	var wg sync.WaitGroup
	for w := range workers {
		for i := range each {
			body := fmt.Sprintf("%02d-%02d-%s", w, i, strings.Repeat("z", (w*each+i)*37%5000))
			want[body] = true
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				body := fmt.Sprintf("%02d-%02d-%s", w, i, strings.Repeat("z", (w*each+i)*37%5000))
				assert.NoError(t, deliver(s, body))
			}
		}()
	}
	wg.Wait()
	got := map[string]bool{}
	for range workers * each {
		got[p.next(t)] = true
	}
	assert.Equal(t, want, got, "every frame arrives whole")
	assert.Equal(t, int64(1), p.accepted.Load())
}

func TestTCPSinkHealth(t *testing.T) {
	ctx := context.Background()
	p := newPeer(t, framing{})
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	require.NoError(t, s.Health(ctx))
	require.NoError(t, deliver(s, "a"))
	assert.Equal(t, "a", p.next(t))
	assert.Equal(t, int64(1), p.accepted.Load(), "the health check's connection carries the deliveries")

	rp := newPeer(t, mustFraming(t, "raw", 0))
	raw := newTestSink(t, config.Endpoint{URL: rp.url(), Frame: "raw"})
	require.NoError(t, raw.Health(ctx))
	require.Eventually(t, func() bool { return rp.accepted.Load() == 1 }, 2*time.Second, time.Millisecond)
	require.NoError(t, deliver(raw, "b"))
	assert.Equal(t, "b", rp.next(t), "the health check's empty stream is no message")

	down := newTestSink(t, config.Endpoint{URL: "tcp://" + closedAddr(t)})
	assertClass(t, down.Health(ctx), plugin.Transient, plugin.TagConnect)
}

// A delivery can hold the connection for its whole timeout (a peer that
// reads slowly); a readiness probe meanwhile must not fail for waiting.
func TestTCPSinkHealthDoesNotWaitForADelivery(t *testing.T) {
	ctx := context.Background()
	p := newPeer(t, framing{})
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	s.healthTimeout = 100 * time.Millisecond

	require.NoError(t, s.acquire(ctx)) // a delivery dialing
	assert.NoError(t, s.Health(ctx), "probes with a connection of its own")
	assert.Nil(t, s.conn)
	s.release()

	require.NoError(t, deliver(s, "a"))
	assert.Equal(t, "a", p.next(t))
	require.NoError(t, s.acquire(ctx)) // a delivery writing on the live connection
	assert.NoError(t, s.Health(ctx))
	s.release()
	// The peer counts a connection when it accepts it, which may come after
	// the dial returned.
	require.Eventually(t, func() bool { return p.accepted.Load() == 2 }, 2*time.Second, time.Millisecond,
		"the probe and the persistent connection")
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int64(2), p.accepted.Load(), "the second Health used the live connection")
}

func TestTCPSinkClose(t *testing.T) {
	ctx := context.Background()
	p := newPeer(t, framing{})
	got := wireBytes(t, p)
	s := newTestSink(t, config.Endpoint{URL: p.url()})
	require.NoError(t, deliver(s, "last words"))
	require.NoError(t, s.Close(ctx))
	assert.Equal(t, "last words\n", string(<-got), "the peer read everything, then EOF")

	assertClass(t, deliver(s, "too late"), plugin.Transient)
	assert.Error(t, s.Health(ctx))
	assert.NoError(t, s.Close(ctx), "closing twice is fine")

	raw := newTestSink(t, config.Endpoint{URL: p.url(), Frame: "raw"})
	require.NoError(t, raw.Close(ctx))
	assertClass(t, deliver(raw, "too late"), plugin.Transient)
	assert.Equal(t, int64(1), p.accepted.Load())
}

func TestErrorClasses(t *testing.T) {
	ctx := context.Background()
	expired, cancel := context.WithTimeout(ctx, -time.Second)
	defer cancel()
	canceled, cancel2 := context.WithCancel(ctx)
	cancel2()
	op := func(errno syscall.Errno) error {
		return &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", errno)}
	}
	cases := []struct {
		name  string
		err   error
		class plugin.Class
		tags  []string
	}{
		{"dial refused", dialError(ctx, "tcp", "tcp://h:1", op(syscall.ECONNREFUSED)), plugin.Transient, []string{"connect", "tcp:ECONNREFUSED"}},
		{"dial unreachable", dialError(ctx, "tcp", "tcp://h:1", op(syscall.EHOSTUNREACH)), plugin.Transient, []string{"connect", "tcp:EHOSTUNREACH"}},
		{"dial past the deadline", dialError(expired, "tcp", "tcp://h:1", context.DeadlineExceeded), plugin.Timeout, []string{"timeout"}},
		{"dial canceled", dialError(canceled, "tcp", "tcp://h:1", context.Canceled), plugin.Transient, nil},
		{"broken pipe", writeError(ctx, "tcp", "tcp://h:1", op(syscall.EPIPE)), plugin.Transient, []string{"connect", "tcp:EPIPE"}},
		{"reset", writeError(ctx, "tcp", "tcp://h:1", op(syscall.ECONNRESET)), plugin.Transient, []string{"connect", "tcp:ECONNRESET"}},
		{"closed", writeError(ctx, "tcp", "tcp://h:1", net.ErrClosed), plugin.Transient, []string{"connect"}},
		{"write deadline", writeError(ctx, "tcp", "tcp://h:1", os.ErrDeadlineExceeded), plugin.Timeout, []string{"timeout"}},
		{"attempt deadline", writeError(expired, "tcp", "tcp://h:1", os.ErrDeadlineExceeded), plugin.Timeout, []string{"timeout"}},
		{"datagram too long", writeError(ctx, "udp", "udp://h:1", op(syscall.EMSGSIZE)), plugin.Permanent, []string{"udp:frame_too_large"}},
		{"udp refused", writeError(ctx, "udp", "udp://h:1", op(syscall.ECONNREFUSED)), plugin.Transient, []string{"connect", "udp:ECONNREFUSED"}},
		{"frame too large", encodeError("tcp", "tcp://h:1", errFrameTooLarge), plugin.Permanent, []string{"tcp:frame_too_large"}},
		{"newline in body", encodeError("udp", "udp://h:1", errNewlineInBody), plugin.Permanent, []string{"udp:newline_in_body"}},
		{"empty body", encodeError("tcp", "tcp://h:1", errEmptyBody), plugin.Permanent, []string{"tcp:empty_body"}},
		// DTLS failures carry the udp kind.
		{"dtls dial refused", dialError(ctx, "udp", "dtls://h:1", fmt.Errorf("handshake failed: %w", op(syscall.ECONNREFUSED))), plugin.Transient, []string{"connect", "udp:ECONNREFUSED"}},
		{"dtls handshake alert", dialError(ctx, "udp", "dtls://h:1", errors.New("handshake failed: alert: Alert Fatal: BadCertificate")), plugin.Transient, []string{"connect"}},
		{"dtls handshake past the deadline", dialError(expired, "udp", "dtls://h:1", context.DeadlineExceeded), plugin.Timeout, []string{"timeout"}},
		{"dtls write deadline", writeError(ctx, "udp", "dtls://h:1", context.DeadlineExceeded), plugin.Timeout, []string{"timeout"}},
		{"dtls association closed", writeError(ctx, "udp", "dtls://h:1", dtls.ErrConnClosed), plugin.Transient, []string{"connect"}},
		{"dtls record too large", encodeError("udp", "dtls://h:1", fmt.Errorf("%w: 16385 bytes do not fit one DTLS record", errFrameTooLarge)), plugin.Permanent, []string{"udp:frame_too_large"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class, _ := plugin.Classify(tc.err)
			assert.Equal(t, tc.class, class)
			assert.ElementsMatch(t, tc.tags, plugin.Tags(tc.err))
		})
	}
}
