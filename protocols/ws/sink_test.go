package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

type frame struct {
	conn int
	typ  websocket.MessageType
	data string
}

func newTestSink(t *testing.T, ep config.Endpoint) *Sink {
	t.Helper()
	if ep.Type == "" {
		ep.Type = "ws"
	}
	s, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: ep.Type, Config: &ep})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// recorder is a server that hands every frame it reads to got; after
// reads, connection 1 closes with status (0 keeps reading).
func recorder(t *testing.T, got chan<- frame, closeAfter int, status websocket.StatusCode) string {
	t.Helper()
	url, _ := wsServer(t, func(n int, c *websocket.Conn, _ *http.Request) {
		for i := 1; ; i++ {
			typ, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			got <- frame{n, typ, string(data)}
			if n == 1 && i == closeAfter {
				_ = c.Close(status, "boom")
				return
			}
		}
	})
	return url
}

func next(t *testing.T, got <-chan frame) frame {
	t.Helper()
	select {
	case f := <-got:
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("the server got no frame")
		return frame{}
	}
}

// ended reports whether the sink's connection has stopped reading.
func ended(s *Sink) bool {
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

func msg(body string) *envelope.Envelope { return envelope.New("in", "http", []byte(body)) }

func TestSinkWritesFramesOnOneConnection(t *testing.T) {
	got := make(chan frame, 8)
	headers := make(chan http.Header, 4)
	url, conns := wsServer(t, func(n int, c *websocket.Conn, r *http.Request) {
		headers <- r.Header.Clone()
		for {
			typ, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			got <- frame{n, typ, string(data)}
		}
	})
	ctx := context.Background()

	text := newTestSink(t, config.Endpoint{URL: url + "/ingest", Headers: map[string]string{"X-Api-Key": "k1"}})
	for _, body := range []string{`{"n":1}`, "two"} {
		res, err := text.Deliver(ctx, msg(body))
		require.NoError(t, err)
		assert.Equal(t, url+"/ingest", res.Endpoint)
		assert.Equal(t, frame{1, websocket.MessageText, body}, next(t, got))
	}
	assert.Equal(t, int32(1), conns.Load(), "the connection persists")
	assert.Equal(t, "k1", (<-headers).Get("X-Api-Key"))

	bin := newTestSink(t, config.Endpoint{Type: "wss", URL: url, Frame: "binary"})
	_, err := bin.Deliver(ctx, msg("\x00\x01"))
	require.NoError(t, err)
	assert.Equal(t, frame{2, websocket.MessageBinary, "\x00\x01"}, next(t, got))
}

func TestSinkPeerCloseStatusThenRedial(t *testing.T) {
	got := make(chan frame, 8)
	s := newTestSink(t, config.Endpoint{URL: recorder(t, got, 1, websocket.StatusInternalError)})
	ctx := context.Background()

	_, err := s.Deliver(ctx, msg("one"))
	require.NoError(t, err)
	assert.Equal(t, "one", next(t, got).data)
	require.Eventually(t, func() bool { return ended(s) }, 2*time.Second, 2*time.Millisecond)

	_, err = s.Deliver(ctx, msg("two"))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{"ws:close_1011"}, plugin.Tags(err), "not connect: the default policy leaves it to retry_on")
	assert.Contains(t, err.Error(), "1011")

	_, err = s.Deliver(ctx, msg("three"))
	require.NoError(t, err, "the delivery after a failure redials")
	assert.Equal(t, frame{2, websocket.MessageText, "three"}, next(t, got))
}

func TestSinkNormalCloseRedialsSilently(t *testing.T) {
	got := make(chan frame, 8)
	s := newTestSink(t, config.Endpoint{URL: recorder(t, got, 1, websocket.StatusGoingAway)})
	ctx := context.Background()
	_, err := s.Deliver(ctx, msg("one"))
	require.NoError(t, err)
	next(t, got)
	require.Eventually(t, func() bool { return ended(s) }, 2*time.Second, 2*time.Millisecond)
	_, err = s.Deliver(ctx, msg("two"))
	require.NoError(t, err, "a server going away between messages is no delivery failure")
	assert.Equal(t, frame{2, websocket.MessageText, "two"}, next(t, got))
}

func TestSinkDialFailureIsConnect(t *testing.T) {
	s := newTestSink(t, config.Endpoint{URL: "ws://" + closedAddr(t) + "/"})
	_, err := s.Deliver(context.Background(), msg("x"))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))
	assert.Error(t, s.Health(context.Background()))
}

func TestSinkHandshakeStatus(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(srv.Close)
	s := newTestSink(t, config.Endpoint{URL: "ws" + strings.TrimPrefix(srv.URL, "http")})

	_, err := s.Deliver(context.Background(), msg("x"))
	class, retryAfter := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, 7*time.Second, retryAfter)
	assert.ElementsMatch(t, []string{"503", "5xx"}, plugin.Tags(err))

	status.Store(http.StatusUnauthorized)
	_, err = s.Deliver(context.Background(), msg("x"))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a refused handshake will be refused again")
}

func TestSinkTimeout(t *testing.T) {
	got := make(chan frame, 8)
	s := newTestSink(t, config.Endpoint{URL: recorder(t, got, 0, 0)})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := s.Deliver(ctx, msg("late"))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.Contains(t, plugin.Tags(err), plugin.TagTimeout)

	// A delivery waiting behind another honours its own deadline.
	require.NoError(t, s.acquire(context.Background()))
	short, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	_, err = s.Deliver(short, msg("queued"))
	s.release()
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)

	_, err = s.Deliver(context.Background(), msg("fine"))
	require.NoError(t, err)
	assert.Equal(t, "fine", next(t, got).data)
}

func TestSinkHealthDialsAndCloseSaysGoodbye(t *testing.T) {
	closed := make(chan error, 1)
	url, conns := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		_, _, err := c.Read(context.Background())
		closed <- err
	})
	s := newTestSink(t, config.Endpoint{URL: url})
	require.NoError(t, s.Health(context.Background()))
	require.NoError(t, s.Health(context.Background()))
	assert.Equal(t, int32(1), conns.Load(), "health dials once and keeps the connection")

	require.NoError(t, s.Close(context.Background()))
	select {
	case err := <-closed:
		assert.Equal(t, websocket.StatusNormalClosure, websocket.CloseStatus(err))
	case <-time.After(3 * time.Second):
		t.Fatal("the server never saw the close")
	}
	_, err := s.Deliver(context.Background(), msg("x"))
	assert.ErrorIs(t, err, errClosed)
	assert.ErrorIs(t, s.Health(context.Background()), errClosed)
	assert.NoError(t, s.Close(context.Background()), "closing twice is fine")
}

func TestSinkAnswersPeerPings(t *testing.T) {
	pong := make(chan error, 1)
	url, _ := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		done := make(chan struct{})
		go func() { // a pending read is what receives the pong
			defer close(done)
			_, _, _ = c.Read(context.Background())
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		pong <- c.Ping(ctx)
		<-done
	})
	s := newTestSink(t, config.Endpoint{URL: url})
	require.NoError(t, s.Health(context.Background()))
	select {
	case err := <-pong:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("no pong")
	}
}
