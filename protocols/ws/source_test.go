package ws

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func newTestSource(t *testing.T, kind string, cfg config.Source) *Source {
	t.Helper()
	cfg.Name, cfg.Type = "in", kind
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg})
	require.NoError(t, err)
	s.closeGrace = 200 * time.Millisecond
	return s
}

// run starts s. The returned stop (also run at cleanup) cancels it and
// checks that Start returned nil.
func run(t *testing.T, s *Source) (<-chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 64)
	return out, runOut(t, s, out)
}

func runOut(t *testing.T, s *Source, out chan *envelope.Envelope) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("Start did not return after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func listening(t *testing.T, s *Source) string {
	t.Helper()
	require.Eventually(t, func() bool { return s.Addr() != "" }, 2*time.Second, 2*time.Millisecond)
	return s.Addr()
}

func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no envelope arrived")
		return nil
	}
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// wsServer serves WebSocket connections with handle, numbering them from 1.
func wsServer(t *testing.T, handle func(n int, c *websocket.Conn, r *http.Request)) (string, *atomic.Int32) {
	t.Helper()
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		handle(int(conns.Add(1)), c, r)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), &conns
}

func TestRegister(t *testing.T) {
	Register()
	for _, kind := range []string{"ws", "wss"} {
		_, ok := plugin.LookupSource(kind)
		assert.True(t, ok, kind)
		_, ok = plugin.LookupSink(kind)
		assert.True(t, ok, kind)
	}
}

func TestListenStripsCredentials(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0", Path: "/live", Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t1"}}}})
	out, _ := run(t, s)
	addr := listening(t, s)
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, "ws://"+addr+"/live", &websocket.DialOptions{HTTPHeader: http.Header{
		"Authorization": {"Bearer t1"}, "Cookie": {"session=abc"}, "X-Api-Key": {"k"}, "X-Tenant": {"acme"},
	}})
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("hello")))
	env := recv(t, out)
	assert.Empty(t, env.Header("Authorization"), "the caller's credentials never reach a destination")
	assert.Empty(t, env.Header("Cookie"))
	assert.Empty(t, env.Header("X-Api-Key"))
	assert.Equal(t, "acme", env.Header("X-Tenant"))
	require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
}

func TestListenFramesBecomeEnvelopes(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0", Path: "/live"})
	out, _ := run(t, s)
	addr := listening(t, s)
	require.NoError(t, s.Health(context.Background()))

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, "ws://"+addr+"/live", &websocket.DialOptions{HTTPHeader: http.Header{"x-tenant": {"acme"}}})
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("hello")))
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte(`{"a":1}`)))
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, []byte{0, 1, 2}))

	text, js, bin := recv(t, out), recv(t, out), recv(t, out)
	assert.Equal(t, "hello", text.BodyText())
	assert.Equal(t, "text/plain; charset=utf-8", text.ContentType)
	assert.Equal(t, "text", text.Attributes["ws.frame"])
	assert.Equal(t, `{"a":1}`, js.BodyText())
	assert.Equal(t, "application/json", js.ContentType)
	assert.Equal(t, "text", js.Attributes["ws.frame"])
	assert.Equal(t, []byte{0, 1, 2}, bin.Body)
	assert.Equal(t, "application/octet-stream", bin.ContentType)
	assert.Equal(t, "binary", bin.Attributes["ws.frame"])
	for _, env := range []*envelope.Envelope{text, js, bin} {
		assert.Equal(t, "in", env.Source)
		assert.Equal(t, "ws", env.Protocol)
		assert.Equal(t, "/live", env.Subject)
		assert.Equal(t, []string{"acme"}, env.Headers["X-Tenant"], "handshake headers, canonical keys")
		assert.Contains(t, env.Attributes["remote_addr"], "127.0.0.1:")
		assert.NotContains(t, env.Attributes, "url")
	}
	text.Headers["X-Tenant"][0] = "changed"
	assert.Equal(t, "acme", js.Header("X-Tenant"), "every envelope has its own headers")
	require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
}

func TestListenBearerAuth(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0",
		Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"s3cret"}}}})
	out, _ := run(t, s)
	url := "ws://" + listening(t, s) + "/"
	ctx := context.Background()
	// Refused handshakes leave plain HTTP connections behind: keep none idle,
	// or http.Server.Shutdown waits up to 5s for a spare one at the end.
	httpc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	dial := func(auth string) (*websocket.Conn, *http.Response, error) {
		h := http.Header{}
		if auth != "" {
			h.Set("Authorization", auth)
		}
		return websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: httpc, HTTPHeader: h})
	}

	_, resp, err := dial("")
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_, resp, err = dial("Bearer nope")
	require.Error(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	c, _, err := dial("Bearer s3cret")
	require.NoError(t, err)
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("in")))
	assert.Equal(t, "in", recv(t, out).BodyText())
	require.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
}

func TestFactoryRejectsHMAC(t *testing.T) {
	Register()
	for _, mode := range []string{"listen", "subscribe"} {
		cfg := &config.Source{Name: "in", Type: "wss", Mode: mode, Listen: ":0", URL: "wss://x/y",
			Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "k"}}}
		_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: "wss", Config: cfg})
		assert.EqualError(t, err, "source in: auth.hmac applies to http sources: a WebSocket or SSE request carries no signed body", mode)
	}
}

func TestListenReadLimit(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0", MaxBodyBytes: 8})
	out, _ := run(t, s)
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, "ws://"+listening(t, s), nil)
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("12345678")))
	assert.Equal(t, "12345678", recv(t, out).BodyText(), "max_body_bytes itself is accepted")
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("123456789")))
	_, _, err = c.Read(ctx)
	assert.Equal(t, websocket.StatusMessageTooBig, websocket.CloseStatus(err))
}

func TestListenDropsPeerThatStopsAnsweringPings(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0"})
	s.pingEvery, s.pingTimeout = 150*time.Millisecond, 50*time.Millisecond
	run(t, s)
	// This client never reads, so it never answers a ping.
	c, _, err := websocket.Dial(context.Background(), "ws://"+listening(t, s), nil)
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	require.Eventually(t, func() bool { return s.live.Load() == 1 }, 2*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return s.live.Load() == 0 }, 2*time.Second, 5*time.Millisecond)
}

func TestListenBackpressureKeepsThePeer(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0"})
	s.pingEvery, s.pingTimeout = 20*time.Millisecond, 20*time.Millisecond
	out := make(chan *envelope.Envelope) // the relay takes nothing until the test reads
	runOut(t, s, out)
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, "ws://"+listening(t, s), nil)
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	dropped := make(chan error, 1)
	go func() { // answers pings, and returns if the source drops the connection
		_, _, err := c.Read(ctx)
		dropped <- err
	}()
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("one")))
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("two")))
	assert.Never(t, func() bool { return len(dropped) > 0 }, 200*time.Millisecond, 5*time.Millisecond,
		"a source held back by backpressure cannot read pongs, and must not drop the peer for it")
	assert.Equal(t, "one", recv(t, out).BodyText())
	assert.Equal(t, "two", recv(t, out).BodyText())
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte("three")))
	assert.Equal(t, "three", recv(t, out).BodyText())
}

func TestListenShutdownSaysGoingAway(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "127.0.0.1:0"})
	_, stop := run(t, s)
	c, _, err := websocket.Dial(context.Background(), "ws://"+listening(t, s), nil)
	require.NoError(t, err)
	defer func() { _ = c.CloseNow() }()
	read := make(chan error, 1)
	go func() {
		_, _, err := c.Read(context.Background())
		read <- err
	}()
	require.Eventually(t, func() bool { return s.live.Load() == 1 }, 2*time.Second, time.Millisecond)
	stop()
	select {
	case err := <-read:
		assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
	case <-time.After(3 * time.Second):
		t.Fatal("the client was not closed")
	}
	assert.ErrorIs(t, s.Health(context.Background()), errStopped)
}

func TestListenBindFailure(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{Listen: "256.0.0.1:1"})
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	assert.Error(t, err)
	assert.Error(t, s.Health(context.Background()))
	assert.EqualError(t, s.Start(context.Background(), nil), "already started")
}

func TestSubscribeReconnectsAfterClose(t *testing.T) {
	url, conns := wsServer(t, func(n int, c *websocket.Conn, _ *http.Request) {
		for i := 1; i <= 2; i++ {
			if err := c.Write(context.Background(), websocket.MessageText, fmt.Appendf(nil, "conn%d-msg%d", n, i)); err != nil {
				return
			}
		}
		_ = c.Close(websocket.StatusNormalClosure, "bye")
	})
	// max_attempts 1: a connection that delivered resets the failure count,
	// so the source never gives up here.
	s := newTestSource(t, "ws", config.Source{URL: url + "/feed",
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxBackoff: config.Duration(20 * time.Millisecond), MaxAttempts: 1}})
	require.Equal(t, modeSubscribe, s.opts.mode, "a url without listen subscribes")
	out, _ := run(t, s)

	var got []string
	for range 4 {
		env := recv(t, out)
		got = append(got, env.BodyText())
		assert.Equal(t, "/feed", env.Subject)
		assert.Equal(t, url+"/feed", env.Attributes["url"])
		assert.Equal(t, "text", env.Attributes["ws.frame"])
		assert.Equal(t, "ws", env.Protocol)
		assert.Empty(t, env.Headers)
	}
	assert.Equal(t, []string{"conn1-msg1", "conn1-msg2", "conn2-msg1", "conn2-msg2"}, got)
	assert.GreaterOrEqual(t, conns.Load(), int32(2))
}

func TestSubscribeFailsOverToTheNextURL(t *testing.T) {
	url, _ := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		_ = c.Write(context.Background(), websocket.MessageText, []byte("from the failover"))
		_, _, _ = c.Read(context.Background()) // hold the connection
	})
	primary := "ws://" + closedAddr(t) + "/events"
	s := newTestSource(t, "wss", config.Source{URL: primary, FailoverURLs: []string{url + "/events"},
		Reconnect: &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond)}})
	out, _ := run(t, s)
	env := recv(t, out)
	assert.Equal(t, "from the failover", env.BodyText())
	assert.Equal(t, url+"/events", env.Attributes["url"])
	assert.Equal(t, "wss", env.Protocol)
	assert.NoError(t, s.Health(context.Background()), "connected")
}

func TestSubscribeGivesUpAfterMaxAttempts(t *testing.T) {
	s := newTestSource(t, "ws", config.Source{URL: "ws://" + closedAddr(t) + "/",
		Reconnect: &config.Reconnect{Backoff: config.Duration(5 * time.Millisecond), MaxAttempts: 2}})
	errc := make(chan error, 1)
	go func() { errc <- s.Start(context.Background(), make(chan *envelope.Envelope)) }()
	select {
	case err := <-errc:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "giving up after 2 failed connects")
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not give up")
	}
	assert.Error(t, s.Health(context.Background()))
}

func TestSubscribeStableConnectionResetsFailures(t *testing.T) {
	url, conns := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		time.Sleep(30 * time.Millisecond) // up long enough, but silent
		_ = c.Close(websocket.StatusNormalClosure, "")
	})
	s := newTestSource(t, "ws", config.Source{URL: url,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}})
	s.stableAfter = 10 * time.Millisecond
	run(t, s) // fails at cleanup if Start gave up
	require.Eventually(t, func() bool { return conns.Load() >= 4 }, 3*time.Second, 5*time.Millisecond,
		"connections that stayed up do not use up max_attempts")
}

func TestSubscribeShortSessionsCountAsFailures(t *testing.T) {
	// The server accepts and hangs up at once: that is no better than a
	// refused connect, so max_attempts still applies.
	url, conns := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		_ = c.Close(websocket.StatusPolicyViolation, "go away")
	})
	s := newTestSource(t, "ws", config.Source{URL: url,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 3}})
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
	assert.Equal(t, int32(3), conns.Load())
}

func TestSubscribeHandshakeHeadersAndTLS(t *testing.T) {
	seen := make(chan http.Header, 4)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"v2"}})
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		_ = c.Write(context.Background(), websocket.MessageText, []byte("proto="+c.Subprotocol()))
		_, _, _ = c.Read(context.Background())
	}))
	t.Cleanup(srv.Close)
	s := newTestSource(t, "wss", config.Source{URL: "wss" + strings.TrimPrefix(srv.URL, "https"),
		Headers: map[string]string{"Authorization": "Bearer t0ken", "Sec-WebSocket-Protocol": "v2, v1"},
		TLS:     &config.TLS{InsecureSkipVerify: true}})
	out, _ := run(t, s)
	assert.Equal(t, "proto=v2", recv(t, out).BodyText())
	h := <-seen
	assert.Equal(t, "Bearer t0ken", h.Get("Authorization"))
	assert.Equal(t, "v2,v1", h.Get("Sec-WebSocket-Protocol"))
}

func TestSubscribeStopsCleanly(t *testing.T) {
	closed := make(chan error, 1)
	url, _ := wsServer(t, func(_ int, c *websocket.Conn, _ *http.Request) {
		_, _, err := c.Read(context.Background())
		closed <- err
	})
	s := newTestSource(t, "ws", config.Source{URL: url})
	_, stop := run(t, s)
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 2*time.Second, 2*time.Millisecond)
	stop()
	assert.ErrorIs(t, s.Health(context.Background()), errStopped)
	select {
	case err := <-closed:
		assert.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err), "the server is told why")
	case <-time.After(3 * time.Second):
		t.Fatal("the server never saw the connection close")
	}
}
