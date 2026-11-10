package sse

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// httpc keeps no idle connections. A pooled transport can dial a spare
// connection that never sends a request, and http.Server.Shutdown (behind
// httpmux's Release) waits up to 5s for such a connection.
var httpc = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

func newTestSource(t *testing.T, cfg config.Source) *Source {
	t.Helper()
	cfg.Name, cfg.Type = "in", "sse"
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "sse", Config: &cfg})
	require.NoError(t, err)
	return s
}

// run starts s. The returned stop (also run at cleanup) cancels it and
// checks that Start returned nil.
func run(t *testing.T, s *Source) (<-chan *envelope.Envelope, func()) {
	t.Helper()
	out := make(chan *envelope.Envelope, 64)
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
	return out, stop
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

func listening(t *testing.T, s *Source) string {
	t.Helper()
	require.Eventually(t, func() bool { return s.Addr() != "" }, 2*time.Second, 2*time.Millisecond)
	return s.Addr()
}

func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestRegister(t *testing.T) {
	Register()
	_, ok := plugin.LookupSource("sse")
	assert.True(t, ok)
	_, ok = plugin.LookupSink("sse")
	assert.True(t, ok)
}

func TestSubscribeResumesWithLastEventIDAndHonoursRetry(t *testing.T) {
	type hello struct{ lastID, accept, cache, auth string }
	seen := make(chan hello, 8)
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- hello{r.Header.Get("Last-Event-ID"), r.Header.Get("Accept"), r.Header.Get("Cache-Control"), r.Header.Get("Authorization")}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		if conns.Add(1) == 1 {
			// A short retry, two events, then the stream drops.
			_, _ = io.WriteString(w, "retry: 20\n\nid: 1\ndata: a\n\nid: 2\nevent: tick\ndata: {\"n\":2}\n\n")
			return
		}
		_, _ = io.WriteString(w, ": resumed\n\nid: 3\ndata: c\n\ndata: d\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	// Without the server's retry the reconnect would wait 10s. max_attempts
	// 1 holds no threat: a stream that delivered resets the failure count.
	s := newTestSource(t, config.Source{URL: srv.URL + "/stream", Headers: map[string]string{"Authorization": "Bearer k"},
		Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Second), Jitter: "none", MaxAttempts: 1}})
	require.Equal(t, modeSubscribe, s.opts.mode)
	out, _ := run(t, s)

	a, b, c, d := recv(t, out), recv(t, out), recv(t, out), recv(t, out)
	assert.Equal(t, "a", a.BodyText())
	assert.Equal(t, "1", a.Attributes["sse.id"])
	assert.Equal(t, "message", a.Attributes["sse.event"])
	assert.Equal(t, "/stream", a.Subject, "no event name: the URL path")
	assert.Equal(t, "text/plain; charset=utf-8", a.ContentType)
	assert.Equal(t, srv.URL+"/stream", a.Attributes["url"])
	assert.Equal(t, "sse", a.Protocol)
	assert.Equal(t, "in", a.Source)
	assert.Empty(t, a.Headers)

	assert.Equal(t, `{"n":2}`, b.BodyText())
	assert.Equal(t, "application/json", b.ContentType)
	assert.Equal(t, "2", b.Attributes["sse.id"])
	assert.Equal(t, "tick", b.Attributes["sse.event"])
	assert.Equal(t, "tick", b.Subject, "the event name is the subject")

	assert.Equal(t, "c", c.BodyText())
	assert.Equal(t, "3", c.Attributes["sse.id"])
	assert.Equal(t, "d", d.BodyText())
	assert.Equal(t, "3", d.Attributes["sse.id"], "the id carries over")

	first, second := <-seen, <-seen
	assert.Equal(t, hello{"", "text/event-stream", "no-cache", "Bearer k"}, first)
	assert.Equal(t, hello{"2", "text/event-stream", "no-cache", "Bearer k"}, second, "the reconnect resumes after the last id")
	assert.NoError(t, s.Health(context.Background()))
}

func TestSubscribeFailedConnects(t *testing.T) {
	cases := map[string]struct {
		handler http.HandlerFunc
		want    string
	}{
		"status": {func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "down", http.StatusServiceUnavailable)
		}, "answered 503 Service Unavailable"},
		"content type": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, "data: x\n\n")
		}, `content type "application/json", not text/event-stream`},
	}
	for name, c := range cases {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			c.handler(w, r)
		}))
		s := newTestSource(t, config.Source{URL: srv.URL,
			Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}})
		out := make(chan *envelope.Envelope, 4)
		err := s.Start(context.Background(), out)
		srv.Close()
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "giving up after 2 failed connects", name)
		assert.Contains(t, err.Error(), c.want, name)
		assert.Equal(t, int32(2), hits.Load(), name)
		assert.Empty(t, out, name)
		assert.Error(t, s.Health(context.Background()), name)
	}
}

func TestSubscribeFailsOverToTheNextURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: from the failover\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	s := newTestSource(t, config.Source{URL: "http://" + closedAddr(t) + "/events", FailoverURLs: []string{srv.URL + "/events"},
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond)}})
	out, _ := run(t, s)
	env := recv(t, out)
	assert.Equal(t, "from the failover", env.BodyText())
	assert.Equal(t, srv.URL+"/events", env.Attributes["url"])
}

// push POSTs an event stream that the test writes as it goes.
func push(t *testing.T, url string, header http.Header) (*io.PipeWriter, <-chan *http.Response) {
	t.Helper()
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPost, url, pr)
	require.NoError(t, err)
	req.Header = header
	resps := make(chan *http.Response, 1)
	go func() {
		resp, err := httpc.Do(req)
		if err != nil {
			_ = pr.CloseWithError(err)
			close(resps)
			return
		}
		_ = resp.Body.Close()
		resps <- resp
	}()
	t.Cleanup(func() { _ = pw.Close() })
	return pw, resps
}

func status(t *testing.T, resps <-chan *http.Response) int {
	t.Helper()
	select {
	case resp, ok := <-resps:
		require.True(t, ok, "the request failed")
		return resp.StatusCode
	case <-time.After(5 * time.Second):
		t.Fatal("no response")
		return 0
	}
}

func TestListenStripsCredentials(t *testing.T) {
	s := newTestSource(t, config.Source{Listen: "127.0.0.1:0", Path: "/push"})
	out, _ := run(t, s)
	url := "http://" + listening(t, s) + "/push"
	pw, resps := push(t, url, http.Header{"Content-Type": {"text/event-stream"}, "Authorization": {"Bearer x"}, "Cookie": {"a=b"}, "X-Origin": {"edge"}})
	_, err := io.WriteString(pw, "data: one\n\n")
	require.NoError(t, err)
	env := recv(t, out)
	require.NoError(t, pw.Close())
	assert.Equal(t, http.StatusOK, status(t, resps))
	assert.Empty(t, env.Header("Authorization"), "the caller's credentials never reach a destination")
	assert.Empty(t, env.Header("Cookie"))
	assert.Equal(t, "edge", env.Header("X-Origin"))
}

func TestListenParsesAPushedStream(t *testing.T) {
	s := newTestSource(t, config.Source{Listen: "127.0.0.1:0", Path: "/push"})
	out, _ := run(t, s)
	url := "http://" + listening(t, s) + "/push"
	pw, resps := push(t, url, http.Header{"Content-Type": {"text/event-stream"}, "X-Origin": {"edge"}})

	// Each event arrives while the body is still streaming.
	_, err := io.WriteString(pw, "id: 1\nevent: created\ndata: one\n\n")
	require.NoError(t, err)
	one := recv(t, out)
	_, err = io.WriteString(pw, ": keepalive\n\ndata: {\"n\":2}\n\n")
	require.NoError(t, err)
	two := recv(t, out)
	_, err = io.WriteString(pw, "id: 3\r\ndata: thr\r\ndata: ee\r\n\r\n")
	require.NoError(t, err)
	three := recv(t, out)
	require.NoError(t, pw.Close())
	assert.Equal(t, http.StatusOK, status(t, resps), "200 once the body ends")

	assert.Equal(t, "one", one.BodyText())
	assert.Equal(t, "1", one.Attributes["sse.id"])
	assert.Equal(t, "created", one.Attributes["sse.event"])
	assert.Equal(t, "created", one.Subject)
	assert.Equal(t, `{"n":2}`, two.BodyText())
	assert.Equal(t, "application/json", two.ContentType)
	assert.Equal(t, "1", two.Attributes["sse.id"])
	assert.Equal(t, "message", two.Attributes["sse.event"])
	assert.Equal(t, "/push", two.Subject)
	assert.Equal(t, "thr\nee", three.BodyText())
	assert.Equal(t, "3", three.Attributes["sse.id"])
	for _, env := range []*envelope.Envelope{one, two, three} {
		assert.Equal(t, "edge", env.Header("X-Origin"))
		assert.Contains(t, env.Attributes["remote_addr"], "127.0.0.1:")
		assert.Equal(t, "sse", env.Protocol)
	}
}

func TestListenRefusals(t *testing.T) {
	s := newTestSource(t, config.Source{Listen: "127.0.0.1:0", MaxBodyBytes: 8,
		Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t"}}}})
	out, _ := run(t, s)
	url := "http://" + listening(t, s) + "/"
	post := func(h http.Header, body string) int {
		req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		require.NoError(t, err)
		req.Header = h
		resp, err := httpc.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	authed := func(ct string) http.Header {
		h := http.Header{"Authorization": {"Bearer t"}}
		if ct != "" {
			h.Set("Content-Type", ct)
		}
		return h
	}
	assert.Equal(t, http.StatusUnauthorized, post(http.Header{}, "data: x\n\n"))
	assert.Equal(t, http.StatusUnsupportedMediaType, post(authed("application/json"), `{"a":1}`))
	assert.Equal(t, http.StatusRequestEntityTooLarge, post(authed("text/event-stream"), "data: 123456789\n\n"))
	assert.Equal(t, http.StatusOK, post(authed(""), "data: 12345678\n\n"), "no content type is fine")
	assert.Equal(t, "12345678", recv(t, out).BodyText())

	resp, err := httpc.Get(url)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "POST", resp.Header.Get("Allow"))
	assert.Empty(t, out)
}

func TestListenShutdownInterruptsAStream(t *testing.T) {
	s := newTestSource(t, config.Source{Listen: "127.0.0.1:0"})
	out, stop := run(t, s)
	pw, resps := push(t, "http://"+listening(t, s)+"/", http.Header{})
	_, err := io.WriteString(pw, "data: one\n\n")
	require.NoError(t, err)
	assert.Equal(t, "one", recv(t, out).BodyText())

	start := time.Now()
	stop() // the client is still sending
	assert.Less(t, time.Since(start), 2*time.Second, "shutdown does not wait for the client")
	assert.Equal(t, http.StatusServiceUnavailable, status(t, resps))
	assert.ErrorIs(t, s.Health(context.Background()), errStopped)
}

func TestFactoryRejectsHMAC(t *testing.T) {
	Register()
	cfg := &config.Source{Name: "in", Type: "sse", Listen: ":0",
		Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "k"}}}
	_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: "sse", Config: cfg})
	assert.EqualError(t, err, "source in: auth.hmac applies to http sources: a WebSocket or SSE request carries no signed body")
}

func TestListenBindFailure(t *testing.T) {
	s := newTestSource(t, config.Source{Listen: "256.0.0.1:1"})
	assert.Error(t, s.Start(context.Background(), make(chan *envelope.Envelope)))
	assert.Error(t, s.Health(context.Background()))
	assert.EqualError(t, s.Start(context.Background(), nil), "already started")
}
