package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

type okSink struct{}

func (okSink) Name() string { return "ok" }
func (okSink) Deliver(context.Context, *envelope.Envelope) (*plugin.Result, error) {
	return &plugin.Result{Status: 200}, nil
}
func (okSink) Health(context.Context) error { return nil }
func (okSink) Close(context.Context) error  { return nil }

func init() {
	plugin.RegisterSink("fault", func(context.Context, plugin.Spec) (plugin.Sink, error) { return okSink{}, nil })
}

const spec = `
version: 1
name: admin
stores: [{ name: main, type: memory }]
sources: [{ name: in, type: stdin }]
destinations: [{ name: billing, type: fault }, { name: support, type: fault }]
routes:
  - name: triage
    from: [in]
    router:
      type: sticky
      history: { store: main, key: "{{ .json.customer }}" }
      fallback: [{ type: static, to: [support] }]
`

func server(t *testing.T, token string) (*Server, *engine.Engine, store.Store) {
	t.Helper()
	l, err := config.LoadBytes(context.Background(), "s.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	m := metrics.New()
	e, err := engine.New(context.Background(), l.Spec, engine.Options{NoSources: true, Metrics: m})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })
	st, err := e.Store("main")
	require.NoError(t, err)
	reloads := 0
	s := New(Options{
		Token: token, Metrics: m, Engine: func() *engine.Engine { return e }, Version: "test",
		Reload: func(context.Context) error {
			reloads++
			if reloads > 1 {
				return errors.New("bad spec")
			}
			return nil
		},
		SpecYAML:            func() []byte { return []byte("name: admin\n") },
		ReadyOnDestinations: true,
	})
	return s, e, st
}

func do(t *testing.T, s *Server, method, path, body string, hdr map[string]string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:5000"
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestHealthReadyAndMetrics(t *testing.T) {
	s, _, _ := server(t, "")
	code, body := do(t, s, "GET", "/healthz", "", nil)
	assert.Equal(t, 200, code)
	assert.Contains(t, body, `"version": "test"`)
	code, body = do(t, s, "GET", "/readyz", "", nil)
	assert.Equal(t, 200, code)
	assert.Contains(t, body, `"ready": true`)
	code, body = do(t, s, "GET", "/metrics", "", nil)
	assert.Equal(t, 200, code)
	assert.Contains(t, body, "go_goroutines")
	code, body = do(t, s, "GET", "/debug/config", "", nil)
	assert.Equal(t, 200, code)
	assert.Equal(t, "name: admin\n", body)
	code, body = do(t, s, "GET", "/debug/state", "", nil)
	assert.Equal(t, 200, code)
	assert.Contains(t, body, `"destinations"`)
}

func TestProtectTokenAndLoopback(t *testing.T) {
	s, _, _ := server(t, "")
	req := httptest.NewRequest("POST", "/reload", nil)
	req.RemoteAddr = "10.0.0.8:4000"
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	assert.Equal(t, 403, rec.Code, "without a token, only loopback clients may reload")
	code, _ := do(t, s, "POST", "/reload", "", nil)
	assert.Equal(t, 200, code)
	code, body := do(t, s, "POST", "/reload", "", nil)
	assert.Equal(t, 422, code, "a reload that fails keeps the running relay")
	assert.Contains(t, body, "bad spec")

	ts, _, _ := server(t, "t0ken")
	code, _ = do(t, ts, "GET", "/v1/history?labeled=true", "", nil)
	assert.Equal(t, 401, code)
	code, _ = do(t, ts, "GET", "/v1/history?labeled=true", "", map[string]string{"Authorization": "Bearer t0ken"})
	assert.Equal(t, 200, code)
	code, _ = do(t, ts, "GET", "/healthz", "", nil)
	assert.Equal(t, 200, code, "probes need no token")
}

func TestFeedbackHistoryAndForget(t *testing.T) {
	s, _, st := server(t, "")
	ctx := context.Background()
	require.NoError(t, st.Append(ctx, "triage", "cust-1", store.CallRecord{MsgID: "m-1", At: time.Now(), Route: "triage", Key: "cust-1",
		State: "refund", Outcome: store.OutcomeDelivered, Decision: []store.Answer{{Candidate: "support", Probability: 0.8, Chosen: true}}},
		store.AppendOptions{TTL: time.Hour}))

	code, body := do(t, s, "POST", "/v1/feedback", `{"msg_id":"m-1","correct":["billing"],"note":"an invoice"}`, nil)
	require.Equal(t, 200, code, body)
	assert.Contains(t, body, `"correct"`)
	code, _ = do(t, s, "POST", "/v1/feedback", `{"msg_id":"m-1","correct":["ghost"]}`, nil)
	assert.Equal(t, 400, code, "a correction names declared destinations only")
	code, _ = do(t, s, "POST", "/v1/feedback", `{"msg_id":"nope","correct":["billing"]}`, nil)
	assert.Equal(t, 404, code)
	code, _ = do(t, s, "POST", "/v1/feedback", `{}`, nil)
	assert.Equal(t, 400, code)

	code, body = do(t, s, "GET", "/v1/history/triage/cust-1", "", nil)
	require.Equal(t, 200, code)
	var view HistoryView
	require.NoError(t, json.Unmarshal([]byte(body), &view))
	require.Len(t, view.Records, 1)
	assert.Equal(t, "routed to support (p=0.80), outcome: delivered, correct destination: billing", view.Turns[1].Content, "exactly as the model sees it")
	assert.Contains(t, view.System, store.HistoryPreamble)

	code, body = do(t, s, "GET", "/v1/history?labeled=true&since=30d&route=triage", "", nil)
	require.Equal(t, 200, code)
	assert.Equal(t, 1, strings.Count(strings.TrimSpace(body), "\n")+1, "one labeled record")
	code, _ = do(t, s, "GET", "/v1/history?since=soon", "", nil)
	assert.Equal(t, 400, code)

	code, _ = do(t, s, "DELETE", "/v1/history/triage/cust-1", "", nil)
	assert.Equal(t, 200, code)
	_, body = do(t, s, "GET", "/v1/history/triage/cust-1", "", nil)
	assert.Contains(t, body, `"records": []`)
	code, _ = do(t, s, "GET", "/v1/history/nope/k", "", nil)
	assert.Equal(t, 404, code)
}

func TestTailStreamsEvents(t *testing.T) {
	s, e, _ := server(t, "")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/tail?source=in")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Eventually(t, func() bool { return s.Hub().Subscribers() == 1 }, time.Second, 5*time.Millisecond)
	env := envelope.New("in", "test", []byte(`{"customer":"c"}`))
	s.Hub().Publish(engine.Event{Type: "received", Envelope: env})
	s.Hub().Publish(engine.Event{Type: "received", Envelope: envelope.New("other", "test", nil)})
	_ = e
	r := bufio.NewReader(resp.Body)
	var lines []string
	for len(lines) < 2 {
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		if strings.TrimSpace(line) != "" {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	assert.Equal(t, "event: received", lines[0])
	assert.Contains(t, lines[1], env.ID, "filtered by source")
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 0))
}

func TestMetricsEndpointCanBeOff(t *testing.T) {
	s, _, _ := server(t, "")
	code, _ := do(t, s, "GET", "/metrics", "", nil)
	assert.Equal(t, 200, code)

	off := New(Options{Metrics: metrics.New(), NoMetricsEndpoint: true, Engine: s.opts().Engine})
	code, _ = do(t, off, "GET", "/metrics", "", nil)
	assert.Equal(t, 404, code, "observability.metrics.prometheus: false")
}

func TestTailRedactsSecrets(t *testing.T) {
	s, _, _ := server(t, "")
	s.opts().Redact = func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("s3cret"), []byte("[REDACTED]")) }
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/tail")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Eventually(t, func() bool { return s.Hub().Subscribers() == 1 }, time.Second, 5*time.Millisecond)
	s.Hub().Publish(engine.Event{Type: "received", Envelope: envelope.New("in", "test", []byte(`{"token":"s3cret"}`))})
	r := bufio.NewReader(resp.Body)
	for {
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "data: ") {
			assert.NotContains(t, line, "s3cret")
			assert.Contains(t, line, "[REDACTED]")
			return
		}
	}
}

func TestHubDropsSlowSubscribers(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe(1)
	h.Publish(engine.Event{Type: "a"})
	h.Publish(engine.Event{Type: "b"}) // dropped: the subscriber is full
	assert.Equal(t, "a", (<-ch).Type)
	cancel()
	cancel()
	h.Close()
	ch2, _ := h.Subscribe(1)
	_, open := <-ch2
	assert.False(t, open, "a closed hub hands out closed channels")
}

func TestStartAndShutdown(t *testing.T) {
	s, _, _ := server(t, "")
	s.opts().Listen = "127.0.0.1:0"
	require.NoError(t, s.Start())
	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.NoError(t, s.Shutdown(context.Background()))
	bad := New(Options{Listen: "256.0.0.1:1"})
	assert.Error(t, bad.Start())
	assert.Empty(t, bad.Addr())
}

// A reload gives a running server new options: the token, the readiness
// checks and the /metrics endpoint follow them from the next request.
func TestConfigureAppliesFromTheNextRequest(t *testing.T) {
	s, _, _ := server(t, "")
	code, _ := do(t, s, "GET", "/debug/config", "", nil)
	require.Equal(t, 200, code)

	o := *s.opts()
	o.Token, o.NoMetricsEndpoint = "t0ken", true
	s.Configure(o)
	code, _ = do(t, s, "GET", "/debug/config", "", nil)
	assert.Equal(t, 401, code, "the new token guards the endpoint")
	code, _ = do(t, s, "GET", "/debug/config", "", map[string]string{"Authorization": "Bearer t0ken"})
	assert.Equal(t, 200, code)
	code, _ = do(t, s, "GET", "/metrics", "", nil)
	assert.Equal(t, 404, code, "observability.metrics.prometheus: false")

	o.Token, o.NoMetricsEndpoint, o.Listen = "", false, "127.0.0.1:1"
	s.Configure(o)
	code, _ = do(t, s, "GET", "/debug/config", "", nil)
	assert.Equal(t, 200, code, "no token: loopback clients again")
	code, _ = do(t, s, "GET", "/metrics", "", nil)
	assert.Equal(t, 200, code)
	assert.Empty(t, s.opts().Listen, "Listen keeps the value New had")
}

// Two servers on one hub (an admin API that moved on reload): shutting
// the old one down ends its own tail streams, and the new one's keep their
// events.
func TestShutdownLeavesASharedHubOpen(t *testing.T) {
	first, _, _ := server(t, "")
	hub := NewHub()
	o := *first.opts()
	o.Hub = hub
	old, cur := New(o), New(o)
	oldSrv, curSrv := httptest.NewServer(old.Handler()), httptest.NewServer(cur.Handler())
	t.Cleanup(oldSrv.Close)
	t.Cleanup(curSrv.Close)
	t.Cleanup(func() { _ = cur.Shutdown(context.Background()) }) // ends its stream first

	tail := func(url string) *bufio.Reader {
		resp, err := http.Get(url + "/v1/tail")
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, 200, resp.StatusCode)
		return bufio.NewReader(resp.Body)
	}
	oldTail, curTail := tail(oldSrv.URL), tail(curSrv.URL)
	require.Eventually(t, func() bool { return hub.Subscribers() == 2 }, time.Second, 5*time.Millisecond)

	require.NoError(t, old.Shutdown(context.Background()))
	_, err := io.ReadAll(oldTail)
	require.NoError(t, err, "the old server's stream ends")
	require.Eventually(t, func() bool { return hub.Subscribers() == 1 }, time.Second, 5*time.Millisecond)

	env := envelope.New("in", "test", nil)
	hub.Publish(engine.Event{Type: "received", Envelope: env})
	for {
		line, err := curTail.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "data: ") {
			assert.Contains(t, line, env.ID, "the new server's stream goes on")
			break
		}
	}

	own := New(Options{Engine: o.Engine})
	ch, _ := own.Hub().Subscribe(1)
	require.NoError(t, own.Shutdown(context.Background()))
	_, open := <-ch
	assert.False(t, open, "a hub New made closes with its server")
}

// A POST /reload that retires the server it came through still gets its
// answer, and the address closes after it.
func TestReloadAnsweredWhileTheServerShutsDown(t *testing.T) {
	var s *Server
	stopped := make(chan error, 1)
	s = New(Options{Listen: "127.0.0.1:0", Engine: func() *engine.Engine { return nil },
		Reload: func(context.Context) error {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				stopped <- s.Shutdown(ctx)
			}()
			time.Sleep(50 * time.Millisecond) // the shutdown is underway
			return nil
		}})
	require.NoError(t, s.Start())
	addr := s.Addr()
	resp, err := http.Post("http://"+addr+"/reload", "application/json", nil)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(body), "reloaded")
	select {
	case err := <-stopped:
		assert.NoError(t, err, "the shutdown waited for the answer, not for its timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("the server did not shut down")
	}
	_, err = http.Get("http://" + addr + "/healthz")
	assert.Error(t, err, "the old address refuses connections")
}

// Listen binds without answering, so a reload can claim a new address
// before it swaps; a server bound but never served frees it on Shutdown.
func TestListenThenServe(t *testing.T) {
	s := New(Options{Listen: "127.0.0.1:0", Version: "v"})
	require.NoError(t, s.Listen())
	addr := s.Addr()
	taken := New(Options{Listen: addr})
	assert.Error(t, taken.Listen(), "the address is bound")

	got := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err != nil {
			got <- 0
			return
		}
		_ = resp.Body.Close()
		got <- resp.StatusCode
	}()
	select {
	case <-got:
		t.Fatal("answered before Serve")
	case <-time.After(100 * time.Millisecond):
	}
	s.Serve()
	assert.Equal(t, 200, <-got, "the waiting connection is answered once the server serves")
	require.NoError(t, s.Shutdown(context.Background()))

	unused := New(Options{Listen: "127.0.0.1:0"})
	require.NoError(t, unused.Listen())
	addr = unused.Addr()
	require.NoError(t, unused.Shutdown(context.Background()))
	again := New(Options{Listen: addr})
	require.NoError(t, again.Listen(), "the address is free again")
	require.NoError(t, again.Shutdown(context.Background()))
}
