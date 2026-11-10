package http

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

func init() { Register() }

func sinkFor(t *testing.T, ep config.Endpoint, egress plugin.Egress) plugin.Sink {
	t.Helper()
	if ep.Type == "" {
		ep.Type = "http"
	}
	s, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "dest", Kind: ep.Type, Config: &ep, Env: plugin.Env{Egress: egress}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func msg(body string) *envelope.Envelope {
	e := envelope.New("webhooks", "http", []byte(body))
	e.ContentType = "application/json"
	return e
}

func TestSinkClassifiesStatus(t *testing.T) {
	var code atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch c := int(code.Load()); c {
		case 503:
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(503)
		case 200:
			w.Header().Set(RerouteHeader, "billing, support")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			w.WriteHeader(c)
			_, _ = io.WriteString(w, "nope")
		}
	}))
	defer srv.Close()
	s := sinkFor(t, config.Endpoint{URL: srv.URL + "/hook?token=secret"}, plugin.Egress{})

	code.Store(503)
	_, err := s.Deliver(context.Background(), msg(`{}`))
	class, ra := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, 2*time.Second, ra)
	assert.Contains(t, plugin.Tags(err), "503")

	code.Store(404)
	_, err = s.Deliver(context.Background(), msg(`{}`))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "nope")

	code.Store(200)
	r, err := s.Deliver(context.Background(), msg(`{}`))
	require.NoError(t, err)
	assert.Equal(t, 200, r.Status)
	assert.JSONEq(t, `{"ok":true}`, string(r.Body))
	assert.Equal(t, []string{"billing", "support"}, r.Reroute)
	assert.NotContains(t, r.Endpoint, "secret", "reports never show the query")
}

func TestSinkForwardsHeadersAndIdempotency(t *testing.T) {
	var got *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(204)
	}))
	defer srv.Close()
	s := sinkFor(t, config.Endpoint{URL: srv.URL, Method: "put", Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}", "Authorization": "Bearer downstream"}, Body: `{"t":"{{ .json.tenant }}"}`}, plugin.Egress{})
	e := msg(`{"tenant":"acme"}`)
	e.Headers["Authorization"] = []string{"Bearer caller-secret"}
	e.Headers["X-Request-Id"] = []string{"r-1"}
	e.SetHeader("Idempotency-Key", e.ID)
	_, err := s.Deliver(context.Background(), e)
	require.NoError(t, err)
	assert.Equal(t, "PUT", got.Method)
	assert.Equal(t, "acme", got.Header.Get("X-Tenant"))
	assert.Equal(t, "Bearer downstream", got.Header.Get("Authorization"), "the caller's credentials are not forwarded; the configured ones are")
	assert.Equal(t, "r-1", got.Header.Get("X-Request-Id"))
	assert.Equal(t, e.ID, got.Header.Get("Idempotency-Key"))
	assert.Equal(t, "application/json", got.Header.Get("Content-Type"))
	assert.JSONEq(t, `{"t":"acme"}`, string(body))
}

func TestSinkTemplatedURLEgress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	ep := config.Endpoint{URL: fmt.Sprintf("http://{{ .json.host }}:%d/hook", port)}

	s := sinkFor(t, ep, plugin.Egress{})
	_, err := s.Deliver(context.Background(), msg(`{"host":"127.0.0.1"}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a templated host may not reach a private address")
	assert.ErrorContains(t, err, "private address")

	allowed := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"127.0.0.1"}})
	_, err = allowed.Deliver(context.Background(), msg(`{"host":"127.0.0.1"}`))
	assert.NoError(t, err)

	listed := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"*.example.com"}})
	_, err = listed.Deliver(context.Background(), msg(`{"host":"evil.internal"}`))
	assert.ErrorContains(t, err, "not in egress.allow")

	_, err = s.Deliver(context.Background(), msg(`{"host":""}`))
	assert.ErrorContains(t, err, "not an http(s) URL")
	assert.NoError(t, s.Health(context.Background()), "templated destinations have no host to probe")
}

// With host names and CIDRs in egress.allow, a host that is not listed by
// name must dial into one of the CIDRs.
func TestSinkEgressMixedHostsAndCIDRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	ep := config.Endpoint{URL: fmt.Sprintf("http://{{ .json.host }}:%d/hook", port)}

	s := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"api.example.com", "10.0.0.0/8"}})
	_, err := s.Deliver(context.Background(), msg(`{"host":"localhost"}`))
	require.Error(t, err, "localhost is not listed by name and 127.0.0.1 is outside 10.0.0.0/8")
	assert.ErrorContains(t, err, "is not in egress.allow")

	named := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"localhost", "10.0.0.0/8"}})
	_, err = named.Deliver(context.Background(), msg(`{"host":"localhost"}`))
	assert.NoError(t, err, "a host listed by name may dial outside the CIDRs")

	cidr := sinkFor(t, ep, plugin.Egress{Allow: []string{"api.example.com", "127.0.0.0/8"}})
	_, err = cidr.Deliver(context.Background(), msg(`{"host":"localhost"}`))
	assert.NoError(t, err, "an unlisted name that dials into an allowed CIDR passes")
}

func TestSinkRedirectPassesEgress(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hook" {
			http.Redirect(w, r, srv.URL+"/elsewhere", http.StatusFound) // to 127.0.0.1, not "localhost"
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	ep := config.Endpoint{URL: fmt.Sprintf("http://{{ .json.host }}:%d/hook", port)}

	s := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"localhost"}})
	_, err := s.Deliver(context.Background(), msg(`{"host":"localhost"}`))
	require.Error(t, err, "the redirect's host is not in egress.allow")
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "host 127.0.0.1 is not in egress.allow")

	both := sinkFor(t, ep, plugin.Egress{AllowPrivate: true, Allow: []string{"localhost", "127.0.0.1"}})
	_, err = both.Deliver(context.Background(), msg(`{"host":"localhost"}`))
	assert.NoError(t, err, "an allowed redirect is followed")
}

func TestSinkConnectAndTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	s := sinkFor(t, config.Endpoint{URL: "http://" + addr}, plugin.Egress{})
	_, err = s.Deliver(context.Background(), msg(`{}`))
	assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
	assert.Error(t, s.Health(context.Background()))

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	s = sinkFor(t, config.Endpoint{URL: slow.URL}, plugin.Egress{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = s.Deliver(ctx, msg(`{}`))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	assert.NoError(t, s.Health(context.Background()))

	_, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "x", Kind: "http", Config: &config.Endpoint{}})
	assert.Error(t, err)
}

// listener starts a listen source and returns its base URL and queue.
// refused counts the messages test listeners refused on a full buffer.
var refused atomic.Int32

func listener(t *testing.T, src config.Source, queue int) (string, chan *envelope.Envelope) {
	t.Helper()
	return listenerOn(t, src, queue, "memory")
}

// listenerOn is listener for a relay with the given buffer type.
func listenerOn(t *testing.T, src config.Source, queue int, buffer string) (string, chan *envelope.Envelope) {
	t.Helper()
	src.Listen = "127.0.0.1:0"
	if src.Mode == "" {
		src.Mode = "listen"
	}
	mem := store.NewMemory("default") // each listener its own: dedupe keys do not leak between tests (or -count runs)
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "webhooks", Kind: "http", Config: &src, Env: plugin.Env{Buffer: buffer, Store: func(string) (store.Store, error) { return mem, nil },
		Refused: func(string) { refused.Add(1) }}})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope, queue)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Start(ctx, out) }()
	t.Cleanup(func() { cancel(); assert.NoError(t, <-done) })
	l := s.(*Listener)
	path := src.Path
	if path == "" {
		path = "/"
	}
	return "http://" + l.Addr() + path, out
}

// acker answers every queued message with an outcome.
func acker(out chan *envelope.Envelope, o func(*envelope.Envelope) envelope.Outcome) {
	go func() {
		for e := range out {
			e.Ack(context.Background(), o(e))
		}
	}()
}

func post(t *testing.T, url, body string, headers map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	if m == nil {
		m = map[string]any{"raw": string(data)}
	}
	return resp, m
}

func TestListenAckDelivered(t *testing.T) {
	url, out := listener(t, config.Source{Path: "/ingest"}, 10)
	status := envelope.Delivered
	var mu sync.Mutex
	acker(out, func(*envelope.Envelope) envelope.Outcome {
		mu.Lock()
		defer mu.Unlock()
		return envelope.Outcome{Status: status}
	})
	resp, m := post(t, url, `{"a":1}`, map[string]string{"Content-Type": "application/json"})
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "delivered", m["status"])

	mu.Lock()
	status = envelope.DeadLettered
	mu.Unlock()
	resp, m = post(t, url, `{}`, nil)
	assert.Equal(t, 202, resp.StatusCode, "safely dead-lettered is accepted")
	assert.Equal(t, "dead_lettered", m["status"])

	mu.Lock()
	status = envelope.Failed
	mu.Unlock()
	resp, _ = post(t, url, `{}`, nil)
	assert.Equal(t, 503, resp.StatusCode, "a message the relay could not keep is a nack: the caller retries")
	assert.Equal(t, "5", resp.Header.Get("Retry-After"))
}

// A body a route's validate step refused is the caller's mistake: 422, with
// what is wrong, though the message is kept in the DLQ like any other.
func TestListenSchemaFailureIs422(t *testing.T) {
	url, out := listener(t, config.Source{Path: "/ingest"}, 10)
	acker(out, func(*envelope.Envelope) envelope.Outcome {
		return envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{plugin.TagSchema}, "schema: id: got number, want string")}
	})
	resp, m := post(t, url, `{"id":7}`, nil)
	assert.Equal(t, 422, resp.StatusCode)
	assert.Equal(t, "dead_lettered", m["status"])
	assert.Equal(t, "schema: id: got number, want string", m["error"])
}

func TestListenEnvelopeFields(t *testing.T) {
	url, out := listener(t, config.Source{Path: "/ingest", Ack: "accepted"}, 10)
	resp, m := post(t, url+"?x=1", `{"a":1}`, map[string]string{"Content-Type": "application/json", "Idempotency-Key": "evt-42", "X-Event": "invoice.paid", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	assert.Equal(t, 202, resp.StatusCode, "ack: accepted answers once queued")
	assert.Equal(t, "evt-42", m["id"])
	e := <-out
	assert.Equal(t, "evt-42", e.ID, "the caller's Idempotency-Key becomes the envelope ID")
	assert.Equal(t, "/ingest", e.Subject)
	assert.Equal(t, "application/json", e.ContentType)
	assert.Equal(t, "invoice.paid", e.Header("X-Event"))
	assert.Equal(t, "x=1", e.Attributes["http.query"])
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", e.Trace.TraceID().String(), "W3C trace context is picked up")
}

func TestListenStripsCredentials(t *testing.T) {
	url, out := listener(t, config.Source{Ack: "accepted", Auth: &config.Auth{APIKey: &config.APIKeyAuth{Header: "X-Relay-Key", Keys: []string{"k1"}}}}, 10)
	resp, _ := post(t, url, `{}`, map[string]string{
		"X-Relay-Key": "k1", "Authorization": "Bearer caller", "Proxy-Authorization": "Basic x",
		"Cookie": "session=abc", "X-Api-Key": "other", "Api-Key": "another", "X-Event": "kept",
	})
	require.Equal(t, 202, resp.StatusCode)
	e := <-out
	for _, h := range []string{"X-Relay-Key", "Authorization", "Proxy-Authorization", "Cookie", "X-Api-Key", "Api-Key"} {
		assert.Empty(t, e.Header(h), "%s never reaches a destination", h)
	}
	assert.Equal(t, "kept", e.Header("X-Event"))

	// A key in the query string authenticates, and never reaches the message either.
	url, out = listener(t, config.Source{Ack: "accepted", Auth: &config.Auth{APIKey: &config.APIKeyAuth{Query: "key", Keys: []string{"k1"}}}}, 10)
	resp, _ = post(t, url+"?key=k1&page=2", `{}`, nil)
	require.Equal(t, 202, resp.StatusCode)
	assert.Equal(t, "page=2", (<-out).Attributes["http.query"])
	resp, _ = post(t, url+"?key=nope", `{}`, nil)
	assert.Equal(t, 401, resp.StatusCode)
}

func TestListenRespondFrom(t *testing.T) {
	url, out := listener(t, config.Source{Respond: &config.Respond{From: "billing", Timeout: config.Duration(2 * time.Second)}}, 10)
	acker(out, func(*envelope.Envelope) envelope.Outcome {
		return envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"billing": {Status: 201, Headers: map[string][]string{"X-Billing": {"ok"}}, Body: []byte(`{"invoice":"in_1"}`)},
		}}
	})
	resp, m := post(t, url, `{}`, nil)
	assert.Equal(t, 201, resp.StatusCode)
	assert.Equal(t, "ok", resp.Header.Get("X-Billing"))
	assert.Equal(t, "in_1", m["invoice"], "the destination's reply goes back to the caller")
}

func TestListenRespondFromFailure(t *testing.T) {
	url, out := listener(t, config.Source{Respond: &config.Respond{From: "billing", Timeout: config.Duration(100 * time.Millisecond)}}, 10)
	go func() {
		e := <-out
		e.Ack(context.Background(), envelope.Outcome{Status: envelope.DeadLettered})
		<-out // the second message is never acked
	}()
	resp, _ := post(t, url, `{}`, nil)
	assert.Equal(t, 502, resp.StatusCode)
	resp, m := post(t, url, `{}`, nil)
	assert.Equal(t, 504, resp.StatusCode)
	assert.Equal(t, "pending", m["status"])
}

func TestListenBackpressure(t *testing.T) {
	url, _ := listener(t, config.Source{}, 0)
	before := refused.Load()
	resp, m := post(t, url, `{}`, nil)
	assert.Equal(t, 503, resp.StatusCode, "a full buffer pushes back instead of dropping")
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
	assert.Equal(t, "busy", m["status"])
	assert.Equal(t, before+1, refused.Load(), "counted in hikyaku_backpressure_total")
}

func TestListenAuthMethodAndSize(t *testing.T) {
	url, out := listener(t, config.Source{
		Ack: "accepted", MaxBodyBytes: 16,
		Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "s3cret"}},
	}, 10)
	go func() {
		for range out {
		}
	}()
	body := `{"ok":true}`
	resp, _ := post(t, url, body, map[string]string{"X-Signature-256": "sha256=" + inbound.Sign("s3cret", []byte(body))})
	assert.Equal(t, 202, resp.StatusCode)
	resp, _ = post(t, url, body, map[string]string{"X-Signature-256": "sha256=deadbeef"})
	assert.Equal(t, 401, resp.StatusCode)
	resp, _ = post(t, url, strings.Repeat("x", 17), nil)
	assert.Equal(t, 413, resp.StatusCode)
	r, err := http.Get(url)
	require.NoError(t, err)
	_ = r.Body.Close()
	assert.Equal(t, 405, r.StatusCode)
	assert.Equal(t, "POST, PUT", r.Header.Get("Allow"))
}

func TestHTTPListenClientCancelStillDelivers(t *testing.T) {
	url, out := listener(t, config.Source{}, 10)
	got := make(chan *envelope.Envelope, 1)
	go func() {
		e := <-out
		time.Sleep(150 * time.Millisecond) // delivery takes longer than the caller waits
		e.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
		got <- e
	}()
	client := &http.Client{Timeout: 50 * time.Millisecond}
	req, _ := http.NewRequest("POST", url, strings.NewReader(`{}`))
	req.Header.Set("Idempotency-Key", "retry-safe-1")
	_, err := client.Do(req)
	require.Error(t, err, "the caller gave up")
	e := <-got
	assert.Equal(t, "retry-safe-1", e.ID, "the message was still delivered, under the caller's key")
}

func TestListenDedupe(t *testing.T) {
	url, out := listener(t, config.Source{Ack: "accepted", Dedupe: &config.Dedupe{Key: "{{ .json.id }}"}}, 10)
	_, m := post(t, url, `{"id":"dup-1"}`, nil)
	assert.Equal(t, "accepted", m["status"])
	_, m = post(t, url, `{"id":"dup-1"}`, nil)
	assert.Equal(t, "duplicate", m["status"])
	_, m = post(t, url, `{"other":1}`, nil)
	assert.Equal(t, "accepted", m["status"], "a key that renders empty is never a duplicate")
	assert.Len(t, out, 2)
}

func TestListenHTTPSNeedsCert(t *testing.T) {
	_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "s", Kind: "https", Config: &config.Source{Mode: "listen", Listen: ":0"}})
	assert.ErrorContains(t, err, "needs tls.cert_file")
	_, err = plugin.NewSource(context.Background(), plugin.Spec{Name: "s", Kind: "http"})
	assert.Error(t, err)
}

func TestPollSplitDedupeAndETag(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.Header.Get("If-None-Match") == `"v1"` && n == 2 {
			w.WriteHeader(304)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, `{"items":[{"id":1},{"id":2}]}`)
	}))
	defer srv.Close()
	src := config.Source{Mode: "poll", URL: srv.URL, Interval: config.Duration(20 * time.Millisecond), Split: ".items[]",
		Dedupe: &config.Dedupe{Key: "{{ .json.id }}"}}
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "status", Kind: "http", Config: &src, Env: plugin.Env{Store: func(string) (store.Store, error) { return store.NewMemory("p"), nil }}})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope, 10)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Start(ctx, out) }()
	require.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	assert.Len(t, out, 2, "split into items; the 304 and the repeat are not emitted again")
	e := <-out
	assert.JSONEq(t, `{"id":1}`, e.BodyText())
	assert.NoError(t, s.Health(context.Background()))
}

// A poll keeps the configured User-Agent, and its messages carry no
// credentials or query in their subject.
func TestPollSubjectAndUserAgent(t *testing.T) {
	agents := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case agents <- r.Header.Get("User-Agent"):
		default:
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()
	u := strings.Replace(srv.URL, "http://", "http://user:s3cret@", 1) + "/status?token=abc"
	src := config.Source{Mode: "poll", URL: u, Interval: config.Duration(time.Hour), Headers: map[string]string{"User-Agent": "acme-poller/2"}}
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "status", Kind: "http", Config: &src})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx, out) }()
	e := <-out
	assert.Equal(t, "acme-poller/2", <-agents, "a configured User-Agent is kept")
	assert.Equal(t, srv.URL+"/status", e.Subject)
	assert.NotContains(t, e.Subject, "s3cret")
	assert.Equal(t, e.Subject, e.Attributes["url"])
}

func TestPollRetriesWithinAPoll(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(503)
			return
		}
		_, _ = io.WriteString(w, `ok`)
	}))
	defer srv.Close()
	src := config.Source{Mode: "poll", URL: srv.URL, Interval: config.Duration(time.Hour), Retry: &config.Retry{Attempts: 3, Backoff: config.Duration(time.Millisecond)}}
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "p", Kind: "http", Config: &src})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx, out) }()
	select {
	case e := <-out:
		assert.Equal(t, "ok", e.BodyText())
	case <-time.After(2 * time.Second):
		t.Fatal("no message after the retries")
	}
	assert.Equal(t, int32(3), calls.Load())
}

// A poll's retry follows retry_on and never_on, like a destination's.
func TestPollRetryFollowsNeverOn(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
	}))
	defer srv.Close()
	src := config.Source{Mode: "poll", URL: srv.URL, Interval: config.Duration(time.Hour),
		Retry: &config.Retry{Attempts: 3, Backoff: config.Duration(time.Millisecond), NeverOn: config.TagList{"503"}}}
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "p", Kind: "http", Config: &src})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Start(ctx, make(chan *envelope.Envelope, 1)) }()
	require.Eventually(t, func() bool { return s.Health(context.Background()) != nil }, 2*time.Second, time.Millisecond)
	assert.Equal(t, int32(1), calls.Load(), "never_on: [503] stops at the first 503")
}

// With a disk buffer, ack: accepted answers 202 only once the message is on
// disk (the engine reports it queued), and 503 when the buffer failed it.
func TestListenAcceptedWaitsForTheDiskBuffer(t *testing.T) {
	url, out := listenerOn(t, config.Source{Ack: "accepted"}, 10, "disk")
	go func() {
		e := <-out
		time.Sleep(80 * time.Millisecond) // the write-ahead log's fsync
		e.Queued()
	}()
	start := time.Now()
	resp, m := post(t, url, `{"a":1}`, nil)
	assert.Equal(t, 202, resp.StatusCode)
	assert.Equal(t, "accepted", m["status"])
	assert.GreaterOrEqual(t, time.Since(start), 80*time.Millisecond, "the answer waits for the disk")

	go func() {
		e := <-out
		e.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	}()
	resp, m = post(t, url, `{"a":2}`, nil)
	assert.Equal(t, 503, resp.StatusCode, "a message the buffer could not write is a nack")
	assert.Equal(t, "failed", m["status"])
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
}
