package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/admin"
	"github.com/xavidop/hikyaku/internal/cli"
)

// fakeTail is a relay's admin API as hikyaku forward sees it: /v1/tail,
// behind a token, sending the events put on its channel.
type fakeTail struct {
	*httptest.Server
	events chan admin.Event
	mu     sync.Mutex
	opens  int
	query  string
}

func newFakeTail(t *testing.T, token string) *fakeTail {
	f := &fakeTail{events: make(chan admin.Event, 16)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tail" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":"unauthorized"}`)
			return
		}
		f.mu.Lock()
		f.opens++
		n := f.opens
		f.query = r.URL.RawQuery
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-f.events:
				b, _ := json.Marshal(ev)
				_, _ = fmt.Fprintf(w, ": ping\n\nevent: %s\ndata: %s\n\n", ev.Type, b)
				w.(http.Flusher).Flush()
				if n == 1 && ev.Envelope.ID == "last-of-the-first-stream" {
					return // the relay goes away: forward must connect again
				}
			}
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeTail) lastQuery() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.query
}

func (f *fakeTail) opened() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens
}

// got is one request the local server received.
type got struct {
	method, path, query, body string
	header                    http.Header
}

func localServer(t *testing.T, status int) (*httptest.Server, chan got) {
	ch := make(chan got, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ch <- got{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, body: string(b), header: r.Header.Clone()}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

// forward runs hikyaku forward until the test ends, and returns its output.
func forward(t *testing.T, args ...string) (out *syncBuffer, done chan int, cancel context.CancelFunc) {
	t.Helper()
	out = &syncBuffer{}
	deps := cli.Deps{LookupEnv: func(string) (string, bool) { return "", false }}
	root := cli.NewRoot(cli.BuildInfo{Version: "1.2.3"}, cli.Streams{Out: out, Err: out}, deps)
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan int, 1)
	args = append([]string{"forward", "-f", t.TempDir() + "/none.yaml", "--color", "never"}, args...)
	go func() { done <- cli.Execute(ctx, root, args) }()
	t.Cleanup(cancel)
	return out, done, cancel
}

func received(id, source string, body string) admin.Event {
	env := envelope.New(source, "http", []byte(body))
	env.ID = id
	env.ContentType = "application/json"
	env.Headers = map[string][]string{
		"Stripe-Signature": {"t=1,v1=abc"},
		"Content-Length":   {"999"},
		"Host":             {"relay.example.com"},
		"Accept-Encoding":  {"gzip"},
	}
	env.Attributes = map[string]any{"http.method": "PUT", "http.path": "/stripe/events", "http.query": "a=1"}
	return admin.Event{Type: "received", Envelope: env}
}

func next(t *testing.T, ch chan got) got {
	t.Helper()
	select {
	case g := <-ch:
		return g
	case <-time.After(10 * time.Second):
		t.Fatal("the local server got no request")
		return got{}
	}
}

func TestForwardSendsEveryReceivedMessage(t *testing.T) {
	tail := newFakeTail(t, "s3cret")
	local, reqs := localServer(t, http.StatusNoContent)
	out, done, cancel := forward(t, "--admin", tail.URL, "--token", "s3cret", "--to", local.URL+"/hook?x=1",
		"--source", "stripe", "--header", "X-Dev: yes", "--header", "Stripe-Signature: mine")

	// A finished message is not forwarded: only what the relay receives.
	env := envelope.New("stripe", "http", []byte(`{"done":true}`))
	tail.events <- admin.Event{Type: "done", Envelope: env, Report: &engine.Report{ID: env.ID, Status: "delivered"}}
	tail.events <- received("evt_1", "stripe", `{"id":"evt_1"}`)

	g := next(t, reqs)
	assert.Equal(t, "PUT", g.method, "the method the message came with")
	assert.Equal(t, "/hook", g.path)
	assert.Equal(t, "x=1", g.query)
	assert.JSONEq(t, `{"id":"evt_1"}`, g.body)
	assert.Equal(t, "application/json", g.header.Get("Content-Type"))
	assert.Equal(t, "evt_1", g.header.Get(cli.ForwardIDHeader))
	assert.Equal(t, "stripe", g.header.Get(cli.ForwardSourceHeader))
	assert.Equal(t, "yes", g.header.Get("X-Dev"))
	assert.Equal(t, "mine", g.header.Get("Stripe-Signature"), "--header replaces a header of the message")
	assert.NotEqual(t, "999", g.header.Get("Content-Length"))
	assert.Equal(t, "source=stripe", tail.lastQuery())

	require.Eventually(t, func() bool {
		o := out.String()
		return strings.Contains(o, "forwarding") && strings.Contains(o, "evt_1") && strings.Contains(o, "→ 204")
	}, 5*time.Second, 10*time.Millisecond, out.String())

	// The stream ends (a relay that restarts): forward connects again and
	// goes on.
	tail.events <- received("last-of-the-first-stream", "stripe", `{}`)
	next(t, reqs)
	require.Eventually(t, func() bool { return tail.opened() == 2 }, 10*time.Second, 20*time.Millisecond, "it connects again")
	tail.events <- received("evt_2", "stripe", `{"id":"evt_2"}`)
	assert.JSONEq(t, `{"id":"evt_2"}`, next(t, reqs).body)

	cancel()
	select {
	case code := <-done:
		assert.Equal(t, 0, code, "stopping it is not an error")
	case <-time.After(10 * time.Second):
		t.Fatal("forward did not stop")
	}
	assert.Contains(t, out.String(), "connecting again")
}

func TestForwardKeepPathAndFailures(t *testing.T) {
	tail := newFakeTail(t, "s3cret")
	local, reqs := localServer(t, http.StatusInternalServerError)
	out, _, _ := forward(t, "--admin", tail.URL, "--token", "s3cret", "--to", local.URL+"/base/", "--keep-path")

	tail.events <- received("evt_1", "stripe", `{"id":"evt_1"}`)
	g := next(t, reqs)
	assert.Equal(t, "/base/stripe/events", g.path)
	assert.Equal(t, "a=1", g.query)
	// What the local server answers is shown, and changes nothing else.
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "→ 500") }, 5*time.Second, 10*time.Millisecond, out.String())

	// A local server that is not there is a line, not the end.
	local.Close()
	tail.events <- received("evt_2", "stripe", `{}`)
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "evt_2 ✗") }, 5*time.Second, 10*time.Millisecond, out.String())
}

func TestForwardStopsOnABadStart(t *testing.T) {
	tail := newFakeTail(t, "s3cret")
	wait := func(done chan int) int {
		select {
		case c := <-done:
			return c
		case <-time.After(10 * time.Second):
			t.Fatal("forward did not stop")
			return -1
		}
	}
	out, done, _ := forward(t, "--admin", tail.URL, "--token", "wrong", "--to", "http://127.0.0.1:1/x")
	assert.Equal(t, 3, wait(done), "a wrong token is not tried again")
	assert.Contains(t, out.String(), "401")

	out, done, _ = forward(t, "--admin", tail.URL, "--token", "s3cret")
	assert.Equal(t, 2, wait(done))
	assert.Contains(t, out.String(), "--to is required")

	out, done, _ = forward(t, "--admin", tail.URL, "--to", "localhost:3000")
	assert.Equal(t, 2, wait(done))
	assert.Contains(t, out.String(), "http or https URL")

	out, done, _ = forward(t, "--admin", tail.URL, "--to", "http://localhost:3000", "--header", "nope")
	assert.Equal(t, 2, wait(done))
	assert.Contains(t, out.String(), "Name: value")
}
