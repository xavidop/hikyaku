package sse

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestSink(t *testing.T, ep config.Endpoint) *Sink {
	t.Helper()
	ep.Type = "sse"
	if ep.Listen == "" {
		ep.Listen = "127.0.0.1:0"
	}
	s, err := newSink(context.Background(), plugin.Spec{Name: "dash", Kind: "sse", Config: &ep})
	require.NoError(t, err)
	require.NoError(t, s.Start(context.Background()))
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// stream connects a client, resuming after lastID when it is set.
func stream(t *testing.T, s *Sink, lastID string) (*http.Response, *Parser) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+s.opts.path, nil)
	require.NoError(t, err)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := httpc.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp, NewParser(resp.Body, 0)
}

// nextEvent reads one event, failing the test after a while.
func nextEvent(t *testing.T, p *Parser) Event {
	t.Helper()
	type result struct {
		ev  Event
		err error
	}
	c := make(chan result, 1)
	go func() {
		ev, err := p.Next()
		c <- result{ev, err}
	}()
	select {
	case r := <-c:
		require.NoError(t, r.err)
		return r.ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event arrived")
		return Event{}
	}
}

func msg(subject, body string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	env.Subject = subject
	return env
}

func clients(s *Sink) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

func TestSinkBroadcastsEvents(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Path: "/events"})
	resp, p := stream(t, s, "")
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	_, p2 := stream(t, s, "")

	env := msg("orders", "line one\nline two")
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:0/events", res.Endpoint)
	want := Event{ID: env.ID, Name: "orders", Data: []byte("line one\nline two")}
	assert.Equal(t, want, nextEvent(t, p))
	assert.Equal(t, want, nextEvent(t, p2), "every client gets every event")

	unnamed := msg("", `{"a":1}`)
	_, err = s.Deliver(context.Background(), unnamed)
	require.NoError(t, err)
	assert.Equal(t, Event{ID: unnamed.ID, Data: []byte(`{"a":1}`)}, nextEvent(t, p), "no subject: no event line")
	assert.NoError(t, s.Health(context.Background()))
}

func TestSinkEventTemplate(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Event: "{{ .json.type }}"})
	_, p := stream(t, s, "")
	_, err := s.Deliver(context.Background(), msg("ignored", `{"type":"invoice.paid"}`))
	require.NoError(t, err)
	assert.Equal(t, "invoice.paid", nextEvent(t, p).Name)
	_, err = s.Deliver(context.Background(), msg("ignored", `{}`))
	require.NoError(t, err)
	assert.Empty(t, nextEvent(t, p).Name, "a template that renders nothing names nothing")

	bad := newTestSink(t, config.Endpoint{Event: `{{ index .json "a" "b" }}`})
	_, err = bad.Deliver(context.Background(), msg("", `{"a":1}`))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a message the template cannot render will never render")
}

func TestSinkReplaysAfterLastEventID(t *testing.T) {
	s := newTestSink(t, config.Endpoint{})
	ctx := context.Background()
	var sent []*envelope.Envelope
	for _, b := range []string{"1", "2", "3"} {
		env := msg("n", b)
		_, err := s.Deliver(ctx, env)
		require.NoError(t, err, "delivering with no client connected succeeds")
		sent = append(sent, env)
	}

	_, p := stream(t, s, sent[0].ID)
	assert.Equal(t, "2", string(nextEvent(t, p).Data))
	third := nextEvent(t, p)
	assert.Equal(t, "3", string(third.Data))
	assert.Equal(t, sent[2].ID, third.ID)
	live := msg("n", "4")
	_, err := s.Deliver(ctx, live)
	require.NoError(t, err)
	assert.Equal(t, "4", string(nextEvent(t, p).Data), "then it goes live")

	_, fresh := stream(t, s, "")
	_, unknown := stream(t, s, "not-an-id")
	_, err = s.Deliver(ctx, msg("n", "5"))
	require.NoError(t, err)
	assert.Equal(t, "5", string(nextEvent(t, fresh).Data), "no Last-Event-ID: live only")
	assert.Equal(t, "5", string(nextEvent(t, unknown).Data), "an unknown id: nothing replayed, live only")

	_, latest := stream(t, s, live.ID)
	_, err = s.Deliver(ctx, msg("n", "6"))
	require.NoError(t, err)
	assert.Equal(t, "5", string(nextEvent(t, latest).Data))
	assert.Equal(t, "6", string(nextEvent(t, latest).Data))
}

func TestSinkReplayRingIsBounded(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Replay: 2})
	ctx := context.Background()
	var sent []*envelope.Envelope
	for _, b := range []string{"1", "2", "3", "4"} {
		env := msg("", b)
		_, err := s.Deliver(ctx, env)
		require.NoError(t, err)
		sent = append(sent, env)
	}
	_, gone := stream(t, s, sent[1].ID) // "2" is no longer kept
	_, kept := stream(t, s, sent[2].ID)
	_, err := s.Deliver(ctx, msg("", "5"))
	require.NoError(t, err)
	assert.Equal(t, "5", string(nextEvent(t, gone).Data))
	assert.Equal(t, "4", string(nextEvent(t, kept).Data))
	assert.Equal(t, "5", string(nextEvent(t, kept).Data))
}

func TestSinkDropsAClientThatFallsBehind(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Replay: 4})
	slow, _ := stream(t, s, "") // never reads
	_, fast := stream(t, s, "")
	fastEvents := make(chan Event, 1)
	go func() {
		for {
			ev, err := fast.Next()
			if err != nil {
				close(fastEvents)
				return
			}
			fastEvents <- ev
		}
	}()

	ctx := context.Background()
	big := strings.Repeat("x", 64<<10)
	delivered := 0
	for clients(s) == 2 {
		require.Less(t, delivered, 5000, "the slow client was never dropped")
		start := time.Now()
		_, err := s.Deliver(ctx, msg("", big))
		require.NoError(t, err)
		require.Less(t, time.Since(start), time.Second, "Deliver never waits for a client")
		delivered++
		select { // keep pace with the fast client, so only the slow one falls behind
		case _, ok := <-fastEvents:
			require.True(t, ok, "the fast client's stream ended")
		case <-time.After(5 * time.Second):
			t.Fatal("the fast client got nothing")
		}
	}
	assert.Greater(t, delivered, clientBuffer)

	// The dropped client's stream is cut: reading it ends (in an error, as
	// the stream stops mid-event).
	ended := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, slow.Body)
		close(ended)
	}()
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the slow client's stream stayed open")
	}
	_, err := s.Deliver(ctx, msg("", "still here"))
	require.NoError(t, err)
	select {
	case ev := <-fastEvents:
		assert.Equal(t, "still here", string(ev.Data))
	case <-time.After(5 * time.Second):
		t.Fatal("the fast client stopped getting events")
	}
}

func TestSinkPings(t *testing.T) {
	s := newTestSink(t, config.Endpoint{})
	s.mu.Lock()
	s.pingEvery = 10 * time.Millisecond
	s.mu.Unlock()
	resp, _ := stream(t, s, "")
	lines := bufio.NewScanner(resp.Body)
	done := make(chan bool, 1)
	go func() {
		for lines.Scan() {
			if lines.Text() == ": ping" {
				done <- true
				return
			}
		}
		done <- false
	}()
	select {
	case ok := <-done:
		assert.True(t, ok)
	case <-time.After(3 * time.Second):
		t.Fatal("no ping comment")
	}
}

func TestSinkClose(t *testing.T) {
	s := newTestSink(t, config.Endpoint{})
	resp, _ := stream(t, s, "")
	addr := s.Addr()
	start := time.Now()
	require.NoError(t, s.Close(context.Background()))
	assert.Less(t, time.Since(start), 2*time.Second, "close does not wait for clients")
	_, err := io.Copy(io.Discard, resp.Body)
	assert.NoError(t, err, "the client's stream ended")

	_, err = s.Deliver(context.Background(), msg("", "x"))
	assert.ErrorIs(t, err, errClosed)
	assert.ErrorIs(t, s.Health(context.Background()), errClosed)
	assert.NoError(t, s.Close(context.Background()))
	_, err = net.DialTimeout("tcp", addr, time.Second)
	assert.Error(t, err, "nothing serves the address any more")
}

func TestSinkServesOnlyGet(t *testing.T) {
	s := newTestSink(t, config.Endpoint{})
	resp, err := httpc.Post("http://"+s.Addr()+"/", "text/plain", strings.NewReader("x"))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	resp, err = httpc.Head("http://" + s.Addr() + "/")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Zero(t, clients(s))
}

func TestSinkAuth(t *testing.T) {
	s := newTestSink(t, config.Endpoint{Path: "/events", Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"dash-token"}}}})
	get := func(method, token string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, "http://"+s.Addr()+"/events", nil)
		require.NoError(t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpc.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}
	assert.Equal(t, http.StatusUnauthorized, get(http.MethodGet, "").StatusCode)
	assert.Equal(t, http.StatusUnauthorized, get(http.MethodGet, "wrong").StatusCode)
	assert.Equal(t, http.StatusUnauthorized, get(http.MethodHead, "").StatusCode)
	assert.Zero(t, clients(s), "a refused client never joins the stream")

	resp := get(http.MethodGet, "dash-token")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Eventually(t, func() bool { return clients(s) == 1 }, 5*time.Second, 5*time.Millisecond)
	env := msg("orders", "paid")
	_, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, Event{ID: env.ID, Name: "orders", Data: []byte("paid")}, nextEvent(t, NewParser(resp.Body, 0)))

	// A browser's EventSource cannot set headers: the key goes in the query.
	browser := newTestSink(t, config.Endpoint{Auth: &config.Auth{APIKey: &config.APIKeyAuth{Query: "access_token", Keys: []string{"k1"}}}})
	for target, want := range map[string]int{"/?access_token=k1": http.StatusOK, "/?access_token=no": http.StatusUnauthorized, "/": http.StatusUnauthorized} {
		r, err := httpc.Head("http://" + browser.Addr() + target)
		require.NoError(t, err)
		_ = r.Body.Close()
		assert.Equal(t, want, r.StatusCode, target)
	}

	// ip_allow: loopback is not in 10.0.0.0/8.
	fenced := newTestSink(t, config.Endpoint{Auth: &config.Auth{IPAllow: []string{"10.0.0.0/8"}}})
	r, err := httpc.Get("http://" + fenced.Addr() + "/")
	require.NoError(t, err)
	_ = r.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, r.StatusCode)

	_, err = newSink(context.Background(), plugin.Spec{Name: "dash", Kind: "sse", Config: &config.Endpoint{
		Type: "sse", Listen: "127.0.0.1:0", Auth: &config.Auth{HMAC: &config.HMACAuth{Secret: "s", Header: "X-Sig"}},
	}})
	assert.ErrorContains(t, err, "auth.hmac does not apply")
}

func TestEncode(t *testing.T) {
	assert.Equal(t, "id: 1\nevent: e\ndata: a\ndata: b\ndata: \ndata: c\ndata: d\n\n",
		string(encode("1", "e", []byte("a\r\nb\n\rc\rd"))), "LF, CR and CRLF all split lines")
	assert.Equal(t, "data: \n\n", string(encode("", "", nil)))
	assert.Equal(t, "data: x\ndata: \n\n", string(encode("", "", []byte("x\n"))))
	assert.Equal(t, "ab", clean("a\r\nb", false))
	assert.Equal(t, "ab", clean("a\x00b", true))

	// What encode writes, the parser reads back.
	for _, body := range []string{"plain", "two\nlines", "", " leading space", "trailing\n", "crlf\r\nin"} {
		p := NewParser(strings.NewReader(string(encode("i", "n", []byte(body)))), 0)
		ev, err := p.Next()
		require.NoError(t, err, body)
		want := strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(body)
		assert.Equal(t, Event{ID: "i", Name: "n", Data: []byte(want)}, ev, body)
	}
}

func TestSinkBindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	s, err := newSink(context.Background(), plugin.Spec{Name: "dash", Kind: "sse",
		Config: &config.Endpoint{Type: "sse", Listen: ln.Addr().String()}})
	require.NoError(t, err, "building the sink binds nothing")
	assert.Empty(t, s.Addr())
	_, err = s.Deliver(context.Background(), envelope.New("s", "t", []byte("x")))
	assert.NoError(t, err, "before Start, deliveries only reach the replay buffer")
	assert.ErrorContains(t, s.Start(context.Background()), "destination dash: listen")
	assert.NoError(t, s.Close(context.Background()))
}
