package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// feed is a polled API that wants a bearer token: it records the
// Authorization header of each poll and refuses the tokens in revoked.
func feed(t *testing.T, revoked ...string) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a := r.Header.Get("Authorization")
		mu.Lock()
		seen = append(seen, a)
		mu.Unlock()
		for _, tok := range revoked {
			if a == "Bearer "+tok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func oauthPoller(t *testing.T, url string, o config.OAuth2) *Poller {
	t.Helper()
	src := config.Source{
		Mode: "poll", URL: url, Interval: config.Duration(time.Hour), OAuth2: &o,
		Retry: &config.Retry{Attempts: 1},
	}
	p, err := newPoller(plugin.Spec{Name: "tickets", Kind: "http", Config: &src}, &src)
	require.NoError(t, err)
	return p
}

func TestPollOAuth2SendsABearerTokenAndReusesIt(t *testing.T) {
	ts := newTokenServer(t)
	srv, seen := feed(t)
	p := oauthPoller(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "s3cret", Scopes: []string{"tickets.read"}})
	out := make(chan *envelope.Envelope, 4)

	require.NoError(t, p.pollOnce(context.Background(), out))
	require.NoError(t, p.pollOnce(context.Background(), out))
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-1"}, seen())
	assert.Equal(t, 1, ts.count(), "one token for both polls")
	assert.Equal(t, "tickets.read", ts.last.PostForm.Get("scope"))
	assert.Equal(t, "client_credentials", ts.last.PostForm.Get("grant_type"))
	require.Len(t, out, 2)
	e := <-out
	assert.JSONEq(t, `{"ok":true}`, string(e.Body))
	assert.Empty(t, e.Headers["Authorization"], "the token is not on the message")
}

func TestPollOAuth2A401GetsANewTokenOnce(t *testing.T) {
	ts := newTokenServer(t)
	srv, seen := feed(t, "tok-1")
	p := oauthPoller(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "s3cret"})
	out := make(chan *envelope.Envelope, 1)
	require.NoError(t, p.pollOnce(context.Background(), out))
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-2"}, seen())
	require.Len(t, out, 1)

	// An API that refuses every token: the poll fails after one new token.
	ts2 := newTokenServer(t)
	srv2, seen2 := feed(t, "tok-1", "tok-2")
	p = oauthPoller(t, srv2.URL, config.OAuth2{TokenURL: ts2.URL, ClientID: "relay", ClientSecret: "s3cret"})
	err := p.pollOnce(context.Background(), out)
	require.Error(t, err)
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-2"}, seen2())
	assert.Error(t, p.Health(context.Background()))
}

func TestPollOAuth2WithoutATokenPollsNothing(t *testing.T) {
	ts := newTokenServer(t)
	ts.status = 401
	srv, seen := feed(t)
	p := oauthPoller(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "wrong"})
	out := make(chan *envelope.Envelope, 1)
	err := p.pollOnce(context.Background(), out)
	require.Error(t, err)
	assert.Contains(t, plugin.Tags(err), TagOAuth2)
	assert.Empty(t, seen(), "the API is not called without a token")
	assert.Empty(t, out)
	assert.NotContains(t, err.Error(), "wrong")
}
