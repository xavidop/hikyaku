package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// tokenServer is an OAuth 2.0 token endpoint: it hands out tok-1, tok-2, …
type tokenServer struct {
	*httptest.Server
	mu       sync.Mutex
	calls    int
	last     *http.Request // with its form parsed
	status   int           // answer with this instead, when set
	lifetime int
}

func newTokenServer(t *testing.T) *tokenServer {
	t.Helper()
	ts := &tokenServer{lifetime: 3600}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		_ = r.ParseForm()
		ts.calls++
		ts.last = r
		w.Header().Set("Content-Type", "application/json")
		if ts.status != 0 {
			w.WriteHeader(ts.status)
			_, _ = fmt.Fprint(w, `{"error":"invalid_client","error_description":"unknown client"}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"access_token":"tok-%d","token_type":"Bearer","expires_in":%d}`, ts.calls, ts.lifetime)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func (ts *tokenServer) count() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.calls
}

// api records the Authorization header of each request, and refuses the
// tokens in revoked.
func api(t *testing.T, revoked ...string) (*httptest.Server, func() []string) {
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
				w.WriteHeader(401)
				return
			}
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func oauthSink(t *testing.T, apiURL string, o config.OAuth2) *Sink {
	t.Helper()
	return sinkFor(t, config.Endpoint{URL: apiURL, OAuth2: &o}, plugin.Egress{}).(*Sink)
}

func TestOAuth2FetchesOneTokenAndReusesIt(t *testing.T) {
	ts := newTokenServer(t)
	srv, seen := api(t)
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "s3cret/+", Scopes: []string{"read", "write"}, Audience: "https://api.test", Params: map[string]string{"resource": "crm"}})

	e := msg(`{}`)
	e.Headers["Authorization"] = []string{"Bearer the-callers"}
	for range 3 {
		_, err := s.Deliver(t.Context(), e)
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-1", "Bearer tok-1"}, seen())
	assert.Equal(t, 1, ts.count())

	req := ts.last
	assert.Equal(t, "client_credentials", req.PostForm.Get("grant_type"))
	assert.Equal(t, "read write", req.PostForm.Get("scope"))
	assert.Equal(t, "https://api.test", req.PostForm.Get("audience"))
	assert.Equal(t, "crm", req.PostForm.Get("resource"))
	assert.Empty(t, req.PostForm.Get("client_secret"))
	id, secret, ok := req.BasicAuth()
	require.True(t, ok)
	assert.Equal(t, "relay", id)
	assert.Equal(t, "s3cret%2F%2B", secret) // form-encoded, as RFC 6749 asks
}

func TestOAuth2ClientAuthInTheBody(t *testing.T) {
	ts := newTokenServer(t)
	srv, _ := api(t)
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "s3cret", ClientAuth: "body"})
	_, err := s.Deliver(t.Context(), msg(`{}`))
	require.NoError(t, err)
	assert.Equal(t, "relay", ts.last.PostForm.Get("client_id"))
	assert.Equal(t, "s3cret", ts.last.PostForm.Get("client_secret"))
	_, _, basic := ts.last.BasicAuth()
	assert.False(t, basic)
}

func TestOAuth2RefreshesBeforeTheTokenExpires(t *testing.T) {
	ts := newTokenServer(t)
	ts.lifetime = 300
	srv, seen := api(t)
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "k"})
	now := time.Now()
	s.oauth.now = func() time.Time { return now }

	deliver := func() {
		_, err := s.Deliver(t.Context(), msg(`{}`))
		require.NoError(t, err)
	}
	deliver()
	now = now.Add(4 * time.Minute) // 60s left: still good
	deliver()
	now = now.Add(45 * time.Second) // 15s left: inside the 30s margin
	deliver()
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-1", "Bearer tok-2"}, seen())
}

func TestOAuth2A401GetsANewTokenOnce(t *testing.T) {
	ts := newTokenServer(t)
	srv, seen := api(t, "tok-1")
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "k"})
	_, err := s.Deliver(t.Context(), msg(`{}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-2"}, seen())

	// A destination that refuses the new token too is a plain 401.
	srv2, seen2 := api(t, "tok-3", "tok-4")
	s2 := oauthSink(t, srv2.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "k"})
	_, err = s2.Deliver(t.Context(), msg(`{}`))
	require.Error(t, err)
	assert.Contains(t, plugin.Tags(err), "401")
	assert.Len(t, seen2(), 2)
}

func TestOAuth2TokenEndpointFailures(t *testing.T) {
	ts := newTokenServer(t)
	srv, seen := api(t)
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "s3cret"})

	ts.status = 401
	_, err := s.Deliver(t.Context(), msg(`{}`))
	require.Error(t, err)
	assert.Equal(t, plugin.Permanent, classOf(err))
	assert.Contains(t, plugin.Tags(err), TagOAuth2)
	assert.Contains(t, err.Error(), "invalid_client: unknown client")
	assert.NotContains(t, err.Error(), "s3cret")

	ts.status = 503
	_, err = s.Deliver(t.Context(), msg(`{}`))
	require.Error(t, err)
	assert.Equal(t, plugin.Transient, classOf(err))
	assert.Empty(t, seen(), "no request goes out without a token")

	ts.Close()
	_, err = s.Deliver(t.Context(), msg(`{}`))
	require.Error(t, err)
	assert.Equal(t, plugin.Transient, classOf(err))
	assert.Subset(t, plugin.Tags(err), []string{TagOAuth2, plugin.TagConnect})
}

func TestOAuth2ConcurrentDeliveriesShareOneFetch(t *testing.T) {
	ts := newTokenServer(t)
	srv, _ := api(t)
	s := oauthSink(t, srv.URL, config.OAuth2{TokenURL: ts.URL, ClientID: "relay", ClientSecret: "k"})
	var wg sync.WaitGroup
	var failed atomic.Int32
	for range 20 {
		wg.Go(func() {
			if _, err := s.Deliver(t.Context(), msg(`{}`)); err != nil {
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	assert.Zero(t, failed.Load())
	assert.Equal(t, 1, ts.count())
}

func TestOAuth2Settings(t *testing.T) {
	for want, o := range map[string]config.OAuth2{
		"oauth2.token_url is required":        {ClientID: "a", ClientSecret: "b"},
		"oauth2.client_id is required":        {TokenURL: "https://x.test/token", ClientSecret: "b"},
		"oauth2.client_secret is required":    {TokenURL: "https://x.test/token", ClientID: "a"},
		"is not an http(s) URL":               {TokenURL: "ftp://x.test", ClientID: "a", ClientSecret: "b"},
		"oauth2.client_auth must be basic or": {TokenURL: "https://x.test/token", ClientID: "a", ClientSecret: "b", ClientAuth: "jwt"},
	} {
		_, err := plugin.NewSink(t.Context(), plugin.Spec{Name: "dest", Kind: "http", Config: &config.Endpoint{Type: "http", URL: "http://example.com", OAuth2: &o}})
		assert.ErrorContains(t, err, want)
	}
}

func classOf(err error) plugin.Class {
	c, _ := plugin.Classify(err)
	return c
}
