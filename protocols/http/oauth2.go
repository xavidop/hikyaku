package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// TagOAuth2 marks a delivery that failed because no access token could be
// fetched.
const TagOAuth2 = "oauth2"

const (
	// tokenTimeout bounds one request to the token endpoint.
	tokenTimeout = 15 * time.Second
	// tokenSkew is how long before its expiry a token is replaced, so a
	// request never leaves with a token about to run out.
	tokenSkew = 30 * time.Second
	// maxTokenResponse bounds the token endpoint's answer.
	maxTokenResponse = 1 << 20
)

// tokenSource fetches an OAuth 2.0 access token with the client credentials
// grant (RFC 6749, section 4.4) and keeps it until shortly before it
// expires. One fetch runs at a time: the deliveries that need a token while
// it runs wait for it.
type tokenSource struct {
	tokenURL string
	id       string
	secret   string
	inBody   bool // client_auth: body
	form     url.Values
	client   *http.Client
	now      func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time // zero: the server gave no lifetime
}

func newTokenSource(o *config.OAuth2, client *http.Client) (*tokenSource, error) {
	switch {
	case o.TokenURL == "":
		return nil, errors.New("oauth2.token_url is required")
	case o.ClientID == "":
		return nil, errors.New("oauth2.client_id is required")
	case o.ClientSecret == "":
		return nil, errors.New("oauth2.client_secret is required")
	}
	u, err := url.Parse(o.TokenURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("oauth2.token_url %q is not an http(s) URL", o.TokenURL)
	}
	ts := &tokenSource{tokenURL: o.TokenURL, id: o.ClientID, secret: o.ClientSecret, client: client, now: time.Now}
	switch o.ClientAuth {
	case "", "basic":
	case "body":
		ts.inBody = true
	default:
		return nil, fmt.Errorf("oauth2.client_auth must be basic or body, not %q", o.ClientAuth)
	}
	ts.form = url.Values{"grant_type": {"client_credentials"}}
	if len(o.Scopes) > 0 {
		ts.form.Set("scope", strings.Join(o.Scopes, " "))
	}
	if o.Audience != "" {
		ts.form.Set("audience", o.Audience)
	}
	for k, v := range o.Params {
		ts.form.Set(k, v)
	}
	if ts.inBody {
		ts.form.Set("client_id", ts.id)
		ts.form.Set("client_secret", ts.secret)
	}
	return ts, nil
}

// get returns a token that is good for a while yet, fetching one if needed.
func (t *tokenSource) get(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && (t.expires.IsZero() || t.now().Before(t.expires.Add(-tokenSkew))) {
		return t.token, nil
	}
	tok, life, err := t.fetch(ctx)
	if err != nil {
		return "", err
	}
	t.token, t.expires = tok, time.Time{}
	if life > 0 {
		t.expires = t.now().Add(life)
	}
	return tok, nil
}

// drop forgets tok, which the destination refused: the next get fetches a
// new one. A token that was already replaced is kept.
func (t *tokenSource) drop(tok string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token == tok {
		t.token = ""
	}
}

func (t *tokenSource) fetch(ctx context.Context) (string, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, tokenTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.tokenURL, strings.NewReader(t.form.Encode()))
	if err != nil {
		return "", 0, plugin.Wrap(plugin.Permanent, []string{TagOAuth2}, fmt.Errorf("oauth2: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "hikyaku")
	if !t.inBody {
		// RFC 6749, section 2.3.1: the ID and secret are form-encoded first.
		req.SetBasicAuth(url.QueryEscape(t.id), url.QueryEscape(t.secret))
	}
	resp, err := t.client.Do(req)
	if err != nil {
		class, tag := plugin.Transient, plugin.TagConnect
		if errors.Is(err, context.DeadlineExceeded) {
			class, tag = plugin.Timeout, plugin.TagTimeout
		}
		if errors.Is(err, context.Canceled) {
			return "", 0, err
		}
		return "", 0, plugin.Wrap(class, []string{TagOAuth2, tag}, fmt.Errorf("oauth2: the token request failed: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponse))
	if err != nil {
		return "", 0, plugin.Wrap(plugin.Transient, []string{TagOAuth2, plugin.TagConnect}, fmt.Errorf("oauth2: reading the token answer: %w", err))
	}
	var ans struct {
		AccessToken string      `json:"access_token"`
		TokenType   string      `json:"token_type"`
		ExpiresIn   json.Number `json:"expires_in"`
		Error       string      `json:"error"`
		Description string      `json:"error_description"`
	}
	_ = json.Unmarshal(data, &ans)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		why := ans.Error
		if ans.Description != "" {
			why += ": " + ans.Description
		}
		if why == "" {
			why = snippet(data)
		}
		// A server in trouble may recover; refused credentials will not.
		class := plugin.Permanent
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			class = plugin.Transient
		}
		return "", 0, plugin.Wrap(class, []string{TagOAuth2}, fmt.Errorf("oauth2: the token endpoint answered %d: %s", resp.StatusCode, why))
	}
	if ans.AccessToken == "" {
		return "", 0, plugin.Wrap(plugin.Permanent, []string{TagOAuth2}, errors.New("oauth2: the token endpoint's answer has no access_token"))
	}
	if ans.TokenType != "" && !strings.EqualFold(ans.TokenType, "bearer") {
		return "", 0, plugin.Wrap(plugin.Permanent, []string{TagOAuth2}, fmt.Errorf("oauth2: token type %q is not supported (bearer is)", ans.TokenType))
	}
	var life time.Duration
	if f, err := ans.ExpiresIn.Float64(); err == nil && f > 0 {
		life = time.Duration(f * float64(time.Second))
	}
	return ans.AccessToken, life, nil
}
