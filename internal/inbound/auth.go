// Package inbound authenticates requests to listen sources: HMAC body
// signatures, the signatures of well-known webhook senders, bearer tokens,
// JWTs verified against a JWKS, API keys and IP allowlists. Every
// configured method must pass.
package inbound

import (
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // sha1 HMAC is what some webhook senders still sign with
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/xavidop/hikyaku/internal/config"
)

// ErrUnauthorized is the failure of every check; the reason is wrapped.
var ErrUnauthorized = errors.New("unauthorized")

// Authenticator checks requests against a source's auth: block.
type Authenticator struct {
	hmac    *config.HMACAuth
	hmacNew func() hash.Hash
	bearer  [][]byte
	apiHdr  string
	apiKeys [][]byte
	// apiQuery is the query parameter that may carry the key ("": none).
	apiQuery string
	jwt      *jwtCheck
	allow    []*net.IPNet
	webhook  *webhookCheck
}

type jwtCheck struct {
	kf   keyfunc.Keyfunc
	opts []jwt.ParserOption
}

// New builds an authenticator; nil auth accepts everything. A JWKS is
// fetched in the background and refreshed.
func New(ctx context.Context, a *config.Auth) (*Authenticator, error) {
	out := &Authenticator{}
	if a == nil {
		return out, nil
	}
	if h := a.HMAC; h != nil {
		out.hmac = h
		switch h.Algo {
		case "", "sha256":
			out.hmacNew = sha256.New
		case "sha1":
			out.hmacNew = sha1.New
		case "sha512":
			out.hmacNew = sha512.New
		default:
			return nil, fmt.Errorf("auth.hmac.algo %q: use sha256, sha1 or sha512", h.Algo)
		}
	}
	if b := a.Bearer; b != nil {
		for _, t := range b.Tokens {
			out.bearer = append(out.bearer, []byte(t))
		}
	}
	if k := a.APIKey; k != nil {
		out.apiHdr = k.Header
		if out.apiHdr == "" {
			out.apiHdr = "X-API-Key"
		}
		out.apiQuery = k.Query
		for _, key := range k.Keys {
			out.apiKeys = append(out.apiKeys, []byte(key))
		}
	}
	if j := a.JWT; j != nil {
		kf, err := keyfunc.NewDefaultCtx(ctx, []string{j.JWKSURL})
		if err != nil {
			return nil, fmt.Errorf("auth.jwt: %w", err)
		}
		algs := j.Algorithms
		if len(algs) == 0 {
			algs = []string{"RS256", "ES256"}
		}
		opts := []jwt.ParserOption{jwt.WithValidMethods(algs), jwt.WithLeeway(30 * time.Second)}
		if j.Issuer != "" {
			opts = append(opts, jwt.WithIssuer(j.Issuer))
		}
		if j.Audience != "" {
			opts = append(opts, jwt.WithAudience(j.Audience))
		}
		out.jwt = &jwtCheck{kf: kf, opts: opts}
	}
	if w := a.Webhook; w != nil {
		wc, err := newWebhookCheck(w)
		if err != nil {
			return nil, err
		}
		out.webhook = wc
	}
	for _, ip := range a.IPAllow {
		if !strings.Contains(ip, "/") {
			if strings.Contains(ip, ":") {
				ip += "/128"
			} else {
				ip += "/32"
			}
		}
		_, n, err := net.ParseCIDR(ip)
		if err != nil {
			return nil, fmt.Errorf("auth.ip_allow %q: %w", ip, err)
		}
		out.allow = append(out.allow, n)
	}
	return out, nil
}

// NeedsBody reports whether the checks read the request body (HMAC, or a
// webhook sender's signature).
func (a *Authenticator) NeedsBody() bool { return a != nil && (a.hmac != nil || a.webhook != nil) }

// Check verifies a request; body is the request body for HMAC (nil when
// there is none, as on a WebSocket upgrade).
func (a *Authenticator) Check(r *http.Request, body []byte) error {
	if a == nil {
		return nil
	}
	if len(a.allow) > 0 {
		if err := a.checkIP(r); err != nil {
			return err
		}
	}
	if a.hmac != nil {
		if err := a.checkHMAC(r, body); err != nil {
			return err
		}
	}
	if a.webhook != nil {
		if err := a.webhook.check(r, body); err != nil {
			return err
		}
	}
	if len(a.bearer) > 0 {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !anyEqual(a.bearer, []byte(strings.TrimSpace(token))) {
			return fmt.Errorf("%w: missing or wrong bearer token", ErrUnauthorized)
		}
	}
	if len(a.apiKeys) > 0 {
		key, where := r.Header.Get(a.apiHdr), a.apiHdr
		if key == "" && a.apiQuery != "" {
			key, where = r.URL.Query().Get(a.apiQuery), a.apiHdr+" (or the "+a.apiQuery+" query parameter)"
		}
		if !anyEqual(a.apiKeys, []byte(key)) {
			return fmt.Errorf("%w: missing or wrong %s", ErrUnauthorized, where)
		}
	}
	if a.jwt != nil {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			return fmt.Errorf("%w: missing bearer JWT", ErrUnauthorized)
		}
		if _, err := jwt.Parse(strings.TrimSpace(raw), a.jwt.kf.Keyfunc, a.jwt.opts...); err != nil {
			return fmt.Errorf("%w: jwt: %v", ErrUnauthorized, err)
		}
	}
	return nil
}

func (a *Authenticator) checkIP(r *http.Request) error {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	for _, n := range a.allow {
		if ip != nil && n.Contains(ip) {
			return nil
		}
	}
	return fmt.Errorf("%w: client %s is not in ip_allow", ErrUnauthorized, host)
}

func (a *Authenticator) checkHMAC(r *http.Request, body []byte) error {
	if body == nil {
		return fmt.Errorf("%w: hmac needs a request body", ErrUnauthorized)
	}
	got := strings.TrimSpace(r.Header.Get(a.hmac.Header))
	if got == "" {
		return fmt.Errorf("%w: missing %s", ErrUnauthorized, a.hmac.Header)
	}
	if a.hmac.Prefix != "" {
		got = strings.TrimPrefix(got, a.hmac.Prefix)
	} else if i := strings.IndexByte(got, '='); i > 0 && i < 10 {
		got = got[i+1:] // a conventional "sha256=" prefix
	}
	mac := hmac.New(a.hmacNew, []byte(a.hmac.Secret))
	mac.Write(body)
	sum := mac.Sum(nil)
	var want string
	if a.hmac.Encoding == "base64" {
		want = base64.StdEncoding.EncodeToString(sum)
	} else {
		want = hex.EncodeToString(sum)
		got = strings.ToLower(got)
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return fmt.Errorf("%w: bad %s signature", ErrUnauthorized, a.hmac.Header)
	}
	return nil
}

// credentialHeaders carry a caller's credentials.
var credentialHeaders = map[string]bool{
	"Authorization": true, "Proxy-Authorization": true, "Cookie": true,
	"X-Api-Key": true, "Api-Key": true,
}

// StripCredentials removes a caller's credentials from the headers of a
// message: Authorization, Proxy-Authorization, Cookie, X-Api-Key, Api-Key
// and the auth.api_key header. Listen sources call it before a message is
// built, so no destination, DLQ record, history entry or tail sees them.
// a may be nil (a source without auth).
func (a *Authenticator) StripCredentials(h map[string][]string) {
	apiHdr := ""
	if a != nil && a.apiHdr != "" {
		apiHdr = http.CanonicalHeaderKey(a.apiHdr)
	}
	for k := range h {
		ck := http.CanonicalHeaderKey(k)
		if credentialHeaders[ck] || ck == apiHdr {
			delete(h, k)
		}
	}
}

// StripQuery removes the auth.api_key query parameter from a raw query, so
// a key passed in the URL reaches no message either. a may be nil.
func (a *Authenticator) StripQuery(raw string) string {
	if a == nil || a.apiQuery == "" || raw == "" {
		return raw
	}
	// Pairs are dropped by name, as url.Values reads them; the others keep
	// their encoding.
	parts := strings.Split(raw, "&")
	kept := parts[:0]
	for _, p := range parts {
		k, _, _ := strings.Cut(p, "=")
		if uk, err := url.QueryUnescape(k); k == a.apiQuery || (err == nil && uk == a.apiQuery) {
			continue
		}
		kept = append(kept, p)
	}
	return strings.Join(kept, "&")
}

func anyEqual(keys [][]byte, got []byte) bool {
	ok := 0
	for _, k := range keys {
		ok |= subtle.ConstantTimeCompare(k, got)
	}
	return ok == 1 && len(got) > 0
}

// Sign computes the hex HMAC-SHA256 a sender would put in a signature
// header (tests and hikyaku send).
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
