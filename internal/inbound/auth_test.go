package inbound

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

func req(headers map[string]string, remote string) *http.Request {
	r := httptest.NewRequest("POST", "/ingest", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	if remote != "" {
		r.RemoteAddr = remote
	}
	return r
}

func TestNoAuthAcceptsAll(t *testing.T) {
	a, err := New(context.Background(), nil)
	require.NoError(t, err)
	assert.NoError(t, a.Check(req(nil, ""), nil))
	assert.False(t, a.NeedsBody())
	var nilA *Authenticator
	assert.NoError(t, nilA.Check(req(nil, ""), nil))
}

func TestHMAC(t *testing.T) {
	body := []byte(`{"event":"invoice.paid"}`)
	a, err := New(context.Background(), &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "s3cret"}})
	require.NoError(t, err)
	assert.True(t, a.NeedsBody())
	sig := Sign("s3cret", body)
	assert.NoError(t, a.Check(req(map[string]string{"X-Signature-256": "sha256=" + sig}, ""), body), "GitHub-style prefix")
	assert.NoError(t, a.Check(req(map[string]string{"X-Signature-256": sig}, ""), body))
	assert.ErrorIs(t, a.Check(req(map[string]string{"X-Signature-256": "sha256=" + Sign("other", body)}, ""), body), ErrUnauthorized)
	assert.ErrorIs(t, a.Check(req(nil, ""), body), ErrUnauthorized)
	assert.ErrorContains(t, a.Check(req(map[string]string{"X-Signature-256": sig}, ""), nil), "needs a request body")

	b64, err := New(context.Background(), &config.Auth{HMAC: &config.HMACAuth{Header: "X-Sig", Secret: "k", Algo: "sha512", Encoding: "base64", Prefix: "v1,"}})
	require.NoError(t, err)
	assert.Error(t, b64.Check(req(map[string]string{"X-Sig": "v1,nope"}, ""), body))
	_, err = New(context.Background(), &config.Auth{HMAC: &config.HMACAuth{Algo: "md5"}})
	assert.Error(t, err)
}

func TestBearerAPIKeyAndIP(t *testing.T) {
	a, err := New(context.Background(), &config.Auth{
		Bearer:  &config.BearerAuth{Tokens: []string{"t1", "t2"}},
		APIKey:  &config.APIKeyAuth{Keys: []string{"k1"}},
		IPAllow: []string{"10.0.0.0/8", "192.168.1.7"},
	})
	require.NoError(t, err)
	ok := map[string]string{"Authorization": "Bearer t2", "X-API-Key": "k1"}
	assert.NoError(t, a.Check(req(ok, "10.1.2.3:5555"), nil))
	assert.NoError(t, a.Check(req(ok, "192.168.1.7:1"), nil))
	assert.ErrorContains(t, a.Check(req(ok, "8.8.8.8:1"), nil), "ip_allow")
	assert.ErrorContains(t, a.Check(req(map[string]string{"Authorization": "Bearer nope", "X-API-Key": "k1"}, "10.0.0.1:1"), nil), "bearer")
	assert.ErrorContains(t, a.Check(req(map[string]string{"Authorization": "Bearer t1"}, "10.0.0.1:1"), nil), "X-API-Key")
	_, err = New(context.Background(), &config.Auth{IPAllow: []string{"nope"}})
	assert.Error(t, err)
}

func TestAPIKeyInTheQuery(t *testing.T) {
	a, err := New(context.Background(), &config.Auth{APIKey: &config.APIKeyAuth{Query: "access_token", Keys: []string{"k1"}}})
	require.NoError(t, err)
	inQuery := func(target string, headers map[string]string) *http.Request {
		r := httptest.NewRequest("GET", target, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return r
	}
	assert.NoError(t, a.Check(inQuery("/events?access_token=k1", nil), nil), "a browser's EventSource cannot set headers")
	assert.NoError(t, a.Check(inQuery("/events", map[string]string{"X-API-Key": "k1"}), nil), "the header still works")
	assert.ErrorContains(t, a.Check(inQuery("/events?access_token=nope", nil), nil), "access_token query parameter")
	assert.ErrorIs(t, a.Check(inQuery("/events", nil), nil), ErrUnauthorized)
	assert.ErrorIs(t, a.Check(inQuery("/events?access_token=k1", map[string]string{"X-API-Key": "wrong"}), nil), ErrUnauthorized,
		"a header that is present wins over the query")

	headerOnly, err := New(context.Background(), &config.Auth{APIKey: &config.APIKeyAuth{Keys: []string{"k1"}}})
	require.NoError(t, err)
	assert.ErrorIs(t, headerOnly.Check(inQuery("/events?access_token=k1", nil), nil), ErrUnauthorized, "no query unless configured")
}

func TestStripQuery(t *testing.T) {
	a, err := New(context.Background(), &config.Auth{APIKey: &config.APIKeyAuth{Query: "access_token", Keys: []string{"k"}}})
	require.NoError(t, err)
	assert.Equal(t, "page=2&q=a%20b", a.StripQuery("access_token=k&page=2&q=a%20b"))
	assert.Equal(t, "page=2", a.StripQuery("page=2&access%5Ftoken=k"), "an escaped name is the same parameter")
	assert.Equal(t, "", a.StripQuery("access_token=k"))
	assert.Equal(t, "page=2", a.StripQuery("page=2"))

	var none *Authenticator
	assert.Equal(t, "access_token=k", none.StripQuery("access_token=k"), "no auth: nothing is a credential")
}

func TestJWTAgainstJWKS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := New(ctx, &config.Auth{JWT: &config.JWTAuth{JWKSURL: srv.URL, Issuer: "https://issuer", Audience: "hikyaku"}})
	require.NoError(t, err)

	sign := func(claims jwt.MapClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = "k1"
		s, err := tok.SignedString(key)
		require.NoError(t, err)
		return s
	}
	good := sign(jwt.MapClaims{"iss": "https://issuer", "aud": "hikyaku", "exp": time.Now().Add(time.Hour).Unix()})
	require.Eventually(t, func() bool {
		return a.Check(req(map[string]string{"Authorization": "Bearer " + good}, ""), nil) == nil
	}, 3*time.Second, 20*time.Millisecond)
	bad := sign(jwt.MapClaims{"iss": "https://evil", "aud": "hikyaku", "exp": time.Now().Add(time.Hour).Unix()})
	assert.ErrorContains(t, a.Check(req(map[string]string{"Authorization": "Bearer " + bad}, ""), nil), "jwt")
	expired := sign(jwt.MapClaims{"iss": "https://issuer", "aud": "hikyaku", "exp": time.Now().Add(-time.Hour).Unix()})
	assert.Error(t, a.Check(req(map[string]string{"Authorization": "Bearer " + expired}, ""), nil))
	assert.ErrorContains(t, a.Check(req(nil, ""), nil), "missing bearer JWT")
}

func TestStripCredentials(t *testing.T) {
	a, err := New(context.Background(), &config.Auth{APIKey: &config.APIKeyAuth{Header: "x-relay-key", Keys: []string{"k"}}})
	require.NoError(t, err)
	h := map[string][]string{
		"Authorization": {"Bearer t"}, "Proxy-Authorization": {"Basic x"}, "Cookie": {"a=b"},
		"X-Api-Key": {"1"}, "Api-Key": {"2"}, "X-Relay-Key": {"k"}, "x-other": {"kept"}, "X-Event": {"kept"},
	}
	a.StripCredentials(h)
	assert.Equal(t, map[string][]string{"x-other": {"kept"}, "X-Event": {"kept"}}, h)

	var none *Authenticator
	h = map[string][]string{"authorization": {"Bearer t"}, "X-Event": {"kept"}}
	none.StripCredentials(h)
	assert.Equal(t, map[string][]string{"X-Event": {"kept"}}, h, "a source without auth strips too, whatever the key's case")
}
