package http

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

// capture serves one endpoint and keeps the last request it got.
func capture(t *testing.T) (url string, last func() (*http.Request, []byte)) {
	t.Helper()
	var got *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(204)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() (*http.Request, []byte) { return got, body }
}

// A signed destination and a source's auth.hmac with the same settings
// agree: hikyaku can verify what hikyaku sent.
func TestSinkSignsWhatASourceVerifies(t *testing.T) {
	url, last := capture(t)
	s := sinkFor(t, config.Endpoint{URL: url, Body: `{"order":"{{ .json.id }}"}`,
		Sign: &config.Sign{Secret: "s3cret", Prefix: "sha256="}}, plugin.Egress{})
	e := msg(`{"id":"o-1"}`)
	e.Headers["X-Signature-256"] = []string{"sha256=the-sender's-own"}
	_, err := s.Deliver(context.Background(), e)
	require.NoError(t, err)
	req, body := last()
	assert.JSONEq(t, `{"order":"o-1"}`, string(body))

	mac := hmac.New(sha256.New, []byte("s3cret"))
	mac.Write(body)
	assert.Equal(t, []string{"sha256=" + hex.EncodeToString(mac.Sum(nil))}, req.Header.Values("X-Signature-256"),
		"the signature covers the body that went out, and replaces one the message came in with")

	auth, err := inbound.New(context.Background(), &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "s3cret", Prefix: "sha256="}})
	require.NoError(t, err)
	assert.NoError(t, auth.Check(req, body))
	assert.Error(t, auth.Check(req, []byte(`{"order":"o-2"}`)), "a changed body no longer verifies")
}

func TestSinkSignOptions(t *testing.T) {
	url, last := capture(t)
	s := sinkFor(t, config.Endpoint{URL: url, Sign: &config.Sign{
		Secret: "k", Header: "Webhook-Signature", Algo: "sha512", Encoding: "base64", TimestampHeader: "Webhook-Timestamp",
	}}, plugin.Egress{})
	s.(*Sink).signer.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	_, err := s.Deliver(context.Background(), msg(`{"a":1}`))
	require.NoError(t, err)
	req, body := last()
	assert.Equal(t, "1700000000", req.Header.Get("Webhook-Timestamp"))
	mac := hmac.New(sha512.New, []byte("k"))
	mac.Write([]byte("1700000000." + string(body)))
	assert.Equal(t, base64.StdEncoding.EncodeToString(mac.Sum(nil)), req.Header.Get("Webhook-Signature"))
	assert.Empty(t, req.Header.Get(DefaultSignatureHeader))
}

func TestSinkSignErrors(t *testing.T) {
	for _, sg := range []*config.Sign{{}, {Secret: "k", Algo: "md5"}, {Secret: "k", Encoding: "base32"}} {
		_, err := newSink(plugin.Spec{Name: "out", Config: &config.Endpoint{Type: "http", URL: "http://localhost", Sign: sg}})
		assert.ErrorContains(t, err, "destination out: sign.")
	}
}
