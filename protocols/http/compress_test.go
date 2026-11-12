package http

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

func gz(t *testing.T, s string) string {
	t.Helper()
	b, err := gzipBody([]byte(s))
	require.NoError(t, err)
	return string(b)
}

func TestSinkCompressesWithGzip(t *testing.T) {
	var encoding, signature string
	var raw []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding, signature = r.Header.Get("Content-Encoding"), r.Header.Get("X-Signature-256")
		raw, _ = io.ReadAll(r.Body)
		w.WriteHeader(204)
	}))
	defer srv.Close()
	s := sinkFor(t, config.Endpoint{URL: srv.URL, Compression: "gzip", Sign: &config.Sign{Secret: "s3cret"}}, plugin.Egress{})
	body := `{"text":"` + strings.Repeat("hello ", 200) + `"}`
	_, err := s.Deliver(t.Context(), msg(body))
	require.NoError(t, err)

	assert.Equal(t, "gzip", encoding)
	assert.Less(t, len(raw), len(body))
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	plain, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, body, string(plain))
	// The signature covers the bytes that went out: the compressed ones.
	assert.Equal(t, inbound.Sign("s3cret", raw), signature)
}

func TestSinkDoesNotForwardAnOldContentEncoding(t *testing.T) {
	var encoding string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		w.WriteHeader(204)
	}))
	defer srv.Close()
	s := sinkFor(t, config.Endpoint{URL: srv.URL}, plugin.Egress{})
	e := msg(`{}`)
	e.Headers["Content-Encoding"] = []string{"gzip"}
	_, err := s.Deliver(t.Context(), e)
	require.NoError(t, err)
	assert.Empty(t, encoding)
}

func TestSinkCompressionSetting(t *testing.T) {
	_, err := plugin.NewSink(t.Context(), plugin.Spec{Name: "dest", Kind: "http", Config: &config.Endpoint{Type: "http", URL: "http://example.com", Compression: "zstd"}})
	assert.ErrorContains(t, err, "compression must be gzip or none")
}

func TestListenDecodesGzip(t *testing.T) {
	url, out := listener(t, config.Source{Ack: "accepted", MaxBodyBytes: 64}, 10)
	resp, _ := post(t, url, gz(t, `{"ok":true}`), map[string]string{"Content-Encoding": "gzip"})
	assert.Equal(t, 202, resp.StatusCode)
	env := <-out
	assert.JSONEq(t, `{"ok":true}`, string(env.Body))
	assert.Empty(t, env.Header("Content-Encoding"))

	// A small request that expands past max_body_bytes is refused.
	resp, _ = post(t, url, gz(t, strings.Repeat("a", 4096)), map[string]string{"Content-Encoding": "gzip"})
	assert.Equal(t, 413, resp.StatusCode)
	resp, _ = post(t, url, "not gzip", map[string]string{"Content-Encoding": "gzip"})
	assert.Equal(t, 400, resp.StatusCode)
	resp, _ = post(t, url, "x", map[string]string{"Content-Encoding": "br"})
	assert.Equal(t, 400, resp.StatusCode)
}

func TestListenVerifiesTheSignatureOfTheCompressedBody(t *testing.T) {
	url, out := listener(t, config.Source{
		Ack:  "accepted",
		Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Signature-256", Secret: "s3cret"}},
	}, 10)
	sent := gz(t, `{"ok":true}`)
	resp, _ := post(t, url, sent, map[string]string{"Content-Encoding": "gzip", "X-Signature-256": "sha256=" + inbound.Sign("s3cret", []byte(sent))})
	assert.Equal(t, 202, resp.StatusCode)
	assert.JSONEq(t, `{"ok":true}`, string((<-out).Body))
}
