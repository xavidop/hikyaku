package httpmux

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

func text(s string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, s) })
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestSharedListenerPathsAndReplace(t *testing.T) {
	a, err := Register("127.0.0.1:0", "/a", Options{}, text("A"))
	require.NoError(t, err)
	addr := a.Addr()
	b, err := Register(addr, "/b/", Options{}, text("B"))
	require.NoError(t, err)
	base := "http://" + addr

	code, body := get(t, base+"/a")
	assert.Equal(t, 200, code)
	assert.Equal(t, "A", body)
	_, body = get(t, base+"/b/deep/path")
	assert.Equal(t, "B", body, "a trailing slash serves the subtree")
	code, _ = get(t, base+"/nope")
	assert.Equal(t, 404, code)

	// A reload registers /a again: the new handler wins, and releasing the
	// old handle leaves it in place.
	a2, err := Register(addr, "/a", Options{}, text("A2"))
	require.NoError(t, err)
	a.Release()
	_, body = get(t, base+"/a")
	assert.Equal(t, "A2", body)

	_, err = Register(addr, "/c", Options{TLSKey: "other"}, text("C"))
	assert.ErrorContains(t, err, "different TLS settings")

	a2.Release()
	b.Release()
	b.Release() // idempotent
	_, err = http.Get(base + "/a")
	assert.Error(t, err, "the last release stops the server")
}

func TestRegisterBindError(t *testing.T) {
	_, err := Register("256.0.0.1:1", "/", Options{}, text("x"))
	assert.Error(t, err)
}

func TestTLSConfigs(t *testing.T) {
	cfg, key, err := ServerTLS(nil)
	assert.NoError(t, err)
	assert.Nil(t, cfg)
	assert.Empty(t, key)
	_, _, err = ServerTLS(&config.TLS{Enabled: config.Ptr(true)})
	assert.ErrorContains(t, err, "cert_file and key_file")
	_, _, err = ServerTLS(&config.TLS{CertFile: "/nope.pem", KeyFile: "/nope.key"})
	assert.Error(t, err)

	c, err := ClientTLS(nil)
	assert.NoError(t, err)
	assert.Nil(t, c)
	c, err = ClientTLS(&config.TLS{InsecureSkipVerify: true, ServerName: "x"})
	require.NoError(t, err)
	assert.True(t, c.InsecureSkipVerify)
	_, err = ClientTLS(&config.TLS{CAFile: "/nope.pem"})
	assert.Error(t, err)
}
