package egress

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

func TestCheckHost(t *testing.T) {
	open, err := New(plugin.Egress{})
	require.NoError(t, err)
	assert.NoError(t, open.CheckHost("anything.example.com"), "an empty allow list allows any name")

	g, err := New(plugin.Egress{Allow: []string{"api.example.com", "*.svc.example.com", "10.20.0.0/16", "192.0.2.7"}})
	require.NoError(t, err)
	assert.NoError(t, g.CheckHost("API.example.com"))
	assert.NoError(t, g.CheckHost("a.svc.example.com"))
	assert.NoError(t, g.CheckHost("10.20.3.4"))
	assert.NoError(t, g.CheckHost("192.0.2.7"))
	assert.NoError(t, g.CheckHost("other.example.com"), "a name may still resolve into an allowed CIDR: the dial decides")
	err = g.CheckHost("192.0.2.8")
	require.ErrorIs(t, err, ErrDenied)
	assert.Equal(t, plugin.Permanent, classOf(err))

	names, err := New(plugin.Egress{Allow: []string{"api.example.com"}})
	require.NoError(t, err)
	assert.ErrorIs(t, names.CheckHost("other.example.com"), ErrDenied)

	_, err = New(plugin.Egress{Allow: []string{"10.0.0.0/99"}})
	assert.ErrorContains(t, err, "egress.allow")
}

func classOf(err error) plugin.Class {
	c, _ := plugin.Classify(err)
	return c
}

// The dial is checked on the address dialed: loopback is private.
func TestDialer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	base := &net.Dialer{Timeout: 2 * time.Second}

	closed, err := New(plugin.Egress{})
	require.NoError(t, err)
	_, err = closed.Dialer(base)(context.Background(), "tcp", addr)
	require.ErrorIs(t, err, ErrDenied)
	assert.Contains(t, err.Error(), "private address")

	for name, e := range map[string]plugin.Egress{
		"allow_private": {AllowPrivate: true},
		"a CIDR":        {Allow: []string{"127.0.0.0/8"}},
	} {
		g, err := New(e)
		require.NoError(t, err)
		c, err := g.Dialer(base)(context.Background(), "tcp", addr)
		require.NoError(t, err, name)
		_ = c.Close()
	}

	listed, err := New(plugin.Egress{Allow: []string{"api.example.com"}, AllowPrivate: true})
	require.NoError(t, err)
	_, err = listed.Dialer(base)(context.Background(), "tcp", addr)
	assert.ErrorIs(t, err, ErrDenied, "an address that is neither listed nor reached by a listed name")
}
