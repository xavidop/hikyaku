package llm

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

func TestPinnedReflectionPort(t *testing.T) {
	for _, c := range []struct {
		name                   string
		env, enabled, v2, port string
		want                   string
	}{
		{name: "no reflection server", port: "3105"},
		{name: "dev, Genkit picks the port", env: "dev"},
		{name: "dev, pinned", env: "dev", port: "3105", want: "3105"},
		{name: "dev, pinned to 0", env: "dev", port: "0"},
		{name: "dev, turned off", env: "dev", enabled: "false", port: "3105"},
		{name: "turned on outside dev", enabled: "true", port: "3105", want: "3105"},
		{name: "v2 connects to the CLI", env: "dev", v2: "ws://127.0.0.1:3200", port: "3105"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("GENKIT_ENV", c.env)
			t.Setenv("GENKIT_REFLECTION_ENABLED", c.enabled)
			t.Setenv("GENKIT_REFLECTION_V2_SERVER", c.v2)
			t.Setenv("GENKIT_REFLECTION_PORT", c.port)
			assert.Equal(t, c.want, pinnedReflectionPort())
		})
	}
}

// A reload that changes genkit: starts a second Genkit instance next to
// the running one. Each instance runs its own reflection server: with a
// port of its own they coexist; a pinned port fits one, so the second is
// refused with an error the CLI counts as needs_restart, until the first
// one's context ends and frees the port.
func TestASecondReflectionServerNeedsAPortOfItsOwn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	require.NoError(t, ln.Close())
	t.Setenv("GENKIT_ENV", "") // no runtime file in the source tree
	t.Setenv("GENKIT_REFLECTION_ENABLED", "true")
	t.Setenv("GENKIT_REFLECTION_V2_SERVER", "")
	t.Setenv("GENKIT_REFLECTION_PORT", port)

	first, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	_, err = New(first, &config.Spec{}, Options{})
	require.NoError(t, err)
	resp, err := http.Get("http://127.0.0.1:" + port + "/api/__health")
	require.NoError(t, err, "the first instance's reflection server holds the pinned port")
	_ = resp.Body.Close()

	_, err = New(context.Background(), &config.Spec{}, Options{})
	require.Error(t, err)
	var nr interface{ NeedsRestart() bool }
	require.ErrorAs(t, err, &nr)
	assert.True(t, nr.NeedsRestart())
	assert.Contains(t, err.Error(), "GENKIT_REFLECTION_PORT="+port)

	t.Setenv("GENKIT_REFLECTION_PORT", "0")
	second, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	_, err = New(second, &config.Spec{}, Options{})
	require.NoError(t, err, "a reflection server on a port Genkit picks starts next to the first")

	stopFirst()
	stopSecond()
	require.Eventually(t, func() bool { return reflecting.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
	t.Setenv("GENKIT_REFLECTION_PORT", port)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := New(ctx, &config.Spec{}, Options{})
		return err == nil
	}, 10*time.Second, 50*time.Millisecond, "once the first instance is gone, its port is free again")
}
