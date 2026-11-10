package grpc

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// accepted reports which of the listeners is handed the next connection.
func accepted(t *testing.T, addr string, ls ...*portListener) int {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, l := range ls {
			select {
			case conn := <-l.conns:
				_ = conn.Close()
				return i
			default:
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no listener was handed the connection")
	return -1
}

func TestPortsAreShared(t *testing.T) {
	first, err := listenPort("127.0.0.1:0")
	require.NoError(t, err)
	addr := first.Addr().String()
	other, err := listenPort("127.0.0.1:0")
	require.NoError(t, err)
	assert.NotEqual(t, addr, other.Addr().String(), ":0 binds a fresh port every time")
	require.NoError(t, other.Close())

	second, err := listenPort(addr)
	require.NoError(t, err, "a second source on the same address shares the port")
	assert.Equal(t, addr, second.Addr().String())
	assert.Equal(t, 1, accepted(t, addr, first, second), "the newest takes new connections")

	require.NoError(t, second.Close())
	require.NoError(t, second.Close(), "closing twice is fine")
	_, err = second.Accept()
	assert.ErrorIs(t, err, net.ErrClosed)
	assert.Equal(t, 0, accepted(t, addr, first), "then the one before it")

	require.NoError(t, first.Close())
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err, "the port closes with its last source")
	require.NoError(t, ln.Close())
}

func TestPortBindError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	_, err = listenPort(ln.Addr().String())
	assert.Error(t, err, "a port another program holds")
}
