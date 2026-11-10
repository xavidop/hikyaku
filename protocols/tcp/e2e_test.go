package tcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
)

// A relay over DTLS: devices send to a udp source that requires their
// client certificates, and a route forwards each message through a dtls://
// destination to a second DTLS server.
func TestEndToEndOverDTLS(t *testing.T) {
	Register()
	p := newPKI(t)
	upstream := newTestSource(t, kindUDP, config.Source{TLS: p.serverTLS, Frame: "length"})
	upOut, _ := run(t, upstream)
	addr := closedUDPAddr(t)
	spec := fmt.Sprintf(`
version: 1
name: dtls-e2e
sources:
  - name: devices
    type: udp
    listen: "%s"
    tls: { cert_file: %q, key_file: %q, client_ca_file: %q }
destinations:
  - name: upstream
    type: udp
    url: dtls://%s
    frame: length
    tls: { ca_file: %q }
    retry: { attempts: 3, backoff: 1ms }
routes:
  - { name: relay, from: [devices], to: [upstream] }
server: { admin: { disabled: true } }
`, addr, p.serverCert, p.serverKey, p.ca, listening(t, upstream), p.ca)
	l, err := config.LoadBytes(context.Background(), "e2e.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	device := newTestSink(t, config.Endpoint{URL: "dtls://" + addr, TLS: p.clientTLS})
	require.Eventually(t, func() bool { return deliver(device, `{"temp":21.5}`) == nil }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, deliver(device, "second\twith a tab"))
	var got []string
	for range 2 {
		env := recv(t, upOut)
		got = append(got, env.BodyText())
		assert.Equal(t, kindUDP, env.Protocol)
	}
	assert.ElementsMatch(t, []string{`{"temp":21.5}`, "second\twith a tab"}, got)

	intruder := newTestSink(t, config.Endpoint{URL: "dtls://" + addr, TLS: p.anonymous})
	intruder.verdictWait = 3 * time.Second
	require.Error(t, deliver(intruder, "forged"), "the source requires a client certificate")
	noEnvelope(t, upOut, 50*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
