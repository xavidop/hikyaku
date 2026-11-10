//go:build integration

package tcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

// TestRelayOverRealSockets runs a two-hop relay over real sockets on two
// ports: a client delivers to source A, and each envelope A produces is
// delivered by a destination to source B, which must hand it over whole and
// in order. No containers: both ends are this package.
func TestRelayOverRealSockets(t *testing.T) {
	p := newPKI(t)
	messages := []string{
		"hello",
		`{"order":42,"items":["a","b"]}`,
		"{\n  \"pretty\": true\n}", // compacted by newline framing
		strings.Repeat("big ", 4000),
		"\x00binary\xff",
	}
	cases := []struct {
		name        string
		kind        string
		frame       string
		lengthBytes int
		scheme      string      // the forwarding destination's URL scheme
		serverTLS   *config.TLS // source B's
		clientTLS   *config.TLS // the forwarding destination's
	}{
		{name: "tcp newline", kind: kindTCP, frame: "newline", scheme: "tcp"},
		{name: "tcp length", kind: kindTCP, frame: "length", lengthBytes: 2, scheme: "tcp"},
		{name: "tcp raw", kind: kindTCP, frame: "raw", scheme: "tcp"},
		{name: "mtls length", kind: kindTCP, frame: "length", lengthBytes: 4, scheme: "tls", serverTLS: p.mutualTLS, clientTLS: p.clientTLS},
		{name: "udp newline", kind: kindUDP, frame: "newline", scheme: "udp"},
		{name: "udp length", kind: kindUDP, frame: "length", lengthBytes: 2, scheme: "udp"},
		{name: "dtls length", kind: kindUDP, frame: "length", lengthBytes: 2, scheme: "dtls", serverTLS: p.serverTLS, clientTLS: p.anonymous},
		{name: "dtls with client certificates, raw", kind: kindUDP, frame: "raw", scheme: "dtls", serverTLS: p.mutualTLS, clientTLS: p.clientTLS},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestSource(t, tc.kind, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes, TLS: tc.serverTLS})
			bOut, _ := run(t, b)
			forward := newTestSink(t, config.Endpoint{Type: tc.kind, URL: tc.scheme + "://" + listening(t, b),
				Frame: tc.frame, LengthBytes: tc.lengthBytes, TLS: tc.clientTLS})

			a := newTestSource(t, tc.kind, config.Source{Frame: tc.frame, LengthBytes: tc.lengthBytes})
			aOut, _ := run(t, a)
			client := newTestSink(t, config.Endpoint{Type: tc.kind, URL: tc.kind + "://" + listening(t, a),
				Frame: tc.frame, LengthBytes: tc.lengthBytes})
			require.NotEqual(t, a.Addr(), b.Addr())

			for i, m := range messages {
				if tc.kind == kindUDP && len(m) > 1000 {
					continue // over what macOS sends in one datagram by default
				}
				require.NoError(t, deliver(client, m), "message %d into A", i)
				env := recv(t, aOut)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := forward.Deliver(ctx, env)
				cancel()
				require.NoError(t, err, "message %d from A to B", i)
				assert.Equal(t, tc.scheme+"://"+b.Addr(), res.Endpoint)

				got := recv(t, bOut)
				want := m
				if tc.frame == "newline" && strings.Contains(m, "\n") {
					want = `{"pretty":true}`
				}
				assert.Equal(t, want, got.BodyText(), "message %d", i)
				assert.Equal(t, env.ContentType, got.ContentType, "message %d", i)
				assert.Equal(t, tc.kind, got.Protocol)
				assert.NotEmpty(t, got.Attributes[tc.kind+".remote_addr"])
			}
		})
	}
}

// TestManyClientsOverRealSockets: several clients write at once on their
// own connections; every message arrives, and each client's keep their order
// under one ordering key.
func TestManyClientsOverRealSockets(t *testing.T) {
	src := newTestSource(t, kindTCP, config.Source{Frame: "length"})
	out, _ := run(t, src)
	addr := listening(t, src)
	const clients, each = 8, 200
	errs := make(chan error, clients)
	for c := range clients {
		go func() {
			sink, err := buildSink(config.Endpoint{URL: "tcp://" + addr, Frame: "length"})
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = sink.Close(context.Background()) }()
			for i := range each {
				if err := deliver(sink, fmt.Sprintf("%d:%d", c, i)); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	next := map[string]int{}    // client → next index expected
	keys := map[string]string{} // client → its ordering key
	for range clients * each {
		env := recv(t, out)
		var c, i int
		_, err := fmt.Sscanf(env.BodyText(), "%d:%d", &c, &i)
		require.NoError(t, err)
		client := fmt.Sprint(c)
		assert.Equal(t, next[client], i, "client %d in order", c)
		next[client] = i + 1
		key := fmt.Sprint(env.Attributes["ordering_key"])
		if k, ok := keys[client]; ok {
			assert.Equal(t, k, key, "one ordering key per connection")
		}
		keys[client] = key
	}
	for range clients {
		require.NoError(t, <-errs)
	}
	assert.Len(t, keys, clients)
}
