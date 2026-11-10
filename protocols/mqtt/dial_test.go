package mqtt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// selfSigned makes a certificate for 127.0.0.1. It returns the PEM file a
// client trusts it with, and a server configuration that serves it.
func selfSigned(t *testing.T) (string, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
	require.NoError(t, err)
	certFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	return certFile, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
}

func TestTransports(t *testing.T) {
	caFile, serverTLS := selfSigned(t)
	trust := &config.TLS{CAFile: caFile}
	for _, tc := range []struct {
		name  string
		start func(t *testing.T) *broker
		tls   *config.TLS
	}{
		{"ws", func(t *testing.T) *broker { return startWSBroker(t, nil) }, nil},
		{"wss", func(t *testing.T) *broker { return startWSBroker(t, serverTLS) }, trust},
		{"mqtts", func(t *testing.T) *broker { return startTLSBroker(t, serverTLS) }, trust},
	} {
		for _, v := range versions {
			t.Run(tc.name+"/v"+v, func(t *testing.T) {
				b := tc.start(t)
				src := newTestSource(t, sourceConfig(b, v, func(c *config.Source) { c.TLS = tc.tls }))
				out, _ := run(t, src)
				b.waitSubscribed(defaultClientID("in"), "orders/#")
				sink := newTestSink(t, sinkConfig(b, v, func(c *config.Endpoint) {
					c.Topic, c.TLS, c.QoS = "orders/{{ .subject }}", tc.tls, config.Ptr(2)
				}))
				_, err := sink.Deliver(deliverCtx(t), testMessage("x", `{"over":"`+tc.name+`"}`))
				require.NoError(t, err)
				env := recv(t, out)
				assert.Equal(t, "orders/x", env.Subject)
				assert.JSONEq(t, `{"over":"`+tc.name+`"}`, string(env.Body))
				settle(env, envelope.Delivered)
				eventuallyInflight(t, b, defaultClientID("in"), 0)
			})
		}
	}
}

func TestTLSRejectsAnUntrustedBroker(t *testing.T) {
	_, serverTLS := selfSigned(t)
	b := startTLSBroker(t, serverTLS)
	s := newTestSource(t, sourceConfig(b, version5, func(c *config.Source) {
		c.Reconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 1}
	}))
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate")
}

// wsServer upgrades and hands the connection to serve.
func wsServer(t *testing.T, serve func(c *websocket.Conn)) server {
	t.Helper()
	up := websocket.Upgrader{Subprotocols: []string{"mqtt"}}
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		serve(c)
	}))
	t.Cleanup(hs.Close)
	srv, err := parseServer("url", strings.Replace(hs.URL, "http://", "ws://", 1)+"/mqtt", false)
	require.NoError(t, err)
	return srv
}

func TestWebSocketStream(t *testing.T) {
	srv := wsServer(t, func(c *websocket.Conn) {
		// One MQTT packet may span WebSocket messages, and one message may
		// hold several packets.
		_ = c.WriteMessage(websocket.BinaryMessage, []byte("hel"))
		_ = c.WriteMessage(websocket.BinaryMessage, []byte{})
		_ = c.WriteMessage(websocket.BinaryMessage, []byte("lo, world"))
		_, msg, err := c.ReadMessage()
		if err == nil {
			_ = c.WriteMessage(websocket.BinaryMessage, msg)
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte("text"))
		time.Sleep(100 * time.Millisecond)
	})
	conn, err := dial(context.Background(), srv, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	assert.IsType(t, &wsConn{}, conn)
	assert.NotNil(t, conn.LocalAddr())
	assert.NotNil(t, conn.RemoteAddr())
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	buf := make([]byte, len("hello, world"))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "hello, world", string(buf))

	_, err = conn.Write([]byte("echo"))
	require.NoError(t, err)
	buf = make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "echo", string(buf))

	_, err = conn.Read(buf)
	assert.ErrorContains(t, err, "text message")
}

func TestWebSocketHandshakeRefused(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer hs.Close()
	srv, err := parseServer("url", strings.Replace(hs.URL, "http://", "ws://", 1), false)
	require.NoError(t, err)
	_, err = dial(context.Background(), srv, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
	assert.Contains(t, err.Error(), "dial ws://")
}
