package ws

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// selfSigned writes a certificate for 127.0.0.1 and its key.
func selfSigned(t *testing.T) (certFile, keyFile string) {
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
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))
	return certFile, keyFile
}

func TestWSSSinkToWSSListenSource(t *testing.T) {
	certFile, keyFile := selfSigned(t)
	src := newTestSource(t, "wss", config.Source{Listen: "127.0.0.1:0", Path: "/in",
		TLS: &config.TLS{CertFile: certFile, KeyFile: keyFile}})
	out, _ := run(t, src)
	url := "wss://" + listening(t, src) + "/in"

	untrusted := newTestSink(t, config.Endpoint{Type: "wss", URL: url})
	_, err := untrusted.Deliver(context.Background(), msg("x"))
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err), "an unknown certificate fails the dial")

	sink := newTestSink(t, config.Endpoint{Type: "wss", URL: url, TLS: &config.TLS{CAFile: certFile}})
	res, err := sink.Deliver(context.Background(), msg(`{"via":"tls"}`))
	require.NoError(t, err)
	assert.Equal(t, url, res.Endpoint)
	env := recv(t, out)
	assert.Equal(t, `{"via":"tls"}`, env.BodyText())
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, "wss", env.Protocol)
	assert.Equal(t, "/in", env.Subject)
}

// records keeps what a source logs.
type records struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *records) Enabled(context.Context, slog.Level) bool { return true }
func (r *records) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *records) WithGroup(string) slog.Handler            { return r }
func (r *records) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec.Clone())
	return nil
}

func (r *records) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level == level && rec.Message == msg {
			n++
		}
	}
	return n
}

func TestSubscribeLogsReconnectsAtWarn(t *testing.T) {
	logs := &records{}
	cfg := &config.Source{Name: "in", Type: "ws", URL: "ws://" + closedAddr(t) + "/",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 3}}
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "ws", Config: cfg, Env: plugin.Env{Logger: slog.New(logs)}})
	require.NoError(t, err)
	require.Error(t, s.Start(context.Background(), make(chan *envelope.Envelope)))
	assert.Equal(t, 2, logs.count(slog.LevelWarn, "ws source reconnecting"), "one warning per reconnect; none once it gives up")
}
