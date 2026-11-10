package redis

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// selfSigned writes a certificate for 127.0.0.1 and localhost, and its key.
func selfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
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

// startMiniTLS runs a miniredis that only speaks TLS.
func startMiniTLS(t *testing.T) (*miniredis.Miniredis, string) {
	t.Helper()
	certFile, keyFile := selfSigned(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	mr := miniredis.NewMiniRedis()
	require.NoError(t, mr.StartTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}))
	t.Cleanup(mr.Close)
	return mr, certFile
}

func TestTLSRoundTrip(t *testing.T) {
	mr, caFile := startMiniTLS(t)
	ctx := context.Background()

	untrusted := newTestSink(t, config.Endpoint{URL: "rediss://" + mr.Addr(), Stream: "secure"})
	_, err := untrusted.Deliver(ctx, msg(`{"n":0}`))
	require.Error(t, err)
	class, tags := classOf(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, tags, "an unknown certificate fails the dial")

	sink := newTestSink(t, config.Endpoint{URL: "rediss://" + mr.Addr(), Stream: "secure", TLS: &config.TLS{CAFile: caFile}})
	require.NoError(t, sink.Health(ctx))
	_, err = sink.Deliver(ctx, msg(`{"n":1}`))
	require.NoError(t, err)

	// redis:// with a tls block is TLS too.
	src := newStreamSourceT(t, config.Source{URL: "redis://" + mr.Addr(), TLS: &config.TLS{CAFile: caFile},
		Stream: "secure", Group: "g", Consumer: "c", Start: "earliest"})
	out, _ := run(t, src)
	env := recv(t, out)
	assert.Equal(t, `{"n":1}`, env.BodyText())
	settle(env, envelope.Delivered)
	require.NoError(t, src.Health(ctx))
}
