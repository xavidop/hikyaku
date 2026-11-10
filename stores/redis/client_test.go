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

	"github.com/xavidop/hikyaku/internal/config"
)

// testPKI is a throwaway CA with a server and a client certificate, written
// to PEM files.
type testPKI struct {
	caFile, clientCert, clientKey string
	server                        tls.Certificate
	pool                          *x509.CertPool
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return p
	}
	newKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		return k
	}
	keyDER := func(k *ecdsa.PrivateKey) []byte {
		der, err := x509.MarshalECPrivateKey(k)
		require.NoError(t, err)
		return der
	}

	caKey := newKey()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "hikyaku test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	issue := func(serial int64, cn string, usage x509.ExtKeyUsage) ([]byte, *ecdsa.PrivateKey) {
		k := newKey()
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: cn},
			DNSNames:     []string{"localhost"},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
		require.NoError(t, err)
		return der, k
	}
	serverDER, serverKey := issue(2, "localhost", x509.ExtKeyUsageServerAuth)
	clientDER, clientKey := issue(3, "hikyaku", x509.ExtKeyUsageClientAuth)

	server, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER(serverKey)}),
	)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return testPKI{
		caFile:     write("ca.pem", "CERTIFICATE", caDER),
		clientCert: write("client.pem", "CERTIFICATE", clientDER),
		clientKey:  write("client-key.pem", "EC PRIVATE KEY", keyDER(clientKey)),
		server:     server,
		pool:       pool,
	}
}

func runTLS(t *testing.T, cfg *tls.Config) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.NewMiniRedis()
	require.NoError(t, mr.StartTLS(cfg))
	t.Cleanup(mr.Close)
	return mr
}

func TestTLS(t *testing.T) {
	ctx := context.Background()
	pki := newTestPKI(t)

	t.Run("server verified with ca_file", func(t *testing.T) {
		mr := runTLS(t, &tls.Config{Certificates: []tls.Certificate{pki.server}, MinVersion: tls.VersionTLS12})
		s := newStore(t, config.Store{Addr: mr.Addr(), TLS: &config.TLS{CAFile: pki.caFile, ServerName: "localhost"}})
		assert.NoError(t, s.Health(ctx))
	})

	t.Run("unknown CA is refused", func(t *testing.T) {
		mr := runTLS(t, &tls.Config{Certificates: []tls.Certificate{pki.server}, MinVersion: tls.VersionTLS12})
		s := newStore(t, config.Store{Addr: mr.Addr(), TLS: &config.TLS{Enabled: config.Ptr(true)}})
		assert.Error(t, s.Health(ctx))
	})

	t.Run("mutual TLS", func(t *testing.T) {
		mr := runTLS(t, &tls.Config{
			Certificates: []tls.Certificate{pki.server},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    pki.pool,
			MinVersion:   tls.VersionTLS12,
		})
		s := newStore(t, config.Store{Addr: mr.Addr(), TLS: &config.TLS{
			CAFile: pki.caFile, CertFile: pki.clientCert, KeyFile: pki.clientKey, ServerName: "localhost",
		}})
		assert.NoError(t, s.Health(ctx))

		noCert := newStore(t, config.Store{Addr: mr.Addr(), TLS: &config.TLS{CAFile: pki.caFile, ServerName: "localhost"}})
		assert.Error(t, noCert.Health(ctx), "the server wants a client certificate")
	})
}

func TestTLSConfig(t *testing.T) {
	pki := newTestPKI(t)

	for _, tc := range []struct {
		name string
		tls  *config.TLS
		on   bool
	}{
		{"absent", nil, false},
		{"empty", &config.TLS{}, false},
		{"enabled", &config.TLS{Enabled: config.Ptr(true)}, true},
		{"implied by ca_file", &config.TLS{CAFile: pki.caFile}, true},
		{"implied by server_name", &config.TLS{ServerName: "redis.internal"}, true},
		{"implied by insecure_skip_verify", &config.TLS{InsecureSkipVerify: true}, true},
		{"explicitly off", &config.TLS{Enabled: config.Ptr(false), CAFile: pki.caFile}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := tlsConfig(tc.tls)
			require.NoError(t, err)
			assert.Equal(t, tc.on, cfg != nil)
		})
	}

	cfg, err := tlsConfig(&config.TLS{CAFile: pki.caFile, CertFile: pki.clientCert, KeyFile: pki.clientKey, ServerName: "redis.internal"})
	require.NoError(t, err)
	assert.Equal(t, "redis.internal", cfg.ServerName)
	assert.NotNil(t, cfg.RootCAs)
	assert.Len(t, cfg.Certificates, 1)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)

	_, err = tlsConfig(&config.TLS{CertFile: pki.clientCert})
	assert.ErrorContains(t, err, "go together")
	_, err = tlsConfig(&config.TLS{CAFile: pki.clientKey})
	assert.ErrorContains(t, err, "no PEM certificate")
	_, err = tlsConfig(&config.TLS{CertFile: pki.clientCert, KeyFile: pki.caFile})
	assert.ErrorContains(t, err, "cert_file/key_file")
}
