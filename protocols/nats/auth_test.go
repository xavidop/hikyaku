package nats

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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

// selfSigned writes a certificate for 127.0.0.1 and its key, and returns
// them loaded too.
func selfSigned(t *testing.T) (certFile, keyFile string, cert tls.Certificate) {
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
	cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	require.NoError(t, err)
	return certFile, keyFile, cert
}

func TestTLS(t *testing.T) {
	certFile, _, cert := selfSigned(t)
	ts := startServer(t, func(o *server.Options) {
		o.TLS = true
		o.TLSTimeout = 5
		o.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	})
	url := strings.Replace(ts.url(), "nats://", "tls://", 1)

	sink := newTestSink(t, config.Endpoint{URL: url, Subject: "orders", TLS: &config.TLS{CAFile: certFile}})
	require.NoError(t, sink.Health(ctxT(t)))
	_, err := sink.Deliver(ctxT(t), jsonEnv(`{}`))
	require.NoError(t, err)

	// A nats:// URL with a tls block upgrades the connection too.
	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", TLS: &config.TLS{CAFile: certFile}})
	run(t, src)
	require.Eventually(t, func() bool { return src.Health(context.Background()) == nil }, 10*time.Second, 10*time.Millisecond)

	// Without the CA, the server's certificate is not trusted; Health says
	// so once the failed connect is reported.
	untrusted := newTestSink(t, config.Endpoint{URL: url, Subject: "orders"})
	require.Eventually(t, func() bool {
		err := untrusted.Health(ctxT(t))
		return err != nil && strings.Contains(err.Error(), "certificate")
	}, 10*time.Second, 10*time.Millisecond)
}

func TestTokenAuth(t *testing.T) {
	ts := startServer(t, func(o *server.Options) { o.Authorization = "s3cr3t-token" })
	logs := &logBuffer{}
	good := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders", Token: "s3cr3t-token"})
	require.NoError(t, good.Health(ctxT(t)))

	cfg := config.Endpoint{Type: KindNATS, URL: ts.url(), Subject: "orders", Token: "wrong-token"}
	sp := spec(t, KindNATS, &cfg)
	sp.Env.Logger = testLogger(t, logs)
	bad, err := newSink(sp)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bad.Close(context.Background()) })
	// Health says why once the failed connect is reported.
	require.Eventually(t, func() bool {
		err := bad.Health(ctxT(t))
		return err != nil && strings.Contains(strings.ToLower(err.Error()), "authorization violation")
	}, 10*time.Second, 10*time.Millisecond)

	// The token in the URL works too, and is never logged.
	sp2 := spec(t, KindNATS, &config.Source{Name: "tok", Type: KindNATS, Subject: "orders", Reconnect: fast(),
		URL: strings.Replace(ts.url(), "nats://", "nats://s3cr3t-token@", 1)})
	sp2.Name = "tok"
	sp2.Env.Logger = testLogger(t, logs)
	inURL, err := newSource(sp2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inURL.Close(context.Background()) })
	run(t, inURL)
	ts.subscribed("hikyaku-test-tok", "orders")
	require.NoError(t, inURL.Health(context.Background()))
	assert.Contains(t, logs.String(), "nats://xxxxx@127.0.0.1")
	assert.NotContains(t, logs.String(), "s3cr3t-token")
	assert.NotContains(t, logs.String(), "wrong-token")
}

func TestCredsFileAuth(t *testing.T) {
	// An operator-mode server, with one account and a user of it.
	okp, err := nkeys.CreateOperator()
	require.NoError(t, err)
	opk, err := okp.PublicKey()
	require.NoError(t, err)
	oc := jwt.NewOperatorClaims(opk)
	oc.Name = "op"
	ojwt, err := oc.Encode(okp)
	require.NoError(t, err)
	operator, err := jwt.DecodeOperatorClaims(ojwt)
	require.NoError(t, err)

	akp, err := nkeys.CreateAccount()
	require.NoError(t, err)
	apk, err := akp.PublicKey()
	require.NoError(t, err)
	ac := jwt.NewAccountClaims(apk)
	ac.Name = "relays"
	ajwt, err := ac.Encode(okp)
	require.NoError(t, err)

	ukp, err := nkeys.CreateUser()
	require.NoError(t, err)
	upk, err := ukp.PublicKey()
	require.NoError(t, err)
	uc := jwt.NewUserClaims(upk)
	uc.Name = "hikyaku"
	ujwt, err := uc.Encode(akp)
	require.NoError(t, err)
	seed, err := ukp.Seed()
	require.NoError(t, err)
	creds, err := jwt.FormatUserConfig(ujwt, seed)
	require.NoError(t, err)
	credsFile := filepath.Join(t.TempDir(), "hikyaku.creds")
	require.NoError(t, os.WriteFile(credsFile, creds, 0o600))

	resolver := &server.MemAccResolver{}
	require.NoError(t, resolver.Store(apk, ajwt))
	ts := startServer(t, func(o *server.Options) {
		o.JetStream = false
		o.TrustedOperators = []*jwt.OperatorClaims{operator}
		o.AccountResolver = resolver
	})

	src := newTestSource(t, config.Source{URL: ts.url(), Subject: "orders", CredsFile: credsFile})
	out, _ := run(t, src)
	ts.subscribed(connIn, "orders")
	sink := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders", CredsFile: credsFile})
	_, err = sink.Deliver(ctxT(t), jsonEnv(`{"signed":true}`))
	require.NoError(t, err)
	assert.Equal(t, `{"signed":true}`, string(recv(t, out).Body))

	// Without credentials the server refuses the connection.
	anonymous := newTestSink(t, config.Endpoint{URL: ts.url(), Subject: "orders"})
	require.Error(t, anonymous.Health(ctxT(t)))
}
