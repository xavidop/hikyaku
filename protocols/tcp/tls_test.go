package tcp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// testPKI is a CA with a server certificate for 127.0.0.1 and a client
// certificate, as PEM files.
type testPKI struct {
	ca                    string
	serverCert, serverKey string
	clientCert, clientKey string
	otherCert, otherKey   string // a client certificate from another CA

	serverTLS *config.TLS // a listener's certificate
	mutualTLS *config.TLS // and client verification
	clientTLS *config.TLS // a destination that trusts the CA and has a certificate
	anonymous *config.TLS // a destination that trusts the CA only
}

func newPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600))
		return path
	}
	newCA := func(name string) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		require.NoError(t, err)
		cert, err := x509.ParseCertificate(der)
		require.NoError(t, err)
		return cert, key, der
	}
	leaf := func(name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, tmpl *x509.Certificate) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl.SerialNumber = big.NewInt(time.Now().UnixNano())
		tmpl.NotBefore, tmpl.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		kb, err := x509.MarshalECPrivateKey(key)
		require.NoError(t, err)
		return write(name+".crt", "CERTIFICATE", der), write(name+".key", "EC PRIVATE KEY", kb)
	}
	ca, caKey, caDER := newCA("hikyaku test CA")
	other, otherKey, _ := newCA("another CA")
	p := testPKI{ca: write("ca.crt", "CERTIFICATE", caDER)}
	p.serverCert, p.serverKey = leaf("server", ca, caKey, &x509.Certificate{
		Subject: pkix.Name{CommonName: "127.0.0.1"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	p.clientCert, p.clientKey = leaf("client", ca, caKey, &x509.Certificate{
		Subject: pkix.Name{CommonName: "sensor-7"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	p.otherCert, p.otherKey = leaf("other", other, otherKey, &x509.Certificate{
		Subject: pkix.Name{CommonName: "intruder"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	p.serverTLS = &config.TLS{CertFile: p.serverCert, KeyFile: p.serverKey}
	p.mutualTLS = &config.TLS{CertFile: p.serverCert, KeyFile: p.serverKey, ClientCAFile: p.ca}
	p.clientTLS = &config.TLS{CAFile: p.ca, CertFile: p.clientCert, KeyFile: p.clientKey}
	p.anonymous = &config.TLS{CAFile: p.ca}
	return p
}

func TestTLSSinkToTLSSource(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindTCP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	url := "tls://" + listening(t, src)

	sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.anonymous})
	res, err := sink.Deliver(context.Background(), msg(`{"via":"tls"}`))
	require.NoError(t, err)
	assert.Equal(t, url, res.Endpoint)
	env := recv(t, out)
	assert.Equal(t, `{"via":"tls"}`, env.BodyText())
	assert.Equal(t, contentJSON, env.ContentType)
	assert.Equal(t, kindTCP, env.Protocol)

	untrusted := newTestSink(t, config.Endpoint{URL: url})
	err = deliver(untrusted, "x")
	assertClass(t, err, plugin.Transient, plugin.TagConnect)
	assert.Contains(t, err.Error(), "certificate")
	noEnvelope(t, out, 50*time.Millisecond)
}

func TestTLSSourceRefusesPlainClients(t *testing.T) {
	p := newPKI(t)
	src, logs := newLoggedSource(t, kindTCP, config.Source{TLS: p.serverTLS})
	out, _ := run(t, src)
	c := dial(t, listening(t, src))
	_, err := c.Write([]byte("plain text\n"))
	require.NoError(t, err)
	waitClosed(t, c)
	noEnvelope(t, out, 50*time.Millisecond)
	assert.Equal(t, 1, logs.count(slog.LevelDebug, "tcp tls handshake failed"))
}

func TestTLSSourceHandshakeTimeout(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindTCP, config.Source{TLS: p.serverTLS})
	src.handshake = 100 * time.Millisecond
	run(t, src)
	c := dial(t, listening(t, src)) // and says nothing
	start := time.Now()
	waitClosed(t, c)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestTLSRawFraming(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindTCP, config.Source{TLS: p.serverTLS, Frame: "raw"})
	out, _ := run(t, src)
	sink := newTestSink(t, config.Endpoint{URL: "tls://" + listening(t, src), TLS: p.anonymous, Frame: "raw"})
	for _, body := range []string{"first\nstream", "second"} {
		require.NoError(t, deliver(sink, body))
		assert.Equal(t, body, recv(t, out).BodyText(), "closing the TLS connection ends the message")
	}
}

// A write to a TLS peer that stopped reading times out within the attempt:
// dropping that connection does not wait to say close_notify.
func TestTLSSinkWriteTimeoutKeepsToTheAttempt(t *testing.T) {
	p := newPKI(t)
	cert, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	require.NoError(t, err)
	peer := newPeer(t, framing{})
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	peer.serve(func(c net.Conn) {
		_ = tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}).Handshake()
		<-hold // and never reads
	})
	s := newTestSink(t, config.Endpoint{URL: "tls://" + peer.addr(), TLS: p.anonymous})
	big := envelope.New("in", "test", bytes.Repeat([]byte("x"), 1<<20))
	for range 128 {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		start := time.Now()
		_, err = s.Deliver(ctx, big)
		took := time.Since(start)
		cancel()
		if err != nil {
			assert.Less(t, took, 2*time.Second, "the attempt keeps to its timeout")
			break
		}
	}
	assertClass(t, err, plugin.Timeout, plugin.TagTimeout)

	// Close says close_notify, which a full connection cannot take: ctx
	// bounds it.
	s = newTestSink(t, config.Endpoint{URL: "tls://" + peer.addr(), TLS: p.anonymous})
	require.NoError(t, deliver(s, "connected"))
	chunk := bytes.Repeat([]byte("y"), 1<<20)
	for {
		require.NoError(t, s.conn.c.SetWriteDeadline(time.Now().Add(200*time.Millisecond)))
		if _, err := s.conn.c.Write(chunk); err != nil {
			break // the socket buffers are full
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, s.Close(ctx))
	assert.Less(t, time.Since(start), 2*time.Second, "Close keeps to its context")
}

func TestMutualTLS(t *testing.T) {
	p := newPKI(t)
	src := newTestSource(t, kindTCP, config.Source{TLS: p.mutualTLS})
	out, _ := run(t, src)
	url := "tls://" + listening(t, src)

	sink := newTestSink(t, config.Endpoint{URL: url, TLS: p.clientTLS})
	require.NoError(t, sink.Health(context.Background()))
	require.NoError(t, deliver(sink, "signed"))
	assert.Equal(t, "signed", recv(t, out).BodyText())

	for _, tc := range []struct {
		name       string
		tls        *config.TLS
		maxVersion uint16
	}{
		// TLS 1.3 refuses a certificate after the client's handshake is
		// done: the destination waits for that verdict instead of reporting
		// a message delivered that the server drops.
		{name: "no client certificate", tls: p.anonymous},
		{name: "a client certificate from another CA", tls: &config.TLS{CAFile: p.ca, CertFile: p.otherCert, KeyFile: p.otherKey}},
		// TLS 1.2 refuses it inside the handshake.
		{name: "no client certificate over TLS 1.2", tls: p.anonymous, maxVersion: tls.VersionTLS12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refused := newTestSink(t, config.Endpoint{URL: url, TLS: tc.tls})
			refused.opts.tls.MaxVersion = tc.maxVersion
			refused.verdictWait = 3 * time.Second // a busy machine answers late; the refusal ends the wait
			err := deliver(refused, "forged")
			assertClass(t, err, plugin.Transient, plugin.TagConnect)
			assert.Contains(t, err.Error(), "remote error: tls:")
			assert.Error(t, refused.Health(context.Background()))
			noEnvelope(t, out, 50*time.Millisecond)
		})
	}
}
