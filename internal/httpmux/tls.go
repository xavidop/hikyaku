package httpmux

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/xavidop/hikyaku/internal/config"
)

// ServerTLS builds a server TLS config from a spec: the certificate, and
// client verification (mTLS) when client_ca_file is set. It returns nil for
// a spec without TLS, plus the key that identifies the settings.
func ServerTLS(t *config.TLS) (*tls.Config, string, error) {
	if t == nil || (!config.Bool(t.Enabled, false) && t.CertFile == "" && t.KeyFile == "") {
		return nil, "", nil
	}
	if t.CertFile == "" || t.KeyFile == "" {
		return nil, "", fmt.Errorf("tls: a listener needs cert_file and key_file")
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, "", fmt.Errorf("tls: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if t.ClientCAFile != "" {
		pool, err := loadPool(t.ClientCAFile)
		if err != nil {
			return nil, "", err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, t.CertFile + "|" + t.KeyFile + "|" + t.ClientCAFile, nil
}

// ClientTLS builds a client TLS config: the CA that verifies the server,
// and a client certificate for mTLS. It returns nil when nothing is set.
func ClientTLS(t *config.TLS) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	if !config.Bool(t.Enabled, false) && t.CAFile == "" && t.CertFile == "" && t.ServerName == "" && !t.InsecureSkipVerify {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: t.ServerName, InsecureSkipVerify: t.InsecureSkipVerify} //nolint:gosec // opt-in for development
	if t.CAFile != "" {
		pool, err := loadPool(t.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls: %s holds no PEM certificates", path)
	}
	return pool, nil
}
