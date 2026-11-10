package redis

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/xavidop/hikyaku/internal/config"
)

// mode is the Redis deployment a store talks to.
type mode int

const (
	modeStandalone mode = iota
	modeSentinel
	modeCluster
)

func (m mode) String() string {
	switch m {
	case modeSentinel:
		return "sentinel"
	case modeCluster:
		return "cluster"
	default:
		return "standalone"
	}
}

// defaultRetry is the store's own retry default (design §5): a second try
// after 10ms, all within the per-operation timeout.
var defaultRetry = &config.Retry{Attempts: 2, Backoff: config.Duration(10 * time.Millisecond)}

// clientConfig is the go-redis configuration a spec resolves to. Only the
// options of its mode are set.
type clientConfig struct {
	mode     mode
	single   *goredis.Options
	failover *goredis.FailoverOptions
	cluster  *goredis.ClusterOptions
}

// newClientConfig picks the client a spec asks for: cluster when cluster: is
// set, Sentinel when sentinel: is, otherwise a standalone client on addr.
//
// Every mode gets the same knobs:
//   - the per-operation timeout as the socket read and write timeout, with
//     context deadlines honoured, so the timeout also bounds pool waits,
//     handshakes and retries;
//   - the spec's retry policy as go-redis command retries;
//   - one dial per try. go-redis otherwise retries a failed dial 5 times,
//     100ms apart, in the background, so a call to a dead Redis would wait
//     out the whole timeout and fail with a bare deadline error instead of
//     "connection refused";
//   - no maintenance notifications: go-redis would otherwise send CLIENT
//     MAINT_NOTIFICATIONS (a Redis Enterprise feature) when it opens a
//     connection, one more round trip within the timeout.
func newClientConfig(spec config.Store, timeout time.Duration) (clientConfig, error) {
	tlsCfg, err := tlsConfig(spec.TLS)
	if err != nil {
		return clientConfig{}, err
	}
	r := config.ResolveRetry(spec.Retry, defaultRetry)
	retries := r.Attempts - 1
	if retries <= 0 {
		retries = -1 // go-redis reads 0 as "its default", 3; -1 means none
	}
	minBackoff, maxBackoff := r.Backoff.Std(), r.MaxBackoff.Std()

	switch {
	case spec.Cluster != nil:
		return clientConfig{mode: modeCluster, cluster: &goredis.ClusterOptions{
			Addrs:                    slices.Clone(spec.Cluster.Addrs),
			Username:                 spec.Username,
			Password:                 spec.Password,
			MaxRetries:               retries,
			MinRetryBackoff:          minBackoff,
			MaxRetryBackoff:          maxBackoff,
			DialerRetries:            1,
			ReadTimeout:              timeout,
			WriteTimeout:             timeout,
			ContextTimeoutEnabled:    true,
			TLSConfig:                tlsCfg,
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		}}, nil
	case spec.Sentinel != nil:
		return clientConfig{mode: modeSentinel, failover: &goredis.FailoverOptions{
			MasterName:            spec.Sentinel.MasterName,
			SentinelAddrs:         slices.Clone(spec.Sentinel.Addrs),
			SentinelPassword:      spec.Sentinel.Password,
			Username:              spec.Username,
			Password:              spec.Password,
			DB:                    spec.DB,
			MaxRetries:            retries,
			MinRetryBackoff:       minBackoff,
			MaxRetryBackoff:       maxBackoff,
			DialerRetries:         1,
			ReadTimeout:           timeout,
			WriteTimeout:          timeout,
			ContextTimeoutEnabled: true,
			TLSConfig:             tlsCfg,
		}}, nil
	case spec.Addr != "":
		return clientConfig{mode: modeStandalone, single: &goredis.Options{
			Addr:                     spec.Addr,
			Username:                 spec.Username,
			Password:                 spec.Password,
			DB:                       spec.DB,
			MaxRetries:               retries,
			MinRetryBackoff:          minBackoff,
			MaxRetryBackoff:          maxBackoff,
			DialerRetries:            1,
			ReadTimeout:              timeout,
			WriteTimeout:             timeout,
			ContextTimeoutEnabled:    true,
			TLSConfig:                tlsCfg,
			MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		}}, nil
	default:
		return clientConfig{}, errors.New("needs one of addr, sentinel or cluster")
	}
}

// newClient builds the go-redis client. It does not connect: go-redis dials
// on first use.
func (c clientConfig) newClient() goredis.UniversalClient {
	switch c.mode {
	case modeCluster:
		return goredis.NewClusterClient(c.cluster)
	case modeSentinel:
		return goredis.NewFailoverClient(c.failover)
	default:
		return goredis.NewClient(c.single)
	}
}

// tlsConfig builds the client TLS configuration, or nil for plain TCP. TLS is
// on when tls.enabled says so, or, when enabled is not set, when any other
// field is set.
func tlsConfig(t *config.TLS) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	implied := t.CAFile != "" || t.CertFile != "" || t.KeyFile != "" || t.ServerName != "" || t.InsecureSkipVerify
	if !config.Bool(t.Enabled, implied) {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, // development only, says the spec
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls ca_file %s: no PEM certificate found", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return nil, errors.New("tls cert_file and key_file go together")
		}
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls cert_file/key_file: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
