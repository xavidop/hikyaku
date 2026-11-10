package sse

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Modes.
const (
	modeListen    = "listen"
	modeSubscribe = "subscribe"
)

// Defaults.
const (
	defaultPath       = "/"
	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
	defaultReplay     = 1000
)

// errHMAC is why an sse source refuses auth.hmac.
const errHMAC = "auth.hmac applies to http sources: a WebSocket or SSE request carries no signed body"

// sourceOptions is an sse source's configuration, defaults applied.
type sourceOptions struct {
	mode     string
	maxBytes int64

	// listen
	listen    string
	path      string
	serverTLS *tls.Config
	tlsKey    string

	// subscribe
	urls      []string // url first, then failover_urls
	header    http.Header
	host      string
	clientTLS *tls.Config
	reconnect reconnectOptions
}

// reconnectOptions is a subscribe source's reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// sinkOptions is an sse destination's configuration.
type sinkOptions struct {
	listen    string
	path      string
	serverTLS *tls.Config
	tlsKey    string
	event     *transform.Template // nil: the event name is the subject
	replay    int
	auth      *config.Auth // the clients that read the stream; nil lets everyone read
}

// parseSource turns the spec of an sse source into its options.
func parseSource(spec plugin.Spec) (*config.Source, sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return nil, sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if src.Auth != nil && src.Auth.HMAC != nil {
		return nil, sourceOptions{}, fail("%s", errHMAC)
	}
	o := sourceOptions{mode: src.Mode, maxBytes: int64(src.MaxBodyBytes)}
	if o.mode == "" {
		typed := *src
		if typed.Type == "" {
			typed.Type = spec.Kind
		}
		o.mode = config.DefaultMode(typed)
	}
	switch {
	case o.maxBytes < 0:
		return nil, sourceOptions{}, fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch o.mode {
	case modeListen:
		var err error
		if o.listen, o.path, err = listenAddr(src.Listen, src.Path); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		if o.serverTLS, o.tlsKey, err = httpmux.ServerTLS(src.TLS); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
	case modeSubscribe:
		u, err := streamURL("url", src.URL)
		if err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		o.urls = []string{u}
		for i, f := range src.FailoverURLs {
			u, err := streamURL(fmt.Sprintf("failover_urls[%d]", i), f)
			if err != nil {
				return nil, sourceOptions{}, fail("%v", err)
			}
			o.urls = append(o.urls, u)
		}
		o.header = http.Header{}
		for k, v := range src.Headers {
			if strings.Contains(v, "{{") {
				return nil, sourceOptions{}, fail("headers.%s: request headers are static; a per-message template has no message to render", k)
			}
			if textproto.CanonicalMIMEHeaderKey(k) == "Host" {
				o.host = v
				continue
			}
			o.header.Set(k, v)
		}
		if o.clientTLS, err = httpmux.ClientTLS(src.TLS); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
	default:
		return nil, sourceOptions{}, fail("sse sources run in listen or subscribe mode, not %q", o.mode)
	}
	return src, o, nil
}

// parseSink turns the spec of an sse destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if ep.Listen == "" {
		return sinkOptions{}, fail("listen is required: the address the event stream is served on")
	}
	if ep.Auth != nil && ep.Auth.HMAC != nil {
		return sinkOptions{}, fail("auth.hmac does not apply: a client reading the stream sends no body to sign")
	}
	o := sinkOptions{auth: ep.Auth}
	var err error
	if o.listen, o.path, err = listenAddr(ep.Listen, ep.Path); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if o.serverTLS, o.tlsKey, err = httpmux.ServerTLS(ep.TLS); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if ep.Event != "" {
		if o.event, err = transform.CompileTemplate("event", ep.Event); err != nil {
			return sinkOptions{}, fail("event: %v", err)
		}
	}
	switch {
	case ep.Replay < 0:
		return sinkOptions{}, fail("replay must not be negative")
	case ep.Replay == 0:
		o.replay = defaultReplay
	default:
		o.replay = ep.Replay
	}
	return o, nil
}

// listenAddr checks a listen address and path, defaulting the path to /.
func listenAddr(listen, path string) (string, string, error) {
	if listen == "" {
		return "", "", fmt.Errorf("listen is required in listen mode (such as :8080)")
	}
	if _, _, err := net.SplitHostPort(listen); err != nil {
		return "", "", fmt.Errorf("listen %q is not host:port (%v)", listen, err)
	}
	if path == "" {
		path = defaultPath
	}
	if !strings.HasPrefix(path, "/") {
		return "", "", fmt.Errorf("path must start with /")
	}
	return listen, path, nil
}

// streamURL checks a URL to subscribe to: http:// or https://, with a host.
func streamURL(field, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if strings.Contains(raw, "{{") {
		return "", fmt.Errorf("%s: a stream is opened before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %v", field, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%s %q must start with http:// or https://", field, redactURL(raw))
	}
	if u.Host == "" {
		return "", fmt.Errorf("%s %q has no host", field, redactURL(raw))
	}
	return raw, nil
}

// parseReconnect applies the reconnect defaults: backoff 500ms, max_backoff
// 30s, jitter full, max_attempts 0 (forever).
func parseReconnect(r *config.Reconnect) (reconnectOptions, error) {
	o := reconnectOptions{backoff: defaultBackoff, maxBackoff: defaultMaxBackoff, jitter: defaultJitter}
	if r == nil {
		return o, nil
	}
	if r.Backoff > 0 {
		o.backoff = r.Backoff.Std()
	}
	if r.MaxBackoff > 0 {
		o.maxBackoff = r.MaxBackoff.Std()
	}
	o.maxBackoff = max(o.maxBackoff, o.backoff)
	switch r.Jitter {
	case "":
	case "full", "equal", "none":
		o.jitter = r.Jitter
	default:
		return reconnectOptions{}, fmt.Errorf("reconnect.jitter must be full, equal or none, not %q", r.Jitter)
	}
	if r.MaxAttempts < 0 {
		return reconnectOptions{}, fmt.Errorf("reconnect.max_attempts must not be negative (0 retries forever)")
	}
	o.maxAttempts = r.MaxAttempts
	return o, nil
}

// httpClient is the client a subscribe source streams through, with the
// configured TLS. It has no timeout: a stream lasts as long as it lasts.
func httpClient(cfg *tls.Config) *http.Client {
	t := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	}
	if cfg != nil {
		t.TLSClientConfig = cfg
	}
	// A server that accepts the connection but never answers is a failed
	// connect, not a stream.
	t.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: t}
}
