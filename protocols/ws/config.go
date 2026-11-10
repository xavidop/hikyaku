package ws

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
)

// Modes.
const (
	modeListen    = "listen"
	modeSubscribe = "subscribe"
)

// Defaults of the reconnect policy and the listen path.
const (
	defaultPath       = "/"
	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
)

// errHMAC is why a ws source refuses auth.hmac.
const errHMAC = "auth.hmac applies to http sources: a WebSocket or SSE request carries no signed body"

// sourceOptions is a ws or wss source's configuration, defaults applied.
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
	dial      handshake
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

// handshake is what a client sends when it dials.
type handshake struct {
	header       http.Header
	host         string
	subprotocols []string
}

// sinkOptions is a ws or wss destination's configuration.
type sinkOptions struct {
	url       string
	dial      handshake
	clientTLS *tls.Config
	frame     websocket.MessageType
}

// parseSource turns the spec of a ws or wss source into its options.
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
		if src.Listen == "" {
			return nil, sourceOptions{}, fail("listen is required in listen mode (such as :8080)")
		}
		if _, _, err := net.SplitHostPort(src.Listen); err != nil {
			return nil, sourceOptions{}, fail("listen %q is not host:port (%v)", src.Listen, err)
		}
		o.listen, o.path = src.Listen, src.Path
		if o.path == "" {
			o.path = defaultPath
		}
		if !strings.HasPrefix(o.path, "/") {
			return nil, sourceOptions{}, fail("path must start with /")
		}
		t := src.TLS
		if spec.Kind == "wss" {
			// wss is TLS: a listener without a certificate cannot serve it.
			forced := config.TLS{}
			if t != nil {
				forced = *t
			}
			forced.Enabled = config.Ptr(true)
			t = &forced
		}
		cfg, key, err := httpmux.ServerTLS(t)
		if err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		o.serverTLS, o.tlsKey = cfg, key
	case modeSubscribe:
		u, err := dialURL("url", src.URL)
		if err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		o.urls = []string{u}
		for i, f := range src.FailoverURLs {
			u, err := dialURL(fmt.Sprintf("failover_urls[%d]", i), f)
			if err != nil {
				return nil, sourceOptions{}, fail("%v", err)
			}
			o.urls = append(o.urls, u)
		}
		if o.dial, err = parseHandshake(src.Headers); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		if o.clientTLS, err = httpmux.ClientTLS(src.TLS); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
		if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
			return nil, sourceOptions{}, fail("%v", err)
		}
	default:
		return nil, sourceOptions{}, fail("%s sources run in listen or subscribe mode, not %q", spec.Kind, o.mode)
	}
	return src, o, nil
}

// parseSink turns the spec of a ws or wss destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	u, err := dialURL("url", ep.URL)
	if err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	o := sinkOptions{url: u}
	switch ep.Frame {
	case "", "text":
		o.frame = websocket.MessageText
	case "binary":
		o.frame = websocket.MessageBinary
	default:
		return sinkOptions{}, fail("frame must be text or binary, not %q", ep.Frame)
	}
	if o.dial, err = parseHandshake(ep.Headers); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if o.clientTLS, err = httpmux.ClientTLS(ep.TLS); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	return o, nil
}

// dialURL checks a URL to dial: ws:// or wss://, with a host, and not a
// per-message template (a connection is dialed before any message exists).
func dialURL(field, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if strings.Contains(raw, "{{") {
		return "", fmt.Errorf("%s: a WebSocket connection is dialed before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %v", field, err)
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return "", fmt.Errorf("%s %q must start with ws:// or wss://", field, redactURL(raw))
	}
	if u.Host == "" {
		return "", fmt.Errorf("%s %q has no host", field, redactURL(raw))
	}
	return raw, nil
}

// parseHandshake turns configured headers into handshake options. The
// values are sent as they are: a per-message template has no message to
// render against. Host and Sec-WebSocket-Protocol go where the WebSocket
// client expects them.
func parseHandshake(headers map[string]string) (handshake, error) {
	h := handshake{header: http.Header{}}
	for k, v := range headers {
		if strings.Contains(v, "{{") {
			return handshake{}, fmt.Errorf("headers.%s: handshake headers are static; a per-message template has no message to render", k)
		}
		switch textproto.CanonicalMIMEHeaderKey(k) {
		case "Host":
			h.host = v
		case "Sec-Websocket-Protocol":
			for _, p := range strings.Split(v, ",") {
				if p = strings.TrimSpace(p); p != "" {
					h.subprotocols = append(h.subprotocols, p)
				}
			}
		default:
			h.header.Set(k, v)
		}
	}
	return h, nil
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

// options returns the dial options for a handshake over client.
func (h handshake) options(client *http.Client) *websocket.DialOptions {
	return &websocket.DialOptions{
		HTTPClient:   client,
		HTTPHeader:   h.header.Clone(),
		Host:         h.host,
		Subprotocols: h.subprotocols,
	}
}

// httpClient is the client a dial goes through, with the configured TLS.
// It has no timeout: the caller's context bounds the handshake.
func httpClient(cfg *tls.Config) *http.Client {
	t := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	}
	if cfg != nil {
		t.TLSClientConfig = cfg
	}
	return &http.Client{Transport: t}
}
