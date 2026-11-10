package grpc

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Modes and ack modes.
const (
	modeListen    = "listen"
	modeSubscribe = "subscribe"
	ackDelivered  = "delivered"
	ackAccepted   = "accepted"
)

// Defaults of the reconnect policy.
const (
	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
)

// errHMAC is why a grpc source refuses auth.hmac.
const errHMAC = "auth.hmac applies to http sources: a gRPC call has no raw body to sign"

// listenOptions is a listen source's configuration, defaults applied.
type listenOptions struct {
	listen     string
	serverTLS  *tls.Config
	maxBytes   int
	ack        string
	respond    string        // respond.from, "" without respond
	wait       time.Duration // how long a call waits for its outcome
	reflection bool
	descriptor string   // descriptor_set
	services   []string // of descriptor_set; empty serves them all
}

// subscribeOptions is a subscribe source's configuration, defaults applied.
type subscribeOptions struct {
	targets    []target // url first, then failover_urls
	method     string   // /package.Service/Method
	body       []byte   // the request, JSON
	md         metadata.MD
	descriptor string
	reflection bool
	reconnect  reconnectOptions
	maxBytes   int
	ack        string
}

// reconnectOptions is a subscribe source's reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// sinkOptions is a destination's configuration.
type sinkOptions struct {
	target     target
	method     string
	headers    map[string]*transform.Template // by metadata key
	body       *transform.Template
	descriptor string
	reflection bool
}

// target is a server to dial.
type target struct {
	shown string // the URL as configured, password hidden
	addr  string // host:port
	creds credentials.TransportCredentials
}

// parseListen turns a listen source's spec into its options.
func parseListen(name string, src *config.Source) (listenOptions, error) {
	fail := func(format string, args ...any) (listenOptions, error) {
		return listenOptions{}, fmt.Errorf("source %s: %s", name, fmt.Sprintf(format, args...))
	}
	if src.Auth != nil && src.Auth.HMAC != nil {
		return fail("%s", errHMAC)
	}
	if src.Listen == "" {
		return fail("listen is required in listen mode (such as :50051)")
	}
	if _, _, err := net.SplitHostPort(src.Listen); err != nil {
		return fail("listen %q is not host:port (%v)", src.Listen, err)
	}
	if src.Path != "" {
		return fail("path does not apply: gRPC calls are routed by method, /package.Service/Method")
	}
	o := listenOptions{listen: src.Listen, reflection: config.Bool(src.Reflection, true), descriptor: src.DescriptorSet, services: src.Services}
	var err error
	if o.maxBytes, err = maxBytes(src.MaxBodyBytes); err != nil {
		return fail("%v", err)
	}
	if o.ack, err = ackMode(src.Ack); err != nil {
		return fail("%v", err)
	}
	o.wait = config.DefaultRespondWait
	if r := src.Respond; r != nil {
		if r.From == "" {
			return fail("respond.from is required: the destination whose reply answers the call")
		}
		if o.ack == ackAccepted {
			return fail("respond needs ack: delivered (a reply exists only once the message is delivered)")
		}
		o.respond = r.From
		if r.Timeout < 0 {
			return fail("respond.timeout must not be negative")
		}
		if r.Timeout > 0 {
			o.wait = r.Timeout.Std()
		}
	}
	if len(o.services) > 0 && o.descriptor == "" {
		return fail("services needs descriptor_set: it names services of the descriptor set")
	}
	if o.serverTLS, _, err = httpmux.ServerTLS(src.TLS); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// parseSubscribe turns a subscribe source's spec into its options.
func parseSubscribe(name string, src *config.Source) (subscribeOptions, error) {
	fail := func(format string, args ...any) (subscribeOptions, error) {
		return subscribeOptions{}, fmt.Errorf("source %s: %s", name, fmt.Sprintf(format, args...))
	}
	if src.Auth != nil {
		return fail("auth applies to listen sources")
	}
	o := subscribeOptions{descriptor: src.DescriptorSet, reflection: config.Bool(src.Reflection, true)}
	var err error
	urls := append([]string{src.URL}, src.FailoverURLs...)
	if o.targets, err = parseTargets(urls, src.TLS); err != nil {
		return fail("%v", err)
	}
	if src.Method == "" {
		return fail("method is required: the server-streaming method subscribed to, /package.Service/Method")
	}
	svc, m, err := splitMethod(src.Method)
	if err != nil {
		return fail("%v", err)
	}
	o.method = "/" + svc + "/" + m
	o.body = []byte(strings.TrimSpace(src.Body))
	if len(o.body) == 0 {
		o.body = []byte("{}")
	}
	if !json.Valid(o.body) {
		return fail("body must be the request as JSON, such as {\"topic\": \"orders\"}")
	}
	if o.md, err = staticMetadata(src.Headers); err != nil {
		return fail("%v", err)
	}
	if !o.reflection && o.descriptor == "" {
		return fail("reflection: false needs descriptor_set: the method must be described by one or the other")
	}
	if len(src.Services) > 0 {
		return fail("services applies to listen sources")
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return fail("%v", err)
	}
	if o.maxBytes, err = maxBytes(src.MaxBodyBytes); err != nil {
		return fail("%v", err)
	}
	if o.ack, err = ackMode(src.Ack); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// parseSink turns a destination's spec into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sinkOptions, error) {
		return sinkOptions{}, fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	targets, err := parseTargets([]string{ep.URL}, ep.TLS)
	if err != nil {
		return fail("%v", err)
	}
	o := sinkOptions{target: targets[0], descriptor: ep.DescriptorSet, reflection: config.Bool(ep.Reflection, true),
		headers: map[string]*transform.Template{}}
	if ep.Method == "" {
		return fail("method is required: the method called, /package.Service/Method")
	}
	svc, m, err := splitMethod(ep.Method)
	if err != nil {
		return fail("%v", err)
	}
	o.method = "/" + svc + "/" + m
	for name, v := range ep.Headers {
		k, err := headerKey(name)
		if err != nil {
			return fail("%v", err)
		}
		t, err := transform.CompileTemplate("headers."+name, v)
		if err != nil {
			return fail("%v", err)
		}
		if t.IsStatic() {
			if _, err := metadataValue(k, v); err != nil {
				return fail("headers.%s: %v", name, err)
			}
		}
		o.headers[k] = t
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail("%v", err)
		}
	}
	if !o.reflection && o.descriptor == "" {
		return fail("reflection: false needs descriptor_set: the method must be described by one or the other")
	}
	return o, nil
}

// parseTargets checks the URLs dialed: grpc://host:port (plaintext) or
// grpcs://host:port (TLS, with tls for the CA, client certificate and
// server name). The port defaults to 443 for grpcs and 80 for grpc.
func parseTargets(urls []string, t *config.TLS) ([]target, error) {
	cfg, err := httpmux.ClientTLS(t)
	if err != nil {
		return nil, err
	}
	out := make([]target, 0, len(urls))
	secure := false
	for i, raw := range urls {
		field := "url"
		if i > 0 {
			field = fmt.Sprintf("failover_urls[%d]", i-1)
		}
		tg, isTLS, err := parseTarget(field, raw, cfg)
		if err != nil {
			return nil, err
		}
		secure = secure || isTLS
		out = append(out, tg)
	}
	if cfg != nil && !secure {
		return nil, fmt.Errorf("tls applies to grpcs:// URLs; grpc:// is plaintext")
	}
	return out, nil
}

func parseTarget(field, raw string, cfg *tls.Config) (target, bool, error) {
	if raw == "" {
		return target{}, false, fmt.Errorf("%s is required: the server, grpc://host:port or grpcs://host:port", field)
	}
	if strings.Contains(raw, "{{") {
		return target{}, false, fmt.Errorf("%s: a gRPC connection is dialed before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return target{}, false, fmt.Errorf("%s: %v", field, err)
	}
	shown := u.Redacted()
	var port string
	switch u.Scheme {
	case "grpc":
		port = "80"
	case "grpcs":
		port = "443"
	default:
		return target{}, false, fmt.Errorf("%s %q must start with grpc:// or grpcs://", field, shown)
	}
	switch {
	case u.Hostname() == "":
		return target{}, false, fmt.Errorf("%s %q has no host", field, shown)
	case u.User != nil:
		return target{}, false, fmt.Errorf("%s %q: credentials go in headers (such as authorization), not the URL", field, shown)
	case (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "":
		return target{}, false, fmt.Errorf("%s %q: a gRPC URL is scheme://host:port; the method goes in method", field, shown)
	}
	if p := u.Port(); p != "" {
		port = p
	}
	tg := target{shown: shown, addr: net.JoinHostPort(u.Hostname(), port), creds: insecure.NewCredentials()}
	if u.Scheme == "grpcs" {
		c := cfg
		if c == nil {
			c = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tg.creds = credentials.NewTLS(c)
	}
	return tg, u.Scheme == "grpcs", nil
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

// maxBytes is max_body_bytes with its default, 4MiB.
func maxBytes(s config.Size) (int, error) {
	switch {
	case s < 0:
		return 0, fmt.Errorf("max_body_bytes must not be negative")
	case s == 0:
		return defaultMaxBytes, nil
	case s > math.MaxInt32:
		return 0, fmt.Errorf("max_body_bytes must be at most 2GiB, gRPC's limit")
	}
	return int(s), nil
}

// ackMode is ack with its default, delivered.
func ackMode(ack string) (string, error) {
	switch ack {
	case "", ackDelivered:
		return ackDelivered, nil
	case ackAccepted:
		return ackAccepted, nil
	}
	return "", fmt.Errorf("ack must be delivered or accepted, not %q", ack)
}
