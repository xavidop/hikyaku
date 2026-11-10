package amqp

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Defaults.
const (
	defaultPrefetch   = 64
	maxPrefetch       = 65535 // basic.qos carries the count in 16 bits
	defaultBindingKey = "#"
	exchangeType      = "topic"

	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
)

// connOptions is how a source or destination reaches the broker.
type connOptions struct {
	tls *tls.Config
	// username and password replace the URL's when set.
	username string
	password string
	// name is the connection_name client property.
	name string
}

// config is the client configuration of one dial to rawURL. The TLS
// configuration is cloned because the client names the server in it.
func (o connOptions) config(rawURL string) amqp091.Config {
	props := amqp091.NewConnectionProperties()
	props.SetClientConnectionName(o.name)
	cfg := amqp091.Config{Properties: props, Locale: "en_US"}
	if o.tls != nil {
		cfg.TLSClientConfig = o.tls.Clone()
	}
	if o.username != "" || o.password != "" {
		uri, _ := amqp091.ParseURI(rawURL) // checked when the options were parsed
		user, pass := uri.Username, uri.Password
		if o.username != "" {
			user = o.username
		}
		if o.password != "" {
			pass = o.password
		}
		cfg.SASL = []amqp091.Authentication{&amqp091.PlainAuth{Username: user, Password: pass}}
	}
	return cfg
}

// sourceOptions is an amqp source's configuration, defaults applied.
type sourceOptions struct {
	conn       connOptions
	urls       []string // url first, then failover_urls
	queue      string
	prefetch   int
	declare    bool
	exchange   string
	bindingKey string
	maxBytes   int64
	ack        string
	reconnect  reconnectOptions
}

// reconnectOptions is a source's reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// sinkOptions is an amqp destination's configuration.
type sinkOptions struct {
	conn       connOptions
	url        string
	exchange   string
	routingKey *transform.Template
	headers    []headerTemplate
	body       *transform.Template
	persistent bool
	mandatory  bool
	declare    bool
}

// headerTemplate is one configured header of a destination.
type headerTemplate struct {
	name string
	tmpl *transform.Template
}

// parseSource turns the spec of an amqp source into its options.
func parseSource(spec plugin.Spec) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sourceOptions, error) {
		return sourceOptions{}, fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return fail("amqp sources run in subscribe mode, not %q", src.Mode)
	}
	o := sourceOptions{
		queue: src.Queue, prefetch: src.Prefetch, declare: src.Declare, exchange: src.Exchange,
		bindingKey: src.RoutingKey, maxBytes: int64(src.MaxBodyBytes), ack: src.Ack,
	}
	if o.queue == "" {
		return fail("queue is required: the queue consumed")
	}
	u, err := dialURL("url", src.URL)
	if err != nil {
		return fail("%v", err)
	}
	o.urls = []string{u}
	for i, f := range src.FailoverURLs {
		u, err := dialURL(fmt.Sprintf("failover_urls[%d]", i), f)
		if err != nil {
			return fail("%v", err)
		}
		o.urls = append(o.urls, u)
	}
	switch {
	case o.prefetch < 0 || o.prefetch > maxPrefetch:
		return fail("prefetch must be 1 to %d", maxPrefetch)
	case o.prefetch == 0:
		o.prefetch = defaultPrefetch
	}
	switch {
	case o.maxBytes < 0:
		return fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch o.ack {
	case "", "delivered", "accepted":
	default:
		return fail("ack must be delivered or accepted, not %q", o.ack)
	}
	if o.bindingKey != "" && o.exchange == "" {
		return fail("routing_key needs exchange (the exchange the queue is bound to)")
	}
	if o.exchange != "" && !o.declare {
		return fail("exchange binds the queue to it, which needs declare: true (or bind the queue where it is provisioned and leave exchange out)")
	}
	if o.bindingKey == "" {
		o.bindingKey = defaultBindingKey
	}
	if o.conn, err = parseConn(spec, src.TLS, src.Username, src.Password, o.urls); err != nil {
		return fail("%v", err)
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// parseSink turns the spec of an amqp destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sinkOptions, error) {
		return sinkOptions{}, fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	u, err := dialURL("url", ep.URL)
	if err != nil {
		return fail("%v", err)
	}
	o := sinkOptions{
		url: u, exchange: ep.Exchange, persistent: config.Bool(ep.Persistent, true),
		mandatory: ep.Mandatory, declare: ep.Declare,
	}
	if ep.Exchange == "" && ep.RoutingKey == "" {
		return fail("routing_key is required on the default exchange: it is the queue the message goes to")
	}
	if ep.Declare && ep.Exchange == "" {
		return fail("declare declares exchange, which is not set (the default exchange always exists, and a destination does not declare queues)")
	}
	if o.routingKey, err = transform.CompileTemplate("routing_key", ep.RoutingKey); err != nil {
		return fail("%v", err)
	}
	for k, v := range ep.Headers {
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return fail("%v", err)
		}
		o.headers = append(o.headers, headerTemplate{name: k, tmpl: t})
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail("%v", err)
		}
	}
	if o.conn, err = parseConn(spec, ep.TLS, ep.Username, ep.Password, []string{u}); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// parseConn builds the connection options shared by sources and sinks.
// TLS applies to amqps:// URLs, which use it by default; a tls block next to
// an amqp:// URL would be ignored, so it is refused.
func parseConn(spec plugin.Spec, t *config.TLS, username, password string, urls []string) (connOptions, error) {
	cfg, err := httpmux.ClientTLS(t)
	if err != nil {
		return connOptions{}, err
	}
	if cfg != nil {
		for _, u := range urls {
			if strings.HasPrefix(u, "amqp://") {
				return connOptions{}, fmt.Errorf("tls applies to amqps:// URLs, and %q is amqp://", redactURL(u))
			}
		}
	}
	return connOptions{tls: cfg, username: username, password: password, name: connectionName(spec.Env.Relay, spec.Name)}, nil
}

// dialURL checks a broker URL: amqp:// or amqps://, with a host, and not a
// per-message template (a connection is dialed before any message exists).
// Errors show the URL without its password.
func dialURL(field, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%s is required: the broker, amqp://user:pass@host:5672/vhost", field)
	}
	shown := redactURL(raw)
	if strings.Contains(raw, "{{") {
		return "", fmt.Errorf("%s: the connection is dialed before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%s %q is not a valid URL (percent-encode reserved characters in the user name and password)", field, shown)
	}
	if u.Scheme != "amqp" && u.Scheme != "amqps" {
		return "", fmt.Errorf("%s %q must start with amqp:// or amqps://", field, shown)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("%s %q has no host", field, shown)
	}
	if _, err := amqp091.ParseURI(raw); err != nil {
		return "", fmt.Errorf("%s %q: %v", field, shown, err)
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
