package redis

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Source modes: a stream read through a consumer group, or a pub/sub
// channel.
const (
	modeStream  = "stream"
	modeChannel = "channel"
)

// Ack modes of a stream source.
const (
	ackDelivered = "delivered"
	ackAccepted  = "accepted"
)

// Defaults of a stream source and of the reconnect policy.
const (
	defaultPrefetch   = 64
	defaultAckWait    = 30 * time.Second
	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
)

// sourceOptions is a redis source's configuration, defaults applied.
type sourceOptions struct {
	mode      string
	servers   []server // url first, then failover_urls
	maxBytes  int64
	reconnect reconnectOptions

	// stream
	stream   string
	group    string
	consumer string
	start    string // where a new group starts: "0" (earliest) or "$" (latest)
	prefetch int
	ackWait  time.Duration
	ack      string

	// channel
	channel string
	pattern bool // PSUBSCRIBE instead of SUBSCRIBE
}

// reconnectOptions is a source's reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// sinkOptions is a redis destination's configuration.
type sinkOptions struct {
	server  server
	stream  string
	channel *transform.Template
	maxLen  int64
	body    *transform.Template
	headers map[string]*transform.Template
}

// server is one Redis server: the go-redis options of its URL and the URL
// without its password, for logs.
type server struct {
	opt   *goredis.Options
	shown string
}

// client returns a new client to the server. go-redis dials on first use.
func (s server) client() *goredis.Client {
	o := *s.opt
	o.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return goredis.NewClient(&o)
}

// parseSource turns the spec of a redis source into its options.
func parseSource(spec plugin.Spec) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sourceOptions, error) {
		return sourceOptions{}, fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return fail("redis sources run in subscribe mode, not %q", src.Mode)
	}
	o := sourceOptions{maxBytes: int64(src.MaxBodyBytes)}
	switch {
	case o.maxBytes < 0:
		return fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch {
	case src.Stream != "" && src.Channel != "":
		return fail("set stream or channel, not both")
	case src.Stream != "":
		if err := streamSource(&o, src, spec.Name); err != nil {
			return fail("%v", err)
		}
	case src.Channel != "":
		if err := channelSource(&o, src); err != nil {
			return fail("%v", err)
		}
	default:
		return fail("stream (with group) or channel is required")
	}
	name := clientName(spec.Env.Relay, spec.Name)
	for i, raw := range append([]string{src.URL}, src.FailoverURLs...) {
		field := "url"
		if i > 0 {
			field = fmt.Sprintf("failover_urls[%d]", i-1)
		}
		sv, err := newServer(field, raw, src.Username, src.Password, src.TLS, name)
		if err != nil {
			return fail("%v", err)
		}
		o.servers = append(o.servers, sv)
	}
	var err error
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// streamSource reads the settings of a stream source.
func streamSource(o *sourceOptions, src *config.Source, name string) error {
	if err := fixed("stream", src.Stream); err != nil {
		return err
	}
	if src.Group == "" {
		return errors.New("group is required with stream: the consumer group, whose pending entries let a restart resume")
	}
	if err := fixed("group", src.Group); err != nil {
		return err
	}
	if err := fixed("consumer", src.Consumer); err != nil {
		return err
	}
	o.mode, o.stream, o.group, o.consumer = modeStream, src.Stream, src.Group, src.Consumer
	if o.consumer == "" {
		o.consumer = defaultConsumer(name)
	}
	switch src.Start {
	case "", "latest":
		o.start = "$"
	case "earliest":
		o.start = "0"
	default:
		return fmt.Errorf("start must be earliest or latest, not %q", src.Start)
	}
	switch {
	case src.Prefetch < 0:
		return errors.New("prefetch must not be negative")
	case src.Prefetch == 0:
		o.prefetch = defaultPrefetch
	default:
		o.prefetch = src.Prefetch
	}
	switch {
	case src.AckWait < 0:
		return errors.New("ack_wait must not be negative")
	case src.AckWait == 0:
		o.ackWait = defaultAckWait
	default:
		o.ackWait = src.AckWait.Std()
	}
	switch src.Ack {
	case "", ackDelivered:
		o.ack = ackDelivered
	case ackAccepted:
		o.ack = ackAccepted
	default:
		return fmt.Errorf("ack must be delivered or accepted, not %q", src.Ack)
	}
	return nil
}

// channelSource reads the settings of a pub/sub source, refusing the
// stream settings: pub/sub has no group, no position and no acks.
func channelSource(o *sourceOptions, src *config.Source) error {
	if err := fixed("channel", src.Channel); err != nil {
		return err
	}
	for _, f := range []struct {
		key string
		set bool
	}{
		{"group", src.Group != ""},
		{"consumer", src.Consumer != ""},
		{"start", src.Start != ""},
		{"prefetch", src.Prefetch != 0},
		{"ack_wait", src.AckWait != 0},
		{"ack: accepted", src.Ack == ackAccepted},
	} {
		if f.set {
			return fmt.Errorf("%s applies to streams; pub/sub (channel) has no consumer group and no acknowledgements", f.key)
		}
	}
	o.mode, o.channel = modeChannel, src.Channel
	o.pattern = strings.ContainsAny(src.Channel, "*?[")
	return nil
}

// fixed refuses a per-message template where there is no message yet.
func fixed(field, v string) error {
	if strings.Contains(v, "{{") {
		return fmt.Errorf("%s is a fixed name: it is used before any message exists, so it cannot be a per-message template", field)
	}
	return nil
}

// defaultConsumer is a stream source's consumer name: the host name plus
// the source name, so two sources of one host do not share a consumer.
func defaultConsumer(source string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "hikyaku"
	}
	return host + "-" + source
}

// parseSink turns the spec of a redis destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sinkOptions, error) {
		return sinkOptions{}, fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	sv, err := newServer("url", ep.URL, ep.Username, ep.Password, ep.TLS, clientName(spec.Env.Relay, spec.Name))
	if err != nil {
		return fail("%v", err)
	}
	o := sinkOptions{server: sv, maxLen: ep.MaxLen, headers: map[string]*transform.Template{}}
	if o.maxLen < 0 {
		return fail("max_len must not be negative")
	}
	switch {
	case ep.Stream != "" && ep.Channel != "":
		return fail("set stream or channel, not both")
	case ep.Stream != "":
		if strings.Contains(ep.Stream, "{{") {
			return fail("stream is a fixed key; per-message templates apply to channel")
		}
		o.stream = ep.Stream
	case ep.Channel != "":
		if o.maxLen != 0 {
			return fail("max_len applies to streams only")
		}
		if len(ep.Headers) > 0 {
			return fail("headers apply to streams: a pub/sub message is the raw body")
		}
		if o.channel, err = transform.CompileTemplate("channel", ep.Channel); err != nil {
			return fail("%v", err)
		}
	default:
		return fail("stream (XADD) or channel (PUBLISH) is required")
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail("%v", err)
		}
	}
	for k, v := range ep.Headers {
		if strings.TrimSpace(k) == "" {
			return fail("headers: a header name is empty")
		}
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return fail("%v", err)
		}
		o.headers[k] = t
	}
	return o, nil
}

// newServer checks a URL to dial and builds its client options: redis:// or
// rediss://, with a host, and not a per-message template. username and
// password override the URL's. TLS is on with rediss://, or when the tls
// block asks for it. A delivery or a read is one attempt (no go-redis
// retries): the relay and the source loops retry with their own policies.
func newServer(field, raw, username, password string, t *config.TLS, name string) (server, error) {
	if raw == "" {
		return server{}, fmt.Errorf("%s is required: the server, redis://host:6379/0 or rediss://", field)
	}
	if strings.Contains(raw, "{{") {
		return server{}, fmt.Errorf("%s: the connection is dialed before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url.Error repeats the whole URL, password included.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return server{}, fmt.Errorf("%s is not a valid URL: %v", field, err)
	}
	shown := u.Redacted()
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return server{}, fmt.Errorf("%s %q must start with redis:// or rediss://", field, shown)
	}
	if u.Host == "" {
		return server{}, fmt.Errorf("%s %q has no host", field, shown)
	}
	opt, err := goredis.ParseURL(raw)
	if err != nil {
		return server{}, fmt.Errorf("%s %q: %v", field, shown, err)
	}
	if username != "" {
		opt.Username = username
	}
	if password != "" {
		opt.Password = password
	}
	cfg, err := httpmux.ClientTLS(t)
	if err != nil {
		return server{}, fmt.Errorf("%s: %w", field, err)
	}
	switch {
	case cfg != nil:
		if cfg.ServerName == "" {
			cfg.ServerName = u.Hostname()
		}
		opt.TLSConfig = cfg
	case u.Scheme == "rediss" && t != nil && t.Enabled != nil && !*t.Enabled:
		return server{}, fmt.Errorf("%s %q is rediss://, but tls.enabled is false", field, shown)
	}
	opt.MaxRetries = -1   // one attempt; -1 because go-redis reads 0 as 3
	opt.DialerRetries = 1 // fail a dead server at once, not after 5 dials
	opt.ContextTimeoutEnabled = true
	if opt.ClientName == "" {
		opt.ClientName = name
	}
	return server{opt: opt, shown: shown}, nil
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
		return reconnectOptions{}, errors.New("reconnect.max_attempts must not be negative (0 retries forever)")
	}
	o.maxAttempts = r.MaxAttempts
	return o, nil
}
