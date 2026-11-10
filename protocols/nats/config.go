package nats

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Defaults.
const (
	defaultPrefetch = 64
	// defaultCorePending is a core subscription's client-side buffer: core
	// NATS drops what overflows it (a slow consumer), so it is large, like
	// nats.go's own default. JetStream keeps prefetch acks in flight instead.
	defaultCorePending = 65536
	// defaultPendingBytes caps what a nats subscription holds for the relay
	// (nats.go's own default).
	defaultPendingBytes = 64 << 20
	defaultAckWait      = 30 * time.Second
	minAckWait          = 100 * time.Millisecond
	defaultBackoff      = 500 * time.Millisecond
	defaultMaxBackoff   = 30 * time.Second
	defaultJitter       = "full"
)

// Acknowledgement modes of a source (ack:).
const (
	ackDelivered = "delivered"
	ackAccepted  = "accepted"
)

// connOptions is how to connect: the servers, the credentials, TLS and the
// reconnect policy.
type connOptions struct {
	servers   []string // url first, then failover_urls
	name      string   // the connection name the server shows
	username  string
	password  string
	token     string
	credsFile string
	tls       *tls.Config
	reconnect reconnectOptions
}

// reconnectOptions is the reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // per server; 0 = forever
}

// sourceOptions is a nats or jetstream source's configuration, defaults
// applied.
type sourceOptions struct {
	conn     connOptions
	ack      string
	maxBytes int64
	prefetch int
	subject  string // nats: subscribed to; jetstream: the filter ("" = all)

	// nats
	queue string

	// jetstream
	stream  string
	durable string
	ackWait time.Duration
	deliver jetstream.DeliverPolicy
}

// sinkOptions is a nats or jetstream destination's configuration.
type sinkOptions struct {
	conn    connOptions
	subject *transform.Template
	headers map[string]*transform.Template
	body    *transform.Template
	stream  string // jetstream: the stream the subject must land in
}

// credentials are the authentication settings of a source or destination.
type credentials struct {
	username, password, token, credsFile string
}

// parseSource turns the spec of a nats or jetstream (kind) source into its
// options.
func parseSource(spec plugin.Spec, kind string) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return sourceOptions{}, fail("%s sources run in subscribe mode, not %q", kind, src.Mode)
	}
	conn, err := parseConn(src.URL, src.FailoverURLs,
		credentials{src.Username, src.Password, src.Token, src.CredsFile}, src.TLS, src.Reconnect, spec.Env.Relay, spec.Name)
	if err != nil {
		return sourceOptions{}, fail("%v", err)
	}
	o := sourceOptions{conn: conn, subject: src.Subject, maxBytes: int64(src.MaxBodyBytes), prefetch: src.Prefetch}
	switch src.Ack {
	case "", ackDelivered:
		o.ack = ackDelivered
	case ackAccepted:
		o.ack = ackAccepted
	default:
		return sourceOptions{}, fail("ack must be delivered or accepted, not %q", src.Ack)
	}
	switch {
	case o.maxBytes < 0:
		return sourceOptions{}, fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch {
	case o.prefetch < 0:
		return sourceOptions{}, fail("prefetch must not be negative")
	case o.prefetch == 0 && kind == KindJetStream:
		o.prefetch = defaultPrefetch
	case o.prefetch == 0:
		o.prefetch = defaultCorePending
	}
	if kind == KindJetStream {
		err = parseStreamSource(spec.Name, src, &o)
	} else {
		err = parseCoreSource(src, &o)
	}
	if err != nil {
		return sourceOptions{}, fail("%v", err)
	}
	return o, nil
}

// parseCoreSource reads the settings of a nats source.
func parseCoreSource(src *config.Source, o *sourceOptions) error {
	if src.Subject == "" {
		return errors.New("subject is required (wildcards * and > allowed)")
	}
	if err := checkSubject(src.Subject, true); err != nil {
		return fmt.Errorf("subject %q %v", src.Subject, err)
	}
	if strings.ContainsAny(src.Queue, " \t\r\n") {
		return fmt.Errorf("queue %q contains whitespace", src.Queue)
	}
	o.queue = src.Queue
	switch {
	case src.Stream != "":
		return errors.New("stream applies to jetstream sources")
	case src.Durable != "":
		return errors.New("durable applies to jetstream sources")
	case src.AckWait != 0:
		return errors.New("ack_wait applies to jetstream sources: core NATS has no acknowledgements")
	case src.Start != "":
		return errors.New("start applies to jetstream sources: core NATS delivers what is published while subscribed")
	}
	return nil
}

// parseStreamSource reads the settings of a jetstream source.
func parseStreamSource(name string, src *config.Source, o *sourceOptions) error {
	if src.Stream == "" {
		return errors.New("stream is required: the stream consumed")
	}
	if err := checkName("stream", src.Stream); err != nil {
		return err
	}
	o.stream = src.Stream
	if src.Subject != "" {
		if err := checkSubject(src.Subject, true); err != nil {
			return fmt.Errorf("subject %q %v", src.Subject, err)
		}
	}
	if src.Queue != "" {
		return errors.New("queue applies to nats sources: jetstream sources that share a durable share its messages")
	}
	o.durable = src.Durable
	if o.durable == "" {
		o.durable = durableName(name)
	}
	if err := checkName("durable", o.durable); err != nil {
		return err
	}
	switch {
	case src.AckWait == 0:
		o.ackWait = defaultAckWait
	case src.AckWait.Std() < minAckWait:
		// A busy message is reported in progress every ack_wait/2.
		return fmt.Errorf("ack_wait must be at least %s", minAckWait)
	default:
		o.ackWait = src.AckWait.Std()
	}
	switch src.Start {
	case "", "new":
		o.deliver = jetstream.DeliverNewPolicy
	case "all":
		o.deliver = jetstream.DeliverAllPolicy
	case "last":
		o.deliver = jetstream.DeliverLastPolicy
	default:
		return fmt.Errorf("start must be all, new or last, not %q", src.Start)
	}
	return nil
}

// parseSink turns the spec of a nats or jetstream (kind) destination into
// its options.
func parseSink(spec plugin.Spec, kind string) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	conn, err := parseConn(ep.URL, nil,
		credentials{ep.Username, ep.Password, ep.Token, ep.CredsFile}, ep.TLS, nil, spec.Env.Relay, spec.Name)
	if err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	o := sinkOptions{conn: conn, headers: map[string]*transform.Template{}}
	if ep.Subject == "" {
		return sinkOptions{}, fail("subject is required: the subject published to")
	}
	if o.subject, err = transform.CompileTemplate("subject", ep.Subject); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if o.subject.IsStatic() {
		if err := checkSubject(ep.Subject, false); err != nil {
			return sinkOptions{}, fail("subject %q %v", ep.Subject, err)
		}
	}
	for k, v := range ep.Headers {
		if !checkHeaderKey(k) {
			return sinkOptions{}, fail("headers: %q is not a valid header name (printable ASCII, no ':' or spaces)", k)
		}
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return sinkOptions{}, fail("%v", err)
		}
		o.headers[k] = t
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return sinkOptions{}, fail("%v", err)
		}
	}
	if ep.Stream != "" {
		if kind != KindJetStream {
			return sinkOptions{}, fail("stream applies to jetstream destinations")
		}
		if err := checkName("stream", ep.Stream); err != nil {
			return sinkOptions{}, fail("%v", err)
		}
		o.stream = ep.Stream
	}
	return o, nil
}

// parseConn checks the servers and credentials and applies the reconnect
// defaults.
func parseConn(rawURL string, failover []string, cr credentials, t *config.TLS, rc *config.Reconnect, relay, name string) (connOptions, error) {
	o := connOptions{
		name:     connName(relay, name),
		username: cr.username, password: cr.password, token: cr.token, credsFile: cr.credsFile,
	}
	if strings.TrimSpace(rawURL) == "" {
		return connOptions{}, errors.New("url is required: the server, such as nats://host:4222")
	}
	add := func(field, raw string) error {
		if strings.Contains(raw, "{{") {
			return fmt.Errorf("%s: the connection is made before any message, so the URL cannot be a per-message template", field)
		}
		// Like nats.Connect, a field may list several servers, comma separated.
		for _, s := range strings.Split(raw, ",") {
			if s = strings.TrimSpace(s); s == "" {
				continue
			}
			u, err := url.Parse(s)
			if err != nil {
				return fmt.Errorf("%s %q is not a URL", field, redactURL(s))
			}
			switch u.Scheme {
			case "nats", "tls", "ws", "wss":
			default:
				return fmt.Errorf("%s %q must start with nats://, tls://, ws:// or wss://", field, redactURL(s))
			}
			if u.Host == "" {
				return fmt.Errorf("%s %q has no host", field, redactURL(s))
			}
			o.servers = append(o.servers, s)
		}
		return nil
	}
	if err := add("url", rawURL); err != nil {
		return connOptions{}, err
	}
	for i, f := range failover {
		if err := add(fmt.Sprintf("failover_urls[%d]", i), f); err != nil {
			return connOptions{}, err
		}
	}
	ws := isWebSocket(o.servers[0])
	for _, s := range o.servers[1:] {
		if isWebSocket(s) != ws {
			return connOptions{}, errors.New("url and failover_urls mix ws:// or wss:// with nats:// or tls://: a connection speaks one or the other")
		}
	}
	if err := cr.check(); err != nil {
		return connOptions{}, err
	}
	var err error
	if o.tls, err = httpmux.ClientTLS(t); err != nil {
		return connOptions{}, err
	}
	if o.reconnect, err = parseReconnect(rc); err != nil {
		return connOptions{}, err
	}
	return o, nil
}

// check rejects credentials that cannot go together, and a creds file that
// cannot be read (nats.go would only find out when it connects, and retry
// forever).
func (c credentials) check() error {
	methods := 0
	if c.username != "" || c.password != "" {
		methods++
	}
	if c.token != "" {
		methods++
	}
	if c.credsFile != "" {
		methods++
	}
	switch {
	case methods > 1:
		return errors.New("username/password, token and creds_file are exclusive: set one")
	case c.password != "" && c.username == "":
		return errors.New("password needs username")
	}
	if c.credsFile != "" {
		f, err := os.Open(c.credsFile)
		if err != nil {
			return fmt.Errorf("creds_file: %w", err)
		}
		_ = f.Close()
	}
	return nil
}

// connName is the connection name the server shows: hikyaku-<relay>-<name>.
func connName(relay, name string) string {
	if relay == "" {
		return "hikyaku-" + name
	}
	return "hikyaku-" + relay + "-" + name
}

// isWebSocket reports whether a server URL is a WebSocket one.
func isWebSocket(s string) bool {
	return strings.HasPrefix(s, "ws://") || strings.HasPrefix(s, "wss://")
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
