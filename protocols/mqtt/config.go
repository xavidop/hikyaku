package mqtt

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Protocol versions (protocol_version).
const (
	version5   = "5"
	version311 = "3.1.1"
)

// Defaults.
const (
	defaultQoS            = 1
	defaultMaxBodyBytes   = 1 << 20
	defaultReceiveMaximum = 64
	defaultKeepAlive      = 30 * time.Second
	defaultConnectTimeout = 10 * time.Second
	// defaultSessionExpiry is how long (seconds) an MQTT 5 broker keeps a
	// persistent session, and its queued messages, after a disconnect.
	defaultSessionExpiry = 24 * 60 * 60
	defaultBackoff       = 500 * time.Millisecond
	defaultMaxBackoff    = 30 * time.Second
	defaultJitter        = "full"
	// maxTopicBytes is the longest topic or filter MQTT can carry.
	maxTopicBytes = 65535
)

// transport is how a broker is reached.
type transport int

const (
	transportTCP transport = iota
	transportTLS
	transportWS
	transportWSS
)

func (t transport) secure() bool { return t == transportTLS || t == transportWSS }

// server is one broker: url or one of failover_urls.
type server struct {
	transport transport
	// addr is host:port, for tcp and tls.
	addr string
	// wsURL is the URL dialed, without its user info, for ws and wss.
	wsURL string
	// shown is the URL without its password, for logs and errors.
	shown string
	// user and password come from the URL's user info; the username and
	// password fields win over them.
	user, password       string
	hasUser, hasPassword bool
}

// brokerURL is the server's URL without its user info.
func (s server) brokerURL() string {
	switch s.transport {
	case transportTCP:
		return "tcp://" + s.addr
	case transportTLS:
		return "ssl://" + s.addr
	}
	return s.wsURL
}

// connOptions is what one connection is made with.
type connOptions struct {
	version  string
	clientID string
	// username and password are the configured fields; a server's own user
	// info fills what they leave out.
	username, password string
	tls                *tls.Config
	keepAlive          time.Duration
	connectTimeout     time.Duration
	// cleanStart starts a new session (MQTT 5 clean start, 3.1.1 clean
	// session) that the broker forgets at disconnect.
	cleanStart bool
	// sessionExpiry is how long (seconds) an MQTT 5 broker keeps the session
	// after a disconnect.
	sessionExpiry uint32
	// receiveMaximum caps the unacknowledged QoS 1 and 2 messages an MQTT 5
	// broker sends at once; 0 leaves the protocol default.
	receiveMaximum uint16
	// manualAck: the source acknowledges each message itself.
	manualAck bool
}

// credentials returns the user name and password to connect to srv with.
func (o connOptions) credentials(srv server) (user, password string, hasUser, hasPassword bool) {
	user, hasUser = o.username, o.username != ""
	if !hasUser && srv.hasUser {
		user, hasUser = srv.user, true
	}
	password, hasPassword = o.password, o.password != ""
	if !hasPassword && srv.hasPassword {
		password, hasPassword = srv.password, true
	}
	return user, password, hasUser, hasPassword
}

// reconnectOptions is a reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// sourceOptions is an mqtt source's configuration, defaults applied.
type sourceOptions struct {
	servers   []server // url first, then failover_urls
	conn      connOptions
	filters   []string // what is subscribed: shared filters already prefixed
	qos       byte
	maxBytes  int64
	ack       string // delivered (default) or accepted
	reconnect reconnectOptions
}

// sinkOptions is an mqtt destination's configuration, defaults applied.
type sinkOptions struct {
	server  server
	conn    connOptions
	topic   *transform.Template
	qos     byte
	retain  bool
	body    *transform.Template            // nil: the message body
	headers map[string]*transform.Template // canonical names: MQTT 5 user properties
}

// parseSource turns the spec of an mqtt source into its options.
func parseSource(spec plugin.Spec) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sourceOptions, error) {
		return sourceOptions{}, fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return fail("mqtt sources run in subscribe mode, not %q", src.Mode)
	}
	version, err := parseVersion(src.ProtocolVersion)
	if err != nil {
		return fail("%v", err)
	}
	o := sourceOptions{ack: src.Ack, maxBytes: int64(src.MaxBodyBytes)}
	switch o.ack {
	case "", "delivered", "accepted":
	default:
		return fail("ack must be delivered or accepted, not %q", o.ack)
	}
	switch {
	case o.maxBytes < 0:
		return fail("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = defaultMaxBodyBytes
	}
	if o.qos, err = parseQoS(src.QoS); err != nil {
		return fail("%v", err)
	}
	if o.filters, err = parseFilters(src.Topic, src.Topics, src.Group, version); err != nil {
		return fail("%v", err)
	}
	tlsCfg, err := httpmux.ClientTLS(src.TLS)
	if err != nil {
		return fail("%v", err)
	}
	for i, raw := range append([]string{src.URL}, src.FailoverURLs...) {
		field := "url"
		if i > 0 {
			field = fmt.Sprintf("failover_urls[%d]", i-1)
		}
		srv, err := parseServer(field, raw, tlsCfg != nil)
		if err != nil {
			return fail("%v", err)
		}
		o.servers = append(o.servers, srv)
	}
	if src.Prefetch < 0 || src.Prefetch > 65535 {
		return fail("prefetch must be 1 to 65535 (the MQTT 5 receive maximum)")
	}
	receiveMax := uint16(defaultReceiveMaximum)
	if src.Prefetch > 0 {
		receiveMax = uint16(src.Prefetch)
	}
	clean := config.Bool(src.CleanSession, false)
	o.conn = connOptions{
		version:        version,
		clientID:       clientID(src.ClientID, spec.Env.Relay, spec.Name),
		username:       src.Username,
		password:       src.Password,
		tls:            tlsOrDefault(tlsCfg, o.servers),
		keepAlive:      defaultKeepAlive,
		connectTimeout: defaultConnectTimeout,
		cleanStart:     clean,
		receiveMaximum: receiveMax,
		manualAck:      true,
	}
	if !clean {
		o.conn.sessionExpiry = defaultSessionExpiry
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return fail("%v", err)
	}
	return o, nil
}

// parseSink turns the spec of an mqtt destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(format string, args ...any) (sinkOptions, error) {
		return sinkOptions{}, fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	version, err := parseVersion(ep.ProtocolVersion)
	if err != nil {
		return fail("%v", err)
	}
	tlsCfg, err := httpmux.ClientTLS(ep.TLS)
	if err != nil {
		return fail("%v", err)
	}
	srv, err := parseServer("url", ep.URL, tlsCfg != nil)
	if err != nil {
		return fail("%v", err)
	}
	o := sinkOptions{server: srv, retain: ep.Retain, headers: map[string]*transform.Template{}}
	if o.qos, err = parseQoS(ep.QoS); err != nil {
		return fail("%v", err)
	}
	if ep.Topic == "" {
		return fail("topic is required")
	}
	if o.topic, err = transform.CompileTemplate("topic", ep.Topic); err != nil {
		return fail("%v", err)
	}
	if o.topic.IsStatic() {
		if err := validTopic(ep.Topic); err != nil {
			return fail("topic: %v", err)
		}
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail("%v", err)
		}
	}
	for k, v := range ep.Headers {
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return fail("%v", err)
		}
		o.headers[textproto.CanonicalMIMEHeaderKey(k)] = t
	}
	o.conn = connOptions{
		version:        version,
		clientID:       clientID(ep.ClientID, spec.Env.Relay, spec.Name),
		username:       ep.Username,
		password:       ep.Password,
		tls:            tlsOrDefault(tlsCfg, []server{srv}),
		keepAlive:      defaultKeepAlive,
		connectTimeout: defaultConnectTimeout,
		// A destination's session holds nothing worth keeping: the relay
		// retries a delivery whose acknowledgement a dropped connection lost.
		cleanStart: true,
	}
	return o, nil
}

// parseVersion reads protocol_version: "5" (the default) or "3.1.1".
func parseVersion(v string) (string, error) {
	switch v {
	case "", version5:
		return version5, nil
	case version311:
		return version311, nil
	}
	return "", fmt.Errorf("protocol_version must be \"3.1.1\" or \"5\", not %q", v)
}

// parseQoS reads qos: 0, 1 or 2 (default 1).
func parseQoS(q *int) (byte, error) {
	if q == nil {
		return defaultQoS, nil
	}
	if *q < 0 || *q > 2 {
		return 0, fmt.Errorf("qos must be 0, 1 or 2, not %d", *q)
	}
	return byte(*q), nil
}

// clientID is the configured client identifier, or hikyaku-<relay>-<name>.
func clientID(configured, relay, name string) string {
	if configured != "" {
		return configured
	}
	if relay == "" {
		return "hikyaku-" + name
	}
	return "hikyaku-" + relay + "-" + name
}

// parseServer checks a broker URL. mqtt:// and tcp:// are plain TCP (port
// 1883), mqtts://, ssl:// and tls:// are TLS (port 8883), ws:// and wss://
// are WebSockets. A tls block upgrades a plain URL to TLS. The URL's user
// info fills a missing username and password.
func parseServer(field, raw string, forceTLS bool) (server, error) {
	if raw == "" {
		return server{}, fmt.Errorf("%s is required: the broker, such as mqtt://broker:1883", field)
	}
	if strings.Contains(raw, "{{") {
		return server{}, fmt.Errorf("%s: the connection is made before any message, so the URL cannot be a per-message template", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url's error repeats the URL, password included.
		return server{}, fmt.Errorf("%s is not a valid URL", field)
	}
	shown := u.Redacted()
	if u.Host == "" || u.Hostname() == "" {
		return server{}, fmt.Errorf("%s %q has no host", field, shown)
	}
	s := server{shown: shown}
	if u.User != nil {
		s.user, s.hasUser = u.User.Username(), u.User.Username() != ""
		s.password, s.hasPassword = u.User.Password()
	}
	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp":
		s.transport = transportTCP
	case "mqtts", "ssl", "tls":
		s.transport = transportTLS
	case "ws":
		s.transport = transportWS
	case "wss":
		s.transport = transportWSS
	default:
		return server{}, fmt.Errorf("%s %q: the scheme must be mqtt, tcp, mqtts, ssl, tls, ws or wss", field, shown)
	}
	if forceTLS {
		switch s.transport {
		case transportTCP:
			s.transport = transportTLS
		case transportWS:
			s.transport = transportWSS
		}
	}
	switch s.transport {
	case transportTCP, transportTLS:
		if u.Path != "" && u.Path != "/" {
			return server{}, fmt.Errorf("%s %q: a TCP broker URL has no path", field, shown)
		}
		port := u.Port()
		if port == "" {
			port = "1883"
			if s.transport == transportTLS {
				port = "8883"
			}
		}
		s.addr = net.JoinHostPort(u.Hostname(), port)
	default:
		d := *u
		d.User = nil
		d.Scheme = "ws"
		if s.transport == transportWSS {
			d.Scheme = "wss"
		}
		s.wsURL = d.String()
	}
	return s, nil
}

// tlsOrDefault is the TLS configuration of the connections: the tls block,
// or a default one when a server needs TLS and the block is empty.
func tlsOrDefault(cfg *tls.Config, servers []server) *tls.Config {
	if cfg != nil {
		return cfg
	}
	for _, s := range servers {
		if s.transport.secure() {
			return &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}
	return nil
}

// parseFilters reads topic or topics and applies group: each filter becomes
// the shared subscription $share/<group>/<filter>.
func parseFilters(topic string, topics []string, group, version string) ([]string, error) {
	var filters []string
	switch {
	case topic != "" && len(topics) > 0:
		return nil, fmt.Errorf("set topic or topics, not both")
	case topic != "":
		filters = []string{topic}
	case len(topics) > 0:
		filters = append(filters, topics...)
	default:
		return nil, fmt.Errorf("topic is required: the topic filter (or topics) subscribed to")
	}
	if group != "" {
		if version != version5 {
			return nil, fmt.Errorf("group (a shared subscription) needs MQTT 5 (protocol_version: \"5\")")
		}
		if strings.ContainsAny(group, "/+#") || !utf8.ValidString(group) || strings.ContainsRune(group, 0) {
			return nil, fmt.Errorf("group %q must be UTF-8 without /, + or #", group)
		}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(filters))
	for _, f := range filters {
		if err := validFilter(f); err != nil {
			return nil, fmt.Errorf("topic filter %q: %v", f, err)
		}
		if seen[f] {
			return nil, fmt.Errorf("topic filter %q is listed twice", f)
		}
		seen[f] = true
		if group != "" {
			f = "$share/" + group + "/" + f
		}
		out = append(out, f)
	}
	return out, nil
}

// validFilter checks a topic filter: + takes a whole level, # only the last
// one. A shared subscription is set with group, not written out.
func validFilter(f string) error {
	if err := validTopicText(f); err != nil {
		return err
	}
	if strings.HasPrefix(f, "$share/") || f == "$share" {
		return fmt.Errorf("set group: instead of writing $share/<group>/ yourself")
	}
	levels := strings.Split(f, "/")
	for i, l := range levels {
		switch {
		case l == "#" && i != len(levels)-1:
			return fmt.Errorf("# must be the last level")
		case l != "#" && strings.Contains(l, "#"):
			return fmt.Errorf("# must take a whole level")
		case l != "+" && strings.Contains(l, "+"):
			return fmt.Errorf("+ must take a whole level")
		}
	}
	return nil
}

// validTopic checks a topic published to: no wildcards.
func validTopic(t string) error {
	if err := validTopicText(t); err != nil {
		return err
	}
	if strings.ContainsAny(t, "+#") {
		return fmt.Errorf("a topic published to cannot contain the wildcards + or #")
	}
	return nil
}

func validTopicText(t string) error {
	switch {
	case t == "":
		return fmt.Errorf("is empty")
	case len(t) > maxTopicBytes:
		return fmt.Errorf("is longer than %d bytes", maxTopicBytes)
	case !utf8.ValidString(t):
		return fmt.Errorf("is not UTF-8")
	case strings.ContainsRune(t, 0):
		return fmt.Errorf("contains a NUL character")
	}
	return nil
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
