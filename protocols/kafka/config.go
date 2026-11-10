package kafka

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Defaults.
const (
	defaultPrefetch   = 64
	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"

	// recordTimeout bounds how long the producer keeps a record it could not
	// write. A delivery stops waiting at its own timeout; the record may
	// still be written until then.
	recordTimeout = 30 * time.Second
)

// Ack modes of a source.
const (
	ackDelivered = "delivered"
	ackAccepted  = "accepted"
)

// Acks of a destination.
const (
	acksAll    = "all"
	acksLeader = "leader"
	acksNone   = "none"
)

// sourceOptions is a kafka source's configuration, defaults applied.
type sourceOptions struct {
	brokers   []string
	topics    []string
	group     string
	start     string // earliest or latest
	ack       string
	prefetch  int
	maxBytes  int64
	reconnect reconnectOptions
	client    clientOptions
}

// reconnectOptions is how a source backs off while the brokers fail it.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
}

// clientOptions are the connection settings shared by sources and
// destinations.
type clientOptions struct {
	clientID string
	sasl     sasl.Mechanism
	tls      *tls.Config
}

// sinkOptions is a kafka destination's configuration.
type sinkOptions struct {
	brokers     []string
	topic       *transform.Template
	key         *transform.Template // nil: records have no key
	body        *transform.Template // nil: the message body
	headers     []namedTemplate     // sorted by name
	acks        string
	compression kgo.CompressionCodec
	client      clientOptions
}

// namedTemplate is one configured header.
type namedTemplate struct {
	name string
	tmpl *transform.Template
}

// parseSource turns the spec of a kafka source into its options.
func parseSource(spec plugin.Spec) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	bad := func(format string, args ...any) (sourceOptions, error) {
		return sourceOptions{}, fail("source", spec.Name, format, args...)
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return bad("kafka sources run in subscribe mode, not %q", src.Mode)
	}
	if src.URL != "" || len(src.FailoverURLs) > 0 {
		return bad("kafka sources take brokers (host:port), not url or failover_urls")
	}
	brokers, err := parseBrokers(src.Brokers)
	if err != nil {
		return bad("%v", err)
	}
	o := sourceOptions{brokers: brokers, group: src.Group, start: src.Start, ack: src.Ack, prefetch: src.Prefetch, maxBytes: int64(src.MaxBodyBytes)}
	switch {
	case src.Topic != "" && len(src.Topics) > 0:
		return bad("set topic or topics, not both")
	case src.Topic != "":
		o.topics = []string{src.Topic}
	case len(src.Topics) > 0:
		o.topics = slices.Compact(slices.Sorted(slices.Values(src.Topics)))
	default:
		return bad("topic (or topics) is required")
	}
	for _, t := range o.topics {
		if strings.Contains(t, "{{") {
			return bad("topic %q: a consumed topic cannot be a per-message template", t)
		}
		if err := validTopic(t); err != nil {
			return bad("topic %v", err)
		}
	}
	switch {
	case o.group == "":
		return bad("group is required: the consumer group whose committed offsets let a restart resume")
	case strings.Contains(o.group, "{{"):
		return bad("group cannot be a per-message template")
	}
	switch o.start {
	case "":
		o.start = "latest"
	case "earliest", "latest":
	default:
		return bad("start must be earliest or latest, not %q", o.start)
	}
	switch o.ack {
	case "":
		o.ack = ackDelivered
	case ackDelivered, ackAccepted:
	default:
		return bad("ack must be delivered or accepted, not %q", o.ack)
	}
	switch {
	case o.prefetch < 0:
		return bad("prefetch must not be negative")
	case o.prefetch == 0:
		o.prefetch = defaultPrefetch
	}
	switch {
	case o.maxBytes < 0:
		return bad("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return bad("%v", err)
	}
	if o.client, err = parseClient(src.SASL, src.TLS, clientID(src.ClientID, spec.Env.Relay, spec.Name)); err != nil {
		return bad("%v", err)
	}
	return o, nil
}

// parseSink turns the spec of a kafka destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	bad := func(format string, args ...any) (sinkOptions, error) {
		return sinkOptions{}, fail("destination", spec.Name, format, args...)
	}
	if ep.URL != "" {
		return bad("kafka destinations take brokers (host:port), not url")
	}
	brokers, err := parseBrokers(ep.Brokers)
	if err != nil {
		return bad("%v", err)
	}
	o := sinkOptions{brokers: brokers, acks: ep.Acks}
	if ep.Topic == "" {
		return bad("topic is required: the topic produced to (a per-message template)")
	}
	if o.topic, err = transform.CompileTemplate("topic", ep.Topic); err != nil {
		return bad("topic: %v", err)
	}
	if o.topic.IsStatic() {
		if err := validTopic(ep.Topic); err != nil {
			return bad("topic %v", err)
		}
	}
	if ep.Key != "" {
		if o.key, err = transform.CompileTemplate("key", ep.Key); err != nil {
			return bad("key: %v", err)
		}
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return bad("body: %v", err)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(ep.Headers)) {
		if strings.TrimSpace(name) == "" {
			return bad("headers: a header name is empty")
		}
		t, err := transform.CompileTemplate("headers."+name, ep.Headers[name])
		if err != nil {
			return bad("headers.%s: %v", name, err)
		}
		o.headers = append(o.headers, namedTemplate{name: name, tmpl: t})
	}
	switch o.acks {
	case "":
		o.acks = acksAll
	case acksAll, acksLeader, acksNone:
	default:
		return bad("acks must be all, leader or none, not %q", o.acks)
	}
	switch ep.Compression {
	case "", "none":
		o.compression = kgo.NoCompression()
	case "gzip":
		o.compression = kgo.GzipCompression()
	case "snappy":
		o.compression = kgo.SnappyCompression()
	case "lz4":
		o.compression = kgo.Lz4Compression()
	case "zstd":
		o.compression = kgo.ZstdCompression()
	default:
		return bad("compression must be none, gzip, snappy, lz4 or zstd, not %q", ep.Compression)
	}
	if o.client, err = parseClient(ep.SASL, ep.TLS, clientID(ep.ClientID, spec.Env.Relay, spec.Name)); err != nil {
		return bad("%v", err)
	}
	return o, nil
}

// parseBrokers checks the bootstrap brokers: host:port (or a host, on the
// default port 9092), never a URL or a template.
func parseBrokers(brokers []string) ([]string, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers is required: the bootstrap brokers, host:port")
	}
	out := make([]string, 0, len(brokers))
	for i, b := range brokers {
		b = strings.TrimSpace(b)
		switch {
		case b == "":
			return nil, fmt.Errorf("brokers[%d] is empty", i)
		case strings.Contains(b, "{{"):
			return nil, fmt.Errorf("brokers[%d]: the client connects before any message, so a broker cannot be a per-message template", i)
		case strings.Contains(b, "://"):
			return nil, fmt.Errorf("brokers[%d] %q: brokers are host:port, not URLs", i, b)
		}
		if _, port, err := net.SplitHostPort(b); err == nil {
			if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("brokers[%d] %q: bad port", i, b)
			}
		}
		out = append(out, b)
	}
	return out, nil
}

// validTopic checks a Kafka topic name: 1 to 249 letters, digits, '.', '_'
// or '-', and neither "." nor "..".
func validTopic(t string) error {
	switch {
	case t == "":
		return fmt.Errorf("is empty")
	case t == "." || t == "..":
		return fmt.Errorf("%q is not a valid name", t)
	case len(t) > 249:
		return fmt.Errorf("%q is longer than 249 characters", t)
	}
	for _, r := range t {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return fmt.Errorf("%q has %q: a name uses letters, digits, '.', '_' and '-'", t, r)
		}
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

// parseClient builds the SASL mechanism and the TLS config.
func parseClient(s *config.SASL, t *config.TLS, id string) (clientOptions, error) {
	o := clientOptions{clientID: id}
	if s != nil {
		if s.Username == "" || s.Password == "" {
			return clientOptions{}, fmt.Errorf("sasl needs username and password")
		}
		switch s.Mechanism {
		case "plain":
			o.sasl = plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism()
		case "scram-sha-256":
			o.sasl = scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism()
		case "scram-sha-512":
			o.sasl = scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()
		default:
			return clientOptions{}, fmt.Errorf("sasl.mechanism must be plain, scram-sha-256 or scram-sha-512, not %q", s.Mechanism)
		}
	}
	cfg, err := httpmux.ClientTLS(t)
	if err != nil {
		return clientOptions{}, err
	}
	o.tls = cfg
	return o, nil
}

// kgoOpts are the client options of a connection: the seeds, the client
// ID, the logger, SASL and TLS.
func (c clientOptions) kgoOpts(brokers []string, log *slog.Logger) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(c.clientID),
		kgo.WithLogger(logger{log}),
	}
	if c.sasl != nil {
		opts = append(opts, kgo.SASL(c.sasl))
	}
	if c.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(c.tls))
	}
	return opts
}

// startOffset is where a group without a committed offset begins.
func (o sourceOptions) startOffset() kgo.Offset {
	if o.start == "earliest" {
		return kgo.NewOffset().AtStart()
	}
	return kgo.NewOffset().AtEnd()
}

// producerOpts are the producer settings of a destination.
func (o sinkOptions) producerOpts() []kgo.Opt {
	// A codec falls back to none for brokers too old to read it.
	codecs := []kgo.CompressionCodec{o.compression}
	if o.compression != kgo.NoCompression() {
		codecs = append(codecs, kgo.NoCompression())
	}
	opts := []kgo.Opt{
		kgo.ProducerBatchCompression(codecs...),
		kgo.RecordDeliveryTimeout(recordTimeout),
	}
	switch o.acks {
	case acksLeader:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case acksNone:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	return opts
}

// dropHeaders are not copied from the message onto records: hop-by-hop and
// framing headers, and the caller's own credentials.
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
}

// dropped reports whether a message header stays off the records.
func dropped(name string) bool { return dropHeaders[textproto.CanonicalMIMEHeaderKey(name)] }
