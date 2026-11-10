package azure

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// The source and destination kinds.
const (
	kindServiceBus = "servicebus"
	kindEventHubs  = "eventhubs"
)

// Defaults.
const (
	defaultPrefetch      = 64
	maxPrefetch          = 5000 // the Service Bus receiver's credit limit
	defaultConsumerGroup = "$Default"
	startEarliest        = "earliest"
	startLatest          = "latest"

	defaultBackoff    = 500 * time.Millisecond
	defaultMaxBackoff = 30 * time.Second
	defaultJitter     = "full"
)

// Acknowledgement modes of a source.
const (
	ackDelivered = "delivered"
	ackAccepted  = "accepted"
)

// conn is how an entry reaches its namespace: a connection string, or a
// namespace with a token credential. It never prints the connection string.
type conn struct {
	connectionString string
	// namespace is the fully qualified namespace (host, and a port for the
	// emulators), for logs, endpoints and checkpoint keys.
	namespace string
	// entityPath is the connection string's EntityPath, "" when none.
	entityPath string
	credential azcore.TokenCredential
	tls        *tls.Config
}

// String hides the connection string.
func (c conn) String() string { return c.namespace }

// parseConn reads an azure: block. It needs a connection string, or a
// namespace with Microsoft Entra credentials: client secret credentials
// when client_id and client_secret are set (with tenant_id), else
// DefaultAzureCredential (in tenant_id when it is set).
func parseConn(a *config.Azure, t *config.TLS) (conn, error) {
	if a == nil || (a.ConnectionString == "" && a.Namespace == "") {
		return conn{}, errors.New("azure needs connection_string, or namespace with Microsoft Entra credentials")
	}
	if a.ConnectionString != "" && a.Namespace != "" {
		return conn{}, errors.New("azure: set connection_string or namespace, not both")
	}
	tlsCfg, err := httpmux.ClientTLS(t)
	if err != nil {
		return conn{}, err
	}
	c := conn{tls: tlsCfg}
	if a.ConnectionString != "" {
		// The parser's errors never quote the connection string.
		props, err := azeventhubs.ParseConnectionString(a.ConnectionString)
		if err != nil {
			return conn{}, fmt.Errorf("azure.connection_string: %w", err)
		}
		c.connectionString, c.namespace = a.ConnectionString, props.FullyQualifiedNamespace
		if props.EntityPath != nil {
			c.entityPath = *props.EntityPath
		}
		return c, nil
	}
	c.namespace = namespaceHost(a.Namespace)
	if c.namespace == "" || strings.ContainsAny(c.namespace, " /;") {
		return conn{}, fmt.Errorf("azure.namespace %q must be a fully qualified namespace, such as <name>.servicebus.windows.net", a.Namespace)
	}
	switch {
	case a.ClientID != "" || a.ClientSecret != "":
		if a.TenantID == "" || a.ClientID == "" || a.ClientSecret == "" {
			return conn{}, errors.New("azure: client secret credentials need tenant_id, client_id and client_secret")
		}
		if c.credential, err = azidentity.NewClientSecretCredential(a.TenantID, a.ClientID, a.ClientSecret, nil); err != nil {
			return conn{}, fmt.Errorf("azure: client secret credential: %w", err)
		}
	default:
		if c.credential, err = azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{TenantID: a.TenantID}); err != nil {
			return conn{}, fmt.Errorf("azure: default credential: %w", err)
		}
	}
	return c, nil
}

// namespaceHost strips a scheme and slashes from a namespace.
func namespaceHost(ns string) string {
	ns = strings.TrimSpace(ns)
	if i := strings.Index(ns, "://"); i >= 0 {
		ns = ns[i+3:]
	}
	return strings.Trim(ns, "/")
}

// entity is the event hub of an eventhubs entry: topic, or the connection
// string's EntityPath.
func (c conn) hub(topic string) (string, error) {
	switch {
	case topic == "" && c.entityPath == "":
		return "", errors.New("topic is required: the event hub (or an EntityPath in the connection string)")
	case topic != "" && c.entityPath != "" && topic != c.entityPath:
		return "", fmt.Errorf("topic %q differs from the connection string's EntityPath %q", topic, c.entityPath)
	case topic == "":
		return c.entityPath, nil
	}
	return topic, nil
}

// reconnectOptions is a source's reconnect policy.
type reconnectOptions struct {
	backoff     time.Duration
	maxBackoff  time.Duration
	jitter      string
	maxAttempts int // 0 = forever
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

// sourceCommon is what every subscribe source of this module reads.
type sourceCommon struct {
	conn      conn
	maxBytes  int64
	ack       string
	reconnect reconnectOptions
}

// parseSourceCommon checks the fields every source shares.
func parseSourceCommon(src *config.Source, kind string) (sourceCommon, error) {
	if src.Mode != "" && src.Mode != "subscribe" {
		return sourceCommon{}, fmt.Errorf("%s sources run in subscribe mode, not %q", kind, src.Mode)
	}
	c, err := parseConn(src.Azure, src.TLS)
	if err != nil {
		return sourceCommon{}, err
	}
	o := sourceCommon{conn: c, maxBytes: int64(src.MaxBodyBytes), ack: src.Ack}
	switch {
	case o.maxBytes < 0:
		return sourceCommon{}, errors.New("max_body_bytes must not be negative")
	case o.maxBytes == 0:
		o.maxBytes = config.DefaultMaxBodyBytes
	}
	switch o.ack {
	case "":
		o.ack = ackDelivered
	case ackDelivered, ackAccepted:
	default:
		return sourceCommon{}, fmt.Errorf("ack must be delivered or accepted, not %q", o.ack)
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return sourceCommon{}, err
	}
	return o, nil
}

// sbEntity is what a servicebus source receives from: a queue, or a topic
// subscription.
type sbEntity struct {
	queue, topic, subscription string
}

// path is the entity path: the queue, or <topic>/subscriptions/<name>.
func (e sbEntity) path() string {
	if e.queue != "" {
		return e.queue
	}
	return e.topic + "/subscriptions/" + e.subscription
}

// name is the queue, or the topic: the envelope subject of a message
// without one.
func (e sbEntity) name() string {
	if e.queue != "" {
		return e.queue
	}
	return e.topic
}

// sbSourceOptions is a servicebus source's configuration, defaults applied.
type sbSourceOptions struct {
	sourceCommon
	entity   sbEntity
	prefetch int
}

// parseSBSource turns the spec of a servicebus source into its options.
func parseSBSource(spec plugin.Spec) (sbSourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sbSourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(err error) (sbSourceOptions, error) {
		return sbSourceOptions{}, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	common, err := parseSourceCommon(src, kindServiceBus)
	if err != nil {
		return fail(err)
	}
	o := sbSourceOptions{sourceCommon: common, prefetch: src.Prefetch}
	switch {
	case src.Queue != "" && src.Topic != "":
		return fail(errors.New("set queue or topic (with subscription), not both"))
	case src.Queue != "":
		if src.Subscription != "" {
			return fail(errors.New("subscription applies to a topic, not a queue"))
		}
		o.entity = sbEntity{queue: src.Queue}
	case src.Topic != "":
		if src.Subscription == "" {
			return fail(errors.New("subscription is required with topic"))
		}
		o.entity = sbEntity{topic: src.Topic, subscription: src.Subscription}
	default:
		return fail(errors.New("queue is required: the queue (or topic with subscription) received from"))
	}
	switch {
	case o.prefetch < 0:
		return fail(errors.New("prefetch must not be negative"))
	case o.prefetch == 0:
		o.prefetch = defaultPrefetch
	case o.prefetch > maxPrefetch:
		return fail(fmt.Errorf("prefetch must be at most %d (the receiver's credit limit)", maxPrefetch))
	}
	return o, nil
}

// ehSourceOptions is an eventhubs source's configuration, defaults applied.
type ehSourceOptions struct {
	sourceCommon
	hub   string
	group string
	start string
	// store keeps the checkpoints and the partition leases.
	store     store.Store
	storeName string
}

// parseEHSource turns the spec of an eventhubs source into its options and
// resolves its state store.
func parseEHSource(spec plugin.Spec) (ehSourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return ehSourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(err error) (ehSourceOptions, error) {
		return ehSourceOptions{}, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	common, err := parseSourceCommon(src, kindEventHubs)
	if err != nil {
		return fail(err)
	}
	o := ehSourceOptions{sourceCommon: common, group: src.Group, start: src.Start, storeName: src.StateStore}
	if o.hub, err = common.conn.hub(src.Topic); err != nil {
		return fail(err)
	}
	if o.group == "" {
		o.group = defaultConsumerGroup
	}
	switch o.start {
	case "":
		o.start = startLatest
	case startEarliest, startLatest:
	default:
		return fail(fmt.Errorf("start must be earliest or latest, not %q", o.start))
	}
	if spec.Env.Store == nil {
		return fail(errors.New("no store to keep the checkpoints in"))
	}
	if o.store, err = spec.Env.Store(src.StateStore); err != nil {
		return fail(fmt.Errorf("state_store: %w", err))
	}
	if o.store == nil {
		return fail(errors.New("no store to keep the checkpoints in"))
	}
	return o, nil
}

// sbSinkOptions is a servicebus destination's configuration.
type sbSinkOptions struct {
	conn    conn
	entity  string // the queue or topic sent to
	session *transform.Template
	out     outgoing
}

// parseSBSink turns the spec of a servicebus destination into its options.
func parseSBSink(spec plugin.Spec) (sbSinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sbSinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(err error) (sbSinkOptions, error) {
		return sbSinkOptions{}, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	c, err := parseConn(ep.Azure, ep.TLS)
	if err != nil {
		return fail(err)
	}
	o := sbSinkOptions{conn: c}
	switch {
	case ep.Queue != "" && ep.Topic != "":
		return fail(errors.New("set queue or topic, not both"))
	case ep.Queue != "":
		o.entity = ep.Queue
	case ep.Topic != "":
		o.entity = ep.Topic
	case c.entityPath != "":
		o.entity = c.entityPath
	default:
		return fail(errors.New("queue is required: the queue (or topic) sent to"))
	}
	if strings.Contains(o.entity, "{{") {
		return fail(errors.New("the queue or topic cannot be a per-message template: a sender is opened per entity"))
	}
	if ep.GroupID != "" {
		if o.session, err = transform.CompileTemplate("group_id", ep.GroupID); err != nil {
			return fail(err)
		}
	}
	if o.out, err = compileOutgoing(ep.Headers, ep.Body); err != nil {
		return fail(err)
	}
	return o, nil
}

// ehSinkOptions is an eventhubs destination's configuration.
type ehSinkOptions struct {
	conn conn
	hub  string
	key  *transform.Template
	out  outgoing
}

// parseEHSink turns the spec of an eventhubs destination into its options.
func parseEHSink(spec plugin.Spec) (ehSinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return ehSinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(err error) (ehSinkOptions, error) {
		return ehSinkOptions{}, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	c, err := parseConn(ep.Azure, ep.TLS)
	if err != nil {
		return fail(err)
	}
	o := ehSinkOptions{conn: c}
	if o.hub, err = c.hub(ep.Topic); err != nil {
		return fail(err)
	}
	if strings.Contains(o.hub, "{{") {
		return fail(errors.New("the event hub cannot be a per-message template: a producer is opened per event hub"))
	}
	if ep.Key != "" {
		if o.key, err = transform.CompileTemplate("key", ep.Key); err != nil {
			return fail(err)
		}
	}
	if o.out, err = compileOutgoing(ep.Headers, ep.Body); err != nil {
		return fail(err)
	}
	return o, nil
}
