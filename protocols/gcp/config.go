package gcp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Defaults.
const (
	defaultPrefetch = 64
	// defaultMaxExtension is how long the Pub/Sub client keeps extending a
	// message's ack deadline while the relay works on it.
	defaultMaxExtension = 60 * time.Minute
	defaultBackoff      = 500 * time.Millisecond
	defaultMaxBackoff   = 30 * time.Second
	defaultJitter       = "full"
)

// What a gcs source does with an object once its messages are final.
const (
	afterKeep   = "keep"
	afterDelete = "delete"
	afterMove   = "move"
)

// splitLines is the split that makes each non-empty line of an object a
// message. Any other split is a jq program run on the object's JSON.
const splitLines = "lines"

// Attributes the pubsub destination writes and the pubsub source reads.
const (
	attrIdempotencyKey = "Idempotency-Key"
	attrContentType    = "content-type"
	attrSubject        = "subject"
	// internalPrefix marks attributes hikyaku keeps for itself: they do not
	// become headers.
	internalPrefix = "hikyaku-"
)

// clientConfig is how a Google Cloud client is built.
type clientConfig struct {
	project  string
	endpoint string // "" for Google's default endpoint
	emulator bool   // endpoint is an emulator: no credentials, plain text
	opts     []option.ClientOption
}

// parseGCP turns a gcp: block into the options of a pubsub or gcs client.
// Without credentials the client uses Application Default Credentials. An
// endpoint on googleapis.com (a regional or private endpoint) keeps TLS and
// credentials; any other endpoint is an emulator, reached without
// credentials (and, for pubsub, in plain text), the way PUBSUB_EMULATOR_HOST
// and STORAGE_EMULATOR_HOST work.
func parseGCP(g *config.GCP, kind string) (clientConfig, error) {
	if g == nil {
		g = &config.GCP{}
	}
	c := clientConfig{project: strings.TrimSpace(g.Project)}
	creds, err := credentialOptions(g)
	if err != nil {
		return clientConfig{}, err
	}
	if strings.TrimSpace(g.Endpoint) == "" {
		c.opts = creds
		return c, nil
	}
	ep, host, err := normalizeEndpoint(kind, g.Endpoint)
	if err != nil {
		return clientConfig{}, err
	}
	c.endpoint = ep
	if googleHost(host) {
		c.opts = append(creds, option.WithEndpoint(ep))
		return c, nil
	}
	if len(creds) > 0 {
		return clientConfig{}, fmt.Errorf("gcp.endpoint %s is an emulator, which takes no credentials: drop credentials_file and credentials_json (an endpoint on googleapis.com uses them)", ep)
	}
	c.emulator = true
	c.opts = []option.ClientOption{option.WithEndpoint(ep), option.WithoutAuthentication()}
	if kind == kindPubSub {
		c.opts = append(c.opts,
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithTelemetryDisabled())
	}
	return c, nil
}

// credentialOptions reads credentials_file or credentials_json. The
// credential type comes from the file's "type", which the client checks.
func credentialOptions(g *config.GCP) ([]option.ClientOption, error) {
	switch {
	case g.CredentialsFile != "" && g.CredentialsJSON != "":
		return nil, errors.New("gcp: set credentials_file or credentials_json, not both")
	case g.CredentialsFile != "":
		b, err := os.ReadFile(g.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("gcp.credentials_file: %w", err)
		}
		t, err := credentialsType(b)
		if err != nil {
			return nil, fmt.Errorf("gcp.credentials_file %s: %w", g.CredentialsFile, err)
		}
		return []option.ClientOption{option.WithAuthCredentialsFile(t, g.CredentialsFile)}, nil
	case g.CredentialsJSON != "":
		t, err := credentialsType([]byte(g.CredentialsJSON))
		if err != nil {
			return nil, fmt.Errorf("gcp.credentials_json: %w", err)
		}
		return []option.ClientOption{option.WithAuthCredentialsJSON(t, []byte(g.CredentialsJSON))}, nil
	}
	return nil, nil
}

// credentialsType reads the type of JSON credentials. Its errors never quote
// the content, which holds a private key.
func credentialsType(b []byte) (option.CredentialsType, error) {
	var f struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return "", errors.New("is not JSON credentials")
	}
	switch option.CredentialsType(f.Type) {
	case option.ServiceAccount:
		return option.ServiceAccount, nil
	case option.AuthorizedUser:
		return option.AuthorizedUser, nil
	case option.ImpersonatedServiceAccount:
		return option.ImpersonatedServiceAccount, nil
	case option.ExternalAccount:
		return option.ExternalAccount, nil
	case "":
		return "", errors.New(`has no "type"`)
	}
	return "", fmt.Errorf("type %q is not supported (service_account, authorized_user, impersonated_service_account or external_account)", f.Type)
}

// normalizeEndpoint checks an endpoint and returns it with its host. pubsub
// dials host:port (a scheme is dropped; a googleapis.com host without a port
// gets 443). gcs takes the JSON API's base URL: a bare host:port gets http://
// (https:// on googleapis.com), and an empty path gets /storage/v1/.
func normalizeEndpoint(kind, raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if kind == kindPubSub {
		hostport := raw
		if strings.Contains(raw, "://") {
			u, err := url.Parse(raw)
			if err != nil || u.Host == "" {
				return "", "", fmt.Errorf("gcp.endpoint %q is not host:port, such as localhost:8085", raw)
			}
			hostport = u.Host
		}
		host, port, err := net.SplitHostPort(hostport)
		if err != nil {
			if googleHost(hostport) {
				return net.JoinHostPort(hostport, "443"), hostport, nil
			}
			return "", "", fmt.Errorf("gcp.endpoint %q is not host:port, such as localhost:8085", raw)
		}
		if host == "" || port == "" {
			return "", "", fmt.Errorf("gcp.endpoint %q is not host:port, such as localhost:8085", raw)
		}
		return hostport, host, nil
	}
	if !strings.Contains(raw, "://") {
		host, _, _ := strings.Cut(raw, "/")
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		scheme := "http"
		if googleHost(host) {
			scheme = "https"
		}
		raw = scheme + "://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", "", fmt.Errorf("gcp.endpoint %q is not a URL, such as http://localhost:4443/storage/v1/", raw)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/storage/v1/"
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String(), u.Hostname(), nil
}

// googleHost reports whether host is a Google API host.
func googleHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	return host == "googleapis.com" || strings.HasSuffix(host, ".googleapis.com")
}

// resourceName resolves a subscription or topic given as an ID in project or
// as a full projects/<project>/<collection>/<id> path. It returns the full
// name and its project.
func resourceName(field, name, collection, project string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", fmt.Errorf("%s is required", field)
	}
	if strings.HasPrefix(name, "projects/") {
		parts := strings.Split(name, "/")
		if len(parts) != 4 || parts[1] == "" || parts[2] != collection || parts[3] == "" {
			return "", "", fmt.Errorf("%s %q is not a projects/<project>/%s/<name> path", field, name, collection)
		}
		return name, parts[1], nil
	}
	if strings.Contains(name, "/") {
		return "", "", fmt.Errorf("%s %q is neither an ID nor a projects/<project>/%s/<name> path", field, name, collection)
	}
	if project == "" {
		return "", "", fmt.Errorf("gcp.project is required (or give %s as projects/<project>/%s/%s)", field, collection, name)
	}
	return "projects/" + project + "/" + collection + "/" + name, project, nil
}

// reconnectOptions is a subscribe source's reconnect policy.
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

// parseMaxBytes applies the max_body_bytes default (10MiB).
func parseMaxBytes(v config.Size) (int64, error) {
	switch {
	case v < 0:
		return 0, errors.New("max_body_bytes must not be negative")
	case v == 0:
		return config.DefaultMaxBodyBytes, nil
	}
	return int64(v), nil
}

// pubsubSourceOptions is a pubsub source's configuration, defaults applied.
type pubsubSourceOptions struct {
	client       clientConfig
	subscription string // projects/<project>/subscriptions/<id>
	ack          string
	maxBytes     int64
	prefetch     int
	reconnect    reconnectOptions
}

// parsePubSubSource turns the spec of a pubsub source into its options.
func parsePubSubSource(spec plugin.Spec) (pubsubSourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return pubsubSourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(err error) (pubsubSourceOptions, error) {
		return pubsubSourceOptions{}, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	if src.Mode != "" && src.Mode != "subscribe" {
		return fail(fmt.Errorf("pubsub sources run in subscribe mode, not %q", src.Mode))
	}
	c, err := parseGCP(src.GCP, kindPubSub)
	if err != nil {
		return fail(err)
	}
	sub, project, err := resourceName("subscription", src.Subscription, "subscriptions", c.project)
	if err != nil {
		return fail(err)
	}
	if c.project == "" {
		c.project = project
	}
	o := pubsubSourceOptions{client: c, subscription: sub, prefetch: src.Prefetch}
	switch src.Ack {
	case "", "delivered", "accepted":
		o.ack = src.Ack
	default:
		return fail(fmt.Errorf("ack must be delivered or accepted, not %q", src.Ack))
	}
	if o.maxBytes, err = parseMaxBytes(src.MaxBodyBytes); err != nil {
		return fail(err)
	}
	switch {
	case o.prefetch < 0:
		return fail(errors.New("prefetch must not be negative"))
	case o.prefetch == 0:
		o.prefetch = defaultPrefetch
	}
	if o.reconnect, err = parseReconnect(src.Reconnect); err != nil {
		return fail(err)
	}
	return o, nil
}

// pubsubSinkOptions is a pubsub destination's configuration.
type pubsubSinkOptions struct {
	client  clientConfig
	topic   *transform.Template
	static  string              // the full topic name, when topic is not a template
	key     *transform.Template // the ordering key; nil without one
	headers map[string]*transform.Template
	body    *transform.Template
}

// parsePubSubSink turns the spec of a pubsub destination into its options.
func parsePubSubSink(spec plugin.Spec) (pubsubSinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return pubsubSinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(err error) (pubsubSinkOptions, error) {
		return pubsubSinkOptions{}, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	c, err := parseGCP(ep.GCP, kindPubSub)
	if err != nil {
		return fail(err)
	}
	if strings.TrimSpace(ep.Topic) == "" {
		return fail(errors.New("topic is required"))
	}
	o := pubsubSinkOptions{}
	if o.topic, err = transform.CompileTemplate("topic", ep.Topic); err != nil {
		return fail(err)
	}
	switch {
	case o.topic.IsStatic():
		full, project, err := resourceName("topic", ep.Topic, "topics", c.project)
		if err != nil {
			return fail(err)
		}
		o.static = full
		if c.project == "" {
			c.project = project
		}
	case c.project == "":
		if !strings.HasPrefix(ep.Topic, "projects/") {
			return fail(errors.New("gcp.project is required (or give topic as projects/<project>/topics/<name>)"))
		}
		// Every topic is then a full path: the client's project only
		// resolves IDs, so any name does.
		c.project = "-"
		if seg := strings.Split(ep.Topic, "/")[1]; seg != "" && !strings.Contains(seg, "{{") {
			c.project = seg
		}
	}
	o.client = c
	if ep.Key != "" {
		if o.key, err = transform.CompileTemplate("key", ep.Key); err != nil {
			return fail(err)
		}
	}
	if o.headers, err = compileHeaders(ep.Headers); err != nil {
		return fail(err)
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail(err)
		}
	}
	return o, nil
}

// compileHeaders compiles the headers: block, whose values may be
// per-message templates.
func compileHeaders(h map[string]string) (map[string]*transform.Template, error) {
	out := make(map[string]*transform.Template, len(h))
	for k, v := range h {
		if strings.TrimSpace(k) == "" {
			return nil, errors.New("headers: a name is empty")
		}
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return nil, err
		}
		out[k] = t
	}
	return out, nil
}

// gcsSourceOptions is a gcs source's configuration, defaults applied.
type gcsSourceOptions struct {
	client     clientConfig
	bucket     string
	prefix     string
	after      string
	moveTo     string
	stateStore string
	split      string
	jq         *transform.JQ
	interval   time.Duration
	timeout    time.Duration
	maxBytes   int64
	ack        string
}

// parseGCSSource turns the spec of a gcs source into its options.
func parseGCSSource(spec plugin.Spec) (gcsSourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return gcsSourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	fail := func(err error) (gcsSourceOptions, error) {
		return gcsSourceOptions{}, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	if src.Mode != "" && src.Mode != "poll" {
		return fail(fmt.Errorf("gcs sources run in poll mode, not %q", src.Mode))
	}
	c, err := parseGCP(src.GCP, kindGCS)
	if err != nil {
		return fail(err)
	}
	o := gcsSourceOptions{client: c, prefix: src.Prefix, moveTo: src.MoveTo, stateStore: src.StateStore, ack: src.Ack}
	if o.bucket, err = parseBucket(src.Bucket); err != nil {
		return fail(err)
	}
	switch src.After {
	case "":
		o.after = afterDelete
	case afterKeep, afterDelete, afterMove:
		o.after = src.After
	default:
		return fail(fmt.Errorf("after must be keep, delete or move, not %q", src.After))
	}
	switch {
	case o.after == afterMove && o.moveTo == "":
		return fail(errors.New("move_to is required with after: move"))
	case o.after != afterMove && o.moveTo != "":
		return fail(errors.New("move_to applies with after: move only"))
	}
	switch src.Split {
	case "", splitLines:
		o.split = src.Split
	default:
		if o.jq, err = transform.CompileJQ(src.Split); err != nil {
			return fail(fmt.Errorf("split: %w", err))
		}
		o.split = src.Split
	}
	switch src.Ack {
	case "", "delivered":
	default:
		return fail(fmt.Errorf("ack must be delivered for a gcs source: an object is deleted, moved or kept only once its messages are final, not %q", src.Ack))
	}
	if o.interval, err = positive("interval", src.Interval, config.DefaultPollInterval); err != nil {
		return fail(err)
	}
	if o.timeout, err = positive("timeout", src.Timeout, config.DefaultPollTimeout); err != nil {
		return fail(err)
	}
	if o.maxBytes, err = parseMaxBytes(src.MaxBodyBytes); err != nil {
		return fail(err)
	}
	return o, nil
}

// positive returns a duration, def when it is zero.
func positive(field string, d config.Duration, def time.Duration) (time.Duration, error) {
	switch {
	case d < 0:
		return 0, fmt.Errorf("%s must not be negative", field)
	case d == 0:
		return def, nil
	}
	return d.Std(), nil
}

// parseBucket checks a bucket name; a gs:// prefix is dropped.
func parseBucket(b string) (string, error) {
	b = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(b), "gs://"), "/")
	switch {
	case b == "":
		return "", errors.New("bucket is required")
	case strings.Contains(b, "/"):
		return "", fmt.Errorf("bucket %q is a bucket name, without a path (use prefix or key for the path)", b)
	}
	return b, nil
}

// gcsSinkOptions is a gcs destination's configuration.
type gcsSinkOptions struct {
	client      clientConfig
	bucket      string
	key         *transform.Template // nil: <relay>/<yyyy>/<mm>/<dd>/<id>.json
	contentType string
	headers     map[string]*transform.Template
	body        *transform.Template
}

// parseGCSSink turns the spec of a gcs destination into its options.
func parseGCSSink(spec plugin.Spec) (gcsSinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return gcsSinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	fail := func(err error) (gcsSinkOptions, error) {
		return gcsSinkOptions{}, fmt.Errorf("destination %s: %w", spec.Name, err)
	}
	c, err := parseGCP(ep.GCP, kindGCS)
	if err != nil {
		return fail(err)
	}
	o := gcsSinkOptions{client: c, contentType: ep.ContentType}
	if o.bucket, err = parseBucket(ep.Bucket); err != nil {
		return fail(err)
	}
	if ep.Key != "" {
		if o.key, err = transform.CompileTemplate("key", ep.Key); err != nil {
			return fail(err)
		}
	}
	if o.headers, err = compileHeaders(ep.Headers); err != nil {
		return fail(err)
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return fail(err)
		}
	}
	return o, nil
}
