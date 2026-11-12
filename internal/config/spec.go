// Package config is the hikyaku.yaml spec: the Go structs (whose tags
// generate schema/hikyaku.schema.json), loading and merging files,
// {{secrets.X}} / {{env.X}} interpolation, defaults, validation and lint.
package config

// Spec is one relay: sources, destinations, routes and their policies.
type Spec struct {
	Version       int               `yaml:"version" jsonschema:"required,enum=1" jsonschema_description:"Spec format version. Always 1."`
	Name          string            `yaml:"name" jsonschema:"required" jsonschema_description:"Relay name. Used in metrics, logs and the shared-state keys."`
	Secrets       map[string]string `yaml:"secrets,omitempty" jsonschema_description:"Secrets by name, each a mamori ref (env:NAME, file:///path, vault://..., or a comma-separated chain tried in order, such as env:A,file:///run/a). Reference one as {{secrets.NAME}}."`
	Genkit        Genkit            `yaml:"genkit,omitempty" jsonschema_description:"Genkit plugins for the AI router."`
	Models        Models            `yaml:"models,omitempty" jsonschema_description:"Default models, model retries and model fallback chains."`
	Defaults      Policy            `yaml:"defaults,omitempty" jsonschema_description:"Delivery policy inherited by every destination (each field on its own)."`
	Sources       []Source          `yaml:"sources" jsonschema:"required" jsonschema_description:"Where messages come from."`
	Destinations  []Destination     `yaml:"destinations" jsonschema:"required" jsonschema_description:"Where messages go."`
	Stores        []Store           `yaml:"stores,omitempty" jsonschema_description:"Shared-state backends (history, decision cache, dedupe, idempotency, budgets), referenced by name. The first one is the default store; with none, an in-process memory store is used."`
	DeadLetter    *DeadLetter       `yaml:"dead_letter,omitempty" jsonschema_description:"Where a message goes when a required destination and all its fallbacks failed. Defaults to stderr."`
	Routes        []Route           `yaml:"routes" jsonschema:"required" jsonschema_description:"Which destinations get the messages of which sources."`
	Server        Server            `yaml:"server,omitempty" jsonschema_description:"Admin API, workers and shutdown."`
	Observability Observability     `yaml:"observability,omitempty" jsonschema_description:"Logs, OpenTelemetry traces and Prometheus metrics."`
	Buffer        Buffer            `yaml:"buffer,omitempty" jsonschema_description:"The queue between sources and delivery workers."`
	Egress        Egress            `yaml:"egress,omitempty" jsonschema_description:"SSRF guard for destinations whose URL host is a per-message template."`
	Plugins       []PluginBinary    `yaml:"plugins,omitempty" jsonschema_description:"Out-of-process plugins: binaries hikyaku starts and talks gRPC to. Each one adds the source and destination types it declares."`
	Notify        []Notify          `yaml:"notify,omitempty" jsonschema_description:"Alerts: a message sent to a destination when a message is dead-lettered or a circuit breaker changes."`
}

// Notify is one alert rule.
type Notify struct {
	On    []string `yaml:"on,omitempty" jsonschema:"enum=dead_letter,enum=circuit_open,enum=circuit_closed" jsonschema_description:"The events that alert (default dead_letter and circuit_open)."`
	To    []string `yaml:"to" jsonschema:"required" jsonschema_description:"The destinations that get the alert, a JSON message."`
	Every Duration `yaml:"every,omitempty" jsonschema_description:"At most one alert per event and subject in this time; the next one counts what happened meanwhile (default 1m)."`
}

// The events of notify.on.
const (
	NotifyDeadLetter    = "dead_letter"
	NotifyCircuitOpen   = "circuit_open"
	NotifyCircuitClosed = "circuit_closed"
)

// NotifyEvents are the events notify.on accepts.
var NotifyEvents = []string{NotifyDeadLetter, NotifyCircuitOpen, NotifyCircuitClosed}

// PluginBinary is an out-of-process plugin (see the grpcplugin module).
type PluginBinary struct {
	Name    string            `yaml:"name" jsonschema:"required" jsonschema_description:"Plugin name, used in logs and metrics."`
	Command string            `yaml:"command" jsonschema:"required" jsonschema_description:"The plugin binary: a path, relative to the spec file, or a name looked up on PATH."`
	Args    []string          `yaml:"args,omitempty" jsonschema_description:"Arguments passed to the binary."`
	Env     map[string]string `yaml:"env,omitempty" jsonschema_description:"Environment variables set for the plugin (values may use {{secrets.X}})."`
	SHA256  string            `yaml:"sha256,omitempty" jsonschema_description:"Hex SHA-256 of the binary. When set, hikyaku refuses to start a binary that does not match."`
}

// Genkit configures the Genkit plugins of the AI router.
type Genkit struct {
	Plugins   []string  `yaml:"plugins,omitempty" jsonschema_description:"Plugins to initialize: systemone, googlegenai, anthropic, openai, openrouter, ollama, llmfake (test builds). Plugins named by a model's provider prefix are added automatically."`
	SystemOne SystemOne `yaml:"systemone,omitempty" jsonschema_description:"System One decision-model servers."`
	Ollama    Ollama    `yaml:"ollama,omitempty" jsonschema_description:"The Ollama server of the ollama plugin and the ollama decision server."`
	OpenAI    OpenAI    `yaml:"openai,omitempty" jsonschema_description:"The server of the openai plugin."`
}

// OpenAI locates the server the openai plugin calls.
type OpenAI struct {
	BaseURL string `yaml:"base_url,omitempty" jsonschema_description:"Base URL of the OpenAI API (default https://api.openai.com/v1, else OPENAI_BASE_URL when set). Point it at any OpenAI-compatible server, such as vLLM, LM Studio, LiteLLM or a gateway; its models are openai/<name>. OPENAI_API_KEY is sent as a bearer token; a server other than the default may need none."`
}

// SystemOne lists the decision-model servers.
type SystemOne struct {
	Servers           []string          `yaml:"servers,omitempty" jsonschema_description:"Known servers: typesafe (TYPESAFE_API_KEY), openrouter (OPENROUTER_API_KEY), workersai (CLOUDFLARE_API_TOKEN), ollama (no key)."`
	Custom            []SystemOneServer `yaml:"custom,omitempty" jsonschema_description:"Any other server that speaks the System One protocol."`
	WorkersAIAccount  string            `yaml:"workersai_account_id,omitempty" jsonschema_description:"Cloudflare account ID for workersai (else CLOUDFLARE_ACCOUNT_ID)."`
	OllamaURL         string            `yaml:"ollama_url,omitempty" jsonschema_description:"Base URL of the Ollama decision server (default http://localhost:11434)."`
	TypeSafeBaseURL   string            `yaml:"typesafe_base_url,omitempty" jsonschema_description:"Override TypeSafe's base URL (else TYPESAFE_BASE_URL)."`
	OpenRouterBaseURL string            `yaml:"openrouter_base_url,omitempty" jsonschema_description:"Override OpenRouter's base URL."`
}

// SystemOneServer is a custom System One server.
type SystemOneServer struct {
	Provider   string            `yaml:"provider" jsonschema:"required" jsonschema_description:"Plugin name and model prefix: models are <provider>/<id>."`
	BaseURL    string            `yaml:"base_url" jsonschema:"required" jsonschema_description:"Server root."`
	Path       string            `yaml:"path,omitempty" jsonschema_description:"Decision endpoint path (default /v1/systemone)."`
	ModelsPath string            `yaml:"models_path,omitempty" jsonschema_description:"Model listing path."`
	APIKey     string            `yaml:"api_key,omitempty" jsonschema_description:"Bearer key, usually {{secrets.NAME}}."`
	Headers    map[string]string `yaml:"headers,omitempty" jsonschema_description:"Extra request headers."`
	Images     bool              `yaml:"images,omitempty" jsonschema_description:"The server reads images."`
}

// Ollama locates the Ollama server.
type Ollama struct {
	URL string `yaml:"url,omitempty" jsonschema_description:"Base URL (default http://localhost:11434)."`
}

// Models names the default models and how model calls are retried.
type Models struct {
	Default  string        `yaml:"default,omitempty" jsonschema_description:"Default generative model, such as googleai/gemini-2.5-flash."`
	Decision string        `yaml:"decision,omitempty" jsonschema_description:"Default decision model, such as typesafe/jev-1.13.0. Pin a version in production."`
	Retry    ModelRetry    `yaml:"retry,omitempty" jsonschema_description:"Tries per model before the next model of the chain."`
	Fallback ModelFallback `yaml:"fallback,omitempty" jsonschema_description:"Global model fallback chains."`
}

// ModelRetry is the per-model retry policy.
type ModelRetry struct {
	Attempts   int      `yaml:"attempts,omitempty" jsonschema_description:"Tries per model (default 3). Each try may include the System One plugin's own two transport retries."`
	Backoff    Duration `yaml:"backoff,omitempty" jsonschema_description:"Wait before the second try (default 500ms), doubling per try."`
	MaxBackoff Duration `yaml:"max_backoff,omitempty" jsonschema_description:"Cap on the wait (default 10s)."`
}

// ModelFallback are the global model fallback chains.
type ModelFallback struct {
	Default  []string `yaml:"default,omitempty" jsonschema_description:"Generative models tried after models.default."`
	Decision []string `yaml:"decision,omitempty" jsonschema_description:"Decision models tried after models.decision."`
}

// Policy is a destination's delivery policy. Every field left out is
// inherited from defaults:, then from the built-in defaults.
type Policy struct {
	Timeout        Duration        `yaml:"timeout,omitempty" jsonschema_description:"Per-attempt timeout (default 5s)."`
	Deadline       Duration        `yaml:"deadline,omitempty" jsonschema_description:"Overall budget across attempts and fallbacks (default none)."`
	Retry          *Retry          `yaml:"retry,omitempty" jsonschema_description:"Retry policy per endpoint."`
	CircuitBreaker *CircuitBreaker `yaml:"circuit_breaker,omitempty" jsonschema_description:"Circuit breaker per endpoint."`
	RetryBudget    *RetryBudget    `yaml:"retry_budget,omitempty" jsonschema_description:"Caps retries as a fraction of traffic."`
}

// Retry is a retry policy.
type Retry struct {
	Attempts          int      `yaml:"attempts,omitempty" jsonschema_description:"Tries per endpoint; 1 means no retry (default 5)."`
	Backoff           Duration `yaml:"backoff,omitempty" jsonschema_description:"Base wait, doubled per try (default 200ms)."`
	MaxBackoff        Duration `yaml:"max_backoff,omitempty" jsonschema_description:"Cap on the wait (default 10s)."`
	Jitter            string   `yaml:"jitter,omitempty" jsonschema:"enum=full,enum=equal,enum=none" jsonschema_description:"full (default): random in [0, wait]; equal: wait/2 + random half; none."`
	RetryOn           TagList  `yaml:"retry_on,omitempty" jsonschema_description:"Error tags retried (default connect, timeout, 5xx, 429): ranges (5xx), codes (503), classes (connect, timeout) and protocol codes (ws:close_1011)."`
	NeverOn           TagList  `yaml:"never_on,omitempty" jsonschema_description:"Error tags never retried (default 4xx). A more specific retry_on tag wins, so 429 and 408 still retry."`
	RespectRetryAfter *bool    `yaml:"respect_retry_after,omitempty" jsonschema_description:"Wait what Retry-After (or a broker hint) says, capped by max_backoff (default true)."`
}

// CircuitBreaker opens after too many failures and lets a few trial requests
// through once open_for has passed.
type CircuitBreaker struct {
	Enabled      *bool    `yaml:"enabled,omitempty" jsonschema_description:"Default true."`
	FailureRatio float64  `yaml:"failure_ratio,omitempty" jsonschema:"minimum=0,maximum=1" jsonschema_description:"Open when failures/requests in the window reach this (default 0.5)."`
	MinRequests  int      `yaml:"min_requests,omitempty" jsonschema_description:"Requests in the window before the ratio counts (default 20)."`
	OpenFor      Duration `yaml:"open_for,omitempty" jsonschema_description:"How long the breaker stays open (default 30s)."`
	HalfOpenMax  int      `yaml:"half_open_max,omitempty" jsonschema_description:"Trial requests while half-open (default 5)."`
	Window       Duration `yaml:"window,omitempty" jsonschema_description:"Rolling window of the failure ratio (default 60s)."`
}

// RetryBudget caps retries as a fraction of first attempts.
type RetryBudget struct {
	Enabled      *bool   `yaml:"enabled,omitempty" jsonschema_description:"Default true."`
	Ratio        float64 `yaml:"ratio,omitempty" jsonschema_description:"Retries allowed per first attempt in the window (default 0.2)."`
	MinPerSecond float64 `yaml:"min_per_second,omitempty" jsonschema_description:"Retries always allowed per second (default 10)."`
	Store        string  `yaml:"store,omitempty" jsonschema_description:"Store that shares the counters across replicas (default: in-process)."`
}

// Source is where messages come from.
type Source struct {
	Name string `yaml:"name" jsonschema:"required" jsonschema_description:"Unique source name."`
	Type string `yaml:"type" jsonschema:"required" jsonschema_description:"Protocol: http, https, ws, wss, sse, stdin, file (M1); see the protocol matrix for the rest."`
	Mode string `yaml:"mode,omitempty" jsonschema:"enum=listen,enum=poll,enum=subscribe" jsonschema_description:"listen (hikyaku is the server), poll (pull on a schedule) or subscribe (long-lived client). Defaults per protocol."`

	Listen       string   `yaml:"listen,omitempty" jsonschema_description:"listen: address, such as :8080."`
	Path         string   `yaml:"path,omitempty" jsonschema_description:"listen: request path (default /); file: the file to read."`
	Methods      []string `yaml:"methods,omitempty" jsonschema_description:"http listen: accepted methods (default POST, PUT)."`
	TLS          *TLS     `yaml:"tls,omitempty" jsonschema_description:"TLS (and mTLS with client_ca_file) for listen, DTLS for udp; client TLS for poll and subscribe."`
	Auth         *Auth    `yaml:"auth,omitempty" jsonschema_description:"Inbound authentication of listen sources."`
	Ack          string   `yaml:"ack,omitempty" jsonschema:"enum=delivered,enum=accepted" jsonschema_description:"delivered (default) or accepted. http and grpc listen: delivered answers once the message is delivered or dead-lettered, accepted as soon as it is queued (on disk with a disk buffer). Subscribe sources: delivered acknowledges the broker once the outcome is final, accepted once the message is queued (safe with buffer.type: disk only)."`
	Respond      *Respond `yaml:"respond,omitempty" jsonschema_description:"http and grpc listen: send a destination's reply back to the caller (smart reverse proxy; grpc maps it into the method's output type)."`
	MaxBodyBytes Size     `yaml:"max_body_bytes,omitempty" jsonschema_description:"Largest accepted message (default 10MiB; grpc and openai 4MiB; tcp, udp and mqtt 1MiB). A listen source refuses a larger one (http 413, grpc RESOURCE_EXHAUSTED); a subscribe or poll source logs it and acknowledges it, so it cannot loop forever."`

	URL          string            `yaml:"url,omitempty" jsonschema_description:"poll or subscribe: the endpoint."`
	Headers      map[string]string `yaml:"headers,omitempty" jsonschema_description:"poll or subscribe: request headers."`
	Method       string            `yaml:"method,omitempty" jsonschema_description:"poll: request method (default GET)."`
	Body         string            `yaml:"body,omitempty" jsonschema_description:"poll: request body."`
	Interval     Duration          `yaml:"interval,omitempty" jsonschema_description:"poll: time between polls (default 30s)."`
	Timeout      Duration          `yaml:"timeout,omitempty" jsonschema_description:"poll (http, s3, gcs): per-request timeout (default 10s)."`
	Retry        *Retry            `yaml:"retry,omitempty" jsonschema_description:"poll (http, s3): retries per poll before skipping to the next interval. gcs retries its reads itself."`
	Dedupe       *Dedupe           `yaml:"dedupe,omitempty" jsonschema_description:"Drop messages whose key was seen within the ttl, on any source (poll dedupe, cross-replica idempotency). A duplicate is acked as filtered and counted in hikyaku_deduped_total."`
	Split        string            `yaml:"split,omitempty" jsonschema_description:"poll: jq expression that splits one response into several messages, such as .items[]. s3, gcs: also lines, one message per non-empty line of an object (NDJSON)."`
	Reconnect    *Reconnect        `yaml:"reconnect,omitempty" jsonschema_description:"subscribe: reconnect policy."`
	FailoverURLs []string          `yaml:"failover_urls,omitempty" jsonschema_description:"subscribe (ws, wss, sse, grpc, nats, jetstream, mqtt, amqp, redis): URLs rotated through when the primary is down."`

	Format string `yaml:"format,omitempty" jsonschema:"enum=body,enum=envelope" jsonschema_description:"stdin and file: each line is a message body (default) or an envelope in wire form."`
	Follow bool   `yaml:"follow,omitempty" jsonschema_description:"file: keep reading lines appended to the file."`

	Brokers []string `yaml:"brokers,omitempty" jsonschema_description:"kafka: bootstrap brokers (host:port)."`
	Topic   string   `yaml:"topic,omitempty" jsonschema_description:"kafka, mqtt: the topic (mqtt: a topic filter); eventhubs: the event hub; servicebus: the topic (with subscription)."`
	Topics  []string `yaml:"topics,omitempty" jsonschema_description:"kafka, mqtt: several topics (mqtt: filters) instead of topic."`
	Group   string   `yaml:"group,omitempty" jsonschema_description:"kafka, redis streams, eventhubs: consumer group; mqtt v5: shared subscription group ($share/<group>/<filter>)."`
	Start   string   `yaml:"start,omitempty" jsonschema:"enum=earliest,enum=latest,enum=all,enum=new,enum=last" jsonschema_description:"Where a new consumer starts. kafka, redis streams, eventhubs: earliest or latest (default latest); jetstream: all, new (default) or last."`

	SASL            *SASL    `yaml:"sasl,omitempty" jsonschema_description:"kafka: SASL authentication."`
	Username        string   `yaml:"username,omitempty" jsonschema_description:"mqtt, nats, redis, amqp: user name (mqtt, amqp and redis also take it in the URL; this field wins)."`
	Password        string   `yaml:"password,omitempty" jsonschema_description:"mqtt, nats, redis, amqp, smtp: password; use {{secrets.X}}."`
	Token           string   `yaml:"token,omitempty" jsonschema_description:"nats: authentication token; use {{secrets.X}}."`
	CredsFile       string   `yaml:"creds_file,omitempty" jsonschema_description:"nats: a .creds file (JWT and NKey seed)."`
	Subject         string   `yaml:"subject,omitempty" jsonschema_description:"nats: subject (wildcards * and > allowed); jetstream: filter subject of the consumer."`
	Queue           string   `yaml:"queue,omitempty" jsonschema_description:"nats: queue group (members share the messages); amqp: the queue consumed; servicebus: the queue."`
	Prefetch        int      `yaml:"prefetch,omitempty" jsonschema_description:"Messages a subscribe source keeps in flight: amqp prefetch, jetstream max_ack_pending, redis streams entries read and not acknowledged, kafka records buffered, mqtt v5 receive maximum, pubsub max outstanding, servicebus prefetch (default 64); nats: messages buffered before the client drops them (default 65536). sqs: messages per receive, 1 to 10 (default 10)."`
	AckWait         Duration `yaml:"ack_wait,omitempty" jsonschema_description:"jetstream: redelivery delay of an unacknowledged message; redis streams: how long a pending entry stays idle before it is reclaimed and delivered again (default 30s for both)."`
	Stream          string   `yaml:"stream,omitempty" jsonschema_description:"jetstream: the stream; redis: the stream key (XREADGROUP)."`
	Consumer        string   `yaml:"consumer,omitempty" jsonschema_description:"redis streams: this consumer's name in the group (default <host name>-<source name>); keep it stable, a restart resumes the entries it left pending."`
	Durable         string   `yaml:"durable,omitempty" jsonschema_description:"jetstream: durable consumer name, so a restart resumes where it stopped (default the source name)."`
	Channel         string   `yaml:"channel,omitempty" jsonschema_description:"redis pub/sub: channel, or a pattern with * (PSUBSCRIBE)."`
	QoS             *int     `yaml:"qos,omitempty" jsonschema:"enum=0,enum=1,enum=2" jsonschema_description:"mqtt: subscription QoS (default 1)."`
	ClientID        string   `yaml:"client_id,omitempty" jsonschema_description:"mqtt: client identifier, which keys a persistent session; kafka: client.id. Default hikyaku-<relay>-<source>."`
	CleanSession    *bool    `yaml:"clean_session,omitempty" jsonschema_description:"mqtt: start a clean session (default false: the broker keeps the session and queued QoS 1/2 messages across reconnects)."`
	ProtocolVersion string   `yaml:"protocol_version,omitempty" jsonschema:"enum=3.1.1,enum=5" jsonschema_description:"mqtt: protocol version (default 5)."`
	Exchange        string   `yaml:"exchange,omitempty" jsonschema_description:"amqp: exchange the queue is bound to (with routing_key)."`
	RoutingKey      string   `yaml:"routing_key,omitempty" jsonschema_description:"amqp: binding key of the queue on exchange."`
	Declare         bool     `yaml:"declare,omitempty" jsonschema_description:"amqp: declare the queue (durable) and its binding when missing."`

	DescriptorSet string   `yaml:"descriptor_set,omitempty" jsonschema_description:"grpc: a FileDescriptorSet file (protoc --descriptor_set_out --include_imports) describing the services."`
	Services      []string `yaml:"services,omitempty" jsonschema_description:"grpc listen: the services of descriptor_set to serve (default all); hikyaku.relay.v1.Relay is always served."`
	Reflection    *bool    `yaml:"reflection,omitempty" jsonschema_description:"grpc listen: serve the reflection service (default true); subscribe: resolve method through server reflection when there is no descriptor_set (default true)."`
	Frame         string   `yaml:"frame,omitempty" jsonschema:"enum=newline,enum=length,enum=raw" jsonschema_description:"tcp, udp: framing. newline (default): one message per line; length: each message after a length_bytes big-endian length; raw: tcp, a connection's whole stream; udp, each datagram."`
	LengthBytes   int      `yaml:"length_bytes,omitempty" jsonschema:"enum=1,enum=2,enum=4,enum=8" jsonschema_description:"tcp, udp: size of the length prefix with frame: length (default 4)."`

	QueueURL          string   `yaml:"queue_url,omitempty" jsonschema_description:"sqs: the queue URL. sns, eventbridge: the SQS queue subscribed to the topic or rule."`
	WaitTime          Duration `yaml:"wait_time,omitempty" jsonschema_description:"sqs: long-poll wait (default 20s, at most 20s)."`
	VisibilityTimeout Duration `yaml:"visibility_timeout,omitempty" jsonschema_description:"sqs: how long a received message stays hidden (default the queue's)."`
	Bucket            string   `yaml:"bucket,omitempty" jsonschema_description:"s3, gcs: the bucket polled."`
	Prefix            string   `yaml:"prefix,omitempty" jsonschema_description:"s3, gcs: only objects under this prefix."`
	After             string   `yaml:"after,omitempty" jsonschema:"enum=keep,enum=delete,enum=move" jsonschema_description:"s3, gcs: what happens to an object once its messages are final: keep (remembered in state_store), delete (default) or move (to move_to)."`
	MoveTo            string   `yaml:"move_to,omitempty" jsonschema_description:"s3, gcs: prefix objects are moved under with after: move."`
	StateStore        string   `yaml:"state_store,omitempty" jsonschema_description:"Store (by name) that keeps a source's progress: eventhubs checkpoints, s3/gcs objects already read with after: keep. Default the first store."`
	Subscription      string   `yaml:"subscription,omitempty" jsonschema_description:"pubsub: the subscription; servicebus: the topic subscription."`
	AWS               *AWS     `yaml:"aws,omitempty" jsonschema_description:"sqs, sns, eventbridge, s3: AWS region and credentials (default the SDK's chain)."`
	GCP               *GCP     `yaml:"gcp,omitempty" jsonschema_description:"pubsub, gcs: project and credentials (default Application Default Credentials)."`
	Azure             *Azure   `yaml:"azure,omitempty" jsonschema_description:"servicebus, eventhubs: connection string or namespace and credentials."`

	Models []string `yaml:"models,omitempty" jsonschema_description:"openai: the model names /v1/models lists and clients may ask for (default [hikyaku]); route on msg.json.model."`

	Cron     string `yaml:"cron,omitempty" jsonschema_description:"schedule: when to emit a message, as a five-field cron expression (minute hour day-of-month month day-of-week) or @hourly, @daily, @weekly, @monthly, @yearly. Set cron or interval."`
	Timezone string `yaml:"timezone,omitempty" jsonschema_description:"schedule: the time zone cron is read in, such as Europe/Madrid (default UTC)."`

	Ordering *Ordering `yaml:"ordering,omitempty" jsonschema_description:"Deliver messages with the same key one at a time, in arrival order."`

	Options map[string]any `yaml:"options,omitempty" jsonschema_description:"Settings of a plugin type (a source registered by a custom build); the plugin decodes and validates them."`
}

// SASL is Kafka's SASL authentication.
type SASL struct {
	Mechanism string `yaml:"mechanism" jsonschema:"required,enum=plain,enum=scram-sha-256,enum=scram-sha-512" jsonschema_description:"SASL mechanism."`
	Username  string `yaml:"username" jsonschema:"required" jsonschema_description:"User name."`
	Password  string `yaml:"password" jsonschema:"required" jsonschema_description:"Password; use {{secrets.X}}."`
}

// AWS is the region, endpoint and credentials of the AWS protocols.
type AWS struct {
	Region          string `yaml:"region,omitempty" jsonschema_description:"Region (default the SDK's: AWS_REGION, the profile)."`
	Endpoint        string `yaml:"endpoint,omitempty" jsonschema_description:"Custom endpoint URL, such as LocalStack's http://localhost:4566."`
	AccessKeyID     string `yaml:"access_key_id,omitempty" jsonschema_description:"Static credentials (with secret_access_key); use {{secrets.X}}."`
	SecretAccessKey string `yaml:"secret_access_key,omitempty" jsonschema_description:"Static credentials; use {{secrets.X}}."`
	SessionToken    string `yaml:"session_token,omitempty" jsonschema_description:"Session token of temporary static credentials."`
	Profile         string `yaml:"profile,omitempty" jsonschema_description:"Shared config profile."`
	RoleARN         string `yaml:"role_arn,omitempty" jsonschema_description:"Role assumed (STS) on top of the base credentials."`
}

// GCP is the project and credentials of the Google Cloud protocols.
type GCP struct {
	Project         string `yaml:"project,omitempty" jsonschema_description:"Project ID (pubsub: required)."`
	CredentialsFile string `yaml:"credentials_file,omitempty" jsonschema_description:"Service account key file (default Application Default Credentials)."`
	CredentialsJSON string `yaml:"credentials_json,omitempty" jsonschema_description:"Service account key JSON; use {{secrets.X}}."`
	Endpoint        string `yaml:"endpoint,omitempty" jsonschema_description:"Custom endpoint, such as an emulator's localhost:8085 (pubsub) or http://localhost:4443/storage/v1/ (gcs)."`
}

// Azure is the connection of the Azure protocols.
type Azure struct {
	ConnectionString string `yaml:"connection_string,omitempty" jsonschema_description:"Connection string (Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...); use {{secrets.X}}."`
	Namespace        string `yaml:"namespace,omitempty" jsonschema_description:"Fully qualified namespace (<name>.servicebus.windows.net), with Microsoft Entra credentials."`
	TenantID         string `yaml:"tenant_id,omitempty" jsonschema_description:"Client secret credentials: tenant (default: the SDK's DefaultAzureCredential)."`
	ClientID         string `yaml:"client_id,omitempty" jsonschema_description:"Client secret credentials: application (client) ID."`
	ClientSecret     string `yaml:"client_secret,omitempty" jsonschema_description:"Client secret credentials: secret; use {{secrets.X}}."`
}

// Ordering keeps messages with the same key in order: the engine delivers
// them one at a time, in the order the source produced them, while
// messages with other keys proceed in parallel.
type Ordering struct {
	Key string `yaml:"key,omitempty" jsonschema_description:"Template of the ordering key, such as {{ .json.customer_id }}. Empty: the source's natural order unit (kafka partition, mqtt topic, amqp queue, sqs message group, ...). A constant (\"all\") orders the whole source."`
}

// Respond configures sync HTTP mode.
type Respond struct {
	From    string   `yaml:"from" jsonschema:"required" jsonschema_description:"Destination whose reply is returned."`
	Timeout Duration `yaml:"timeout,omitempty" jsonschema_description:"How long the caller waits (default 30s); then 504."`
}

// TLS is a TLS configuration.
type TLS struct {
	Enabled            *bool  `yaml:"enabled,omitempty" jsonschema_description:"Use TLS (implied by any other field and by https/wss)."`
	CertFile           string `yaml:"cert_file,omitempty" jsonschema_description:"Certificate (server cert, or client cert for mTLS)."`
	KeyFile            string `yaml:"key_file,omitempty" jsonschema_description:"Private key of cert_file."`
	CAFile             string `yaml:"ca_file,omitempty" jsonschema_description:"CA bundle that verifies the server."`
	ClientCAFile       string `yaml:"client_ca_file,omitempty" jsonschema_description:"listen: CA bundle that verifies client certificates (mTLS)."`
	ServerName         string `yaml:"server_name,omitempty" jsonschema_description:"SNI and verification name."`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" jsonschema_description:"Skip server verification (development only)."`
}

// Auth is inbound authentication. Every configured method must pass.
type Auth struct {
	HMAC    *HMACAuth    `yaml:"hmac,omitempty" jsonschema_description:"Verify an HMAC signature of the body."`
	Bearer  *BearerAuth  `yaml:"bearer,omitempty" jsonschema_description:"Require a bearer token."`
	JWT     *JWTAuth     `yaml:"jwt,omitempty" jsonschema_description:"Verify a JWT against a JWKS."`
	APIKey  *APIKeyAuth  `yaml:"api_key,omitempty" jsonschema_description:"Require an API key header."`
	Webhook *WebhookAuth `yaml:"webhook,omitempty" jsonschema_description:"Verify the signature of a well-known webhook sender (http sources)."`
	IPAllow []string     `yaml:"ip_allow,omitempty" jsonschema_description:"Allowed client IPs or CIDRs."`
}

// HMACAuth verifies a body signature.
type HMACAuth struct {
	Header   string `yaml:"header" jsonschema:"required" jsonschema_description:"Signature header, such as X-Signature-256."`
	Algo     string `yaml:"algo,omitempty" jsonschema:"enum=sha256,enum=sha1,enum=sha512" jsonschema_description:"Hash (default sha256)."`
	Secret   string `yaml:"secret" jsonschema:"required" jsonschema_description:"Shared secret, usually {{secrets.NAME}}."`
	Prefix   string `yaml:"prefix,omitempty" jsonschema_description:"Prefix before the digest, such as sha256= (stripped if present)."`
	Encoding string `yaml:"encoding,omitempty" jsonschema:"enum=hex,enum=base64" jsonschema_description:"Digest encoding (default hex)."`
}

// WebhookAuth verifies a request the way a well-known sender signs it.
type WebhookAuth struct {
	Provider  string   `yaml:"provider" jsonschema:"required,enum=github,enum=stripe,enum=slack,enum=shopify,enum=twilio" jsonschema_description:"Who sends the webhook."`
	Secret    string   `yaml:"secret" jsonschema:"required" jsonschema_description:"The signing secret the sender gave you (for twilio, the auth token), usually {{secrets.NAME}}."`
	Tolerance Duration `yaml:"tolerance,omitempty" jsonschema_description:"How old a signed timestamp may be (stripe and slack; default 5m)."`
	URL       string   `yaml:"url,omitempty" jsonschema_description:"twilio only: the public URL Twilio calls, when a proxy in front of the relay changes the host or scheme."`
}

// BearerAuth requires Authorization: Bearer <token>.
type BearerAuth struct {
	Tokens []string `yaml:"tokens" jsonschema:"required" jsonschema_description:"Accepted tokens."`
}

// JWTAuth verifies JWTs.
type JWTAuth struct {
	JWKSURL    string   `yaml:"jwks_url" jsonschema:"required" jsonschema_description:"JWKS endpoint."`
	Issuer     string   `yaml:"issuer,omitempty" jsonschema_description:"Required iss."`
	Audience   string   `yaml:"audience,omitempty" jsonschema_description:"Required aud."`
	Algorithms []string `yaml:"algorithms,omitempty" jsonschema_description:"Accepted algorithms (default RS256, ES256)."`
}

// APIKeyAuth requires a key in a header.
type APIKeyAuth struct {
	Header string   `yaml:"header,omitempty" jsonschema_description:"Header (default X-API-Key)."`
	Query  string   `yaml:"query,omitempty" jsonschema_description:"A query parameter that may carry the key when the header is absent, for clients that cannot set headers (a browser's EventSource or WebSocket). URLs end up in proxy logs: prefer the header when the client can send one."`
	Keys   []string `yaml:"keys" jsonschema:"required" jsonschema_description:"Accepted keys."`
}

// Dedupe drops repeated messages.
type Dedupe struct {
	Key   string   `yaml:"key" jsonschema:"required" jsonschema_description:"Per-message template of the dedupe key, such as {{ .json.id }}."`
	TTL   Duration `yaml:"ttl,omitempty" jsonschema_description:"How long a key is remembered (default 1h)."`
	Store string   `yaml:"store,omitempty" jsonschema_description:"Store that shares keys across replicas (default store)."`
}

// Reconnect is a subscribe source's reconnect policy.
type Reconnect struct {
	Backoff     Duration `yaml:"backoff,omitempty" jsonschema_description:"First wait (default 500ms), doubling."`
	MaxBackoff  Duration `yaml:"max_backoff,omitempty" jsonschema_description:"Cap (default 30s)."`
	Jitter      string   `yaml:"jitter,omitempty" jsonschema:"enum=full,enum=equal,enum=none" jsonschema_description:"Default full."`
	MaxAttempts int      `yaml:"max_attempts,omitempty" jsonschema_description:"Give up after this many failed connects in a row; 0 = forever."`
}

// Endpoint is one place to deliver to: a destination, a group member or the
// dead-letter queue.
type Endpoint struct {
	Type    string            `yaml:"type" jsonschema:"required" jsonschema_description:"Protocol: http, https, ws, wss, sse, stdout, stderr, file, fault, group, grpc, tcp, udp, kafka, nats, jetstream, mqtt, amqp, redis, sqs, sns, eventbridge, s3, pubsub, gcs, servicebus, eventhubs, model, or a plugin's type."`
	URL     string            `yaml:"url,omitempty" jsonschema_description:"http/ws: target URL, which may contain per-message templates (hosts are checked against egress). grpc: grpc://host:port or grpcs://; tcp: tcp://host:port or tls://; udp: udp://host:port or dtls://; nats, mqtt, amqp, redis: the server URL."`
	Method  string            `yaml:"method,omitempty" jsonschema_description:"http: method (default POST); grpc: full method name, /package.Service/Method."`
	Headers map[string]string `yaml:"headers,omitempty" jsonschema_description:"Headers set on every request; values may be per-message templates."`
	Body    string            `yaml:"body,omitempty" jsonschema_description:"Per-message template of the outgoing body (default: the message body)."`
	TLS     *TLS              `yaml:"tls,omitempty" jsonschema_description:"Client TLS, DTLS for udp (mTLS with cert_file and key_file)."`
	Sign    *Sign             `yaml:"sign,omitempty" jsonschema_description:"http: sign every request with an HMAC of its body, so the receiver can check who sent it."`
	From    string            `yaml:"from,omitempty" jsonschema_description:"smtp: the sender, as an address or as Name <address>."`
	To      []string          `yaml:"to,omitempty" jsonschema_description:"smtp: the recipients. Each entry is a per-message template that renders one address, or several separated by commas."`
	OAuth2  *OAuth2           `yaml:"oauth2,omitempty" jsonschema_description:"http: fetch an OAuth 2.0 access token with the client credentials grant and send it as a bearer token."`
	Path    string            `yaml:"path,omitempty" jsonschema_description:"file: the file appended to; sse: the stream path (default /)."`
	Listen  string            `yaml:"listen,omitempty" jsonschema_description:"sse: address the event stream is served on."`
	Format  string            `yaml:"format,omitempty" jsonschema:"enum=envelope,enum=body" jsonschema_description:"file/stdout/stderr: write each message as an envelope line (default) or its body."`
	Frame   string            `yaml:"frame,omitempty" jsonschema:"enum=text,enum=binary,enum=newline,enum=length,enum=raw" jsonschema_description:"ws: frame type, text (default) or binary. tcp, udp: framing, newline (default), length (a length_bytes big-endian prefix) or raw."`
	Event   string            `yaml:"event,omitempty" jsonschema_description:"sse: per-message template of the event name."`
	Replay  int               `yaml:"replay,omitempty" jsonschema_description:"sse: events kept for Last-Event-ID resume (default 1000)."`
	Auth    *Auth             `yaml:"auth,omitempty" jsonschema_description:"sse: authentication of the clients that read the stream: bearer, api_key, jwt, ip_allow (401 on failure; hmac does not apply)."`
	Fault   *Fault            `yaml:"fault,omitempty" jsonschema_description:"fault: the faults injected (local testing and the resilience suite)."`
	Brokers []string          `yaml:"brokers,omitempty" jsonschema_description:"kafka: bootstrap brokers (host:port)."`
	Topic   string            `yaml:"topic,omitempty" jsonschema_description:"Per-message template of the topic. kafka, mqtt, pubsub: the topic; sns: the topic ARN; eventhubs: the event hub; servicebus: the topic."`
	Key     string            `yaml:"key,omitempty" jsonschema_description:"Per-message template of the message key. kafka: record key (partitioning); pubsub: ordering key; eventhubs: partition key; s3, gcs: object key (default <relay>/<date>/<id>.json, or .ndjson for a batch)."`

	SASL            *SASL  `yaml:"sasl,omitempty" jsonschema_description:"kafka: SASL authentication."`
	Username        string `yaml:"username,omitempty" jsonschema_description:"mqtt, nats, redis, amqp, smtp: user name."`
	Password        string `yaml:"password,omitempty" jsonschema_description:"mqtt, nats, redis, amqp, smtp: password; use {{secrets.X}}."`
	Token           string `yaml:"token,omitempty" jsonschema_description:"nats: authentication token; use {{secrets.X}}."`
	CredsFile       string `yaml:"creds_file,omitempty" jsonschema_description:"nats: a .creds file (JWT and NKey seed)."`
	Subject         string `yaml:"subject,omitempty" jsonschema_description:"Per-message template of the subject. nats, jetstream: the subject published to; sns: the message subject; smtp: the email's subject."`
	Acks            string `yaml:"acks,omitempty" jsonschema:"enum=all,enum=leader,enum=none" jsonschema_description:"kafka: acknowledgements a produce waits for (default all, with the idempotent producer)."`
	Compression     string `yaml:"compression,omitempty" jsonschema:"enum=none,enum=gzip,enum=snappy,enum=lz4,enum=zstd" jsonschema_description:"kafka: batch compression (default none). http: gzip compresses the request body and sets Content-Encoding (default none)."`
	QoS             *int   `yaml:"qos,omitempty" jsonschema:"enum=0,enum=1,enum=2" jsonschema_description:"mqtt: publish QoS (default 1)."`
	Retain          bool   `yaml:"retain,omitempty" jsonschema_description:"mqtt: publish retained messages."`
	ClientID        string `yaml:"client_id,omitempty" jsonschema_description:"mqtt: client identifier; kafka: client.id. Default hikyaku-<relay>-<destination>."`
	ProtocolVersion string `yaml:"protocol_version,omitempty" jsonschema:"enum=3.1.1,enum=5" jsonschema_description:"mqtt: protocol version (default 5)."`
	Exchange        string `yaml:"exchange,omitempty" jsonschema_description:"amqp: exchange published to (default the default exchange, which routes by queue name)."`
	RoutingKey      string `yaml:"routing_key,omitempty" jsonschema_description:"amqp: per-message template of the routing key."`
	Persistent      *bool  `yaml:"persistent,omitempty" jsonschema_description:"amqp: persistent delivery mode (default true)."`
	Mandatory       bool   `yaml:"mandatory,omitempty" jsonschema_description:"amqp: fail a message no queue is bound to receive (a permanent error) instead of dropping it."`
	Declare         bool   `yaml:"declare,omitempty" jsonschema_description:"amqp: declare the exchange (topic, durable) when missing."`
	Stream          string `yaml:"stream,omitempty" jsonschema_description:"redis: stream key appended to (XADD); jetstream: the stream a publish must land in (an expected-stream check)."`
	Channel         string `yaml:"channel,omitempty" jsonschema_description:"redis: pub/sub channel published to; a per-message template."`
	MaxLen          int64  `yaml:"max_len,omitempty" jsonschema_description:"redis streams: approximate length the stream is trimmed to (MAXLEN ~)."`
	DescriptorSet   string `yaml:"descriptor_set,omitempty" jsonschema_description:"grpc: a FileDescriptorSet file describing method's service."`
	Reflection      *bool  `yaml:"reflection,omitempty" jsonschema_description:"grpc: resolve method through server reflection when there is no descriptor_set (default true)."`
	LengthBytes     int    `yaml:"length_bytes,omitempty" jsonschema:"enum=1,enum=2,enum=4,enum=8" jsonschema_description:"tcp, udp: size of the length prefix with frame: length (default 4)."`

	QueueURL    string   `yaml:"queue_url,omitempty" jsonschema_description:"sqs: the queue URL."`
	GroupID     string   `yaml:"group_id,omitempty" jsonschema_description:"Per-message template of the ordering group. sqs, sns FIFO: message group ID; servicebus: session ID."`
	Delay       Duration `yaml:"delay,omitempty" jsonschema_description:"sqs: delivery delay (at most 15m)."`
	EventBus    string   `yaml:"event_bus,omitempty" jsonschema_description:"eventbridge: event bus name or ARN (default the account's default bus)."`
	EventSource string   `yaml:"event_source,omitempty" jsonschema_description:"eventbridge: the events' source field (default hikyaku.<relay>)."`
	DetailType  string   `yaml:"detail_type,omitempty" jsonschema_description:"eventbridge: per-message template of the detail-type (default the message subject, else hikyaku.message)."`
	Bucket      string   `yaml:"bucket,omitempty" jsonschema_description:"s3, gcs: the bucket written to."`
	ContentType string   `yaml:"content_type,omitempty" jsonschema_description:"s3, gcs: the objects' content type (default the message's)."`
	Queue       string   `yaml:"queue,omitempty" jsonschema_description:"servicebus: the queue (or topic)."`
	AWS         *AWS     `yaml:"aws,omitempty" jsonschema_description:"sqs, sns, eventbridge, s3: AWS region and credentials (default the SDK's chain)."`
	GCP         *GCP     `yaml:"gcp,omitempty" jsonschema_description:"pubsub, gcs: project and credentials (default Application Default Credentials)."`
	Azure       *Azure   `yaml:"azure,omitempty" jsonschema_description:"servicebus, eventhubs: connection string or namespace and credentials."`

	Model      string      `yaml:"model,omitempty" jsonschema_description:"model: the Genkit model that answers, provider/name (googleai/gemini-2.5-flash, openai/gpt-4o-mini, openrouter/anthropic/claude-sonnet-4.5, ollama/llama3.2)."`
	Generation *Generation `yaml:"generation,omitempty" jsonschema_description:"model: generation settings applied when the request does not set them."`

	Options map[string]any `yaml:"options,omitempty" jsonschema_description:"Settings of a plugin type (a destination registered by a custom build); the plugin decodes and validates them."`
}

// Generation is a model destination's default generation settings.
type Generation struct {
	Temperature *float64 `yaml:"temperature,omitempty" jsonschema_description:"Sampling temperature."`
	MaxTokens   int      `yaml:"max_tokens,omitempty" jsonschema_description:"Maximum output tokens."`
	TopP        *float64 `yaml:"top_p,omitempty" jsonschema_description:"Nucleus sampling probability mass."`
	System      string   `yaml:"system,omitempty" jsonschema_description:"System prompt prepended when the request has none."`
}

// Fault describes injected failures.
type Fault struct {
	Fail       Percent  `yaml:"fail,omitempty" jsonschema_description:"Share of attempts that fail, such as 50%."`
	FailFirst  int      `yaml:"fail_first,omitempty" jsonschema_description:"Fail the first N attempts, then follow fail."`
	Latency    Duration `yaml:"latency,omitempty" jsonschema_description:"Added to every attempt."`
	Error      string   `yaml:"error,omitempty" jsonschema:"enum=status,enum=connect,enum=timeout" jsonschema_description:"How an attempt fails (default status)."`
	Status     int      `yaml:"status,omitempty" jsonschema_description:"Status of a failed attempt (default 503)."`
	RetryAfter Duration `yaml:"retry_after,omitempty" jsonschema_description:"Retry-After hint of a failed attempt."`
	Seed       int64    `yaml:"seed,omitempty" jsonschema_description:"Random seed, for repeatable runs."`
}

// Destination is a named endpoint or group with its delivery policy.
type Destination struct {
	Name     string `yaml:"name" jsonschema:"required" jsonschema_description:"Unique destination name."`
	Endpoint `yaml:",inline"`
	Policy   `yaml:",inline"`

	Fallback          []string    `yaml:"fallback,omitempty" jsonschema_description:"Destinations tried in order after this one exhausts its retries, fails permanently, has its breaker open, its budget spent or its deadline hit."`
	Required          *bool       `yaml:"required,omitempty" jsonschema_description:"A required destination (default) must succeed, on its primary or a fallback, before the message is acked; a failure goes to the DLQ. A best-effort one is counted and logged."`
	Idempotent        bool        `yaml:"idempotent,omitempty" jsonschema_description:"The endpoint dedupes by idempotency key; required for hedging."`
	IdempotencyHeader string      `yaml:"idempotency_header,omitempty" jsonschema_description:"Header carrying the idempotency key (default Idempotency-Key)."`
	Transforms        []Transform `yaml:"transforms,omitempty" jsonschema_description:"Transforms applied before delivering here."`

	Strategy    string       `yaml:"strategy,omitempty" jsonschema:"enum=failover,enum=round_robin,enum=weighted,enum=least_latency" jsonschema_description:"group: how members are picked (default failover)."`
	Members     []Member     `yaml:"members,omitempty" jsonschema_description:"group: the endpoints."`
	HealthCheck *HealthCheck `yaml:"health_check,omitempty" jsonschema_description:"group: active health checks; unhealthy members are skipped until they recover."`
	Hedge       *Hedge       `yaml:"hedge,omitempty" jsonschema_description:"group: race another member when the first is slow (idempotent destinations only)."`

	DeliverAfter Duration   `yaml:"deliver_after,omitempty" jsonschema_description:"Hold each message of a route this long before delivering it here, counted from when it was received."`
	DeliverAt    string     `yaml:"deliver_at,omitempty" jsonschema_description:"Per-message template of the time to deliver at, in RFC 3339 (2026-12-24T09:00:00Z). A message it renders empty for follows deliver_after, or goes at once."`
	RateLimit    *RateLimit `yaml:"rate_limit,omitempty" jsonschema_description:"Send at most this many requests per second; the others wait their turn."`

	Shadow bool     `yaml:"shadow,omitempty" jsonschema_description:"A shadow destination gets a copy of what the routes send it, on the side: its failures never hold up, fail or dead-letter a message. For trying a new consumer against real traffic."`
	Sample *float64 `yaml:"sample,omitempty" jsonschema:"exclusiveMinimum=0,maximum=1" jsonschema_description:"shadow: the share of messages copied, from above 0 to 1 (0.05 is 5%; default 1, every message)."`

	Batch *Batch `yaml:"batch,omitempty" jsonschema_description:"Deliver messages in batches: one request (one Kafka produce, one SQS SendMessageBatch, one S3 object, one HTTP body) per batch."`
}

// Batch groups messages for one delivery. A batch is flushed when it holds
// max_messages or max_bytes, or max_wait after its first message. Every
// message of a batch shares the batch's outcome: retries, fallbacks and the
// DLQ apply to the batch, and a dead-lettered batch writes one DLQ record
// per message.
type Batch struct {
	MaxMessages int      `yaml:"max_messages,omitempty" jsonschema_description:"Messages per batch (default 100)."`
	MaxBytes    Size     `yaml:"max_bytes,omitempty" jsonschema_description:"Body bytes per batch (default 1MiB)."`
	MaxWait     Duration `yaml:"max_wait,omitempty" jsonschema_description:"How long a batch waits to fill after its first message (default 1s)."`
	Format      string   `yaml:"format,omitempty" jsonschema:"enum=ndjson,enum=json_array" jsonschema_description:"Body of a batch for protocols without native batches (http, ws, file, ...): ndjson (default), one body per line, or json_array of the bodies."`
}

// Member is one endpoint of a group.
type Member struct {
	Name     string `yaml:"name,omitempty" jsonschema_description:"Member name (default <group>[<index>])."`
	Weight   int    `yaml:"weight,omitempty" jsonschema_description:"weighted: relative share (default 1)."`
	Endpoint `yaml:",inline"`
}

// HealthCheck probes group members.
type HealthCheck struct {
	Path           string   `yaml:"path,omitempty" jsonschema_description:"http members: path probed with GET on the member's origin (default: the sink's own health check)."`
	Interval       Duration `yaml:"interval,omitempty" jsonschema_description:"Default 10s."`
	Timeout        Duration `yaml:"timeout,omitempty" jsonschema_description:"Default 2s."`
	UnhealthyAfter int      `yaml:"unhealthy_after,omitempty" jsonschema_description:"Failed probes in a row before a member is skipped (default 3)."`
	HealthyAfter   int      `yaml:"healthy_after,omitempty" jsonschema_description:"Passed probes in a row before it is used again (default 1)."`
}

// Hedge races a second member.
type Hedge struct {
	After Duration `yaml:"after" jsonschema:"required" jsonschema_description:"Start another member when no answer came in this long."`
	Max   int      `yaml:"max,omitempty" jsonschema_description:"Extra members raced at most (default 1)."`
}

// Store is a shared-state backend.
type Store struct {
	Name           string          `yaml:"name" jsonschema:"required" jsonschema_description:"Unique store name."`
	Type           string          `yaml:"type" jsonschema:"required,enum=redis,enum=memory" jsonschema_description:"redis, or memory (in-process: local runs and tests)."`
	Addr           string          `yaml:"addr,omitempty" jsonschema_description:"redis standalone: host:port."`
	Sentinel       *Sentinel       `yaml:"sentinel,omitempty" jsonschema_description:"redis Sentinel."`
	Cluster        *Cluster        `yaml:"cluster,omitempty" jsonschema_description:"redis Cluster."`
	Username       string          `yaml:"username,omitempty" jsonschema_description:"ACL user."`
	Password       string          `yaml:"password,omitempty" jsonschema_description:"Password, usually {{secrets.NAME}}."`
	TLS            *TLS            `yaml:"tls,omitempty" jsonschema_description:"TLS to Redis."`
	DB             int             `yaml:"db,omitempty" jsonschema_description:"Database number (standalone and Sentinel)."`
	KeyPrefix      string          `yaml:"key_prefix,omitempty" jsonschema_description:"Key prefix (default hikyaku)."`
	Timeout        Duration        `yaml:"timeout,omitempty" jsonschema_description:"Per-operation timeout (default 50ms): reads must never stall routing."`
	Retry          *Retry          `yaml:"retry,omitempty" jsonschema_description:"Retries per operation (default attempts 2, backoff 10ms)."`
	CircuitBreaker *CircuitBreaker `yaml:"circuit_breaker,omitempty" jsonschema_description:"Stops hammering a dead store (default on)."`
}

// Sentinel locates a Sentinel-managed Redis.
type Sentinel struct {
	MasterName string   `yaml:"master_name" jsonschema:"required" jsonschema_description:"Monitored master."`
	Addrs      []string `yaml:"addrs" jsonschema:"required" jsonschema_description:"Sentinel addresses."`
	Password   string   `yaml:"password,omitempty" jsonschema_description:"Sentinel password."`
}

// Cluster locates a Redis Cluster.
type Cluster struct {
	Addrs []string `yaml:"addrs" jsonschema:"required" jsonschema_description:"Seed node addresses."`
}

// RateLimit caps the requests a destination sends: a token bucket of burst
// tokens, refilled per_second times a second. Every attempt takes one
// token, retries included.
type RateLimit struct {
	PerSecond float64 `yaml:"per_second" jsonschema:"required" jsonschema_description:"Requests per second, on average (0.5 is one every two seconds)."`
	Burst     int     `yaml:"burst,omitempty" jsonschema_description:"Requests that may go out at once after a quiet time (default per_second, rounded up)."`
}

// Sign adds an HMAC signature of the body to every request of an http
// destination. It is the sending side of a source's auth.hmac.
type Sign struct {
	Secret          string `yaml:"secret" jsonschema:"required" jsonschema_description:"Shared secret, usually {{secrets.NAME}}."`
	Header          string `yaml:"header,omitempty" jsonschema_description:"Header that carries the signature (default X-Signature-256)."`
	Algo            string `yaml:"algo,omitempty" jsonschema:"enum=sha256,enum=sha512" jsonschema_description:"Hash (default sha256)."`
	Prefix          string `yaml:"prefix,omitempty" jsonschema_description:"Text put before the digest, such as sha256=."`
	Encoding        string `yaml:"encoding,omitempty" jsonschema:"enum=hex,enum=base64" jsonschema_description:"Digest encoding (default hex)."`
	TimestampHeader string `yaml:"timestamp_header,omitempty" jsonschema_description:"When set, this header carries the send time in Unix seconds, and the signature covers <timestamp>.<body>, so a receiver can refuse an old request."`
}

// OAuth2 is the client credentials grant of an http destination: hikyaku
// asks token_url for an access token, keeps it until shortly before it
// expires, and sends it as a bearer token.
type OAuth2 struct {
	TokenURL     string            `yaml:"token_url" jsonschema:"required" jsonschema_description:"The authorization server's token endpoint."`
	ClientID     string            `yaml:"client_id" jsonschema:"required" jsonschema_description:"Client ID."`
	ClientSecret string            `yaml:"client_secret" jsonschema:"required" jsonschema_description:"Client secret, usually {{secrets.NAME}}."`
	Scopes       []string          `yaml:"scopes,omitempty" jsonschema_description:"Scopes to ask for."`
	Audience     string            `yaml:"audience,omitempty" jsonschema_description:"The audience parameter, for servers that need one (Auth0)."`
	Params       map[string]string `yaml:"params,omitempty" jsonschema_description:"More form parameters for the token request, such as resource."`
	ClientAuth   string            `yaml:"client_auth,omitempty" jsonschema:"enum=basic,enum=body" jsonschema_description:"Where the client ID and secret go: basic (HTTP Basic, the default) or body (form fields)."`
}

// OnInvalid is what a route does with a body its validate step refused.
type OnInvalid struct {
	To []string `yaml:"to" jsonschema:"required" jsonschema_description:"Destinations that get the refused message, with the schema.error attribute set."`
}

// DeadLetter is the dead-letter queue.
type DeadLetter struct {
	Endpoint `yaml:",inline"`
	Retry    *Retry   `yaml:"retry,omitempty" jsonschema_description:"Retries of a DLQ write (default attempts 3)."`
	Fallback []string `yaml:"fallback,omitempty" jsonschema_description:"Tried in order when the DLQ fails: destination names, or stdout / stderr. A message is acked only once some hop has it."`
}

// Route connects sources to destinations through a router.
type Route struct {
	Name       string      `yaml:"name" jsonschema:"required" jsonschema_description:"Unique route name."`
	From       []string    `yaml:"from" jsonschema:"required" jsonschema_description:"Source names."`
	To         []string    `yaml:"to,omitempty" jsonschema_description:"Shortcut for a static router."`
	Filter     string      `yaml:"filter,omitempty" jsonschema_description:"CEL expression over msg; false skips the route."`
	Transforms []Transform `yaml:"transforms,omitempty" jsonschema_description:"Transforms applied before routing."`
	Router     *RouterSpec `yaml:"router,omitempty" jsonschema_description:"The router; default static over to:."`
}

// RouterSpec is a router: static, rules (CEL), ai or sticky, with an ordered
// fallback chain.
type RouterSpec struct {
	Type string `yaml:"type" jsonschema:"required,enum=static,enum=rules,enum=ai,enum=sticky" jsonschema_description:"static, rules (CEL), ai, or sticky (last destinations of the history key)."`
	When string `yaml:"when,omitempty" jsonschema_description:"CEL condition over msg: only the messages it is true for reach this router; the others skip it, with no call, and go to the next router of the fallback chain. On an ai router it keeps model spend to the messages that need a model, such as has(msg.json.text), msg.headers[\"X-Priority\"] == \"high\" or msg.json.tier == \"enterprise\". A condition that cannot be evaluated (a missing field) counts as false."`

	To      []string `yaml:"to,omitempty" jsonschema_description:"static: destinations."`
	Rules   []Rule   `yaml:"rules,omitempty" jsonschema_description:"rules: evaluated in order."`
	Match   string   `yaml:"match,omitempty" jsonschema:"enum=first,enum=all" jsonschema_description:"rules: first matching rule (default) or every match."`
	Default []string `yaml:"default,omitempty" jsonschema_description:"rules: destinations when no rule matches (else the router fails over)."`

	Engine                  string      `yaml:"engine,omitempty" jsonschema:"enum=decision,enum=generative" jsonschema_description:"ai: decision (System One, default) or generative (GenerateData on any model)."`
	Model                   string      `yaml:"model,omitempty" jsonschema_description:"ai: model (default models.decision or models.default)."`
	FallbackModels          []string    `yaml:"fallback_models,omitempty" jsonschema_description:"ai: overrides the global model fallback chain."`
	Instructions            string      `yaml:"instructions,omitempty" jsonschema_description:"ai: the system message, in front of every question."`
	State                   string      `yaml:"state,omitempty" jsonschema_description:"ai: per-message template of what the model evaluates (default {{ .bodyText }})."`
	StateJSON               bool        `yaml:"state_json,omitempty" jsonschema_description:"ai: the state is JSON (sent as an object)."`
	Candidates              []Candidate `yaml:"candidates,omitempty" jsonschema_description:"ai: the destinations the model may pick, each with what it is for."`
	Multi                   bool        `yaml:"multi,omitempty" jsonschema_description:"ai: true asks one yes/no per candidate (in one request) and delivers to every one above min_confidence; false asks one choice."`
	MinConfidence           float64     `yaml:"min_confidence,omitempty" jsonschema:"minimum=0,maximum=1" jsonschema_description:"ai: lowest probability accepted."`
	MinMargin               float64     `yaml:"min_margin,omitempty" jsonschema:"minimum=0,maximum=1" jsonschema_description:"ai, single choice: top minus second must exceed this."`
	Input                   *AIInput    `yaml:"input,omitempty" jsonschema_description:"ai: how the state is built."`
	Timeout                 Duration    `yaml:"timeout,omitempty" jsonschema_description:"ai: budget of the whole AI step, model fallbacks included (default 10s)."`
	Cache                   *Cache      `yaml:"cache,omitempty" jsonschema_description:"ai: decision cache."`
	History                 *History    `yaml:"history,omitempty" jsonschema_description:"ai and sticky: route with past calls of a key."`
	OnLowConfidence         []string    `yaml:"on_low_confidence,omitempty" jsonschema_description:"ai: destinations when the model answered but was not sure."`
	FallbackOnLowConfidence bool        `yaml:"fallback_on_low_confidence,omitempty" jsonschema_description:"ai: hand low-confidence answers to the router fallback instead of on_low_confidence."`
	Shadow                  bool        `yaml:"shadow,omitempty" jsonschema_description:"ai: decide and log, but deliver through the fallback chain."`
	Budget                  *AIBudget   `yaml:"budget,omitempty" jsonschema_description:"ai: when exceeded, the route goes straight to the router fallback."`

	Fallback []RouterSpec `yaml:"fallback,omitempty" jsonschema_description:"Routers tried in order when this one errors, times out, is over budget or (fallback_on_low_confidence) is unsure."`
}

// Rule is one CEL rule.
type Rule struct {
	When string   `yaml:"when" jsonschema:"required" jsonschema_description:"CEL expression over msg (msg.json, msg.headers, msg.subject, msg.source, ...)."`
	To   []string `yaml:"to" jsonschema:"required" jsonschema_description:"Destinations when it holds."`
}

// Candidate is a destination the AI may pick.
type Candidate struct {
	Name        string `yaml:"name" jsonschema:"required" jsonschema_description:"A declared destination."`
	Description string `yaml:"description" jsonschema:"required" jsonschema_description:"What it is for: the option's criteria."`
}

// AIInput shapes the state.
type AIInput struct {
	MaxBytes Size     `yaml:"max_bytes,omitempty" jsonschema_description:"Cap on the whole state, history included (default 8KiB): oldest turns go first."`
	Redact   []string `yaml:"redact,omitempty" jsonschema_description:"JSONPaths removed from a JSON state, such as $.card.number."`
	Headers  []string `yaml:"headers,omitempty" jsonschema_description:"Headers included in the state (allowlist)."`
}

// Cache is the AI decision cache.
type Cache struct {
	Store string   `yaml:"store,omitempty" jsonschema_description:"Shared store; default an in-process LRU."`
	Key   string   `yaml:"key,omitempty" jsonschema_description:"Per-message template of the cache key (default: a hash of the state)."`
	TTL   Duration `yaml:"ttl,omitempty" jsonschema_description:"Default 10m."`
	Size  int      `yaml:"size,omitempty" jsonschema_description:"LRU entries (default 10000)."`
}

// History is history-aware routing.
type History struct {
	Store        string        `yaml:"store,omitempty" jsonschema_description:"Store of the history (default store)."`
	Key          string        `yaml:"key" jsonschema:"required" jsonschema_description:"Per-message template of whose history: customer, conversation, session, device."`
	Window       Window        `yaml:"window,omitempty" jsonschema_description:"What is sent to the model."`
	TTL          Duration      `yaml:"ttl" jsonschema:"required" jsonschema_description:"How long records are kept. Mandatory."`
	Record       HistoryRecord `yaml:"record,omitempty" jsonschema_description:"What is stored."`
	Sticky       *Sticky       `yaml:"sticky,omitempty" jsonschema_description:"Send follow-ups where the previous call went."`
	OnStoreError string        `yaml:"on_store_error,omitempty" jsonschema:"enum=ignore,enum=fallback" jsonschema_description:"ignore (default): route without history; fallback: use router.fallback."`
}

// Window bounds the history sent to the model.
type Window struct {
	MaxEntries int      `yaml:"max_entries,omitempty" jsonschema_description:"Newest records sent (default 10)."`
	MaxAge     Duration `yaml:"max_age,omitempty" jsonschema_description:"Oldest record sent."`
}

// HistoryRecord says what a call record stores.
type HistoryRecord struct {
	State   string `yaml:"state,omitempty" jsonschema:"enum=redacted,enum=summary,enum=none" jsonschema_description:"redacted (default): the redacted state; summary: the summary template; none: only decision and outcome."`
	Summary string `yaml:"summary,omitempty" jsonschema_description:"Per-message template of the summary, such as {{ .json.type }} / {{ .json.subject }}."`
}

// Sticky is follow-up affinity.
type Sticky struct {
	Enabled           bool    `yaml:"enabled,omitempty" jsonschema_description:"Turn sticky routing on."`
	Mode              string  `yaml:"mode,omitempty" jsonschema:"enum=model,enum=always_last,enum=off" jsonschema_description:"model (default): ask the model whether it is a follow-up; always_last: always reuse the last destinations, no model call."`
	FollowUpThreshold float64 `yaml:"follow_up_threshold,omitempty" jsonschema:"minimum=0,maximum=1" jsonschema_description:"Follow-up probability needed (default 0.8)."`
}

// AIBudget caps an AI route's spend.
type AIBudget struct {
	MaxCallsPerMinute int    `yaml:"max_calls_per_minute,omitempty" jsonschema_description:"Model calls per minute."`
	MaxTokensPerHour  int    `yaml:"max_tokens_per_hour,omitempty" jsonschema_description:"Model tokens per hour."`
	Store             string `yaml:"store,omitempty" jsonschema_description:"Store that shares the counters across replicas (default: in-process)."`
}

// Transform is one step: exactly one of its fields is set.
type Transform struct {
	Filter      string       `yaml:"filter,omitempty" jsonschema_description:"CEL expression; false drops the message from this route or destination."`
	JQ          string       `yaml:"jq,omitempty" jsonschema_description:"jq program over the JSON body; its output is the new body."`
	Template    string       `yaml:"template,omitempty" jsonschema_description:"Go template rendered as the new body."`
	Headers     *HeaderOps   `yaml:"headers,omitempty" jsonschema_description:"Set, add or remove headers (values are templates)."`
	Codec       string       `yaml:"codec,omitempty" jsonschema:"enum=json,enum=text,enum=base64_encode,enum=base64_decode,enum=form_to_json,enum=cloudevents" jsonschema_description:"Re-encode the body."`
	ContentType string       `yaml:"content_type,omitempty" jsonschema_description:"Set the content type (alone, or after jq or template)."`
	Redact      []string     `yaml:"redact,omitempty" jsonschema_description:"JSONPaths whose values are replaced with [redacted] in the JSON body, such as $.card.number or $..email."`
	Validate    *SchemaCheck `yaml:"validate,omitempty" jsonschema_description:"Check the JSON body against a JSON Schema; a body that does not match is a failure."`
	Enrich      *Enrich      `yaml:"enrich,omitempty" jsonschema_description:"Call an HTTP endpoint and put its JSON answer into the JSON body."`
}

// Enrich is a transform (enrich:) that looks something up over HTTP and
// adds the answer to the message.
type Enrich struct {
	URL     string            `yaml:"url" jsonschema:"required" jsonschema_description:"The endpoint to call, a per-message template such as https://crm.internal/customers/{{ .json.customer_id }}."`
	Method  string            `yaml:"method,omitempty" jsonschema:"enum=GET,enum=POST" jsonschema_description:"GET (default), or POST, which sends the message body."`
	Headers map[string]string `yaml:"headers,omitempty" jsonschema_description:"Request headers (values are templates)."`
	Into    string            `yaml:"into" jsonschema:"required" jsonschema_description:"Where the answer goes in the JSON body: a key, or a dotted path such as meta.customer."`
	Timeout Duration          `yaml:"timeout,omitempty" jsonschema_description:"How long the call may take (default 2s)."`
	Cache   Duration          `yaml:"cache,omitempty" jsonschema_description:"Keep each URL's answer this long, in this process, and skip the call meanwhile (GET only; default: no cache)."`
	OnError string            `yaml:"on_error,omitempty" jsonschema:"enum=fail,enum=skip" jsonschema_description:"When the call fails: fail (default) fails the transform, skip lets the message go on without the answer."`
}

// SchemaCheck is a transform (validate:) that checks the body against a JSON Schema. It
// never changes the message.
type SchemaCheck struct {
	Schema    map[string]any `yaml:"schema,omitempty" jsonschema_description:"The JSON Schema, written inline."`
	OnInvalid *OnInvalid     `yaml:"on_invalid,omitempty" jsonschema_description:"On a route: where a body that does not match goes, instead of the dead-letter queue."`
	File      string         `yaml:"file,omitempty" jsonschema_description:"A JSON Schema file (JSON or YAML), read when the relay starts or reloads. A relative path is relative to the working directory."`
}

// HeaderOps edits headers.
type HeaderOps struct {
	Set    map[string]string `yaml:"set,omitempty" jsonschema_description:"Replace."`
	Add    map[string]string `yaml:"add,omitempty" jsonschema_description:"Append."`
	Remove []string          `yaml:"remove,omitempty" jsonschema_description:"Delete."`
}

// Server is the process-level configuration.
type Server struct {
	Admin         Admin     `yaml:"admin,omitempty" jsonschema_description:"Admin API: /healthz /readyz /metrics /reload /v1/feedback /v1/history /v1/tail."`
	ShutdownGrace Duration  `yaml:"shutdown_grace,omitempty" jsonschema_description:"Time in-flight messages get to finish on shutdown (default 20s)."`
	Workers       int       `yaml:"workers,omitempty" jsonschema_description:"Messages processed at once (default 256)."`
	Readiness     Readiness `yaml:"readiness,omitempty" jsonschema_description:"What /readyz checks."`
}

// Admin is the admin API.
type Admin struct {
	Listen   string `yaml:"listen,omitempty" jsonschema_description:"Address (default :9090)."`
	Token    string `yaml:"token,omitempty" jsonschema_description:"Bearer token for mutating and history endpoints; without one they answer loopback only."`
	Disabled bool   `yaml:"disabled,omitempty" jsonschema_description:"Do not serve the admin API."`
}

// Readiness configures /readyz.
type Readiness struct {
	Destinations *bool `yaml:"destinations,omitempty" jsonschema_description:"Not ready while a required destination can take nothing: every member of it, and of every fallback in its chain, is down (default true)."`
	Models       bool  `yaml:"models,omitempty" jsonschema_description:"Not ready while every model of an AI route's chain fails. A probe calls each model once; /readyz answers from the last probe and refreshes one older than 30s in the background."`
}

// Observability configures logs, traces and metrics.
type Observability struct {
	Logs    Logs    `yaml:"logs,omitempty" jsonschema_description:"Structured logs on stderr."`
	OTel    OTel    `yaml:"otel,omitempty" jsonschema_description:"OpenTelemetry traces over OTLP/HTTP."`
	Metrics Metrics `yaml:"metrics,omitempty" jsonschema_description:"Prometheus metrics on the admin API."`
}

// Logs configures logging.
type Logs struct {
	Level  string `yaml:"level,omitempty" jsonschema:"enum=debug,enum=info,enum=warn,enum=error" jsonschema_description:"Default info."`
	Format string `yaml:"format,omitempty" jsonschema:"enum=json,enum=text" jsonschema_description:"Default text on a terminal, json otherwise."`
}

// OTel configures tracing.
type OTel struct {
	Endpoint    string            `yaml:"endpoint,omitempty" jsonschema_description:"OTLP/HTTP endpoint; empty turns tracing off."`
	Insecure    bool              `yaml:"insecure,omitempty" jsonschema_description:"Plain HTTP to the collector."`
	Headers     map[string]string `yaml:"headers,omitempty" jsonschema_description:"Exporter headers."`
	ServiceName string            `yaml:"service_name,omitempty" jsonschema_description:"Default hikyaku."`
	SampleRatio *float64          `yaml:"sample_ratio,omitempty" jsonschema_description:"Share of traces kept (default 1)."`
}

// Metrics configures Prometheus.
type Metrics struct {
	Prometheus *bool `yaml:"prometheus,omitempty" jsonschema_description:"Serve /metrics (default true)."`
}

// Buffer is the queue between sources and workers.
type Buffer struct {
	Type string `yaml:"type,omitempty" jsonschema:"enum=memory,enum=disk" jsonschema_description:"memory (default) or disk: a bbolt write-ahead log, so queued messages survive a crash or restart and are delivered when the relay starts again."`
	Size int    `yaml:"size,omitempty" jsonschema_description:"Queued messages (default 10000); listen sources answer 503 when full."`
	Path string `yaml:"path,omitempty" jsonschema_description:"disk: the write-ahead log file (required with disk; one relay per file)."`
	Sync *bool  `yaml:"sync,omitempty" jsonschema_description:"disk: fsync every write before the message counts as queued (default true). false is faster but a host crash (not a process crash) can lose the last writes."`
}

// Egress guards templated destination hosts.
type Egress struct {
	Allow        []string `yaml:"allow,omitempty" jsonschema_description:"Hosts (exact, or *.suffix) and CIDRs a templated URL may reach."`
	AllowPrivate bool     `yaml:"allow_private,omitempty" jsonschema_description:"Let templated URLs reach private, loopback and link-local addresses."`
}
