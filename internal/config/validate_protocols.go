package config

import (
	"fmt"
	"slices"
	"strings"
)

// The checks of the broker, cloud, grpc, tcp/udp and model protocols: the
// fields each type needs. Each protocol module validates the finer points
// (URL schemes it dials, value ranges) when it builds the source or sink.

// subscribeNeedsURL lists the subscribe sources that dial a URL.
var subscribeNeedsURL = []string{"ws", "wss", "sse", "grpc", "nats", "jetstream", "mqtt", "amqp", "redis"}

// sourceProtocol checks the fields a source type needs.
func (v *validator) sourceProtocol(p string, src Source) {
	need := func(field string, ok bool, why string) {
		if !ok {
			v.add(p+"."+field, "is required: %s", why)
		}
	}
	switch src.Type {
	case "grpc":
		if src.Mode == "subscribe" {
			need("method", src.Method != "", "the server-streaming method subscribed to, /package.Service/Method")
		}
		if src.Path != "" {
			v.add(p+".path", "does not apply to grpc: a call names its method (/package.Service/Method)")
		}
	case "tcp", "udp":
		v.framing(p, src.Frame, src.LengthBytes)
	case "kafka":
		need("brokers", len(src.Brokers) > 0, "the bootstrap brokers")
		need("topic", src.Topic != "" || len(src.Topics) > 0, "the topic (or topics) consumed")
		need("group", src.Group != "", "the consumer group, whose committed offsets let a restart resume")
		v.oneOf(p, "topic", src.Topic != "", "topics", len(src.Topics) > 0)
		v.start(p, src.Start, "earliest", "latest")
		v.sasl(p, src.SASL)
	case "nats":
		need("subject", src.Subject != "", "the subject subscribed to")
	case "jetstream":
		need("stream", src.Stream != "", "the stream consumed")
		v.start(p, src.Start, "all", "new", "last")
	case "mqtt":
		need("topic", src.Topic != "" || len(src.Topics) > 0, "the topic filter (or topics) subscribed to")
		v.oneOf(p, "topic", src.Topic != "", "topics", len(src.Topics) > 0)
		v.qos(p, src.QoS)
		v.mqttVersion(p, src.ProtocolVersion)
		if src.Group != "" && src.ProtocolVersion == "3.1.1" {
			v.add(p+".group", "shared subscriptions need MQTT 5 (protocol_version: \"5\")")
		}
	case "amqp":
		need("queue", src.Queue != "", "the queue consumed")
		if src.RoutingKey != "" && src.Exchange == "" {
			v.add(p+".routing_key", "needs exchange (the exchange the queue is bound to)")
		}
		if src.Exchange != "" && !src.Declare {
			v.add(p+".exchange", "needs declare: true: the source binds its queue to the exchange (or bind it in RabbitMQ and drop exchange)")
		}
	case "redis":
		need("stream", src.Stream != "" || src.Channel != "", "a stream (with group) or a pub/sub channel")
		v.oneOf(p, "stream", src.Stream != "", "channel", src.Channel != "")
		if src.Stream != "" {
			need("group", src.Group != "", "the consumer group, so entries are acknowledged and a restart resumes")
		}
		v.start(p, src.Start, "earliest", "latest")
	case "sqs", "sns", "eventbridge":
		need("queue_url", src.QueueURL != "", "the SQS queue read")
		if src.Prefetch != 0 && (src.Prefetch < 1 || src.Prefetch > 10) {
			v.add(p+".prefetch", "must be 1 to 10 for %s (SQS receives at most 10 messages at once)", src.Type)
		}
		if src.WaitTime.Std() > 20e9 {
			v.add(p+".wait_time", "must be at most 20s (the SQS long-poll limit)")
		}
	case "s3", "gcs":
		need("bucket", src.Bucket != "", "the bucket polled")
		if src.After != "" && !slices.Contains([]string{"keep", "delete", "move"}, src.After) {
			v.add(p+".after", "must be keep, delete or move")
		}
		if src.After == "move" && src.MoveTo == "" {
			v.add(p+".move_to", "is required with after: move")
		}
		if src.MoveTo != "" && src.After != "move" {
			v.add(p+".move_to", "applies with after: move only")
		}
		if src.Type == "gcs" && src.Retry != nil {
			v.add(p+".retry", "does not apply to gcs: the storage client retries its reads itself")
		}
	case "pubsub":
		need("subscription", src.Subscription != "", "the subscription pulled")
		v.gcpProject(p, src.GCP, src.Subscription, "subscriptions")
	case "servicebus":
		v.azure(p, src.Azure)
		need("queue", src.Queue != "" || src.Topic != "", "the queue (or topic with subscription) received from")
		if src.Topic != "" && src.Subscription == "" {
			v.add(p+".subscription", "is required with topic")
		}
		v.oneOf(p, "queue", src.Queue != "", "topic", src.Topic != "")
	case "eventhubs":
		v.azure(p, src.Azure)
		need("topic", src.Topic != "" || entityPath(src.Azure), "the event hub (or an EntityPath in the connection string)")
		v.start(p, src.Start, "earliest", "latest")
	case "openai":
		if src.Respond != nil {
			v.add(p+".respond", "does not apply: an openai source answers with the reply of the destination that served the request")
		}
	}
	if src.Ack == "accepted" && !slices.Contains([]string{"http", "https", "grpc"}, src.Type) && src.Mode != "subscribe" {
		v.add(p+".ack", "accepted applies to http and grpc listen sources and to subscribe sources")
	}
	if src.Ordering != nil && src.Ordering.Key != "" && strings.TrimSpace(src.Ordering.Key) == "" {
		v.add(p+".ordering.key", "is blank")
	}
}

// endpointProtocol checks the fields a destination type needs.
func (v *validator) endpointProtocol(p string, e Endpoint) {
	need := func(field string, ok bool, why string) {
		if !ok {
			v.add(p+"."+field, "is required: %s", why)
		}
	}
	scheme := func(schemes ...string) {
		if e.URL == "" || strings.Contains(e.URL, "{{") {
			return
		}
		for _, s := range schemes {
			if strings.HasPrefix(e.URL, s+"://") {
				return
			}
		}
		v.add(p+".url", "%q must start with %s", e.URL, strings.Join(schemes, ":// or ")+"://")
	}
	switch e.Type {
	case "grpc":
		need("url", e.URL != "", "the server, grpc://host:port or grpcs://host:port")
		need("method", e.Method != "", "the method called, /package.Service/Method")
		scheme("grpc", "grpcs")
	case "tcp":
		need("url", e.URL != "", "the peer, tcp://host:port or tls://host:port")
		scheme("tcp", "tls")
		v.framing(p, e.Frame, e.LengthBytes)
	case "udp":
		need("url", e.URL != "", "the peer, udp://host:port or dtls://host:port")
		scheme("udp", "dtls")
		v.framing(p, e.Frame, e.LengthBytes)
	case "kafka":
		need("brokers", len(e.Brokers) > 0, "the bootstrap brokers")
		need("topic", e.Topic != "", "the topic produced to")
		v.sasl(p, e.SASL)
	case "nats", "jetstream":
		need("url", e.URL != "", "the server, nats://host:4222")
		need("subject", e.Subject != "", "the subject published to")
		scheme("nats", "tls", "ws", "wss")
	case "mqtt":
		need("url", e.URL != "", "the broker, mqtt://host:1883 or mqtts://")
		need("topic", e.Topic != "", "the topic published to")
		v.qos(p, e.QoS)
		v.mqttVersion(p, e.ProtocolVersion)
	case "amqp":
		need("url", e.URL != "", "the broker, amqp://user:pass@host:5672/vhost")
		need("routing_key", e.Exchange != "" || e.RoutingKey != "", "the routing key (the queue name on the default exchange), or an exchange")
		scheme("amqp", "amqps")
	case "redis":
		need("url", e.URL != "", "the server, redis://host:6379/0 or rediss://")
		need("stream", e.Stream != "" || e.Channel != "", "a stream (XADD) or a pub/sub channel (PUBLISH)")
		v.oneOf(p, "stream", e.Stream != "", "channel", e.Channel != "")
		scheme("redis", "rediss")
		if e.MaxLen != 0 && e.Stream == "" {
			v.add(p+".max_len", "applies to streams only")
		}
	case "sqs":
		need("queue_url", e.QueueURL != "", "the queue sent to")
		if e.Delay.Std() > 15*60e9 {
			v.add(p+".delay", "must be at most 15m (the SQS limit)")
		}
	case "sns":
		need("topic", e.Topic != "", "the topic ARN published to")
	case "s3", "gcs":
		need("bucket", e.Bucket != "", "the bucket written to")
	case "pubsub":
		need("topic", e.Topic != "", "the topic published to")
		v.gcpProject(p, e.GCP, e.Topic, "topics")
	case "servicebus":
		v.azure(p, e.Azure)
		need("queue", e.Queue != "" || e.Topic != "", "the queue (or topic) sent to")
		v.oneOf(p, "queue", e.Queue != "", "topic", e.Topic != "")
	case "eventhubs":
		v.azure(p, e.Azure)
		need("topic", e.Topic != "" || entityPath(e.Azure), "the event hub (or an EntityPath in the connection string)")
	case "model":
		need("model", e.Model != "", "the Genkit model, provider/name")
		if e.Model != "" && !strings.Contains(e.Model, "/") {
			v.add(p+".model", "%q must be provider/name, such as googleai/gemini-2.5-flash", e.Model)
		}
	}
}

func (v *validator) framing(p, frame string, lengthBytes int) {
	if frame != "" && !slices.Contains([]string{"newline", "length", "raw"}, frame) {
		v.add(p+".frame", "must be newline, length or raw")
	}
	if lengthBytes != 0 && !slices.Contains([]int{1, 2, 4, 8}, lengthBytes) {
		v.add(p+".length_bytes", "must be 1, 2, 4 or 8")
	}
	if lengthBytes != 0 && frame != "length" {
		v.add(p+".length_bytes", "applies with frame: length only")
	}
}

func (v *validator) oneOf(p, a string, hasA bool, b string, hasB bool) {
	if hasA && hasB {
		v.add(p, "set %s or %s, not both", a, b)
	}
}

func (v *validator) start(p, start string, allowed ...string) {
	if start != "" && !slices.Contains(allowed, start) {
		v.add(p+".start", "must be %s", strings.Join(allowed, " or "))
	}
}

func (v *validator) sasl(p string, s *SASL) {
	if s == nil {
		return
	}
	if !slices.Contains([]string{"plain", "scram-sha-256", "scram-sha-512"}, s.Mechanism) {
		v.add(p+".sasl.mechanism", "must be plain, scram-sha-256 or scram-sha-512")
	}
	if s.Username == "" || s.Password == "" {
		v.add(p+".sasl", "needs username and password")
	}
}

func (v *validator) qos(p string, q *int) {
	if q != nil && (*q < 0 || *q > 2) {
		v.add(p+".qos", "must be 0, 1 or 2")
	}
}

func (v *validator) mqttVersion(p, version string) {
	if version != "" && version != "3.1.1" && version != "5" {
		v.add(p+".protocol_version", "must be \"3.1.1\" or \"5\"")
	}
}

// gcpProject requires a project unless name is a full resource path
// (projects/<p>/<kind>/<name>).
func (v *validator) gcpProject(p string, g *GCP, name, kind string) {
	if strings.HasPrefix(name, "projects/") {
		if !strings.Contains(name, "/"+kind+"/") {
			v.add(p, "%q is not a %s path (projects/<project>/%s/<name>)", name, kind, kind)
		}
		return
	}
	if g == nil || g.Project == "" {
		v.add(p+".gcp.project", "is required (or give the full projects/<project>/%s/<name> path)", kind)
	}
}

func (v *validator) azure(p string, a *Azure) {
	if a == nil || (a.ConnectionString == "" && a.Namespace == "") {
		v.add(p+".azure", "needs connection_string, or namespace with Microsoft Entra credentials")
		return
	}
	if a.ConnectionString != "" && a.Namespace != "" {
		v.add(p+".azure", "set connection_string or namespace, not both")
	}
	if (a.ClientID != "" || a.ClientSecret != "") && (a.TenantID == "" || a.ClientID == "" || a.ClientSecret == "") {
		v.add(p+".azure", "client secret credentials need tenant_id, client_id and client_secret")
	}
}

// entityPath reports whether an Azure connection string names its entity.
func entityPath(a *Azure) bool {
	return a != nil && strings.Contains(strings.ToLower(a.ConnectionString), "entitypath=")
}

// batch checks a destination's batch: block.
func (v *validator) batch(p string, b *Batch) {
	if b == nil {
		return
	}
	if b.MaxMessages < 0 {
		v.add(p+".batch.max_messages", "must not be negative")
	}
	if b.MaxBytes < 0 {
		v.add(p+".batch.max_bytes", "must not be negative")
	}
	if b.MaxWait.Std() < 0 {
		v.add(p+".batch.max_wait", "must not be negative")
	}
	if b.Format != "" && b.Format != "ndjson" && b.Format != "json_array" {
		v.add(p+".batch.format", "must be ndjson or json_array")
	}
}

// listeners rejects two sources that would bind one port the same way:
// grpc and tcp take a whole TCP port (http, ws, sse and openai sources
// share theirs, by path), udp a whole UDP port.
func (v *validator) listeners() {
	type owner struct{ name, typ string }
	taken := map[string]owner{}
	for _, src := range v.s.Sources {
		if src.Listen == "" || (src.Mode != "" && src.Mode != "listen") {
			continue
		}
		network := "tcp"
		if src.Type == "udp" {
			network = "udp"
		}
		exclusive := src.Type == "grpc" || src.Type == "tcp" || src.Type == "udp"
		key := network + " " + src.Listen
		if o, ok := taken[key]; ok && (exclusive || o.typ == "grpc" || o.typ == "tcp" || o.typ == "udp") {
			v.add(fmt.Sprintf("source %q.listen", src.Name), "%s is also source %q's: a %s listener needs a port of its own", src.Listen, o.name, map[bool]string{true: src.Type, false: o.typ}[exclusive])
			continue
		}
		taken[key] = owner{src.Name, src.Type}
	}
}

// pluginBinaries checks the plugins: section; the host reports what it
// can only learn by starting a binary.
func (v *validator) pluginBinaries() {
	seen := map[string]bool{}
	for i, b := range v.s.Plugins {
		p := fmt.Sprintf("plugins[%d]", i)
		if b.Name != "" {
			p = fmt.Sprintf("plugin %q", b.Name)
		}
		switch {
		case b.Name == "":
			v.add(p+".name", "is required")
		case seen[b.Name]:
			v.add(p+".name", "is declared twice")
		}
		seen[b.Name] = true
		if strings.TrimSpace(b.Command) == "" {
			v.add(p+".command", "is required: the plugin binary, a path or a name on PATH")
		}
		if b.SHA256 != "" && !isHex(b.SHA256, 64) {
			v.add(p+".sha256", "must be the 64 hex digits of the binary's SHA-256")
		}
	}
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range strings.ToLower(s) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
