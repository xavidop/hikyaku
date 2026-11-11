package config

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// The protocol matrix (design §4): source types with their modes, sink
// types, and the protocols only the full build links (their SDKs are heavy;
// hikyaku-lite leaves them out).
var (
	sourceModes = map[string][]string{
		"http": {"listen", "poll"}, "https": {"listen", "poll"},
		"ws": {"listen", "subscribe"}, "wss": {"listen", "subscribe"},
		"sse":   {"listen", "subscribe"},
		"stdin": {""}, "file": {""},
		"grpc": {"listen", "subscribe"}, "tcp": {"listen"}, "udp": {"listen"},
		"kafka": {"subscribe"}, "nats": {"subscribe"}, "jetstream": {"subscribe"}, "mqtt": {"subscribe"},
		"amqp": {"subscribe"}, "redis": {"subscribe"},
		"sqs": {"subscribe"}, "sns": {"subscribe"}, "eventbridge": {"subscribe"}, "pubsub": {"subscribe"},
		"servicebus": {"subscribe"}, "eventhubs": {"subscribe"},
		"s3": {"poll"}, "gcs": {"poll"},
		"openai": {"listen"},
	}
	sinkTypes = []string{
		"http", "https", "ws", "wss", "sse", "stdout", "stderr", "file", "fault", "group",
		"grpc", "tcp", "udp", "kafka", "nats", "jetstream", "mqtt", "amqp", "redis",
		"sqs", "sns", "eventbridge", "pubsub", "servicebus", "eventhubs", "s3", "gcs", "model",
	}
	fullOnly = map[string]bool{
		"grpc": true, "kafka": true, "nats": true, "jetstream": true, "mqtt": true, "amqp": true, "redis": true,
		"sqs": true, "sns": true, "eventbridge": true, "s3": true, "pubsub": true, "gcs": true,
		"servicebus": true, "eventhubs": true,
	}
)

// FullOnly reports whether only the full build links a protocol: the
// brokers and clouds, whose SDKs hikyaku-lite leaves out.
func FullOnly(protocol string) bool { return fullOnly[protocol] }

// NotInBuild is the error for a protocol of the matrix that this binary
// does not link.
func NotInBuild(kind, name, protocol string) error {
	if FullOnly(protocol) {
		return fmt.Errorf("%s %s: protocol %s is not in this build: hikyaku-lite leaves out the broker and cloud protocols, use the full hikyaku binary", kind, name, protocol)
	}
	return fmt.Errorf("%s %s: protocol %s is not in this build", kind, name, protocol)
}

// SourceTypes lists the source types of the protocol matrix.
func SourceTypes() []string {
	out := make([]string, 0, len(sourceModes))
	for t := range sourceModes {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// SinkTypes lists the destination types of the protocol matrix.
func SinkTypes() []string { return slices.Clone(sinkTypes) }

// IsBuiltinSource reports whether t is a source type of the protocol matrix.
func IsBuiltinSource(t string) bool { _, ok := sourceModes[t]; return ok }

// IsBuiltinSink reports whether t is a destination type of the protocol matrix.
func IsBuiltinSink(t string) bool { return slices.Contains(sinkTypes, t) }

var (
	nameRe   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	secretRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// maxRouterDepth bounds nested router fallbacks.
const maxRouterDepth = 8

// Validate checks a merged, defaulted spec and returns every problem.
// Expressions and templates are compiled by engine.Check, which runs after.
func Validate(s *Spec) Errors { return ValidateWith(s, Extensions{}) }

// ValidateWith is Validate with the plugin types of a custom build.
func ValidateWith(s *Spec, x Extensions) Errors {
	v := &validator{s: s, ext: x}
	v.spec()
	return v.errs
}

type validator struct {
	s    *Spec
	ext  Extensions
	errs Errors
}

// typeList names the known types for an error message.
func typeList(builtin []string, ext map[string]ExtType) string {
	all := slices.Clone(builtin)
	for k := range ext {
		all = append(all, k)
	}
	slices.Sort(all)
	return strings.Join(all, ", ")
}

// plugin checks the options: block of a plugin type.
func (v *validator) plugin(p string, t ExtType, options map[string]any) {
	if t.Check == nil {
		return
	}
	if err := t.Check(options); err != nil {
		v.add(p+".options", "%v", err)
	}
}

func (v *validator) add(path, format string, args ...any) { v.errs.Add(path, format, args...) }

func (v *validator) spec() {
	s := v.s
	if s.Version != 1 {
		v.add("version", "must be 1 (got %d)", s.Version)
	}
	switch {
	case s.Name == "":
		v.add("name", "is required: it names the relay in metrics and shared-state keys")
	case !nameRe.MatchString(s.Name):
		v.add("name", "%q may hold letters, digits, '.', '_' and '-' only", s.Name)
	}
	for name, ref := range s.Secrets {
		if !secretRe.MatchString(name) {
			v.add("secrets."+name, "a secret name is an identifier such as WEBHOOK_SECRET")
		}
		if strings.TrimSpace(ref) == "" {
			v.add("secrets."+name, "the ref is empty (use env:NAME, file:/path, ...)")
		}
	}
	if len(s.Sources) == 0 {
		v.add("sources", "at least one source is required")
	}
	if len(s.Destinations) == 0 {
		v.add("destinations", "at least one destination is required")
	}
	if len(s.Routes) == 0 {
		v.add("routes", "at least one route is required")
	}
	v.models()
	v.policy("defaults", s.Defaults)
	stores := v.stores()
	sources := v.sources()
	dests := v.destinations(stores)
	v.fallbackCycles()
	v.listeners()
	v.pluginBinaries()
	v.deadLetter(dests)
	v.notify(dests)
	v.routes(sources, dests, stores)
	v.process()
	for i := range s.Sources {
		src := &s.Sources[i]
		p := fmt.Sprintf("source %q", src.Name)
		if src.Respond != nil && src.Respond.From != "" && !dests[src.Respond.From] {
			v.add(p+".respond.from", "destination %q is not declared", src.Respond.From)
		}
		if src.Dedupe != nil && src.Dedupe.Store != "" && !stores[src.Dedupe.Store] {
			v.add(p+".dedupe.store", "store %q is not declared", src.Dedupe.Store)
		}
	}
}

func (v *validator) models() {
	m := v.s.Models
	if m.Retry.Attempts < 0 {
		v.add("models.retry.attempts", "must not be negative")
	}
	for _, p := range v.s.Genkit.Plugins {
		if !slices.Contains([]string{"systemone", "googlegenai", "anthropic", "openai", "openrouter", "ollama", "llmfake"}, p) {
			v.add("genkit.plugins", "unknown plugin %q (systemone, googlegenai, anthropic, openai, openrouter, ollama, llmfake)", p)
		}
	}
	for _, srv := range v.s.Genkit.SystemOne.Servers {
		if !slices.Contains([]string{"typesafe", "openrouter", "workersai", "ollama", "llmfake"}, srv) {
			v.add("genkit.systemone.servers", "unknown server %q (typesafe, openrouter, workersai, ollama; others go under custom:)", srv)
		}
	}
	if b := v.s.Genkit.OpenAI.BaseURL; b != "" {
		if u, err := url.Parse(b); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			v.add("genkit.openai.base_url", "must be an http:// or https:// URL, such as https://api.openai.com/v1")
		}
	}
	seen := map[string]bool{}
	for i, c := range v.s.Genkit.SystemOne.Custom {
		p := fmt.Sprintf("genkit.systemone.custom[%d]", i)
		if c.Provider == "" {
			v.add(p+".provider", "is required")
		}
		if c.BaseURL == "" {
			v.add(p+".base_url", "is required")
		}
		if seen[c.Provider] {
			v.add(p+".provider", "%q is declared twice", c.Provider)
		}
		seen[c.Provider] = true
	}
}

func (v *validator) policy(path string, p Policy) {
	if p.Retry != nil {
		v.retry(path+".retry", p.Retry)
	}
	if b := p.CircuitBreaker; b != nil {
		if b.FailureRatio < 0 || b.FailureRatio > 1 {
			v.add(path+".circuit_breaker.failure_ratio", "must be within 0..1")
		}
		if b.MinRequests < 0 || b.HalfOpenMax < 0 {
			v.add(path+".circuit_breaker", "counts must not be negative")
		}
	}
	if b := p.RetryBudget; b != nil {
		if b.Ratio < 0 || b.MinPerSecond < 0 {
			v.add(path+".retry_budget", "ratio and min_per_second must not be negative")
		}
	}
}

func (v *validator) retry(path string, r *Retry) {
	if r.Attempts < 0 {
		v.add(path+".attempts", "must not be negative (1 means no retry)")
	}
	if r.Jitter != "" && !slices.Contains([]string{"full", "equal", "none"}, r.Jitter) {
		v.add(path+".jitter", "must be full, equal or none")
	}
	if r.Backoff > 0 && r.MaxBackoff > 0 && r.MaxBackoff < r.Backoff {
		v.add(path+".max_backoff", "is shorter than backoff")
	}
}

func (v *validator) stores() map[string]bool {
	names := map[string]bool{}
	for i, st := range v.s.Stores {
		p := fmt.Sprintf("stores[%d]", i)
		if st.Name != "" {
			p = fmt.Sprintf("store %q", st.Name)
		}
		v.name(p, "store", st.Name, names)
		switch st.Type {
		case "memory":
		case "redis":
			n := 0
			if st.Addr != "" {
				n++
			}
			if st.Sentinel != nil {
				n++
				if st.Sentinel.MasterName == "" || len(st.Sentinel.Addrs) == 0 {
					v.add(p+".sentinel", "needs master_name and addrs")
				}
			}
			if st.Cluster != nil {
				n++
				if len(st.Cluster.Addrs) == 0 {
					v.add(p+".cluster.addrs", "is required")
				}
			}
			if n != 1 {
				v.add(p, "a redis store needs exactly one of addr, sentinel or cluster")
			}
		case "":
			v.add(p+".type", "is required (redis or memory)")
		default:
			v.add(p+".type", "unknown store type %q (redis or memory)", st.Type)
		}
		if st.Retry != nil {
			v.retry(p+".retry", st.Retry)
		}
	}
	return names
}

func (v *validator) name(path, kind, name string, seen map[string]bool) {
	switch {
	case name == "":
		v.add(path+".name", "is required")
		return
	case !nameRe.MatchString(name):
		v.add(path+".name", "%q may hold letters, digits, '.', '_' and '-' only", name)
	case seen[name]:
		v.add(path, "%s %q is declared twice", kind, name)
	}
	seen[name] = true
}

func (v *validator) sources() map[string]bool {
	names := map[string]bool{}
	for i := range v.s.Sources {
		src := &v.s.Sources[i]
		p := fmt.Sprintf("sources[%d]", i)
		if src.Name != "" {
			p = fmt.Sprintf("source %q", src.Name)
		}
		v.name(p, "source", src.Name, names)
		modes, known := sourceModes[src.Type]
		if src.Type == "" {
			v.add(p+".type", "is required")
			continue
		}
		if !known {
			if t, ok := v.ext.source(src.Type); ok {
				v.plugin(p, t, src.Options)
				continue
			}
			v.add(p+".type", "unknown source type %q (one of %s)", src.Type, typeList(SourceTypes(), v.ext.Sources))
			continue
		}
		if len(src.Options) > 0 {
			v.add(p+".options", "applies to plugin types: %s sources take their settings as keys of their own", src.Type)
		}
		if !slices.Contains(modes, src.Mode) {
			if src.Mode == "" || modes[0] == "" {
				v.add(p+".mode", "%s sources take no mode", src.Type)
			} else {
				v.add(p+".mode", "%s sources run in %s mode, not %q", src.Type, strings.Join(modes, " or "), src.Mode)
			}
			continue
		}
		switch src.Mode {
		case "listen":
			if src.Listen == "" {
				v.add(p+".listen", "is required in listen mode (such as :8080)")
			} else if _, _, err := net.SplitHostPort(src.Listen); err != nil {
				v.add(p+".listen", "%q is not host:port (%v)", src.Listen, err)
			}
			if src.Path != "" && !strings.HasPrefix(src.Path, "/") {
				v.add(p+".path", "must start with /")
			}
		case "poll":
			if src.URL == "" && (src.Type == "http" || src.Type == "https") {
				v.add(p+".url", "is required in poll mode")
			}
		case "subscribe":
			if src.URL == "" && slices.Contains(subscribeNeedsURL, src.Type) {
				v.add(p+".url", "is required in subscribe mode")
			}
		}
		if src.Type == "file" && src.Path == "" {
			v.add(p+".path", "is required for a file source")
		}
		v.sourceProtocol(p, *src)
		if src.Mode != "listen" {
			if src.Auth != nil {
				v.add(p+".auth", "inbound auth applies to listen sources only")
			}
			if src.Respond != nil {
				v.add(p+".respond", "applies to http and grpc listen sources only")
			}
		}
		if src.Respond != nil && !slices.Contains([]string{"http", "https", "grpc", "openai"}, src.Type) {
			v.add(p+".respond", "applies to http and grpc listen sources only")
		}
		if src.Ack != "" && !slices.Contains([]string{"delivered", "accepted"}, src.Ack) {
			v.add(p+".ack", "must be delivered or accepted")
		}
		if src.Ack == "accepted" && src.Respond != nil {
			v.add(p+".respond", "needs ack: delivered (a reply exists only once the message is delivered)")
		}
		if src.Split != "" && src.Mode != "poll" {
			v.add(p+".split", "applies to poll sources only")
		}
		if len(src.FailoverURLs) > 0 && (src.Mode != "subscribe" || !slices.Contains(subscribeNeedsURL, src.Type)) {
			v.add(p+".failover_urls", "applies to ws, wss, sse, grpc, nats, jetstream, mqtt, amqp and redis subscribe sources only")
		}
		if src.Format != "" && src.Type != "stdin" && src.Type != "file" {
			v.add(p+".format", "applies to stdin and file sources only")
		}
		if src.Retry != nil {
			v.retry(p+".retry", src.Retry)
		}
		if src.Reconnect != nil && src.Reconnect.Jitter != "" && !slices.Contains([]string{"full", "equal", "none"}, src.Reconnect.Jitter) {
			v.add(p+".reconnect.jitter", "must be full, equal or none")
		}
		if src.Dedupe != nil && src.Dedupe.Key == "" {
			v.add(p+".dedupe.key", "is required")
		}
		if a := src.Auth; a != nil {
			if a.HMAC != nil && (a.HMAC.Header == "" || a.HMAC.Secret == "") {
				v.add(p+".auth.hmac", "needs header and secret")
			}
			if a.HMAC != nil && a.HMAC.Algo != "" && !slices.Contains([]string{"sha256", "sha1", "sha512"}, a.HMAC.Algo) {
				v.add(p+".auth.hmac.algo", "must be sha256, sha1 or sha512")
			}
			if a.Bearer != nil && len(a.Bearer.Tokens) == 0 {
				v.add(p+".auth.bearer.tokens", "is required")
			}
			if w := a.Webhook; w != nil {
				v.webhookAuth(p+".auth.webhook", src.Type, w)
			}
			if a.JWT != nil && a.JWT.JWKSURL == "" {
				v.add(p+".auth.jwt.jwks_url", "is required")
			}
			if a.APIKey != nil && len(a.APIKey.Keys) == 0 {
				v.add(p+".auth.api_key.keys", "is required")
			}
			for _, ip := range a.IPAllow {
				if !validIPOrCIDR(ip) {
					v.add(p+".auth.ip_allow", "%q is not an IP or CIDR", ip)
				}
			}
		}
	}
	return names
}

// WebhookProviders are the senders auth.webhook can verify.
var WebhookProviders = []string{"github", "stripe", "slack", "shopify", "twilio"}

func (v *validator) webhookAuth(p, sourceType string, w *WebhookAuth) {
	if sourceType != "http" {
		v.add(p, "applies to http sources only: it signs the request body")
	}
	if !slices.Contains(WebhookProviders, w.Provider) {
		v.add(p+".provider", "must be one of %s", strings.Join(WebhookProviders, ", "))
	}
	if w.Secret == "" {
		v.add(p+".secret", "is required")
	}
	if w.Tolerance != 0 && w.Provider != "stripe" && w.Provider != "slack" {
		v.add(p+".tolerance", "applies to stripe and slack only: %s signs no timestamp", w.Provider)
	}
	if w.URL != "" && w.Provider != "twilio" {
		v.add(p+".url", "applies to twilio only")
	}
}

func validIPOrCIDR(s string) bool {
	if _, _, err := net.ParseCIDR(s); err == nil {
		return true
	}
	return net.ParseIP(s) != nil
}

func (v *validator) destinations(stores map[string]bool) map[string]bool {
	names := map[string]bool{}
	for i := range v.s.Destinations {
		d := &v.s.Destinations[i]
		p := fmt.Sprintf("destinations[%d]", i)
		if d.Name != "" {
			p = fmt.Sprintf("destination %q", d.Name)
		}
		v.name(p, "destination", d.Name, names)
	}
	for i := range v.s.Destinations {
		d := &v.s.Destinations[i]
		p := fmt.Sprintf("destination %q", d.Name)
		v.policy(p, d.Policy)
		if d.RetryBudget != nil && d.RetryBudget.Store != "" && !stores[d.RetryBudget.Store] {
			v.add(p+".retry_budget.store", "store %q is not declared", d.RetryBudget.Store)
		}
		for j, f := range d.Fallback {
			switch {
			case f == d.Name:
				v.add(fmt.Sprintf("%s.fallback[%d]", p, j), "a destination cannot fall back to itself")
			case !names[f]:
				v.add(fmt.Sprintf("%s.fallback[%d]", p, j), "destination %q is not declared", f)
			}
		}
		v.transforms(p+".transforms", d.Transforms)
		for j, t := range d.Transforms {
			if t.Validate != nil && t.Validate.OnInvalid != nil {
				v.add(fmt.Sprintf("%s.transforms[%d].validate.on_invalid", p, j), "applies to a route's transforms: on a destination, give the destination a fallback")
			}
		}
		if d.DeliverAfter < 0 {
			v.add(p+".deliver_after", "must not be negative")
		}
		if rl := d.RateLimit; rl != nil {
			if rl.PerSecond <= 0 {
				v.add(p+".rate_limit.per_second", "must be above 0")
			}
			if rl.Burst < 0 {
				v.add(p+".rate_limit.burst", "must not be negative")
			}
		}
		v.batch(p, d.Batch)
		if d.Batch != nil && (d.Type == "model" || slices.ContainsFunc(d.Members, func(m Member) bool { return m.Type == "model" })) {
			v.add(p+".batch", "does not apply to a model destination: a model answers one chat request per message")
		}
		if d.Type == "group" {
			v.group(p, d)
			continue
		}
		if len(d.Members) > 0 || d.Strategy != "" || d.HealthCheck != nil || d.Hedge != nil {
			v.add(p, "members, strategy, health_check and hedge apply to type: group only")
		}
		v.endpoint(p, d.Endpoint)
	}
	return names
}

func (v *validator) group(p string, d *Destination) {
	if d.Strategy != "" && !slices.Contains([]string{"failover", "round_robin", "weighted", "least_latency"}, d.Strategy) {
		v.add(p+".strategy", "must be failover, round_robin, weighted or least_latency")
	}
	if len(d.Members) == 0 {
		v.add(p+".members", "a group needs at least one member")
	}
	seen := map[string]bool{}
	for j, m := range d.Members {
		mp := fmt.Sprintf("%s.members[%d]", p, j)
		if m.Type == "group" {
			v.add(mp+".type", "groups do not nest")
			continue
		}
		if m.Name != "" {
			if seen[m.Name] {
				v.add(mp+".name", "%q is used twice in the group", m.Name)
			}
			seen[m.Name] = true
		}
		if m.Weight < 0 {
			v.add(mp+".weight", "must not be negative")
		}
		v.endpoint(mp, m.Endpoint)
	}
	if d.Hedge != nil {
		if !d.Idempotent {
			v.add(p+".hedge", "hedging sends a message twice: set idempotent: true once the endpoint dedupes by idempotency key")
		}
		if d.Hedge.After <= 0 {
			v.add(p+".hedge.after", "is required (such as 800ms)")
		}
		if d.Hedge.Max < 0 {
			v.add(p+".hedge.max", "must not be negative")
		}
	}
	if d.URL != "" || d.Path != "" {
		v.add(p, "a group's endpoints go under members:")
	}
}

func (v *validator) endpoint(p string, e Endpoint) {
	switch {
	case e.Type == "":
		v.add(p+".type", "is required")
		return
	case !slices.Contains(sinkTypes, e.Type) || e.Type == "group":
		if t, ok := v.ext.sink(e.Type); ok && e.Type != "group" {
			v.plugin(p, t, e.Options)
			return
		}
		v.add(p+".type", "unknown destination type %q (one of %s)", e.Type, typeList(sinkTypes, v.ext.Sinks))
		return
	}
	if len(e.Options) > 0 {
		v.add(p+".options", "applies to plugin types: %s destinations take their settings as keys of their own", e.Type)
	}
	if sg := e.Sign; sg != nil {
		if e.Type != "http" && e.Type != "https" {
			v.add(p+".sign", "applies to http and https destinations")
		}
		if sg.Secret == "" {
			v.add(p+".sign.secret", "is required")
		}
		if !slices.Contains([]string{"", "sha256", "sha512"}, sg.Algo) {
			v.add(p+".sign.algo", "must be sha256 or sha512")
		}
		if !slices.Contains([]string{"", "hex", "base64"}, sg.Encoding) {
			v.add(p+".sign.encoding", "must be hex or base64")
		}
	}
	switch e.Type {
	case "http", "https":
		if e.URL == "" {
			v.add(p+".url", "is required")
		} else if !strings.Contains(e.URL, "{{") && !strings.HasPrefix(e.URL, "http://") && !strings.HasPrefix(e.URL, "https://") {
			v.add(p+".url", "%q must start with http:// or https://", e.URL)
		}
	case "ws", "wss":
		if e.URL == "" {
			v.add(p+".url", "is required")
		} else if !strings.Contains(e.URL, "{{") && !strings.HasPrefix(e.URL, "ws://") && !strings.HasPrefix(e.URL, "wss://") {
			v.add(p+".url", "%q must start with ws:// or wss://", e.URL)
		}
	case "file":
		if e.Path == "" {
			v.add(p+".path", "is required")
		}
	case "sse":
		if e.Listen == "" {
			v.add(p+".listen", "is required: the address the event stream is served on")
		}
	default:
		v.endpointProtocol(p, e)
	}
	if e.Format != "" && !slices.Contains([]string{"file", "stdout", "stderr"}, e.Type) {
		v.add(p+".format", "applies to file, stdout and stderr only")
	}
	if e.Auth != nil {
		switch {
		case e.Type != "sse":
			v.add(p+".auth", "applies to sse destinations only: it checks the clients that read the stream")
		case e.Auth.HMAC != nil:
			v.add(p+".auth.hmac", "does not apply: a client reading the stream sends no body to sign")
		case e.Auth.Webhook != nil:
			v.add(p+".auth.webhook", "does not apply: a client reading the stream sends no body to sign")
		}
	}
	if e.Frame != "" && !slices.Contains([]string{"ws", "wss", "tcp", "udp"}, e.Type) {
		v.add(p+".frame", "applies to ws, tcp and udp destinations only")
	}
	if (e.Type == "ws" || e.Type == "wss") && e.Frame != "" && e.Frame != "text" && e.Frame != "binary" {
		v.add(p+".frame", "must be text or binary for ws")
	}
	if e.Fault != nil && e.Type != "fault" {
		v.add(p+".fault", "applies to type: fault only (use hikyaku chaos to wrap a real destination)")
	}
	if f := e.Fault; f != nil {
		if f.Error != "" && !slices.Contains([]string{"status", "connect", "timeout"}, f.Error) {
			v.add(p+".fault.error", "must be status, connect or timeout")
		}
		if f.Status != 0 && (f.Status < 100 || f.Status > 599) {
			v.add(p+".fault.status", "must be an HTTP status")
		}
	}
}

// fallbackCycles rejects destination fallback loops (a → b → a), reporting
// each cycle once with its path.
func (v *validator) fallbackCycles() {
	graph := map[string][]string{}
	for _, d := range v.s.Destinations {
		graph[d.Name] = d.Fallback
	}
	const (
		white = iota
		grey
		black
	)
	color := map[string]int{}
	var stack []string
	reported := map[string]bool{}
	var visit func(n string)
	visit = func(n string) {
		color[n] = grey
		stack = append(stack, n)
		for _, next := range graph[n] {
			if _, ok := graph[next]; !ok {
				continue
			}
			switch color[next] {
			case grey:
				i := slices.Index(stack, next)
				cycle := append(slices.Clone(stack[i:]), next)
				key := canonicalCycle(cycle)
				if !reported[key] {
					reported[key] = true
					v.add(fmt.Sprintf("destination %q.fallback", next), "fallback cycle: %s", strings.Join(cycle, " → "))
				}
			case white:
				visit(next)
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
	}
	for _, d := range v.s.Destinations {
		if color[d.Name] == white {
			visit(d.Name)
		}
	}
}

func (v *validator) notify(dests map[string]bool) {
	for i, n := range v.s.Notify {
		p := fmt.Sprintf("notify[%d]", i)
		if len(n.To) == 0 {
			v.add(p+".to", "is required: the destinations that get the alert")
		}
		v.destRefs(p+".to", n.To, dests)
		for j, ev := range n.On {
			if !slices.Contains(NotifyEvents, ev) {
				v.add(fmt.Sprintf("%s.on[%d]", p, j), "%q is not an event (use %s)", ev, strings.Join(NotifyEvents, ", "))
			}
		}
	}
}

func canonicalCycle(c []string) string {
	nodes := slices.Clone(c[:len(c)-1])
	slices.Sort(nodes)
	return strings.Join(nodes, ",")
}

func (v *validator) deadLetter(dests map[string]bool) {
	dl := v.s.DeadLetter
	if dl == nil {
		return
	}
	switch dl.Type {
	case "group":
		v.add("dead_letter.type", "a dead-letter queue is one endpoint, not a group")
	default:
		v.endpoint("dead_letter", dl.Endpoint)
	}
	if dl.Retry != nil {
		v.retry("dead_letter.retry", dl.Retry)
	}
	for i, f := range dl.Fallback {
		if f != "stdout" && f != "stderr" && !dests[f] {
			v.add(fmt.Sprintf("dead_letter.fallback[%d]", i), "%q is neither a destination nor stdout/stderr", f)
		}
	}
}

func (v *validator) routes(sources, dests, stores map[string]bool) {
	names := map[string]bool{}
	for i := range v.s.Routes {
		r := &v.s.Routes[i]
		p := fmt.Sprintf("routes[%d]", i)
		if r.Name != "" {
			p = fmt.Sprintf("route %q", r.Name)
		}
		v.name(p, "route", r.Name, names)
		if len(r.From) == 0 {
			v.add(p+".from", "at least one source is required")
		}
		for j, f := range r.From {
			if !sources[f] {
				v.add(fmt.Sprintf("%s.from[%d]", p, j), "source %q is not declared", f)
			}
		}
		v.transforms(p+".transforms", r.Transforms)
		for j, t := range r.Transforms {
			if t.Validate == nil || t.Validate.OnInvalid == nil {
				continue
			}
			tp := fmt.Sprintf("%s.transforms[%d].validate.on_invalid.to", p, j)
			if len(t.Validate.OnInvalid.To) == 0 {
				v.add(tp, "is required: the destinations that get a refused message")
			}
			v.destRefs(tp, t.Validate.OnInvalid.To, dests)
		}
		switch {
		case r.Router == nil:
			v.add(p, "needs to: (static) or router:")
			continue
		case len(r.To) > 0 && (r.Router.Type != "static" || !slices.Equal(r.Router.To, r.To)):
			v.add(p, "set to: or router:, not both")
		}
		v.router(p+".router", r.Router, dests, stores, 0)
	}
}

func (v *validator) destRefs(path string, refs []string, dests map[string]bool) {
	for j, d := range refs {
		if !dests[d] {
			v.add(fmt.Sprintf("%s[%d]", path, j), "destination %q is not declared", d)
		}
	}
}

func (v *validator) router(p string, rs *RouterSpec, dests, stores map[string]bool, depth int) {
	if depth > maxRouterDepth {
		v.add(p, "router fallbacks nest deeper than %d", maxRouterDepth)
		return
	}
	switch rs.Type {
	case "static":
		if len(rs.To) == 0 {
			v.add(p+".to", "a static router needs at least one destination")
		}
		v.destRefs(p+".to", rs.To, dests)
	case "rules":
		if len(rs.Rules) == 0 {
			v.add(p+".rules", "a rules router needs at least one rule")
		}
		for j, rule := range rs.Rules {
			rp := fmt.Sprintf("%s.rules[%d]", p, j)
			if strings.TrimSpace(rule.When) == "" {
				v.add(rp+".when", "is required")
			}
			if len(rule.To) == 0 {
				v.add(rp+".to", "is required")
			}
			v.destRefs(rp+".to", rule.To, dests)
		}
		if rs.Match != "" && rs.Match != "first" && rs.Match != "all" {
			v.add(p+".match", "must be first or all")
		}
		v.destRefs(p+".default", rs.Default, dests)
	case "ai":
		v.ai(p, rs, dests, stores)
	case "sticky":
		if rs.History == nil || rs.History.Key == "" {
			v.add(p+".history.key", "a sticky router reads the history of a key: set history.key")
		} else if rs.History.Store != "" && !stores[rs.History.Store] {
			v.add(p+".history.store", "store %q is not declared", rs.History.Store)
		}
	case "":
		v.add(p+".type", "is required (static, rules, ai or sticky)")
	default:
		v.add(p+".type", "unknown router type %q (static, rules, ai or sticky)", rs.Type)
	}
	for j := range rs.Fallback {
		v.router(fmt.Sprintf("%s.fallback[%d]", p, j), &rs.Fallback[j], dests, stores, depth+1)
	}
}

func (v *validator) ai(p string, rs *RouterSpec, dests, stores map[string]bool) {
	if rs.Engine != "" && rs.Engine != "decision" && rs.Engine != "generative" {
		v.add(p+".engine", "must be decision or generative")
	}
	if v.s.DecisionModel(*rs) == "" {
		if rs.Engine == "generative" {
			v.add(p+".model", "a generative router needs a model: set router.model or models.default")
		} else {
			v.add(p+".model", "a decision router needs a model: set router.model or models.decision")
		}
	}
	if len(rs.Candidates) == 0 {
		v.add(p+".candidates", "an AI router needs candidates")
	}
	seen := map[string]bool{}
	for j, c := range rs.Candidates {
		cp := fmt.Sprintf("%s.candidates[%d]", p, j)
		switch {
		case c.Name == "":
			v.add(cp+".name", "is required")
		case !dests[c.Name]:
			v.add(cp+".name", "AI candidate %q is not a destination: the AI only picks declared destinations", c.Name)
		case seen[c.Name]:
			v.add(cp+".name", "%q is a candidate twice", c.Name)
		}
		seen[c.Name] = true
		if strings.TrimSpace(c.Description) == "" {
			v.add(cp+".description", "is required: it is what the model reads to pick %q", c.Name)
		}
	}
	if rs.MinConfidence < 0 || rs.MinConfidence > 1 {
		v.add(p+".min_confidence", "must be within 0..1")
	}
	if rs.MinMargin < 0 || rs.MinMargin > 1 {
		v.add(p+".min_margin", "must be within 0..1")
	}
	v.destRefs(p+".on_low_confidence", rs.OnLowConfidence, dests)
	if rs.Cache != nil && rs.Cache.Store != "" && !stores[rs.Cache.Store] {
		v.add(p+".cache.store", "store %q is not declared", rs.Cache.Store)
	}
	if rs.Budget != nil && rs.Budget.Store != "" && !stores[rs.Budget.Store] {
		v.add(p+".budget.store", "store %q is not declared", rs.Budget.Store)
	}
	if h := rs.History; h != nil {
		hp := p + ".history"
		if h.Key == "" {
			v.add(hp+".key", "is required: whose history (customer, conversation, session, device)")
		}
		if h.TTL <= 0 {
			v.add(hp+".ttl", "is required: history must expire (such as 7d)")
		}
		if h.Store != "" && !stores[h.Store] {
			v.add(hp+".store", "store %q is not declared", h.Store)
		}
		if h.Window.MaxEntries < 0 {
			v.add(hp+".window.max_entries", "must not be negative")
		}
		switch h.Record.State {
		case "", "redacted", "none":
		case "summary":
			if h.Record.Summary == "" {
				v.add(hp+".record.summary", "is required with state: summary (such as {{ .json.type }})")
			}
		default:
			v.add(hp+".record.state", "must be redacted, summary or none")
		}
		if h.OnStoreError != "" && h.OnStoreError != "ignore" && h.OnStoreError != "fallback" {
			v.add(hp+".on_store_error", "must be ignore or fallback")
		}
		if st := h.Sticky; st != nil {
			if st.Mode != "" && !slices.Contains([]string{"model", "always_last", "off"}, st.Mode) {
				v.add(hp+".sticky.mode", "must be model, always_last or off")
			}
			if st.FollowUpThreshold < 0 || st.FollowUpThreshold > 1 {
				v.add(hp+".sticky.follow_up_threshold", "must be within 0..1")
			}
		}
	}
}

func (v *validator) transforms(p string, ts []Transform) {
	for i, t := range ts {
		set := 0
		for _, ok := range []bool{t.Filter != "", t.JQ != "", t.Template != "", t.Headers != nil, t.Codec != "", t.Validate != nil, len(t.Redact) > 0, t.Enrich != nil} {
			if ok {
				set++
			}
		}
		tp := p + "[" + strconv.Itoa(i) + "]"
		switch {
		case set == 0 && t.ContentType == "":
			v.add(tp, "set one of filter, jq, template, headers, codec, validate, redact, enrich or content_type")
		case set > 1:
			v.add(tp, "set only one of filter, jq, template, headers, codec, validate, redact and enrich per transform")
		}
		if en := t.Enrich; en != nil {
			if en.URL == "" {
				v.add(tp+".enrich.url", "is required")
			}
			if en.Into == "" {
				v.add(tp+".enrich.into", "is required: the key of the JSON body that gets the answer")
			} else if slices.Contains(strings.Split(en.Into, "."), "") {
				v.add(tp+".enrich.into", "%q is not a key or a dotted path of keys", en.Into)
			}
			if en.Method != "" && en.Method != "GET" && en.Method != "POST" {
				v.add(tp+".enrich.method", "must be GET or POST")
			}
			if en.OnError != "" && en.OnError != "fail" && en.OnError != "skip" {
				v.add(tp+".enrich.on_error", "must be fail or skip")
			}
			if en.Cache != 0 && en.Method == "POST" {
				v.add(tp+".enrich.cache", "applies to GET only: a POST's answer depends on the body")
			}
			if t.ContentType != "" {
				v.add(tp+".enrich", "content_type does not go with enrich: the body stays JSON")
			}
		}
		if va := t.Validate; va != nil {
			switch {
			case va.Schema == nil && va.File == "":
				v.add(tp+".validate", "set schema (inline) or file")
			case va.Schema != nil && va.File != "":
				v.add(tp+".validate", "set only one of schema and file")
			}
			if t.ContentType != "" {
				v.add(tp+".validate", "content_type does not go with validate: it never changes the message")
			}
		}
	}
}

func (v *validator) process() {
	s := v.s
	switch s.Buffer.Type {
	case "memory", "disk":
	default:
		v.add("buffer.type", "must be memory or disk")
	}
	if s.Buffer.Size < 0 {
		v.add("buffer.size", "must not be negative")
	}
	if s.Buffer.Type == "disk" && s.Buffer.Path == "" {
		v.add("buffer.path", "is required for a disk buffer")
	}
	if s.Buffer.Type != "disk" && (s.Buffer.Path != "" || s.Buffer.Sync != nil) {
		v.add("buffer", "path and sync apply to a disk buffer (type: disk) only")
	}
	if s.Server.Workers < 0 {
		v.add("server.workers", "must not be negative")
	}
	if !s.Server.Admin.Disabled {
		if _, _, err := net.SplitHostPort(s.Server.Admin.Listen); err != nil {
			v.add("server.admin.listen", "%q is not host:port", s.Server.Admin.Listen)
		}
	}
	for _, a := range s.Egress.Allow {
		if strings.ContainsAny(a, " /") && !validIPOrCIDR(a) {
			v.add("egress.allow", "%q is neither a host nor a CIDR", a)
		}
	}
	switch s.Observability.Logs.Level {
	case "", "debug", "info", "warn", "error":
	default:
		v.add("observability.logs.level", "must be debug, info, warn or error")
	}
	switch s.Observability.Logs.Format {
	case "", "json", "text":
	default:
		v.add("observability.logs.format", "must be json or text")
	}
}
