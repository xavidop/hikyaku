package config

import (
	"slices"
	"time"
)

// Built-in defaults (design §7.1). Every destination inherits defaults:
// field by field, then these.
const (
	DefaultTimeout       = 5 * time.Second
	DefaultAttempts      = 5
	DefaultBackoff       = 200 * time.Millisecond
	DefaultMaxBackoff    = 10 * time.Second
	DefaultJitter        = "full"
	DefaultBreakerRatio  = 0.5
	DefaultBreakerMinReq = 20
	DefaultBreakerOpen   = 30 * time.Second
	DefaultBreakerHalf   = 5
	DefaultBreakerWindow = 60 * time.Second
	DefaultBudgetRatio   = 0.2
	DefaultBudgetMinRPS  = 10.0

	DefaultBufferSize    = 10000
	DefaultWorkers       = 256
	DefaultShutdownGrace = 20 * time.Second
	// DefaultNotifyEvery is notify.every when it is not set.
	DefaultNotifyEvery  = time.Minute
	DefaultAdminListen  = ":9090"
	DefaultStoreTimeout = 50 * time.Millisecond
	DefaultKeyPrefix    = "hikyaku"
	DefaultPollInterval = 30 * time.Second
	DefaultPollTimeout  = 10 * time.Second
	DefaultMaxBodyBytes = 10 << 20
	DefaultRespondWait  = 30 * time.Second
	DefaultDedupeTTL    = time.Hour
	DefaultIdemHeader   = "Idempotency-Key"

	DefaultAITimeout         = 10 * time.Second
	DefaultAIMaxBytes        = 8 << 10
	DefaultCacheTTL          = 10 * time.Minute
	DefaultCacheSize         = 10000
	DefaultFollowUpThreshold = 0.8
	DefaultModelAttempts     = 3
	DefaultModelBackoff      = 500 * time.Millisecond
	DefaultModelMaxBackoff   = 10 * time.Second
	DefaultDLQAttempts       = 3
)

// DefaultRetryOn and DefaultNeverOn are the built-in error tag lists.
var (
	DefaultRetryOn = TagList{"connect", "timeout", "5xx", "429"}
	DefaultNeverOn = TagList{"4xx"}
)

// ResolveRetry layers retry policies, first set field wins, then the
// built-in defaults: the result has every field set.
func ResolveRetry(layers ...*Retry) Retry {
	var r Retry
	for _, l := range layers {
		if l == nil {
			continue
		}
		if r.Attempts == 0 {
			r.Attempts = l.Attempts
		}
		if r.Backoff == 0 {
			r.Backoff = l.Backoff
		}
		if r.MaxBackoff == 0 {
			r.MaxBackoff = l.MaxBackoff
		}
		if r.Jitter == "" {
			r.Jitter = l.Jitter
		}
		if r.RetryOn == nil {
			r.RetryOn = slices.Clone(l.RetryOn)
		}
		if r.NeverOn == nil {
			r.NeverOn = slices.Clone(l.NeverOn)
		}
		if r.RespectRetryAfter == nil && l.RespectRetryAfter != nil {
			r.RespectRetryAfter = Ptr(*l.RespectRetryAfter)
		}
	}
	if r.Attempts == 0 {
		r.Attempts = DefaultAttempts
	}
	if r.Backoff == 0 {
		r.Backoff = Duration(DefaultBackoff)
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = Duration(DefaultMaxBackoff)
	}
	if r.MaxBackoff < r.Backoff {
		r.MaxBackoff = r.Backoff
	}
	if r.Jitter == "" {
		r.Jitter = DefaultJitter
	}
	if r.RetryOn == nil {
		r.RetryOn = slices.Clone(DefaultRetryOn)
	}
	if r.NeverOn == nil {
		r.NeverOn = slices.Clone(DefaultNeverOn)
	}
	if r.RespectRetryAfter == nil {
		r.RespectRetryAfter = Ptr(true)
	}
	return r
}

// ResolveBreaker layers circuit-breaker settings like ResolveRetry.
func ResolveBreaker(layers ...*CircuitBreaker) CircuitBreaker {
	var b CircuitBreaker
	for _, l := range layers {
		if l == nil {
			continue
		}
		if b.Enabled == nil && l.Enabled != nil {
			b.Enabled = Ptr(*l.Enabled)
		}
		if b.FailureRatio == 0 {
			b.FailureRatio = l.FailureRatio
		}
		if b.MinRequests == 0 {
			b.MinRequests = l.MinRequests
		}
		if b.OpenFor == 0 {
			b.OpenFor = l.OpenFor
		}
		if b.HalfOpenMax == 0 {
			b.HalfOpenMax = l.HalfOpenMax
		}
		if b.Window == 0 {
			b.Window = l.Window
		}
	}
	if b.Enabled == nil {
		b.Enabled = Ptr(true)
	}
	if b.FailureRatio == 0 {
		b.FailureRatio = DefaultBreakerRatio
	}
	if b.MinRequests == 0 {
		b.MinRequests = DefaultBreakerMinReq
	}
	if b.OpenFor == 0 {
		b.OpenFor = Duration(DefaultBreakerOpen)
	}
	if b.HalfOpenMax == 0 {
		b.HalfOpenMax = DefaultBreakerHalf
	}
	if b.Window == 0 {
		b.Window = Duration(DefaultBreakerWindow)
	}
	return b
}

// ResolveBudget layers retry-budget settings like ResolveRetry.
func ResolveBudget(layers ...*RetryBudget) RetryBudget {
	var b RetryBudget
	for _, l := range layers {
		if l == nil {
			continue
		}
		if b.Enabled == nil && l.Enabled != nil {
			b.Enabled = Ptr(*l.Enabled)
		}
		if b.Ratio == 0 {
			b.Ratio = l.Ratio
		}
		if b.MinPerSecond == 0 {
			b.MinPerSecond = l.MinPerSecond
		}
		if b.Store == "" {
			b.Store = l.Store
		}
	}
	if b.Enabled == nil {
		b.Enabled = Ptr(true)
	}
	if b.Ratio == 0 {
		b.Ratio = DefaultBudgetRatio
	}
	if b.MinPerSecond == 0 {
		b.MinPerSecond = DefaultBudgetMinRPS
	}
	return b
}

// ResolvePolicy is a destination's effective policy: its own fields, then
// defaults:, then the built-in defaults. Every pointer of the result is set.
func ResolvePolicy(defaults, own Policy) Policy {
	p := Policy{
		Timeout:  own.Timeout,
		Deadline: own.Deadline,
	}
	if p.Timeout == 0 {
		p.Timeout = defaults.Timeout
	}
	if p.Timeout == 0 {
		p.Timeout = Duration(DefaultTimeout)
	}
	if p.Deadline == 0 {
		p.Deadline = defaults.Deadline
	}
	r := ResolveRetry(own.Retry, defaults.Retry)
	b := ResolveBreaker(own.CircuitBreaker, defaults.CircuitBreaker)
	g := ResolveBudget(own.RetryBudget, defaults.RetryBudget)
	p.Retry, p.CircuitBreaker, p.RetryBudget = &r, &b, &g
	return p
}

// IsRequired reports whether a destination must succeed before the ack.
// A shadow destination never is.
func (d Destination) IsRequired() bool { return !d.Shadow && Bool(d.Required, true) }

// SampleRate is the share of a shadow destination's messages that is
// copied to it: 1 when sample is unset.
func (d Destination) SampleRate() float64 {
	if d.Sample == nil {
		return 1
	}
	return *d.Sample
}

// IdempotencyHeaderName is the header carrying the idempotency key.
func (d Destination) IdempotencyHeaderName() string {
	if d.IdempotencyHeader != "" {
		return d.IdempotencyHeader
	}
	return DefaultIdemHeader
}

// DefaultMode is the mode a source of this type runs in when mode: is left
// out.
func DefaultMode(s Source) string {
	switch s.Type {
	case "http", "https":
		if s.Listen == "" && s.URL != "" {
			return "poll"
		}
		return "listen"
	case "ws", "wss", "sse", "grpc":
		if s.URL != "" && s.Listen == "" {
			return "subscribe"
		}
		return "listen"
	case "tcp", "udp", "openai":
		return "listen"
	case "s3", "gcs":
		return "poll"
	case "stdin", "file":
		return ""
	}
	return "subscribe"
}

// ApplyDefaults fills what the rest of the program reads without a default
// of its own: source modes, the implicit static router, the default DLQ, and
// the process settings.
func (s *Spec) ApplyDefaults() {
	for i := range s.Sources {
		if s.Sources[i].Mode == "" {
			s.Sources[i].Mode = DefaultMode(s.Sources[i])
		}
	}
	for i := range s.Routes {
		r := &s.Routes[i]
		if r.Router == nil && len(r.To) > 0 {
			r.Router = &RouterSpec{Type: "static", To: slices.Clone(r.To)}
		}
	}
	if s.DeadLetter == nil {
		s.DeadLetter = &DeadLetter{Endpoint: Endpoint{Type: "stderr"}}
	}
	if s.Buffer.Type == "" {
		s.Buffer.Type = "memory"
	}
	if s.Buffer.Size == 0 {
		s.Buffer.Size = DefaultBufferSize
	}
	if s.Server.Workers == 0 {
		s.Server.Workers = DefaultWorkers
	}
	if s.Server.ShutdownGrace == 0 {
		s.Server.ShutdownGrace = Duration(DefaultShutdownGrace)
	}
	if s.Server.Admin.Listen == "" {
		s.Server.Admin.Listen = DefaultAdminListen
	}
}

// DecisionModel is the model of an AI router, with the global default.
func (s *Spec) DecisionModel(r RouterSpec) string {
	if r.Model != "" {
		return r.Model
	}
	if r.Engine == "generative" {
		return s.Models.Default
	}
	return s.Models.Decision
}

// ModelChain is the models an AI router tries, in order: its model, then its
// fallback_models (or the global chain of its engine), each model once.
func (s *Spec) ModelChain(r RouterSpec) []string {
	fallback := r.FallbackModels
	if fallback == nil {
		if r.Engine == "generative" {
			fallback = s.Models.Fallback.Default
		} else {
			fallback = s.Models.Fallback.Decision
		}
	}
	out := []string{}
	for _, m := range append([]string{s.DecisionModel(r)}, fallback...) {
		if m != "" && !slices.Contains(out, m) {
			out = append(out, m)
		}
	}
	return out
}

// DestinationByName finds a destination.
func (s *Spec) DestinationByName(name string) (*Destination, bool) {
	for i := range s.Destinations {
		if s.Destinations[i].Name == name {
			return &s.Destinations[i], true
		}
	}
	return nil, false
}

// SourceByName finds a source.
func (s *Spec) SourceByName(name string) (*Source, bool) {
	for i := range s.Sources {
		if s.Sources[i].Name == name {
			return &s.Sources[i], true
		}
	}
	return nil, false
}

// RouteByName finds a route.
func (s *Spec) RouteByName(name string) (*Route, bool) {
	for i := range s.Routes {
		if s.Routes[i].Name == name {
			return &s.Routes[i], true
		}
	}
	return nil, false
}

// AIRouter returns a route's AI router, looking through its fallback chain,
// or false when the route has none.
func (r Route) AIRouter() (*RouterSpec, bool) {
	var walk func(rs *RouterSpec) *RouterSpec
	walk = func(rs *RouterSpec) *RouterSpec {
		if rs == nil {
			return nil
		}
		if rs.Type == "ai" {
			return rs
		}
		for i := range rs.Fallback {
			if f := walk(&rs.Fallback[i]); f != nil {
				return f
			}
		}
		return nil
	}
	rs := walk(r.Router)
	return rs, rs != nil
}
