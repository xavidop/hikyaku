package hikyaku

import (
	"context"
	"log/slog"
	"os"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/registry"
	"github.com/xavidop/hikyaku/internal/secrets"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

// Spec is a loaded relay spec (hikyaku.yaml).
type Spec = config.Spec

// LoadOption configures Load and Parse.
type LoadOption func(*loadOptions)

type loadOptions struct {
	env     func(string) (string, bool)
	secrets bool
}

// WithEnvLookup replaces the process environment for {{env.X}}
// interpolation. mamori's env: secrets still read the process environment.
func WithEnvLookup(fn func(string) (string, bool)) LoadOption {
	return func(o *loadOptions) { o.env = fn }
}

// WithoutSecrets leaves {{secrets.X}} as placeholders instead of resolving
// the declared secrets: enough to validate a spec without its credentials.
func WithoutSecrets() LoadOption {
	return func(o *loadOptions) { o.secrets = false }
}

func (o loadOptions) config() config.LoadOptions {
	lookup := o.env
	if lookup == nil {
		lookup = os.LookupEnv
	}
	lo := config.LoadOptions{LookupEnv: lookup, Extensions: registry.Extensions()}
	if o.secrets {
		lo.ResolveSecrets = func(ctx context.Context, refs map[string]string) (map[string]string, error) {
			st, err := secrets.Resolve(ctx, refs, secrets.Options{})
			if err != nil {
				return nil, err
			}
			return st.Values(), nil
		}
	}
	return lo
}

func loadOpts(opts []LoadOption) loadOptions {
	o := loadOptions{secrets: true}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Load reads, merges and validates spec files: the same checks as hikyaku
// validate, including every CEL expression, jq program and template.
// Declared secrets are resolved through mamori (env:, file:// and the
// providers the program links).
func Load(ctx context.Context, paths []string, opts ...LoadOption) (*Spec, error) {
	l, err := config.Load(ctx, paths, loadOpts(opts).config())
	if err != nil {
		return nil, err
	}
	return checked(l.Spec)
}

// Parse is Load for one document in memory; name is used in errors.
func Parse(ctx context.Context, name string, data []byte, opts ...LoadOption) (*Spec, error) {
	l, err := config.LoadBytes(ctx, name, data, loadOpts(opts).config())
	if err != nil {
		return nil, err
	}
	return checked(l.Spec)
}

func checked(s *Spec) (*Spec, error) {
	if errs := engine.Check(s); len(errs) > 0 {
		return nil, errs
	}
	return s, nil
}

// Option configures a relay.
type Option func(*engine.Options)

// WithLogger sets the relay's logger (default: discard).
func WithLogger(l *slog.Logger) Option { return func(o *engine.Options) { o.Logger = l } }

// WithMetrics records the relay's Prometheus metrics (metrics.New() makes
// a registry to serve).
func WithMetrics(m *metrics.Metrics) Option { return func(o *engine.Options) { o.Metrics = m } }

// WithAI sets the AI factory behind ai routers and model destinations: an
// llm.Factory from the llm module.
func WithAI(f router.AIFactory) Option { return func(o *engine.Options) { o.AI = f } }

// WithStore provides a store the spec declares under name (instead of the
// relay opening it).
func WithStore(name string, s store.Store) Option {
	return func(o *engine.Options) {
		if o.Stores == nil {
			o.Stores = map[string]store.Store{}
		}
		o.Stores[name] = s
	}
}

// WithStoreFactory opens the spec's stores of a type (redis: the
// stores/redis module's factory).
func WithStoreFactory(kind string, f engine.StoreFactory) Option {
	return func(o *engine.Options) {
		if o.StoreFactories == nil {
			o.StoreFactories = map[string]engine.StoreFactory{}
		}
		o.StoreFactories[kind] = f
	}
}

// Relay is a built relay.
type Relay struct {
	e *engine.Engine
}

// New builds a relay: stores, destinations, routes and sources. It starts
// nothing; Run does.
func New(ctx context.Context, spec *Spec, opts ...Option) (*Relay, error) {
	var o engine.Options
	for _, opt := range opts {
		opt(&o)
	}
	e, err := engine.New(ctx, spec, o)
	if err != nil {
		return nil, err
	}
	return &Relay{e: e}, nil
}

// Run runs the relay until ctx ends, a source fails, or every source ran
// out (stdin at EOF). It finishes queued and in-flight messages within
// server.shutdown_grace and closes everything on the way out.
func (r *Relay) Run(ctx context.Context) error { return r.e.Run(ctx) }

// Send pushes one message through the relay's routes, as if a source had
// produced it, and reports what happened.
func (r *Relay) Send(ctx context.Context, env *envelope.Envelope) *engine.Report {
	return r.e.Inject(ctx, env)
}

// Health reports the relay's readiness: sinks, fallback chains, sources.
func (r *Relay) Health(ctx context.Context) engine.Health { return r.e.Health(ctx) }

// Close closes a relay that never ran (Run closes the one it runs).
func (r *Relay) Close(ctx context.Context) { r.e.Close(ctx) }

// Run builds and runs a relay.
func Run(ctx context.Context, spec *Spec, opts ...Option) error {
	r, err := New(ctx, spec, opts...)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}
