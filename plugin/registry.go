package plugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"

	"go.yaml.in/yaml/v3"

	"github.com/xavidop/hikyaku/store"
)

// Spec is what a factory builds a source or sink from.
type Spec struct {
	// Name is the source or destination name in the spec (for a group
	// member, "<group>[<i>]" unless the member is named).
	Name string
	// Kind is the protocol type: "http", "wss", "file", ...
	Kind string
	// Options is the entry's options: block, the settings of a plugin
	// type. Decode it into the plugin's own struct with Decode.
	Options map[string]any
	// Config is the whole spec entry for the protocols of this repository:
	// a *config.Source for sources, a *config.Endpoint for sinks. Plugins
	// built outside it cannot import that package; they read Options.
	Config any
	Env    Env
}

// Decode decodes the options: block into a plugin's options struct (its
// fields tagged with yaml names). Unknown keys are errors, so a typo in a
// spec is caught instead of ignored.
func (s Spec) Decode(into any) error {
	if err := DecodeOptions(s.Options, into); err != nil {
		return fmt.Errorf("%s %s: options: %w", s.Kind, s.Name, err)
	}
	return nil
}

// DecodeOptions strictly decodes an options: block into a struct.
func DecodeOptions(options map[string]any, into any) error {
	if len(options) == 0 {
		options = map[string]any{}
	}
	b, err := yaml.Marshal(options)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(into); err != nil && !errors.Is(err, io.EOF) {
		return errors.New(strings.TrimPrefix(err.Error(), "yaml: "))
	}
	return nil
}

// Env is what the relay lends a plugin.
type Env struct {
	// Relay is the relay (spec) name.
	Relay  string
	Logger *slog.Logger
	// Store returns a declared store by name; "" is the relay's default store
	// (the first declared one, or the in-process memory store).
	Store func(name string) (store.Store, error)
	// Egress is the SSRF guard for destinations whose host is a per-message
	// template.
	Egress Egress
	// Buffer is the relay's buffer type, memory or disk. With disk, a
	// message counts as queued (env.OnQueued) once it is on disk, so a
	// source that acknowledges early (ack: accepted) waits for that.
	Buffer string
	// Deduped counts a duplicate a source dropped itself (a Deduper); nil
	// counts nothing.
	Deduped func(source string)
	// Refused counts a message a source refused or dropped because the
	// relay's buffer was full (hikyaku_backpressure_total); nil counts
	// nothing.
	Refused func(source string)
}

// Egress lists where templated destination hosts may point.
type Egress struct {
	// Allow holds hosts (exact, or *.suffix) and CIDRs; empty allows any
	// public host.
	Allow []string
	// AllowPrivate lets templated hosts reach private, loopback and
	// link-local addresses.
	AllowPrivate bool
}

// Log returns the logger, or a discarding one.
func (e Env) Log() *slog.Logger {
	if e.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return e.Logger
}

// Factory builds a plugin from its spec.
type Factory[T any] func(ctx context.Context, spec Spec) (T, error)

// Registration describes a registered kind.
type Registration struct {
	Kind string
	// Options is a zero value of the kind's options struct, when it has one:
	// hikyaku validate decodes every options: block into a copy of it, and
	// hikyaku schema describes it from its yaml and jsonschema tags.
	Options any
	// Description is a one-line summary for docs and the schema.
	Description string
}

// RegisterOption adds to a registration.
type RegisterOption func(*Registration)

// WithOptions declares the options struct of a kind (pass a zero value,
// such as MyOptions{}).
func WithOptions(zero any) RegisterOption { return func(r *Registration) { r.Options = zero } }

// WithDescription describes a kind.
func WithDescription(d string) RegisterOption { return func(r *Registration) { r.Description = d } }

type entry[T any] struct {
	f   Factory[T]
	reg Registration
}

var (
	mu      sync.RWMutex
	sources = map[string]entry[Source]{}
	sinks   = map[string]entry[Sink]{}
)

// RegisterSource makes a source kind available. Registering a kind twice
// replaces the earlier factory.
func RegisterSource(kind string, f Factory[Source], opts ...RegisterOption) {
	mu.Lock()
	defer mu.Unlock()
	sources[kind] = entry[Source]{f: f, reg: registration(kind, opts)}
}

// RegisterSink makes a sink (destination) kind available. A custom
// destination is a Sink registered here before the CLI runs; the engine
// gives it retries, breakers, budgets, groups, fallbacks and the DLQ.
func RegisterSink(kind string, f Factory[Sink], opts ...RegisterOption) {
	mu.Lock()
	defer mu.Unlock()
	sinks[kind] = entry[Sink]{f: f, reg: registration(kind, opts)}
}

func registration(kind string, opts []RegisterOption) Registration {
	r := Registration{Kind: kind}
	for _, o := range opts {
		o(&r)
	}
	return r
}

// LookupSource returns the factory of a source kind.
func LookupSource(kind string) (Factory[Source], bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := sources[kind]
	return e.f, ok
}

// LookupSink returns the factory of a sink kind.
func LookupSink(kind string) (Factory[Sink], bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := sinks[kind]
	return e.f, ok
}

// SourceRegistrations lists the registered source kinds, sorted.
func SourceRegistrations() []Registration { return regs(sources) }

// SinkRegistrations lists the registered sink kinds, sorted.
func SinkRegistrations() []Registration { return regs(sinks) }

func regs[T any](m map[string]entry[T]) []Registration {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]Registration, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, m[k].reg)
	}
	return out
}

// SourceKinds lists the registered source kinds, sorted.
func SourceKinds() []string {
	mu.RLock()
	defer mu.RUnlock()
	return slices.Sorted(maps.Keys(sources))
}

// SinkKinds lists the registered sink kinds, sorted.
func SinkKinds() []string {
	mu.RLock()
	defer mu.RUnlock()
	return slices.Sorted(maps.Keys(sinks))
}

// NewSource builds a source with the registered factory of spec.Kind.
func NewSource(ctx context.Context, spec Spec) (Source, error) {
	f, ok := LookupSource(spec.Kind)
	if !ok {
		return nil, fmt.Errorf("source %s: protocol %q is not available in this build (available: %v)", spec.Name, spec.Kind, SourceKinds())
	}
	return f(ctx, spec)
}

// NewSink builds a sink with the registered factory of spec.Kind.
func NewSink(ctx context.Context, spec Spec) (Sink, error) {
	f, ok := LookupSink(spec.Kind)
	if !ok {
		return nil, fmt.Errorf("destination %s: protocol %q is not available in this build (available: %v)", spec.Name, spec.Kind, SinkKinds())
	}
	return f(ctx, spec)
}
