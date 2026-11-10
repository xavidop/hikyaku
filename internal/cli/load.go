package cli

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
	"go.yaml.in/yaml/v3"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/registry"
	"github.com/xavidop/hikyaku/internal/secrets"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
)

// refVars parses --var KEY=VAL.
func (a *app) refVars() (map[string]string, error) {
	out := map[string]string{}
	for _, kv := range a.vars {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return nil, usageError(fmt.Errorf("--var %q: want KEY=VAL", kv))
		}
		out[k] = v
	}
	return out, nil
}

// extensions are the registered plugin types the protocol matrix does not
// know: a custom build's destinations and sources.
func extensions() config.Extensions { return registry.Extensions() }

// load reads, merges, interpolates and validates the spec files, then
// compiles every expression in them. With resolve, the declared secrets
// are resolved through mamori and every value is taught to the redactor;
// without, {{secrets.X}} stays a placeholder.
func (a *app) load(ctx context.Context, resolve bool) (*config.Loaded, error) {
	vars, err := a.refVars()
	if err != nil {
		return nil, err
	}
	o := config.LoadOptions{
		LookupEnv:  secrets.EnvLookup(a.envFile, a.deps.LookupEnv),
		Extensions: extensions(),
	}
	if resolve {
		o.ResolveSecrets = func(ctx context.Context, refs map[string]string) (map[string]string, error) {
			st, err := secrets.Resolve(ctx, refs, secrets.Options{RefVars: vars, EnvFile: a.envFile, Providers: a.deps.Providers})
			if err != nil {
				return nil, err
			}
			values := st.Values()
			for name, v := range values {
				a.redactor.Add(name, v)
			}
			return values, nil
		}
	}
	set, x, err := a.startPlugins(ctx, o)
	if err != nil {
		return nil, err
	}
	o.Extensions = x
	l, err := config.Load(ctx, a.files, o)
	if err == nil {
		if errs := engine.Check(l.Spec); len(errs) > 0 {
			err = errs
		}
	}
	if err != nil {
		if set != nil {
			_ = set.Close()
		}
		if os.IsNotExist(unwrapPathErr(err)) {
			return nil, configError(fmt.Errorf("%w (create one with hikyaku init, or pass -f)", err))
		}
		return nil, err
	}
	a.pluginsMu.Lock()
	a.lastPlugins = set
	if set != nil {
		a.plugins = append(a.plugins, set)
	}
	a.pluginsMu.Unlock()
	return l, nil
}

func unwrapPathErr(err error) error {
	for e := err; e != nil; {
		if pe, ok := e.(*os.PathError); ok {
			return pe
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		e = u.Unwrap()
	}
	return err
}

// credentials looks up a resolved secret by name (for the AI providers'
// keys).
func credentials(l *config.Loaded) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := l.SecretValues[name]
		return v, ok && v != ""
	}
}

// needsAI reports whether any route has an AI router.
func needsAI(spec *config.Spec) bool {
	for _, r := range spec.Routes {
		if _, ok := r.AIRouter(); ok {
			return true
		}
	}
	for _, d := range spec.Destinations {
		if d.Type == "model" {
			return true
		}
		for _, m := range d.Members {
			if m.Type == "model" {
				return true
			}
		}
	}
	return false
}

// build makes the AI factory (when the spec needs one) and the engine.
type built struct {
	loaded *config.Loaded
	ai     router.AIFactory
	m      *metrics.Metrics
}

func (a *app) newAI(ctx context.Context, l *config.Loaded, m *metrics.Metrics) (router.AIFactory, error) {
	if !needsAI(l.Spec) {
		return nil, nil
	}
	if a.deps.NewAI == nil {
		return nil, fmt.Errorf("this build has no AI router (cmd/hikyaku links the llm module)")
	}
	return a.deps.NewAI(ctx, l.Spec, AIOptions{Credentials: credentials(l), Logger: a.quietLogger(), Metrics: m})
}

// oneShot builds an engine without sources for commands that inject
// messages themselves (send, route, ai test, chaos with --data, dlq
// replay).
func (a *app) oneShot(ctx context.Context, wrap func(dest string, s plugin.Sink) plugin.Sink) (*built, *engine.Engine, error) {
	l, err := a.load(ctx, true)
	if err != nil {
		return nil, nil, err
	}
	return a.oneShotFrom(ctx, l, wrap)
}

// oneShotFrom is oneShot for a spec already loaded (its plugins started).
func (a *app) oneShotFrom(ctx context.Context, l *config.Loaded, wrap func(dest string, s plugin.Sink) plugin.Sink) (*built, *engine.Engine, error) {
	m := metrics.New()
	ai, err := a.newAI(ctx, l, m)
	if err != nil {
		return nil, nil, err
	}
	e, err := engine.New(ctx, l.Spec, engine.Options{
		Logger: a.quietLogger(), Metrics: m, AI: ai, StoreFactories: a.deps.StoreFactories,
		NoSources: true, WrapSink: wrap, Now: a.deps.Now,
	})
	if err != nil {
		return nil, nil, err
	}
	return &built{loaded: l, ai: ai, m: m}, e, nil
}

// readData reads --data: @file, @- for stdin, or the literal text.
func (a *app) readData(data string) ([]byte, error) {
	switch {
	case data == "":
		return nil, usageError(fmt.Errorf("--data is required (@msg.json, @- for stdin, or a JSON literal)"))
	case data == "@-":
		b, err := readAll(a.stdin())
		if err != nil {
			return nil, err
		}
		return b, nil
	case strings.HasPrefix(data, "@"):
		b, err := os.ReadFile(data[1:])
		if err != nil {
			return nil, usageError(fmt.Errorf("--data: %w", err))
		}
		return b, nil
	}
	return []byte(data), nil
}

// message builds the envelope of --data with --header and --content-type.
func (a *app) message(source string, m msgFlags) (*envelope.Envelope, error) {
	body, err := a.readData(m.data)
	if err != nil {
		return nil, err
	}
	env := envelope.New(source, "cli", trimNL(body))
	switch {
	case m.contentType != "":
		env.ContentType = m.contentType
	case jsonValid(env.Body):
		env.ContentType = "application/json"
	default:
		env.ContentType = "text/plain; charset=utf-8"
	}
	env.Subject = m.subject
	for _, h := range m.headers {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			return nil, usageError(fmt.Errorf("--header %q: want Name: value", h))
		}
		env.SetHeader(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	for _, kv := range m.attrs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, usageError(fmt.Errorf("--attr %q: want name=value", kv))
		}
		env.SetAttribute(strings.TrimSpace(k), v)
	}
	return env, nil
}

// msgFlags are the flags that make a message on the command line (send,
// route).
type msgFlags struct {
	data, contentType, subject string
	headers, attrs             []string
}

func (m *msgFlags) add(f *pflag.FlagSet) {
	f.StringVar(&m.data, "data", "", "message body: @file, @- for stdin, or literal JSON or text")
	f.StringArrayVar(&m.headers, "header", nil, "header as Name: value (repeatable)")
	f.StringVar(&m.contentType, "content-type", "", "content type (default: application/json for JSON, else text/plain)")
	f.StringVar(&m.subject, "subject", "", "the message's subject (msg.subject), as a source would set it")
	f.StringArrayVar(&m.attrs, "attr", nil, "attribute as name=value, a string (repeatable; msg.attributes)")
}

// redactedSpec renders the spec as YAML with every secret value hidden.
func (a *app) redactedSpec(spec *config.Spec) []byte {
	b, err := yaml.Marshal(spec)
	if err != nil {
		return []byte("# " + err.Error() + "\n")
	}
	return a.redactor.RedactBytes(b)
}

// startPlugins starts the spec's out-of-process plugins (plugins:) and
// registers their kinds, so validation knows them. It reads the spec once
// without validating it: the plugins' types are what make it valid. The
// returned extensions are the build's plus the plugins'.
func (a *app) startPlugins(ctx context.Context, o config.LoadOptions) (PluginSet, config.Extensions, error) {
	x := o.Extensions
	pre := o
	pre.SkipValidate = true
	pre.ResolveSecrets = nil
	l, err := config.Load(ctx, a.files, pre)
	if err != nil || len(l.Spec.Plugins) == 0 {
		return nil, x, nil // no plugins, or the real load reports the error
	}
	if a.deps.Plugins == nil {
		return nil, x, configError(errors.New("plugins: this build cannot run out-of-process plugins"))
	}
	if o.ResolveSecrets != nil {
		// Again, with the secrets a plugin's env may reference.
		pre.ResolveSecrets = o.ResolveSecrets
		if l, err = config.Load(ctx, a.files, pre); err != nil {
			return nil, x, err
		}
	}
	set, err := a.deps.Plugins(ctx, l.Spec.Plugins, filepath.Dir(a.files[0]), a.quietLogger())
	if err != nil {
		return nil, x, configError(fmt.Errorf("plugins: %w", err))
	}
	set.Register()
	px := set.Extensions()
	merged := config.Extensions{Sources: maps.Clone(x.Sources), Sinks: maps.Clone(x.Sinks)}
	if merged.Sources == nil {
		merged.Sources = map[string]config.ExtType{}
	}
	if merged.Sinks == nil {
		merged.Sinks = map[string]config.ExtType{}
	}
	maps.Copy(merged.Sources, px.Sources)
	maps.Copy(merged.Sinks, px.Sinks)
	return set, merged, nil
}

// closePluginsExcept stops every plugin set but keep (nil: all of them).
func (a *app) closePluginsExcept(keep PluginSet) {
	a.pluginsMu.Lock()
	var stop []PluginSet
	var left []PluginSet
	for _, s := range a.plugins {
		if s == keep {
			left = append(left, s)
		} else {
			stop = append(stop, s)
		}
	}
	a.plugins = left
	a.pluginsMu.Unlock()
	for _, s := range stop {
		if err := s.Close(); err != nil {
			a.quietLogger().Warn("stopping plugins", "err", err)
		}
	}
}

// currentPlugins is the plugin set the last successful load started.
func (a *app) currentPlugins() PluginSet {
	a.pluginsMu.Lock()
	defer a.pluginsMu.Unlock()
	return a.lastPlugins
}
