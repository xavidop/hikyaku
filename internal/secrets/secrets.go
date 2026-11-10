// Package secrets resolves the secrets a run needs through mamori, once,
// before any test starts, and holds them as secret.String values that are
// revealed only at the moment of use.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/secret"
)

// DefaultTimeout bounds one provider call.
const DefaultTimeout = 30 * time.Second

var refVarRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Source says where a secret came from: the link of its chain that resolved.
type Source struct {
	Name   string
	Scheme string
	// Ref is the resolving link in redacted form.
	Ref string
	// Link is the 1-based position in the chain of Links refs.
	Link, Links int
}

// Store holds resolved secrets.
type Store struct {
	values  map[string]secret.String
	sources map[string]Source
}

// NewStore builds a Store from values already in hand (the Go API).
func NewStore(values map[string]secret.String) *Store {
	s := &Store{values: map[string]secret.String{}, sources: map[string]Source{}}
	for k, v := range values {
		s.values[k] = v
		s.sources[k] = Source{Name: k, Scheme: "provided", Ref: "provided", Link: 1, Links: 1}
	}
	return s
}

// Get returns the secret.String for name.
func (s *Store) Get(name string) (secret.String, bool) {
	if s == nil {
		return secret.String{}, false
	}
	v, ok := s.values[name]
	return v, ok
}

// Reveal returns the plaintext of name. Call it at the point of use only.
func (s *Store) Reveal(name string) (string, error) {
	v, ok := s.Get(name)
	if !ok {
		return "", &ConfigError{Name: name, Msg: "secret is not resolved"}
	}
	return v.Reveal(), nil
}

// Lookup is Reveal shaped for a name lookup.
func (s *Store) Lookup(name string) (string, bool) {
	v, ok := s.Get(name)
	if !ok {
		return "", false
	}
	return v.Reveal(), true
}

// Names lists the resolved secrets, sorted.
func (s *Store) Names() []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.values))
	for k := range s.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Source says which link of its chain resolved name.
func (s *Store) Source(name string) (Source, bool) {
	if s == nil {
		return Source{}, false
	}
	src, ok := s.sources[name]
	return src, ok
}

// Options configure Resolve.
type Options struct {
	// RefVars feed ${VAR} in refs: --var KEY=VAL and the suite's vars, never
	// the ambient environment.
	RefVars map[string]string
	// EnvFile, when set, is a dotenv file consulted when an env: ref is not
	// set in the process environment (hikyaku run --env-file).
	EnvFile string
	// Providers are registered for this run only (tests; cmd/hikyaku registers
	// the cloud providers globally).
	Providers []mamori.Provider
	// Timeout bounds one provider call; zero means DefaultTimeout.
	Timeout time.Duration
}

// Resolve resolves every ref in refs (name to mamori ref, chains allowed)
// and returns the Store. A ref whose ${VAR} is not in RefVars, an invalid
// ref or a chain exhausted without a value is a *ConfigError; a provider
// failure is an *InfraError. Values never appear in errors.
func Resolve(ctx context.Context, refs map[string]string, o Options) (*Store, error) {
	store := &Store{values: map[string]secret.String{}, sources: map[string]Source{}}
	timeout := o.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	var mopts []mamori.Option
	if len(o.RefVars) > 0 {
		mopts = append(mopts, mamori.WithRefVars(o.RefVars))
	}
	for _, p := range o.Providers {
		mopts = append(mopts, mamori.WithProvider(p))
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		val, src, err := resolveOne(ctx, name, refs[name], o, mopts, timeout)
		if err != nil {
			return nil, err
		}
		store.values[name] = val
		store.sources[name] = src
	}
	return store, nil
}

func resolveOne(ctx context.Context, name, raw string, o Options, mopts []mamori.Option, timeout time.Duration) (secret.String, Source, error) {
	expanded, err := expandRefVars(raw, o.RefVars)
	if err != nil {
		return secret.String{}, Source{}, &ConfigError{Name: name, Msg: err.Error()}
	}
	links, err := mamori.ParseRefs(expanded)
	if err != nil {
		return secret.String{}, Source{}, &ConfigError{Name: name, Msg: "invalid ref: " + err.Error(), Err: err}
	}
	var tried []string
	for i, link := range links {
		candidates := []mamori.Ref{link}
		if link.Scheme == "env" && o.EnvFile != "" {
			if dot, err := mamori.ParseRef("dotenv://" + o.EnvFile + "#" + link.Path); err == nil {
				candidates = append(candidates, dot)
			}
		}
		for _, cand := range candidates {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			v, err := mamori.Resolve(cctx, cand.Raw, mopts...)
			cancel()
			if err == nil {
				return secret.NewStringBytes(v.Bytes), Source{Name: name, Scheme: cand.Scheme, Ref: cand.Redacted(), Link: i + 1, Links: len(links)}, nil
			}
			tried = append(tried, cand.Redacted())
			switch kind := mamori.ErrorKind(err); kind {
			case mamori.KindNotFound:
				continue
			case mamori.KindInvalid:
				return secret.String{}, Source{}, &ConfigError{Name: name, Msg: fmt.Sprintf("%s: %s", cand.Redacted(), strings.TrimPrefix(err.Error(), "mamori: ")), Err: err}
			default:
				if errors.Is(err, context.Canceled) {
					return secret.String{}, Source{}, err
				}
				return secret.String{}, Source{}, &InfraError{Name: name, Scheme: cand.Scheme, Ref: cand.Redacted(), Kind: kind, Err: err}
			}
		}
	}
	return secret.String{}, Source{}, &ConfigError{Name: name, Msg: fmt.Sprintf("not found in any of %d ref(s): %s", len(links), strings.Join(tried, ", "))}
}

func expandRefVars(ref string, vars map[string]string) (string, error) {
	var missing []string
	out := refVarRe.ReplaceAllStringFunc(ref, func(m string) string {
		name := refVarRe.FindStringSubmatch(m)[1]
		if v, ok := vars[name]; ok {
			return v
		}
		if !slices.Contains(missing, name) {
			missing = append(missing, name)
		}
		return m
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("ref var %s is not set (pass --var %s=...)", strings.Join(missing, ", "), missing[0])
	}
	return out, nil
}

// NewStoreFromStrings builds a Store from plaintext values (used to merge
// stores resolved one at a time).
func NewStoreFromStrings(values map[string]string) *Store {
	s := &Store{values: map[string]secret.String{}, sources: map[string]Source{}}
	for k, v := range values {
		s.values[k] = secret.NewString(v)
		s.sources[k] = Source{Name: k, Scheme: "provided", Ref: "provided", Link: 1, Links: 1}
	}
	return s
}

// Values reveals every resolved secret, by name: what the spec loader
// interpolates into {{secrets.NAME}} and the redactor learns to hide.
func (s *Store) Values() map[string]string {
	out := make(map[string]string, len(s.values))
	for k, v := range s.values {
		out[k] = v.Reveal()
	}
	return out
}

// EnvLookup returns a lookup that reads the process environment through
// getenv and then, when envFile is set, the dotenv file through mamori's
// dotenv provider. It resolves {{env.NAME}} in a spec.
func EnvLookup(envFile string, getenv func(string) (string, bool)) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if v, ok := getenv(name); ok {
			return v, true
		}
		if envFile == "" {
			return "", false
		}
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		defer cancel()
		v, err := mamori.Resolve(ctx, "dotenv://"+envFile+"#"+name)
		if err != nil {
			return "", false
		}
		return string(v.Bytes), true
	}
}
