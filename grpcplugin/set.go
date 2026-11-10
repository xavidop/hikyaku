package grpcplugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// registered remembers which kinds of the plugin registry are served out
// of process, by which plugin, so loading the plugins again (a reload)
// replaces them instead of clashing with them.
var registered = struct {
	sync.Mutex
	kinds map[kindKey]string
}{kinds: map[kindKey]string{}}

// Set is the plugins of a spec.
type Set struct {
	plugins []*Plugin
}

// LoadAll launches the spec's plugins (see Launch), in order. A kind that
// clashes with a built-in type, a type this binary registers in process,
// or another plugin's kind is an error naming both. On any error, the
// plugins already launched are stopped.
func LoadAll(ctx context.Context, bins []config.PluginBinary, baseDir string, log *slog.Logger) (*Set, error) {
	s := &Set{}
	seen := map[string]bool{}
	for _, b := range bins {
		if seen[b.Name] {
			_ = s.Close()
			return nil, fmt.Errorf("plugin %s is declared twice", b.Name)
		}
		seen[b.Name] = true
		p, err := Launch(ctx, b, baseDir, log)
		if err != nil {
			_ = s.Close()
			return nil, err
		}
		s.plugins = append(s.plugins, p)
	}
	if err := checkKinds(s.plugins); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// checkKinds finds the kinds that are not free to take.
func checkKinds(plugins []*Plugin) error {
	owner := map[kindKey]string{}
	var errs []error
	for _, p := range plugins {
		for _, k := range p.kinds {
			key := kindKey{k.Role, k.Name}
			if other, ok := owner[key]; ok {
				errs = append(errs, fmt.Errorf("plugin %s: %s type %q is also served by plugin %s", p.name, k.Role, k.Name, other))
				continue
			}
			owner[key] = p.name
			if builtin(k) {
				errs = append(errs, fmt.Errorf("plugin %s: %s type %q clashes with the built-in %s %s type", p.name, k.Role, k.Name, k.Name, k.Role))
				continue
			}
			if inProcess(k) {
				errs = append(errs, fmt.Errorf("plugin %s: %s type %q clashes with the %s %s type this binary registers (a Go plugin)", p.name, k.Role, k.Name, k.Name, k.Role))
			}
		}
	}
	return errors.Join(errs...)
}

func builtin(k Kind) bool {
	if k.Role == RoleSource {
		return config.IsBuiltinSource(k.Name)
	}
	return config.IsBuiltinSink(k.Name)
}

// inProcess reports a kind registered by this binary, not by a plugin.
func inProcess(k Kind) bool {
	var ok bool
	if k.Role == RoleSource {
		_, ok = plugin.LookupSource(k.Name)
	} else {
		_, ok = plugin.LookupSink(k.Name)
	}
	if !ok {
		return false
	}
	registered.Lock()
	defer registered.Unlock()
	_, remote := registered.kinds[kindKey{k.Role, k.Name}]
	return !remote
}

// Plugins are the launched plugins, in spec order.
func (s *Set) Plugins() []*Plugin { return slices.Clone(s.plugins) }

// Register makes every kind of the plugins a source or destination type
// (plugin.RegisterSource, plugin.RegisterSink). Each spec entry of one of
// them opens a handle in its plugin and gets a proxy.
func (s *Set) Register() {
	registered.Lock()
	defer registered.Unlock()
	for _, p := range s.plugins {
		for _, k := range p.kinds {
			opts := []plugin.RegisterOption{plugin.WithDescription(k.Description)}
			switch k.Role {
			case RoleSink:
				plugin.RegisterSink(k.Name, p.sinkFactory(k.Name), opts...)
			case RoleSource:
				plugin.RegisterSource(k.Name, p.sourceFactory(k.Name), opts...)
			}
			registered.kinds[kindKey{k.Role, k.Name}] = p.name
		}
	}
}

// Extensions are the plugins' kinds for hikyaku validate and hikyaku
// schema: each options: block is checked against the JSON Schema its
// plugin declares (any object when it declares none), and the schema
// describes it.
func (s *Set) Extensions() config.Extensions {
	x := config.Extensions{Sources: map[string]config.ExtType{}, Sinks: map[string]config.ExtType{}}
	for _, p := range s.plugins {
		for _, k := range p.kinds {
			t := config.ExtType{Description: k.Description, Schema: k.Schema}
			if v := p.validators[kindKey{k.Role, k.Name}]; v != nil {
				t.Check = v.check
			}
			if k.Role == RoleSink {
				x.Sinks[k.Name] = t
			} else {
				x.Sources[k.Name] = t
			}
		}
	}
	return x
}

// Close stops every plugin (see Plugin.Close).
func (s *Set) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	for _, p := range s.plugins {
		if err := p.Close(); err != nil {
			errs = append(errs, fmt.Errorf("plugin %s: %w", p.name, err))
		}
	}
	return errors.Join(errs...)
}
