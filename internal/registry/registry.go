// Package registry turns the plugin registry into what validation needs:
// the registered source and destination types the protocol matrix does
// not know (a custom build's plugins), with their options checks. The CLI
// and the root library API share it.
package registry

import (
	"reflect"
	"slices"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Extensions are the registered plugin types the protocol matrix does not
// know: a custom build's destinations and sources.
func Extensions() config.Extensions {
	x := config.Extensions{Sources: map[string]config.ExtType{}, Sinks: map[string]config.ExtType{}}
	builtinSrc, builtinSink := config.SourceTypes(), config.SinkTypes()
	for _, r := range plugin.SourceRegistrations() {
		if !slices.Contains(builtinSrc, r.Kind) {
			x.Sources[r.Kind] = extType(r)
		}
	}
	for _, r := range plugin.SinkRegistrations() {
		if !slices.Contains(builtinSink, r.Kind) {
			x.Sinks[r.Kind] = extType(r)
		}
	}
	return x
}

func extType(r plugin.Registration) config.ExtType {
	t := config.ExtType{Options: r.Options, Description: r.Description}
	if r.Options != nil {
		typ := reflect.TypeOf(r.Options)
		t.Check = func(options map[string]any) error {
			return plugin.DecodeOptions(options, reflect.New(typ).Interface())
		}
	}
	return t
}
