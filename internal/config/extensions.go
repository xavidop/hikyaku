package config

import (
	"encoding/json"
	"strings"

	"github.com/invopop/jsonschema"
)

// Extensions are the source and destination types a binary adds to the
// protocol matrix: plugins registered by a custom build. hikyaku validate
// accepts their types and checks their options: blocks; hikyaku schema
// describes those blocks.
type Extensions struct {
	Sources map[string]ExtType
	Sinks   map[string]ExtType
}

// ExtType is one plugin type.
type ExtType struct {
	// Check validates an options: block (nil accepts any).
	Check func(options map[string]any) error
	// Options is a zero value of the plugin's options struct, for the
	// schema (nil: any object).
	Options any
	// Schema is the JSON Schema of the options: block of a type with no Go
	// struct in this process: an out-of-process plugin declares its own.
	// Options wins when both are set; an unparsable one is any object.
	Schema      json.RawMessage
	Description string
}

// optionsSchema is the schema of a type's options: block, nil when it has
// none.
func (t ExtType) optionsSchema() *jsonschema.Schema {
	switch {
	case t.Options != nil:
		return newReflector().Reflect(t.Options)
	case len(t.Schema) > 0:
		var s jsonschema.Schema
		if err := json.Unmarshal(t.Schema, &s); err == nil {
			return &s
		}
	}
	return nil
}

// prefixRefs points the local refs of a plugin's options schema at its
// definitions, which SchemaWith moves to the top as "<kind>.<name>".
func prefixRefs(kind string, s *jsonschema.Schema) {
	if s == nil {
		return
	}
	if name, ok := strings.CutPrefix(s.Ref, "#/$defs/"); ok {
		s.Ref = "#/$defs/" + kind + "." + name
	}
	for _, list := range [][]*jsonschema.Schema{s.AllOf, s.AnyOf, s.OneOf, s.PrefixItems} {
		for _, sub := range list {
			prefixRefs(kind, sub)
		}
	}
	for _, m := range []map[string]*jsonschema.Schema{s.DependentSchemas, s.PatternProperties} {
		for _, sub := range m {
			prefixRefs(kind, sub)
		}
	}
	for _, sub := range []*jsonschema.Schema{s.Not, s.If, s.Then, s.Else, s.Items, s.Contains, s.AdditionalProperties, s.PropertyNames, s.ContentSchema} {
		prefixRefs(kind, sub)
	}
	if s.Properties != nil {
		for pair := s.Properties.Oldest(); pair != nil; pair = pair.Next() {
			prefixRefs(kind, pair.Value)
		}
	}
}

func (x Extensions) source(t string) (ExtType, bool) {
	e, ok := x.Sources[t]
	return e, ok
}

func (x Extensions) sink(t string) (ExtType, bool) {
	e, ok := x.Sinks[t]
	return e, ok
}

// SchemaWith is Schema with the options: blocks of plugin types: each
// becomes an if/then on the destination (or source) type.
func SchemaWith(x Extensions) *jsonschema.Schema {
	s := Schema()
	add := func(def string, types map[string]ExtType) {
		d, ok := s.Definitions[def]
		if !ok {
			return
		}
		for _, kind := range sortedKeys(types) {
			t := types[kind]
			opts := &jsonschema.Schema{Type: "object", Description: t.Description}
			if o := t.optionsSchema(); o != nil {
				opts = o
				opts.Version, opts.ID = "", ""
				if opts.Description == "" {
					opts.Description = t.Description
				}
				prefixRefs(kind, opts)
				for name, sub := range opts.Definitions {
					prefixRefs(kind, sub)
					s.Definitions[kind+"."+name] = sub
				}
				opts.Definitions = nil
			}
			cond := &jsonschema.Schema{Required: []string{"type"}, Properties: jsonschema.NewProperties()}
			cond.Properties.Set("type", &jsonschema.Schema{Const: kind})
			then := &jsonschema.Schema{Properties: jsonschema.NewProperties()}
			then.Properties.Set("options", opts)
			d.AllOf = append(d.AllOf, &jsonschema.Schema{If: cond, Then: then})
		}
	}
	add("Destination", x.Sinks)
	add("Member", x.Sinks)
	add("Source", x.Sources)
	return s
}

func sortedKeys(m map[string]ExtType) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
