package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// LoadOptions controls how a spec is loaded.
type LoadOptions struct {
	// LookupEnv resolves {{env.NAME}}; nil means the process environment.
	LookupEnv func(name string) (string, bool)
	// ResolveSecrets turns the declared refs (name → mamori ref) into values.
	// Nil leaves {{secrets.NAME}} as a placeholder, which is enough to
	// validate a spec without reaching any secret provider.
	ResolveSecrets func(ctx context.Context, refs map[string]string) (map[string]string, error)
	// SkipValidate returns the merged spec without validating it.
	SkipValidate bool
	// Extensions are the plugin types of a custom build.
	Extensions Extensions
}

// Loaded is a spec with what it was made of.
type Loaded struct {
	Spec  *Spec
	Files []string
	// SecretRefs are the declared secrets: name → ref.
	SecretRefs map[string]string
	// SecretValues are the resolved values (nil when not resolved).
	SecretValues map[string]string
}

// IsTestFile reports a file of tests for hikyaku test (*.test.yaml,
// *.test.yml): it may sit next to the spec, and is not part of it.
func IsTestFile(name string) bool {
	n := strings.ToLower(name)
	return strings.HasSuffix(n, ".test.yaml") || strings.HasSuffix(n, ".test.yml")
}

// ExpandPaths turns files and directories into the list of files to load:
// a directory contributes every *.yaml and *.yml directly inside it, sorted,
// except test files (IsTestFile). A test file named on its own is loaded.
func ExpandPaths(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		if p == "-" {
			out = append(out, p)
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("spec %s: %w", p, err)
		}
		if !fi.IsDir() {
			out = append(out, p)
			continue
		}
		entries, err := os.ReadDir(p)
		if err != nil {
			return nil, fmt.Errorf("spec %s: %w", p, err)
		}
		var files []string
		for _, e := range entries {
			ext := strings.ToLower(filepath.Ext(e.Name()))
			if !e.IsDir() && (ext == ".yaml" || ext == ".yml") && !IsTestFile(e.Name()) {
				files = append(files, filepath.Join(p, e.Name()))
			}
		}
		sort.Strings(files)
		if len(files) == 0 {
			return nil, fmt.Errorf("spec %s: the directory has no .yaml files", p)
		}
		out = append(out, files...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no spec file given")
	}
	return out, nil
}

// Load reads, merges, interpolates, defaults and validates the spec files.
// A directory loads every YAML file in it. Problems in the spec come back as
// Errors, all of them at once.
func Load(ctx context.Context, paths []string, o LoadOptions) (*Loaded, error) {
	files, err := ExpandPaths(paths)
	if err != nil {
		return nil, err
	}
	docs := make([]fileDoc, 0, len(files))
	for _, f := range files {
		var data []byte
		if f == "-" {
			data, err = io.ReadAll(os.Stdin)
		} else {
			data, err = os.ReadFile(f)
		}
		if err != nil {
			return nil, fmt.Errorf("spec %s: %w", f, err)
		}
		docs = append(docs, fileDoc{name: f, data: data})
	}
	return load(ctx, docs, o)
}

// LoadBytes is Load for an in-memory spec.
func LoadBytes(ctx context.Context, name string, data []byte, o LoadOptions) (*Loaded, error) {
	return load(ctx, []fileDoc{{name: name, data: data}}, o)
}

type fileDoc struct {
	name string
	data []byte
	root *yaml.Node
}

func load(ctx context.Context, docs []fileDoc, o LoadOptions) (*Loaded, error) {
	var errs Errors
	refs := map[string]string{}
	refFile := map[string]string{}
	for i := range docs {
		d := &docs[i]
		errs = append(errs, strictKeys(d.name, d.data)...)
		var doc yaml.Node
		if err := yaml.Unmarshal(d.data, &doc); err != nil {
			errs = append(errs, &Error{File: d.name, Msg: yamlMsg(err)})
			continue
		}
		if len(doc.Content) == 0 {
			errs = append(errs, &Error{File: d.name, Msg: "the file is empty"})
			continue
		}
		d.root = doc.Content[0]
		if d.root.Kind != yaml.MappingNode {
			errs = append(errs, &Error{File: d.name, Line: d.root.Line, Msg: "a spec is a mapping (version:, name:, sources:, ...)"})
			d.root = nil
			continue
		}
		for name, ref := range secretsBlock(d.name, d.root, &errs) {
			if prev, ok := refs[name]; ok && prev != ref {
				errs = append(errs, &Error{File: d.name, Path: "secrets." + name, Msg: fmt.Sprintf("declared differently in %s", refFile[name])})
				continue
			}
			refs[name], refFile[name] = ref, d.name
		}
	}
	if len(errs) > 0 {
		return nil, errs
	}

	var values map[string]string
	if o.ResolveSecrets != nil && len(refs) > 0 {
		v, err := o.ResolveSecrets(ctx, refs)
		if err != nil {
			return nil, err
		}
		values = v
	}
	lookupEnv := o.LookupEnv
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}
	in := interp{
		secret: func(name string) (string, bool) {
			if _, ok := refs[name]; !ok {
				return "", false
			}
			if values != nil {
				v, ok := values[name]
				return v, ok
			}
			return Placeholder(name), true
		},
		env: lookupEnv,
	}

	specs := make([]*Spec, 0, len(docs))
	for _, d := range docs {
		in.document(d.name, d.root, &errs)
		var s Spec
		if err := d.root.Decode(&s); err != nil {
			errs = append(errs, typeErrors(d.name, err)...)
			continue
		}
		specs = append(specs, &s)
	}
	if len(errs) > 0 {
		return nil, errs
	}
	files := make([]string, len(docs))
	for i, d := range docs {
		files[i] = d.name
	}
	spec, merr := merge(files, specs)
	if len(merr) > 0 {
		return nil, merr
	}
	spec.ApplyDefaults()
	if !o.SkipValidate {
		if verr := ValidateWith(spec, o.Extensions); len(verr) > 0 {
			if len(files) == 1 {
				for _, e := range verr {
					if e.File == "" {
						e.File = files[0]
					}
				}
			}
			return nil, verr
		}
	}
	return &Loaded{Spec: spec, Files: files, SecretRefs: refs, SecretValues: values}, nil
}

// secretsBlock reads the top-level secrets: mapping (name → ref).
func secretsBlock(file string, root *yaml.Node, errs *Errors) map[string]string {
	out := map[string]string{}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "secrets" {
			continue
		}
		m := root.Content[i+1]
		if m.Kind != yaml.MappingNode {
			*errs = append(*errs, &Error{File: file, Line: m.Line, Path: "secrets", Msg: "must map names to refs, such as WEBHOOK_SECRET: env:WEBHOOK_SECRET"})
			return nil
		}
		for j := 0; j+1 < len(m.Content); j += 2 {
			k, v := m.Content[j], m.Content[j+1]
			if v.Kind != yaml.ScalarNode {
				*errs = append(*errs, &Error{File: file, Line: v.Line, Path: "secrets." + k.Value, Msg: "a secret ref is a string, such as env:NAME"})
				continue
			}
			out[k.Value] = v.Value
		}
	}
	return out
}

var unknownFieldRe = regexp.MustCompile(`^line (\d+): field (\S+) not found in type config\.(\w+)$`)

// strictKeys decodes data with unknown keys forbidden and returns only the
// unknown-key problems: type problems are reported after interpolation, when
// a {{env.X}} in a number field has its value.
func strictKeys(file string, data []byte) Errors {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var s Spec
	err := dec.Decode(&s)
	var te *yaml.TypeError
	if err == nil || !errors.As(err, &te) {
		return nil
	}
	var out Errors
	for _, msg := range te.Errors {
		m := unknownFieldRe.FindStringSubmatch(msg)
		if m == nil {
			continue
		}
		line, _ := strconv.Atoi(m[1])
		out = append(out, &Error{File: file, Line: line, Msg: fmt.Sprintf("unknown key %q in %s", m[2], typeLabel(m[3]))})
	}
	return out
}

var lineRe = regexp.MustCompile(`^line (\d+): (.*)$`)

// typeErrors turns a decode error into per-line errors.
func typeErrors(file string, err error) Errors {
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return Errors{{File: file, Msg: yamlMsg(err)}}
	}
	var out Errors
	for _, msg := range te.Errors {
		if m := lineRe.FindStringSubmatch(msg); m != nil {
			line, _ := strconv.Atoi(m[1])
			inner := strings.TrimPrefix(m[2], "line "+m[1]+": ")
			out = append(out, &Error{File: file, Line: line, Msg: inner})
			continue
		}
		out = append(out, &Error{File: file, Msg: msg})
	}
	return out
}

func yamlMsg(err error) string { return strings.TrimPrefix(err.Error(), "yaml: ") }

// typeLabel names a config struct for an error message.
func typeLabel(t string) string {
	labels := map[string]string{
		"Spec": "the spec", "Source": "a source", "Destination": "a destination", "Member": "a group member",
		"Route": "a route", "RouterSpec": "a router", "Store": "a store", "DeadLetter": "dead_letter",
		"Policy": "a policy", "Retry": "retry", "CircuitBreaker": "circuit_breaker", "RetryBudget": "retry_budget",
		"Genkit": "genkit", "SystemOne": "genkit.systemone", "Models": "models", "Server": "server", "Admin": "server.admin",
		"Observability": "observability", "Buffer": "buffer", "History": "router.history", "Cache": "router.cache",
		"Transform": "a transform", "Auth": "auth", "TLS": "tls", "Fault": "fault",
	}
	if l, ok := labels[t]; ok {
		return l
	}
	return strings.ToLower(t)
}

// merge combines files: lists are concatenated (names stay unique), secrets
// are unioned, and every other section may be set by one file only (or set
// identically).
func merge(files []string, specs []*Spec) (*Spec, Errors) {
	if len(specs) == 1 {
		return specs[0], nil
	}
	var errs Errors
	out := &Spec{}
	owner := map[string]string{}
	claim := func(section string, file string, set bool, apply func()) {
		if !set {
			return
		}
		if prev, ok := owner[section]; ok {
			errs = append(errs, &Error{File: file, Path: section, Msg: "also set in " + prev + "; set it in one file"})
			return
		}
		owner[section] = file
		apply()
	}
	names := map[string]string{}
	unique := func(kind, name, file string) bool {
		key := kind + "/" + name
		if prev, ok := names[key]; ok {
			errs = append(errs, &Error{File: file, Path: kind + " " + strconv.Quote(name), Msg: "already declared in " + prev})
			return false
		}
		names[key] = file
		return true
	}
	for i, s := range specs {
		f := files[i]
		if s.Version != 0 {
			if out.Version != 0 && out.Version != s.Version {
				errs = append(errs, &Error{File: f, Path: "version", Msg: fmt.Sprintf("%d differs from %d in %s", s.Version, out.Version, owner["version"])})
			} else if out.Version == 0 {
				out.Version, owner["version"] = s.Version, f
			}
		}
		if s.Name != "" {
			if out.Name != "" && out.Name != s.Name {
				errs = append(errs, &Error{File: f, Path: "name", Msg: fmt.Sprintf("%q differs from %q in %s: one relay per process", s.Name, out.Name, owner["name"])})
			} else if out.Name == "" {
				out.Name, owner["name"] = s.Name, f
			}
		}
		for k, v := range s.Secrets {
			if out.Secrets == nil {
				out.Secrets = map[string]string{}
			}
			out.Secrets[k] = v
		}
		claim("genkit", f, !isZero(s.Genkit), func() { out.Genkit = s.Genkit })
		claim("models", f, !isZero(s.Models), func() { out.Models = s.Models })
		claim("defaults", f, !isZero(s.Defaults), func() { out.Defaults = s.Defaults })
		claim("dead_letter", f, s.DeadLetter != nil, func() { out.DeadLetter = s.DeadLetter })
		claim("server", f, !isZero(s.Server), func() { out.Server = s.Server })
		claim("observability", f, !isZero(s.Observability), func() { out.Observability = s.Observability })
		claim("buffer", f, !isZero(s.Buffer), func() { out.Buffer = s.Buffer })
		claim("egress", f, !isZero(s.Egress), func() { out.Egress = s.Egress })
		for _, x := range s.Sources {
			if unique("source", x.Name, f) {
				out.Sources = append(out.Sources, x)
			}
		}
		for _, x := range s.Destinations {
			if unique("destination", x.Name, f) {
				out.Destinations = append(out.Destinations, x)
			}
		}
		for _, x := range s.Stores {
			if unique("store", x.Name, f) {
				out.Stores = append(out.Stores, x)
			}
		}
		for _, x := range s.Routes {
			if unique("route", x.Name, f) {
				out.Routes = append(out.Routes, x)
			}
		}
		out.Notify = append(out.Notify, s.Notify...)
		for _, x := range s.Plugins {
			if unique("plugin", x.Name, f) {
				x.Command = commandFrom(f, x.Command)
				out.Plugins = append(out.Plugins, x)
			}
		}
	}
	return out, errs
}

// commandFrom makes a plugin's relative path absolute against the file
// that declares it: a merged spec has no one directory to be relative to.
// A bare name (looked up on PATH) is kept.
func commandFrom(file, command string) string {
	if !strings.ContainsRune(command, '/') && !strings.ContainsRune(command, filepath.Separator) || filepath.IsAbs(command) {
		return command
	}
	abs, err := filepath.Abs(filepath.Join(filepath.Dir(file), command))
	if err != nil {
		return command
	}
	return abs
}

func isZero(v any) bool { return reflect.ValueOf(v).IsZero() }

// Names lists the names of the spec's destinations.
func (s *Spec) DestinationNames() []string {
	out := make([]string, 0, len(s.Destinations))
	for _, d := range s.Destinations {
		out = append(out, d.Name)
	}
	return slices.Clip(out)
}
