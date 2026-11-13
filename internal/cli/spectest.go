package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// DefaultTestFile is the file hikyaku test reads when it gets none.
const DefaultTestFile = "hikyaku.test.yaml"

// SpecTests is a test file (hikyaku test): messages, and what the spec
// must do with each.
type SpecTests struct {
	// Decide answers for a route's router in every test, so no model is
	// called: route name → the destinations it picks.
	Decide map[string][]string `yaml:"decide,omitempty" json:"decide,omitempty"`
	// Enrich answers the calls of enrich transforms in every test, so no
	// request is made.
	Enrich []SpecEnrich `yaml:"enrich,omitempty" json:"enrich,omitempty"`
	Tests  []SpecTest   `yaml:"tests" json:"tests"`
}

// SpecEnrich is the answer to the calls of an enrich transform.
type SpecEnrich struct {
	// URL is the URL the step calls, after its template. A * stands for
	// any run of characters.
	URL string `yaml:"url" json:"url"`
	// Answer is the JSON the call returns.
	Answer any `yaml:"answer,omitempty" json:"answer,omitempty"`
	// Error makes the call fail with this text.
	Error string `yaml:"error,omitempty" json:"error,omitempty"`
}

// SpecTest is one message and what is expected of it.
type SpecTest struct {
	Name   string `yaml:"name" json:"name"`
	Source string `yaml:"source" json:"source"`
	// Body is the message: a string is sent as it is, anything else as JSON.
	Body any `yaml:"body,omitempty" json:"body,omitempty"`
	// File reads the body from a file, relative to the test file.
	File        string            `yaml:"file,omitempty" json:"file,omitempty"`
	ID          string            `yaml:"id,omitempty" json:"id,omitempty"`
	Subject     string            `yaml:"subject,omitempty" json:"subject,omitempty"`
	ContentType string            `yaml:"content_type,omitempty" json:"content_type,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Attributes  map[string]any    `yaml:"attributes,omitempty" json:"attributes,omitempty"`
	// Down names destinations, group members or dead_letter: every
	// delivery to one fails, as if it could not be reached.
	Down []string `yaml:"down,omitempty" json:"down,omitempty"`
	// Decide adds to, or replaces, the file's decide for this test.
	Decide map[string][]string `yaml:"decide,omitempty" json:"decide,omitempty"`
	// Enrich comes before the file's enrich for this test.
	Enrich []SpecEnrich `yaml:"enrich,omitempty" json:"enrich,omitempty"`
	Expect SpecExpect   `yaml:"expect" json:"expect"`
}

// SpecExpect is what a test checks. A key left out is not checked.
type SpecExpect struct {
	// Status is the message's outcome: delivered, filtered, unrouted,
	// dead_lettered or failed.
	Status string `yaml:"status,omitempty" json:"status,omitempty"`
	// To is every destination the routes picked, in any order. Shadow
	// destinations are in Shadowed.
	To       *[]string `yaml:"to,omitempty" json:"to,omitempty"`
	NotTo    []string  `yaml:"not_to,omitempty" json:"not_to,omitempty"`
	Shadowed *[]string `yaml:"shadowed,omitempty" json:"shadowed,omitempty"`
	// Failed is every destination whose delivery failed, in any order.
	Failed *[]string `yaml:"failed,omitempty" json:"failed,omitempty"`
	// Delivered checks the message a destination got.
	Delivered map[string]SpecGot `yaml:"delivered,omitempty" json:"delivered,omitempty"`
	// Routes checks single routes.
	Routes map[string]SpecRoute `yaml:"routes,omitempty" json:"routes,omitempty"`
	// FilterErrors allows a filter that cannot be evaluated for the
	// message (the relay keeps such a message). Without it, the test fails.
	FilterErrors bool `yaml:"filter_errors,omitempty" json:"filter_errors,omitempty"`
}

// SpecGot is what a destination must have received.
type SpecGot struct {
	// Via is the destination that took the message: the destination
	// itself, or one of its fallbacks.
	Via string `yaml:"via,omitempty" json:"via,omitempty"`
	// Body is the whole body: a string as text, anything else as JSON.
	Body any `yaml:"body,omitempty" json:"body,omitempty"`
	// Contains is a part of the body: a string must be in it, anything
	// else must be a subset of its JSON.
	Contains    any               `yaml:"contains,omitempty" json:"contains,omitempty"`
	ContentType string            `yaml:"content_type,omitempty" json:"content_type,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Attributes  map[string]any    `yaml:"attributes,omitempty" json:"attributes,omitempty"`
}

// SpecRoute is what one route must have done.
type SpecRoute struct {
	Filtered *bool     `yaml:"filtered,omitempty" json:"filtered,omitempty"`
	To       *[]string `yaml:"to,omitempty" json:"to,omitempty"`
	// Router is the router that decided (ai, cel, static, validate, ...).
	Router string `yaml:"router,omitempty" json:"router,omitempty"`
	// Error is a part of the route's error.
	Error string `yaml:"error,omitempty" json:"error,omitempty"`
}

// SpecResult is the outcome of one test.
type SpecResult struct {
	File     string         `json:"file"`
	Name     string         `json:"name"`
	Passed   bool           `json:"passed"`
	Failures []string       `json:"failures,omitempty"`
	Duration time.Duration  `json:"duration_ns"`
	Report   *engine.Report `json:"report,omitempty"`
}

// SpecReport is hikyaku test --json.
type SpecReport struct {
	Tests   int          `json:"tests"`
	Failed  int          `json:"failed"`
	Results []SpecResult `json:"results"`
}

// standIn is the sink of a destination under test: it keeps what it gets,
// or fails every delivery when the test says it is down.
type standIn struct {
	name string
	down bool

	mu  sync.Mutex
	got []*envelope.Envelope
}

var errDown = errors.New("down (hikyaku test)")

func (s *standIn) Name() string { return s.name }

func (s *standIn) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if s.down {
		return nil, plugin.Wrap(plugin.Transient, []string{"connect"}, fmt.Errorf("%s: %w", s.name, errDown))
	}
	s.mu.Lock()
	s.got = append(s.got, env.Clone())
	s.mu.Unlock()
	return &plugin.Result{Status: 200, Endpoint: s.name}, nil
}

func (s *standIn) Health(context.Context) error {
	if s.down {
		return errDown
	}
	return nil
}

func (s *standIn) Close(context.Context) error { return nil }

// standIns are the stand-in sinks of one test, by destination.
type standIns struct {
	down map[string]bool

	mu    sync.Mutex
	sinks map[string][]*standIn
}

func (s *standIns) sink(dest string, spec plugin.Spec) plugin.Sink {
	out := &standIn{name: spec.Name, down: s.down[dest] || s.down[spec.Name]}
	s.mu.Lock()
	s.sinks[dest] = append(s.sinks[dest], out)
	s.mu.Unlock()
	return out
}

// last is the newest message a destination got, on any of its endpoints.
func (s *standIns) last(dest string) *envelope.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out *envelope.Envelope
	for _, sk := range s.sinks[dest] {
		sk.mu.Lock()
		if n := len(sk.got); n > 0 {
			out = sk.got[n-1]
		}
		sk.mu.Unlock()
	}
	return out
}

// enrichStubs answers the enrich calls of one test from its enrich list.
type enrichStubs struct {
	stubs []SpecEnrich
	match []*regexp.Regexp

	mu      sync.Mutex
	missing []string // calls no entry answers
}

func newEnrichStubs(f *SpecTests, t SpecTest) (*enrichStubs, error) {
	s := &enrichStubs{stubs: append(slices.Clone(t.Enrich), f.Enrich...)}
	for i, st := range s.stubs {
		if st.URL == "" {
			return nil, fmt.Errorf("enrich[%d]: url is required", i)
		}
		if st.Error != "" && st.Answer != nil {
			return nil, fmt.Errorf("enrich[%d] (%s): set answer or error, not both", i, st.URL)
		}
		parts := strings.Split(st.URL, "*")
		for j := range parts {
			parts[j] = regexp.QuoteMeta(parts[j])
		}
		s.match = append(s.match, regexp.MustCompile("^"+strings.Join(parts, ".*")+"$"))
	}
	return s, nil
}

// call answers one enrich call. The first entry whose url matches wins; a
// call nothing answers fails, and fails the test.
func (s *enrichStubs) call(c transform.EnrichCall) (any, error) {
	for i, re := range s.match {
		if !re.MatchString(c.URL) {
			continue
		}
		if st := s.stubs[i]; st.Error != "" {
			return nil, fmt.Errorf("%s %s: %s", c.Method, c.URL, st.Error)
		}
		return plainJSON(s.stubs[i].Answer)
	}
	call := c.Method + " " + c.URL
	s.mu.Lock()
	if !slices.Contains(s.missing, call) {
		s.missing = append(s.missing, call)
	}
	s.mu.Unlock()
	return nil, fmt.Errorf("%s: the test has no answer for this call", call)
}

// readSpecTests reads one test file, strictly: an unknown key is an error.
func readSpecTests(path string) (*SpecTests, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f SpecTests
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(f.Tests) == 0 {
		return nil, fmt.Errorf("%s: no tests (tests: [{name, source, body, expect}])", path)
	}
	names := map[string]bool{}
	for i, t := range f.Tests {
		switch {
		case t.Name == "":
			return nil, fmt.Errorf("%s: tests[%d]: name is required", path, i)
		case names[t.Name]:
			return nil, fmt.Errorf("%s: two tests are named %q", path, t.Name)
		case t.Source == "":
			return nil, fmt.Errorf("%s: test %q: source is required", path, t.Name)
		case t.Body != nil && t.File != "":
			return nil, fmt.Errorf("%s: test %q: set body or file, not both", path, t.Name)
		}
		names[t.Name] = true
	}
	return &f, nil
}

// testFiles expands the arguments of hikyaku test: a directory stands for
// its *.test.yaml and *.test.yml files.
func testFiles(args []string) ([]string, error) {
	if len(args) == 0 {
		args = []string{DefaultTestFile}
	}
	var out []string
	for _, a := range args {
		st, err := os.Stat(a)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			out = append(out, a)
			continue
		}
		var found []string
		for _, pat := range []string{"*.test.yaml", "*.test.yml"} {
			m, _ := filepath.Glob(filepath.Join(a, pat))
			found = append(found, m...)
		}
		if len(found) == 0 {
			return nil, fmt.Errorf("%s: no *.test.yaml file", a)
		}
		sort.Strings(found)
		out = append(out, found...)
	}
	return out, nil
}

// bodyBytes turns a body of a test file into bytes: a string as it is,
// anything else as JSON.
func bodyBytes(v any) ([]byte, error) {
	switch b := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []byte(b), nil
	}
	return json.Marshal(v)
}

func (t SpecTest) envelope(file string, n int) (*envelope.Envelope, error) {
	var body []byte
	var err error
	if t.File != "" {
		if body, err = os.ReadFile(filepath.Join(filepath.Dir(file), t.File)); err != nil {
			return nil, err
		}
		body = trimNL(body)
	} else if body, err = bodyBytes(t.Body); err != nil {
		return nil, fmt.Errorf("body: %w", err)
	}
	env := envelope.New(t.Source, "test", body)
	env.ID = t.ID
	if env.ID == "" {
		// A fixed ID keeps a sampled shadow the same from run to run.
		env.ID = fmt.Sprintf("test-%d", n+1)
	}
	env.Subject = t.Subject
	switch {
	case t.ContentType != "":
		env.ContentType = t.ContentType
	case jsonValid(body):
		env.ContentType = "application/json"
	default:
		env.ContentType = "text/plain; charset=utf-8"
	}
	for k, v := range t.Headers {
		env.SetHeader(k, v)
	}
	for k, v := range t.Attributes {
		env.SetAttribute(k, v)
	}
	return env, nil
}

// decided is the router of a route a test answers for.
func decided(dests []string) plugin.Router {
	return plugin.RouterFunc(func(context.Context, *envelope.Envelope) (plugin.Decision, error) {
		return plugin.Decision{Destinations: slices.Clone(dests), Router: "test"}, nil
	})
}

// decisions merges the file's decide with the test's.
func decisions(f *SpecTests, t SpecTest) map[string][]string {
	out := map[string][]string{}
	for k, v := range f.Decide {
		out[k] = v
	}
	for k, v := range t.Decide {
		out[k] = v
	}
	return out
}

// modelRoutes are the AI routes a test leaves to their model: the ones that
// read its source and that it does not answer for.
func modelRoutes(spec *config.Spec, source string, decide map[string][]string) []string {
	var out []string
	for _, r := range spec.Routes {
		if _, ai := r.AIRouter(); !ai || !slices.Contains(r.From, source) {
			continue
		}
		if _, ok := decide[r.Name]; !ok {
			out = append(out, r.Name)
		}
	}
	return out
}

// runSpecTest runs one test on an engine of its own, whose every sink is a
// stand-in: nothing is connected to, nothing is written.
func (a *app) runSpecTest(ctx context.Context, l *config.Loaded, ai router.AIFactory, file string, f *SpecTests, n int) SpecResult {
	t := f.Tests[n]
	res := SpecResult{File: file, Name: t.Name}
	start := time.Now()
	fail := func(format string, args ...any) SpecResult {
		res.Failures = append(res.Failures, fmt.Sprintf(format, args...))
		res.Duration = time.Since(start)
		return res
	}
	spec := l.Spec
	if _, ok := spec.SourceByName(t.Source); !ok {
		return fail("source %q is not declared", t.Source)
	}
	known := map[string]bool{engine.DeadLetterName: true}
	for _, d := range spec.Destinations {
		known[d.Name] = true
		for _, m := range d.Members {
			known[m.Name] = true
		}
	}
	sinks := &standIns{down: map[string]bool{}, sinks: map[string][]*standIn{}}
	for _, d := range t.Down {
		if !known[d] {
			return fail("down: %q is not a destination, a group member or dead_letter", d)
		}
		sinks.down[d] = true
	}
	routers := map[string]plugin.Router{}
	for route, dests := range decisions(f, t) {
		if _, ok := spec.RouteByName(route); !ok {
			return fail("decide: route %q is not declared", route)
		}
		for _, d := range dests {
			if _, ok := spec.DestinationByName(d); !ok {
				return fail("decide.%s: destination %q is not declared", route, d)
			}
		}
		routers[route] = decided(dests)
	}
	// An AI route the message cannot reach never decides: it gets no model.
	for _, r := range spec.Routes {
		if _, ai := r.AIRouter(); ai && routers[r.Name] == nil && !slices.Contains(r.From, t.Source) {
			routers[r.Name] = decided(nil)
		}
	}
	env, err := t.envelope(file, n)
	if err != nil {
		return fail("%v", err)
	}
	// Stores are in memory, whatever the spec says.
	stores := map[string]store.Store{}
	for _, s := range spec.Stores {
		stores[s.Name] = store.NewMemory(s.Name)
	}
	enrich, err := newEnrichStubs(f, t)
	if err != nil {
		return fail("%v", err)
	}
	e, err := engine.New(ctx, spec, engine.Options{
		Logger: a.quietLogger(), Metrics: metrics.New(), AI: ai, Stores: stores, Routers: routers,
		NoSources: true, NoWait: true, Sinks: sinks.sink, Enrich: enrich.call, Now: a.deps.Now,
	})
	if err != nil {
		return fail("the relay does not build: %v", err)
	}
	rep := e.Inject(ctx, env)
	// Close waits for the shadow copies.
	e.Close(context.Background())
	for _, s := range stores {
		_ = s.Close()
	}
	res.Report = rep
	res.Failures = t.Expect.check(spec, rep, sinks)
	for _, call := range enrich.missing {
		res.Failures = append(res.Failures, fmt.Sprintf("enrich: %s has no answer in the test file (add its url under enrich)", call))
	}
	res.Passed = len(res.Failures) == 0
	res.Duration = time.Since(start)
	return res
}

func sortedSet(xs []string) []string {
	out := uniq(slices.Clone(xs))
	sort.Strings(out)
	return out
}

func sameSet(a, b []string) bool { return slices.Equal(sortedSet(a), sortedSet(b)) }

// check compares what happened with what the test expects.
func (x SpecExpect) check(spec *config.Spec, rep *engine.Report, sinks *standIns) []string {
	var out []string
	bad := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	var picked, shadowed, failed []string
	deliveries := map[string]engine.DeliveryReport{}
	routes := map[string]engine.RouteReport{}
	for _, r := range rep.Routes {
		routes[r.Route] = r
		shadowed = append(shadowed, r.Shadowed...)
		if r.Decision != nil {
			for _, d := range r.Decision.Destinations {
				if ds, ok := spec.DestinationByName(d); ok && ds.Shadow {
					continue
				}
				picked = append(picked, d)
			}
		}
		for _, d := range r.Deliveries {
			deliveries[d.Destination] = d
			if !d.OK() {
				failed = append(failed, d.Destination)
			}
		}
	}

	if !x.FilterErrors {
		const kept = "a filter could not be evaluated, so the message was kept: %s (filter_errors: true allows it)"
		for _, r := range rep.Routes {
			if r.FilterError != "" {
				bad("route %s: "+kept, r.Route, r.FilterError)
			}
			for _, d := range r.Deliveries {
				if d.FilterError != "" {
					bad("destination %s: "+kept, d.Destination, d.FilterError)
				}
			}
		}
	}
	if x.Status != "" && x.Status != rep.Status {
		msg := fmt.Sprintf("status: want %s, got %s", x.Status, rep.Status)
		if rep.Error != "" {
			msg += " (" + rep.Error + ")"
		}
		bad("%s", msg)
	}
	if x.To != nil && !sameSet(*x.To, picked) {
		bad("to: want %v, got %v", sortedSet(*x.To), sortedSet(picked))
	}
	for _, d := range x.NotTo {
		if slices.Contains(picked, d) || slices.Contains(shadowed, d) {
			bad("not_to: %s was picked", d)
		}
	}
	if x.Shadowed != nil && !sameSet(*x.Shadowed, shadowed) {
		bad("shadowed: want %v, got %v", sortedSet(*x.Shadowed), sortedSet(shadowed))
	}
	if x.Failed != nil && !sameSet(*x.Failed, failed) {
		bad("failed: want %v, got %v", sortedSet(*x.Failed), sortedSet(failed))
	}
	for _, name := range sortedKeys(x.Routes) {
		want, got := x.Routes[name], routes[name]
		p := "routes." + name
		if _, ok := routes[name]; !ok {
			bad("%s: the route did not run (it does not read this source)", p)
			continue
		}
		if want.Filtered != nil && *want.Filtered != got.Filtered {
			bad("%s.filtered: want %t, got %t", p, *want.Filtered, got.Filtered)
		}
		var to []string
		router := ""
		if got.Decision != nil {
			to, router = got.Decision.Destinations, got.Decision.Router
		}
		if want.To != nil && !sameSet(*want.To, to) {
			bad("%s.to: want %v, got %v", p, sortedSet(*want.To), sortedSet(to))
		}
		if want.Router != "" && want.Router != router {
			bad("%s.router: want %s, got %s", p, want.Router, orDash(router))
		}
		if want.Error != "" && !strings.Contains(got.Error, want.Error) {
			bad("%s.error: want %q in it, got %q", p, want.Error, got.Error)
		}
	}
	for _, name := range sortedKeys(x.Delivered) {
		want := x.Delivered[name]
		p := "delivered." + name
		served := name
		d, routed := deliveries[name]
		switch {
		case routed && d.Filtered:
			bad("%s: the destination's transforms filtered the message out", p)
			continue
		case routed && !d.OK():
			bad("%s: not delivered: %s", p, d.Error)
			continue
		case routed:
			if d.ServedBy != "" {
				served = d.ServedBy
			}
		case !slices.Contains(shadowed, name):
			bad("%s: no route sent the message there", p)
			continue
		}
		if want.Via != "" && want.Via != served {
			bad("%s.via: want %s, got %s", p, want.Via, served)
		}
		got := sinks.last(served)
		if got == nil {
			bad("%s: nothing reached it", p)
			continue
		}
		out = append(out, want.check(p, got)...)
	}
	return out
}

// check compares one delivered message with what the test expects of it.
func (g SpecGot) check(p string, env *envelope.Envelope) []string {
	var out []string
	bad := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	if g.Body != nil {
		if want, ok := g.Body.(string); ok {
			if want != env.BodyText() {
				bad("%s.body: want %q, got %q", p, want, truncate(env.BodyText(), 400))
			}
		} else if want, got, err := jsonPair(g.Body, env.Body); err != nil {
			bad("%s.body: %v", p, err)
		} else if !reflect.DeepEqual(want, got) {
			bad("%s.body: want %s, got %s", p, compact(want), truncate(compact(got), 400))
		}
	}
	if g.Contains != nil {
		if want, ok := g.Contains.(string); ok {
			if !strings.Contains(env.BodyText(), want) {
				bad("%s.contains: %q is not in %q", p, want, truncate(env.BodyText(), 400))
			}
		} else if want, got, err := jsonPair(g.Contains, env.Body); err != nil {
			bad("%s.contains: %v", p, err)
		} else if path, ok := subset(want, got, ""); !ok {
			bad("%s.contains: %s differs: want %s in %s", p, orDash(path), compact(want), truncate(compact(got), 400))
		}
	}
	if g.ContentType != "" && g.ContentType != env.ContentType {
		bad("%s.content_type: want %s, got %s", p, g.ContentType, orDash(env.ContentType))
	}
	for _, k := range sortedKeys(g.Headers) {
		if got := env.Header(k); got != g.Headers[k] {
			bad("%s.headers.%s: want %q, got %q", p, k, g.Headers[k], got)
		}
	}
	for _, k := range sortedKeys(g.Attributes) {
		v, _ := env.Attribute(k)
		want, got, err := jsonValues(g.Attributes[k], v)
		if err != nil {
			bad("%s.attributes.%s: %v", p, k, err)
		} else if !reflect.DeepEqual(want, got) {
			bad("%s.attributes.%s: want %s, got %s", p, k, compact(want), compact(got))
		}
	}
	return out
}

// jsonPair returns an expected value and a body as plain JSON values.
func jsonPair(want any, body []byte) (any, any, error) {
	var got any
	if err := json.Unmarshal(body, &got); err != nil {
		return nil, nil, fmt.Errorf("the body is not JSON: %q", truncate(string(body), 200))
	}
	w, err := plainJSON(want)
	return w, got, err
}

func jsonValues(want, got any) (any, any, error) {
	w, err := plainJSON(want)
	if err != nil {
		return nil, nil, err
	}
	g, err := plainJSON(got)
	return w, g, err
}

// plainJSON is v after a trip through JSON, so 1 and 1.0 compare equal.
func plainJSON(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	err = json.Unmarshal(b, &out)
	return out, err
}

func compact(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// subset reports whether want is part of got: every key of an object must
// be there with a matching value, a list must match item by item, anything
// else must be equal. It returns where the two first differ.
func subset(want, got any, path string) (string, bool) {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return path, false
		}
		for _, k := range sortedKeys(w) {
			v, ok := g[k]
			if !ok {
				return strings.TrimPrefix(path+"."+k, "."), false
			}
			if at, ok := subset(w[k], v, path+"."+k); !ok {
				return strings.TrimPrefix(at, "."), false
			}
		}
		return "", true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return path, false
		}
		for i := range w {
			if at, ok := subset(w[i], g[i], fmt.Sprintf("%s[%d]", path, i)); !ok {
				return strings.TrimPrefix(at, "."), false
			}
		}
		return "", true
	}
	return path, reflect.DeepEqual(want, got)
}

func (a *app) newTestCmd() *cobra.Command {
	var runExpr string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "test [file or directory ...]",
		Short: "Check that the spec routes and transforms test messages as expected",
		Long: "Run the tests of a test file against the spec. A test is a message from a source,\n" +
			"and what the spec must do with it: the destinations it goes to, the body each one\n" +
			"gets, the fallback that takes over when a destination is down.\n\n" +
			"Every destination is a stand-in, so nothing is connected to and nothing is sent.\n" +
			"Filters, transforms, routers, retries and fallbacks are the real ones, without\n" +
			"their waits. An AI route calls its model unless the test answers for it with\n" +
			"decide. No secret is read unless a model is called.\n\n" +
			"Without an argument it reads " + DefaultTestFile + ". A directory stands for its\n" +
			"*.test.yaml files. It exits 1 when a test fails.",
		Example: "  hikyaku test\n" +
			"  hikyaku test tests/ --run refund\n" +
			"  hikyaku test orders.test.yaml --json",
		RunE: func(cmd *cobra.Command, args []string) error {
			var only *regexp.Regexp
			if runExpr != "" {
				var err error
				if only, err = regexp.Compile(runExpr); err != nil {
					return usageError(fmt.Errorf("--run: %w", err))
				}
			}
			paths, err := testFiles(args)
			if err != nil {
				return usageError(err)
			}
			files := make([]*SpecTests, len(paths))
			for i, p := range paths {
				if files[i], err = readSpecTests(p); err != nil {
					return usageError(err)
				}
			}
			selected := func(t SpecTest) bool { return only == nil || only.MatchString(t.Name) }

			// The spec is loaded without its secrets, unless a test leaves
			// an AI route to its model.
			l, err := a.load(cmd.Context(), false)
			if err != nil {
				return err
			}
			model := false
			for _, f := range files {
				for _, t := range f.Tests {
					model = model || (selected(t) && len(modelRoutes(l.Spec, t.Source, decisions(f, t))) > 0)
				}
			}
			var ai router.AIFactory
			if model {
				if l, err = a.load(cmd.Context(), true); err != nil {
					return err
				}
				if ai, err = a.newAI(cmd.Context(), l, metrics.New()); err != nil {
					return err
				}
			}

			w := cmd.OutOrStdout()
			st := a.style(w)
			rep := SpecReport{Results: []SpecResult{}}
			for i, f := range files {
				for n, t := range f.Tests {
					if !selected(t) {
						continue
					}
					res := a.runSpecTest(cmd.Context(), l, ai, paths[i], f, n)
					rep.Tests++
					rep.Results = append(rep.Results, res)
					if !res.Passed {
						rep.Failed++
					}
					if asJSON {
						continue
					}
					took := st.Dim("(" + res.Duration.Round(time.Millisecond).String() + ")")
					if res.Passed {
						fprintf(w, "%s %s %s\n", st.OK("✓"), res.Name, took)
						continue
					}
					fprintf(w, "%s %s %s\n", st.Fail("✗"), res.Name, took)
					for _, msg := range res.Failures {
						fprintf(w, "    %s\n", msg)
					}
					if res.Report != nil {
						for _, r := range res.Report.Routes {
							a.printRoute(indent{w}, r)
						}
					}
				}
			}
			if asJSON {
				if err := printJSON(w, rep); err != nil {
					return err
				}
			} else {
				switch {
				case rep.Tests == 0:
					fprintf(w, "%s no test matches --run %q\n", st.Warn("!"), runExpr)
				case rep.Failed == 0:
					fprintf(w, "%s %d test(s) passed\n", st.OK("✓"), rep.Tests)
				default:
					fprintf(w, "%s %d of %d test(s) failed\n", st.Fail("✗"), rep.Failed, rep.Tests)
				}
			}
			if rep.Failed > 0 || rep.Tests == 0 {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&runExpr, "run", "", "run only the tests whose name matches this regular expression")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the results as JSON")
	return cmd
}

// indent writes every line four spaces in: the route of a failed test.
type indent struct{ w io.Writer }

func (i indent) Write(p []byte) (int, error) {
	for _, line := range bytes.SplitAfter(p, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if _, err := i.w.Write(append([]byte("    "), line...)); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}
