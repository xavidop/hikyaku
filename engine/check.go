package engine

import (
	"fmt"
	"strings"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/redact"
	"github.com/xavidop/hikyaku/transform"
)

// Check compiles every expression of a spec (CEL filters and rules, jq
// programs, per-message templates, JSONPaths) and returns what does not
// compile. hikyaku validate runs it after config's structural checks.
func Check(spec *config.Spec) config.Errors {
	var errs config.Errors
	tmpl := func(path, text string) {
		if text == "" || !strings.Contains(text, "{{") {
			return
		}
		if _, err := transform.CompileTemplate(path, text); err != nil {
			errs.Add(path, "%v", err)
		}
	}
	cel := func(path, src string) {
		if src == "" {
			return
		}
		if _, err := transform.CompileCEL(src); err != nil {
			errs.Add(path, "%v", err)
		}
	}
	chain := func(path string, ts []config.Transform) {
		if _, err := transform.Compile(path, ts); err != nil {
			errs.Add(path, "%v", err)
		}
	}
	endpoint := func(p string, ep config.Endpoint) {
		tmpl(p+".url", ep.URL)
		tmpl(p+".body", ep.Body)
		tmpl(p+".event", ep.Event)
		for k, v := range ep.Headers {
			tmpl(p+".headers."+k, v)
		}
	}
	for _, s := range spec.Sources {
		p := fmt.Sprintf("source %q", s.Name)
		if s.Dedupe != nil {
			tmpl(p+".dedupe.key", s.Dedupe.Key)
		}
		// s3 and gcs also take "lines": one message per line of an object.
		if s.Split != "" && (s.Split != "lines" || (s.Type != "s3" && s.Type != "gcs")) {
			if _, err := transform.CompileJQ(s.Split); err != nil {
				errs.Add(p+".split", "%v", err)
			}
		}
	}
	for _, d := range spec.Destinations {
		p := fmt.Sprintf("destination %q", d.Name)
		chain(p+".transforms", d.Transforms)
		endpoint(p, d.Endpoint)
		for i, m := range d.Members {
			endpoint(fmt.Sprintf("%s.members[%d]", p, i), m.Endpoint)
		}
	}
	var walk func(p string, rs *config.RouterSpec)
	walk = func(p string, rs *config.RouterSpec) {
		if rs == nil {
			return
		}
		if rs.When != "" {
			cel(p+".when", rs.When)
		}
		for i, r := range rs.Rules {
			cel(fmt.Sprintf("%s.rules[%d].when", p, i), r.When)
		}
		tmpl(p+".state", rs.State)
		if rs.Cache != nil {
			tmpl(p+".cache.key", rs.Cache.Key)
		}
		if h := rs.History; h != nil {
			tmpl(p+".history.key", h.Key)
			tmpl(p+".history.record.summary", h.Record.Summary)
		}
		if rs.Input != nil {
			if _, err := redact.CompilePaths(rs.Input.Redact); err != nil {
				errs.Add(p+".input.redact", "%v", err)
			}
		}
		for i := range rs.Fallback {
			walk(fmt.Sprintf("%s.fallback[%d]", p, i), &rs.Fallback[i])
		}
	}
	for _, r := range spec.Routes {
		p := fmt.Sprintf("route %q", r.Name)
		cel(p+".filter", r.Filter)
		chain(p+".transforms", r.Transforms)
		walk(p+".router", r.Router)
		if rs := r.Router; rs != nil && rs.Shadow && len(rs.Fallback) == 0 {
			errs.Add(p+".router.shadow", "shadow mode delivers through router.fallback: add one")
		}
	}
	return errs
}
