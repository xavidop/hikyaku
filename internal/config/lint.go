package config

import (
	"fmt"
	"strings"
)

// Lint returns the warnings of a valid spec: things that work but probably
// are not what you want in production.
func Lint(s *Spec) []Warning {
	var out []Warning
	warn := func(path, format string, args ...any) {
		out = append(out, Warning{Path: path, Msg: fmt.Sprintf(format, args...)})
	}
	seenModel := map[string]bool{}
	pinned := func(path, model string) {
		if seenModel[model] {
			return
		}
		seenModel[model] = true
		if strings.Contains(model, "latest") || strings.Contains(model, "/~") {
			warn(path, "decision model %q is not pinned: calibration changes between releases, so thresholds tuned on one version drift on the next; pin a version", model)
		}
	}
	if s.Models.Decision != "" {
		pinned("models.decision", s.Models.Decision)
	}
	for _, m := range s.Models.Fallback.Decision {
		pinned("models.fallback.decision", m)
	}
	for _, r := range s.Routes {
		p := fmt.Sprintf("route %q", r.Name)
		var walk func(path string, rs *RouterSpec, top bool)
		walk = func(path string, rs *RouterSpec, top bool) {
			if rs == nil {
				return
			}
			if rs.Type == "ai" {
				if rs.Engine != "generative" {
					for _, m := range s.ModelChain(*rs) {
						pinned(path+".model", m)
					}
				}
				if top && len(rs.Fallback) == 0 {
					warn(path+".fallback", "an AI router without a router fallback: when every model fails, messages go to the DLQ; add rules or static fallbacks")
				}
				if rs.MinMargin > 0 && rs.Multi {
					warn(path+".min_margin", "is ignored with multi: true: it applies to a single choice")
				}
				if rs.MinConfidence == 0 {
					warn(path+".min_confidence", "is 0: every answer passes, so on_low_confidence never applies")
				}
				if len(rs.OnLowConfidence) == 0 && !rs.FallbackOnLowConfidence && rs.MinConfidence > 0 {
					warn(path+".on_low_confidence", "unset: unsure answers go to the router fallback")
				}
				if len(s.ModelChain(*rs)) < 2 {
					warn(path+".fallback_models", "a single model: when it is down the router fallback serves every message; add a fallback model (such as a local ollama-decisions/clef)")
				}
			}
			for i := range rs.Fallback {
				walk(fmt.Sprintf("%s.fallback[%d]", path, i), &rs.Fallback[i], false)
			}
		}
		walk(p+".router", r.Router, true)
	}
	for _, d := range s.Destinations {
		p := fmt.Sprintf("destination %q", d.Name)
		if d.IsRequired() && len(d.Fallback) == 0 && d.Type != "group" && d.Type != "file" && d.Type != "stdout" && d.Type != "stderr" {
			warn(p+".fallback", "required and without a fallback: when it fails, its messages go to the DLQ")
		}
		if (d.DeliverAfter > 0 || d.DeliverAt != "") && s.Buffer.Type != "disk" {
			warn(p+".deliver_after", "held messages live in memory with buffer.type: memory: a restart or a reload nacks them, and a source that cannot send again loses them; use buffer.type: disk")
		}
		if d.Type == "group" && d.Strategy != "failover" && d.Strategy != "" && d.HealthCheck == nil {
			warn(p+".health_check", "a %s group without health checks keeps sending to dead members until their breakers open", d.Strategy)
		}
	}
	if s.DeadLetter != nil && (s.DeadLetter.Type == "stderr" || s.DeadLetter.Type == "stdout") && len(s.DeadLetter.Fallback) == 0 {
		warn("dead_letter", "dead letters go to %s: set a file (or broker) DLQ to replay them with hikyaku dlq replay", s.DeadLetter.Type)
	}
	if s.Server.Admin.Token == "" && !s.Server.Admin.Disabled {
		warn("server.admin.token", "unset: /reload, /debug, /v1/tail, /v1/feedback and /v1/history answer loopback clients only")
	}
	for i, src := range s.Sources {
		if src.Mode == "listen" && src.Auth == nil {
			warn(fmt.Sprintf("source %q.auth", s.Sources[i].Name), "a listen source without auth accepts any caller")
		}
	}
	if s.Buffer.Type != "disk" {
		for _, src := range s.Sources {
			if src.Ack == "accepted" {
				warn(fmt.Sprintf("source %q.ack", src.Name), "accepted with a memory buffer: a crash loses queued messages the sender was told were accepted; use buffer.type: disk")
			}
		}
	}
	for _, r := range s.Routes {
		if r.Router == nil {
			continue
		}
		// The chain runs depth-first: a router, then its fallbacks. Its last
		// router has nothing after it for the messages its when leaves out.
		var last *RouterSpec
		var walk func(rs *RouterSpec)
		walk = func(rs *RouterSpec) {
			last = rs
			for i := range rs.Fallback {
				walk(&rs.Fallback[i])
			}
		}
		walk(r.Router)
		if last.When != "" {
			warn(fmt.Sprintf("route %q.router", r.Name), "the last router of the chain has a when condition: the messages it leaves out go to the DLQ (reason no_route); end the chain with a router without one (such as static)")
		}
	}
	ordered := map[string]bool{}
	for _, src := range s.Sources {
		if src.Ordering != nil {
			ordered[src.Name] = true
		}
	}
	batched := map[string]bool{}
	for _, d := range s.Destinations {
		if d.Batch != nil {
			batched[d.Name] = true
		}
	}
	for _, d := range s.Destinations {
		for _, f := range d.Fallback {
			if !batched[f] {
				continue
			}
			how := "one message at a time"
			if d.Batch != nil {
				how = "in " + d.Name + "'s batches"
			}
			warn(fmt.Sprintf("destination %q.fallback", d.Name), "%s batches, but as a fallback of %s it delivers %s's messages %s: its own batch applies only to messages routed to it", f, d.Name, d.Name, how)
		}
	}
	for _, r := range s.Routes {
		for _, from := range r.From {
			if !ordered[from] {
				continue
			}
			for _, to := range r.To {
				if batched[to] {
					warn(fmt.Sprintf("route %q", r.Name), "source %s is ordered and destination %s batches: each key waits for a batch per message, so batches stay small and slow", from, to)
				}
			}
		}
	}
	return out
}
