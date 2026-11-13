package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func (a *app) newDoctorCmd() *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check the spec, secrets, plugins, destinations, stores and models",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			w := cmd.OutOrStdout()
			st := a.style(w)
			failed := 0
			check := func(ok bool, what, detail string) {
				mark := st.OK("✓")
				if !ok {
					mark, failed = st.Fail("✗"), failed+1
				}
				if detail != "" {
					detail = " " + st.Dim(detail)
				}
				fprintf(w, "%s %s%s\n", mark, what, detail)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout*4)
			defer cancel()
			l, err := a.load(ctx, true)
			if err != nil {
				check(false, "spec", a.redactor.Redact(err.Error()))
				return silentExit(ExitFailed)
			}
			s := l.Spec
			check(true, "spec", fmt.Sprintf("%s: %d sources, %d destinations, %d routes", strings.Join(l.Files, ", "), len(s.Sources), len(s.Destinations), len(s.Routes)))
			check(true, "secrets", fmt.Sprintf("%d resolved", len(l.SecretRefs)))
			var missing []string
			for _, src := range s.Sources {
				if _, ok := plugin.LookupSource(src.Type); !ok {
					missing = append(missing, "source "+src.Name+" ("+src.Type+")")
				}
			}
			for _, d := range s.Destinations {
				types := []string{d.Type}
				if d.Type == "group" {
					types = nil
					for _, m := range d.Members {
						types = append(types, m.Type)
					}
				}
				for _, t := range types {
					if t == "model" {
						continue // built by the AI factory; checked with the models below
					}
					if _, ok := plugin.LookupSink(t); !ok {
						missing = append(missing, "destination "+d.Name+" ("+t+milestone(t)+")")
					}
				}
			}
			check(len(missing) == 0, "plugins", strings.Join(missing, ", "))
			if len(missing) > 0 {
				return silentExit(ExitFailed)
			}
			b, e, err := a.oneShotFrom(ctx, l, nil)
			if err != nil {
				check(false, "engine", a.redactor.Redact(err.Error()))
				return silentExit(ExitFailed)
			}
			defer e.Close(context.Background())
			hctx, hcancel := context.WithTimeout(ctx, timeout)
			sinks := e.CheckSinks(hctx)
			dlq := e.CheckDeadLetter(hctx)
			hcancel()
			errText := func(err error) string {
				if err == nil {
					return ""
				}
				return a.redactor.Redact(err.Error())
			}
			for _, name := range sortedKeys(sinks) {
				check(sinks[name] == nil, "destination "+name, errText(sinks[name]))
			}
			for i, h := range dlq {
				what := "dead_letter"
				if i > 0 {
					what = "dead_letter fallback " + h.Name
				}
				check(h.Err == nil, what, errText(h.Err))
			}
			h := e.Health(ctx)
			for _, name := range sortedKeys(h.Stores) {
				check(h.Stores[name] == "ok", "store "+name, map[bool]string{true: "", false: h.Stores[name]}[h.Stores[name] == "ok"])
			}
			if models := append(modelsOf(s), modelDestinations(s)...); len(models) > 0 && b.ai != nil {
				res := b.ai.Health(ctx, uniq(models))
				for _, m := range sortedKeys(res) {
					check(res[m] == nil, "model "+m, errText(res[m]))
				}
			}
			if failed > 0 {
				fprintf(w, "\n%d check(s) failed\n", failed)
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "time each check may take")
	return cmd
}

func milestone(t string) string {
	if config.FullOnly(t) {
		return ", only in the full build"
	}
	return ""
}

func uniq(xs []string) []string {
	var out []string
	for _, x := range xs {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (a *app) newSendCmd() *cobra.Command {
	var to string
	var m msgFlags
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Push one message to a destination through its full retry and fallback path",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if to == "" {
				return usageError(fmt.Errorf("--to is required"))
			}
			env, err := a.message("cli", m)
			if err != nil {
				return err
			}
			_, e, err := a.oneShot(cmd.Context(), nil)
			if err != nil {
				return err
			}
			defer e.Close(context.Background())
			rep, err := e.DeliverTo(cmd.Context(), to, env)
			if err != nil && rep.Destination == "" {
				return usageError(err)
			}
			w := cmd.OutOrStdout()
			if asJSON {
				_ = printJSON(w, rep)
			} else {
				a.printDelivery(w, env.ID, rep)
			}
			if err != nil {
				return err
			}
			if !rep.OK() {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&to, "to", "", "destination name")
	m.add(f)
	f.BoolVar(&asJSON, "json", false, "print the delivery report as JSON")
	return cmd
}

func (a *app) printDelivery(w io.Writer, id string, rep engine.DeliveryReport) {
	st := a.style(w)
	if rep.FilterError != "" {
		fprintf(w, "%s %s: a filter could not be evaluated, so the message was kept: %s\n", st.Warn("!"), rep.Destination, rep.FilterError)
	}
	switch {
	case rep.Filtered:
		fprintf(w, "%s %s: filtered out by a transform\n", st.Warn("-"), rep.Destination)
	case rep.OK():
		via := ""
		if rep.ViaFallback() {
			via = " via fallback " + st.Info(rep.ServedBy)
		}
		fprintf(w, "%s %s: delivered%s after %d attempt(s)%s\n", st.OK("✓"), st.Info(rep.Destination), via, rep.Attempts, statusOf(rep))
	case rep.DeadLettered:
		fprintf(w, "%s %s: dead-lettered after %d attempt(s) (id %s): %s\n", st.Warn("!"), st.Info(rep.Destination), rep.Attempts, id, rep.Error)
	default:
		fprintf(w, "%s %s: failed after %d attempt(s): %s\n", st.Fail("✗"), st.Info(rep.Destination), rep.Attempts, rep.Error)
	}
	for _, h := range rep.Hops {
		detail := ""
		if h.Error != "" {
			detail = ": " + h.Error
		}
		fprintf(w, "    %s %s ×%d %s%s\n", st.Dim("·"), h.Destination, h.Attempts, h.Outcome, st.Dim(detail))
	}
}

func statusOf(rep engine.DeliveryReport) string {
	if rep.Result != nil && rep.Result.Status != 0 {
		return fmt.Sprintf(" (status %d)", rep.Result.Status)
	}
	return ""
}

func (a *app) newRouteCmd() *cobra.Command {
	var source string
	var m msgFlags
	var dryRun, asJSON bool
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Route one message from a source; --dry-run shows the decision without delivering",
		Long: "Route one message as if a source had received it. With --dry-run it shows the\n" +
			"decision: the router that decided, the model and its probabilities, and the fallback\n" +
			"chain each destination would walk, and delivers nothing. Without it the message is\n" +
			"delivered through the full retry and fallback path.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if source == "" {
				return usageError(fmt.Errorf("--source is required"))
			}
			env, err := a.message(source, m)
			if err != nil {
				return err
			}
			_, e, err := a.oneShot(cmd.Context(), nil)
			if err != nil {
				return err
			}
			defer e.Close(context.Background())
			w := cmd.OutOrStdout()
			if dryRun {
				reps, err := e.DryRun(cmd.Context(), source, env)
				if err != nil {
					return usageError(err)
				}
				if asJSON {
					return printJSON(w, reps)
				}
				for _, r := range reps {
					a.printRoute(w, r)
				}
				return nil
			}
			rep := e.Inject(cmd.Context(), env)
			if asJSON {
				_ = printJSON(w, rep)
			} else {
				for _, r := range rep.Routes {
					a.printRoute(w, r)
				}
				fprintf(w, "message %s: %s\n", rep.ID, rep.Status)
			}
			if rep.Status != envelope.Delivered.String() && rep.Status != envelope.Filtered.String() {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&source, "source", "", "source the message comes from")
	m.add(f)
	f.BoolVar(&dryRun, "dry-run", false, "decide and plan, deliver nothing")
	f.BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func (a *app) printRoute(w io.Writer, r engine.RouteReport) {
	st := a.style(w)
	if r.FilterError != "" {
		fprintf(w, "%s route %s: a filter could not be evaluated, so the message was kept: %s\n", st.Warn("!"), r.Route, r.FilterError)
	}
	switch {
	case r.Filtered:
		fprintf(w, "route %s: %s\n", st.Info(r.Route), st.Dim("filtered out"))
		return
	case r.Decision == nil:
		fprintf(w, "route %s: %s %s\n", st.Info(r.Route), st.Fail("no decision:"), r.Error)
		return
	}
	d := r.Decision
	who := d.Router
	if d.Model != "" {
		who += " (" + d.Model + ")"
	}
	extra := ""
	if d.Cached {
		extra += ", cached"
	}
	if d.LowConfidence {
		extra += ", low confidence"
	}
	fprintf(w, "route %s: %s → %s%s\n", st.Info(r.Route), who, strings.Join(d.Destinations, ", "), extra)
	for _, h := range d.Hops {
		fprintf(w, "    %s %s\n", st.Dim("fell back from"), h)
	}
	if ad, ok := d.Attributes["ai.decision"].(map[string]any); ok {
		if hops, ok := ad["model_hops"].([]string); ok {
			for _, h := range hops {
				fprintf(w, "    %s %s\n", st.Dim("model fallback from"), h)
			}
		}
		if ans, ok := ad["answers"].([]any); ok {
			printAnswers(w, st, ans)
		} else {
			printAnswersTyped(w, st, ad["answers"])
		}
		if r, ok := ad["reason"].(string); ok && r != "" {
			fprintf(w, "    %s %s\n", st.Dim("reason:"), r)
		}
	}
	for _, p := range r.Plans {
		fprintf(w, "    %s %s\n", st.Dim("plan:"), strings.TrimSpace(strings.Join(p.Chain, " → ")+" "+st.Dim(breakers(p))))
	}
	for _, del := range r.Deliveries {
		a.printDelivery(w, "", del)
	}
}

func breakers(p engine.Plan) string {
	var parts []string
	for _, k := range sortedKeys(p.Breakers) {
		if p.Breakers[k] != "closed" {
			parts = append(parts, k+" breaker "+p.Breakers[k])
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func printAnswers(w io.Writer, st Style, ans []any) {
	for _, x := range ans {
		m, _ := x.(map[string]any)
		mark := " "
		if c, _ := m["chosen"].(bool); c {
			mark = "*"
		}
		p, _ := m["p"].(float64)
		fprintf(w, "    %s %-20s p=%.2f\n", mark, m["candidate"], p)
	}
}

func printAnswersTyped(w io.Writer, st Style, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	var ans []map[string]any
	if json.Unmarshal(b, &ans) != nil {
		return
	}
	for _, m := range ans {
		mark := " "
		if c, _ := m["chosen"].(bool); c {
			mark = "*"
		}
		p := toFloat(m["p"])
		fprintf(w, "    %s %-20s p=%.2f\n", mark, m["candidate"], p)
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	}
	return 0
}

// AICase is one labeled case of hikyaku ai test.
type AICase struct {
	Name    string            `yaml:"name" json:"name"`
	Data    any               `yaml:"data,omitempty" json:"data,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	// Subject and Attributes are the message's, as a source would set them
	// (msg.subject, msg.attributes), for routers that read them.
	Subject    string         `yaml:"subject,omitempty" json:"subject,omitempty"`
	Attributes map[string]any `yaml:"attributes,omitempty" json:"attributes,omitempty"`
	Expect     []string       `yaml:"expect" json:"expect"`
}

// AICases is a cases file.
type AICases struct {
	Cases []AICase `yaml:"cases" json:"cases"`
}

// AIResult is one case's outcome.
type AIResult struct {
	Name       string        `json:"name"`
	Expect     []string      `json:"expect"`
	Got        []string      `json:"got"`
	Router     string        `json:"router"`
	Model      string        `json:"model,omitempty"`
	Confidence float64       `json:"confidence"`
	Correct    bool          `json:"correct"`
	Latency    time.Duration `json:"latency_ns"`
	Error      string        `json:"error,omitempty"`
}

// Bin is one reliability bin.
type Bin struct {
	From, To   float64
	N          int
	Confidence float64
	Accuracy   float64
}

// AIReport is the evaluation.
type AIReport struct {
	Route       string                    `json:"route"`
	Cases       int                       `json:"cases"`
	Correct     int                       `json:"correct"`
	Accuracy    float64                   `json:"accuracy"`
	FellBack    int                       `json:"fell_back"`
	PerDest     map[string]map[string]int `json:"per_destination"` // tp, fp, fn
	Confusion   map[string]map[string]int `json:"confusion"`
	Bins        []Bin                     `json:"bins"`
	ECE         float64                   `json:"ece"`
	LatencyP50  time.Duration             `json:"latency_p50_ns"`
	LatencyP95  time.Duration             `json:"latency_p95_ns"`
	LatencyMax  time.Duration             `json:"latency_max_ns"`
	Results     []AIResult                `json:"results"`
	MinAccuracy float64                   `json:"min_accuracy,omitempty"`
}

func (a *app) newAICmd() *cobra.Command {
	cmd := &cobra.Command{Use: "ai", Short: "AI router tools"}
	var route, cases string
	var minAcc float64
	var bins int
	var asJSON bool
	test := &cobra.Command{
		Use:   "test",
		Short: "Labeled evals of an AI route: accuracy, confusion, calibration and latency",
		Long: "Run labeled cases through an AI route and report accuracy, per-destination\n" +
			"precision and recall, the confusion matrix, calibration (reliability bins and their\n" +
			"expected calibration error) and latency. Use it to tune min_confidence; --min-accuracy\n" +
			"makes it a CI gate.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if route == "" || cases == "" {
				return usageError(fmt.Errorf("--route and --cases are required"))
			}
			raw, err := os.ReadFile(cases)
			if err != nil {
				return usageError(err)
			}
			var cs AICases
			if err := yaml.Unmarshal(raw, &cs); err != nil {
				return configError(fmt.Errorf("%s: %w", cases, err))
			}
			if len(cs.Cases) == 0 {
				return configError(fmt.Errorf("%s has no cases", cases))
			}
			b, e, err := a.oneShot(cmd.Context(), nil)
			if err != nil {
				return err
			}
			defer e.Close(context.Background())
			if rt, ok := b.loaded.Spec.RouteByName(route); !ok {
				return usageError(fmt.Errorf("route %q is not declared", route))
			} else if _, ok := rt.AIRouter(); !ok {
				return usageError(fmt.Errorf("route %q has no AI router", route))
			}
			rep, err := evaluate(cmd.Context(), e, route, cs.Cases, bins)
			if err != nil {
				return err
			}
			rep.MinAccuracy = minAcc
			w := cmd.OutOrStdout()
			if asJSON {
				_ = printJSON(w, rep)
			} else {
				a.printEval(w, rep)
			}
			if rep.Accuracy < minAcc {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	f := test.Flags()
	f.StringVar(&route, "route", "", "AI route to evaluate")
	f.StringVar(&cases, "cases", "", "labeled cases (YAML: cases: [{name, data or body, headers, subject, attributes, expect}])")
	f.Float64Var(&minAcc, "min-accuracy", 0, "exit 1 below this accuracy (0..1)")
	f.IntVar(&bins, "bins", 10, "reliability bins")
	f.BoolVar(&asJSON, "json", false, "print the report as JSON")
	cmd.AddCommand(test)
	return cmd
}

func evaluate(ctx context.Context, e *engine.Engine, route string, cases []AICase, bins int) (*AIReport, error) {
	if bins <= 0 {
		bins = 10
	}
	rep := &AIReport{Route: route, Cases: len(cases), PerDest: map[string]map[string]int{}, Confusion: map[string]map[string]int{}}
	var lat []time.Duration
	type cb struct{ conf, acc float64 }
	binned := make([][]cb, bins)
	for i, c := range cases {
		body := []byte(c.Body)
		if c.Data != nil {
			b, err := yamlToJSON(c.Data)
			if err != nil {
				return nil, fmt.Errorf("case %d (%s): %w", i, c.Name, err)
			}
			body = b
		}
		env := envelope.New("", "ai-test", body)
		env.ContentType = "application/json"
		env.Subject = c.Subject
		for k, v := range c.Headers {
			env.SetHeader(k, v)
		}
		for k, v := range c.Attributes {
			env.SetAttribute(k, v)
		}
		start := time.Now()
		rr, err := e.DryRunRoute(ctx, route, env)
		d := time.Since(start)
		lat = append(lat, d)
		res := AIResult{Name: c.Name, Expect: sortedCopy(c.Expect), Latency: d}
		switch {
		case err != nil:
			res.Error = err.Error()
		case rr.Decision == nil:
			res.Error = rr.Error
		default:
			res.Got, res.Router, res.Model, res.Confidence = sortedCopy(rr.Decision.Destinations), rr.Decision.Router, rr.Decision.Model, rr.Decision.Confidence
		}
		if res.Router != "ai" {
			rep.FellBack++
		}
		res.Correct = res.Error == "" && slices.Equal(res.Got, res.Expect)
		if res.Correct {
			rep.Correct++
		}
		for _, x := range res.Expect {
			counts(rep.PerDest, x)
			if slices.Contains(res.Got, x) {
				rep.PerDest[x]["tp"]++
			} else {
				rep.PerDest[x]["fn"]++
			}
		}
		for _, x := range res.Got {
			counts(rep.PerDest, x)
			if !slices.Contains(res.Expect, x) {
				rep.PerDest[x]["fp"]++
			}
		}
		exp, got := first(res.Expect), first(res.Got)
		if rep.Confusion[exp] == nil {
			rep.Confusion[exp] = map[string]int{}
		}
		rep.Confusion[exp][got]++
		if res.Router == "ai" {
			k := min(int(res.Confidence*float64(bins)), bins-1)
			acc := 0.0
			if res.Correct {
				acc = 1
			}
			binned[k] = append(binned[k], cb{res.Confidence, acc})
		}
		rep.Results = append(rep.Results, res)
	}
	rep.Accuracy = float64(rep.Correct) / float64(len(cases))
	n := 0
	for _, b := range binned {
		n += len(b)
	}
	for i, b := range binned {
		bin := Bin{From: float64(i) / float64(bins), To: float64(i+1) / float64(bins), N: len(b)}
		for _, x := range b {
			bin.Confidence += x.conf
			bin.Accuracy += x.acc
		}
		if len(b) > 0 {
			bin.Confidence /= float64(len(b))
			bin.Accuracy /= float64(len(b))
			rep.ECE += float64(len(b)) / float64(n) * math.Abs(bin.Accuracy-bin.Confidence)
		}
		rep.Bins = append(rep.Bins, bin)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	rep.LatencyP50, rep.LatencyP95, rep.LatencyMax = pct(lat, 0.5), pct(lat, 0.95), lat[len(lat)-1]
	return rep, nil
}

func counts(m map[string]map[string]int, k string) {
	if m[k] == nil {
		m[k] = map[string]int{"tp": 0, "fp": 0, "fn": 0}
	}
}

func first(xs []string) string {
	if len(xs) == 0 {
		return "(none)"
	}
	return strings.Join(xs, "+")
}

func sortedCopy(xs []string) []string {
	out := slices.Clone(xs)
	slices.Sort(out)
	if out == nil {
		out = []string{}
	}
	return out
}

func pct(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(xs)))) - 1
	return xs[max(i, 0)]
}

// yamlToJSON encodes a case's data (decoded from YAML: maps with string
// keys) as the JSON body of the message.
func yamlToJSON(v any) ([]byte, error) { return json.Marshal(v) }

func (a *app) printEval(w io.Writer, r *AIReport) {
	st := a.style(w)
	fprintf(w, "%s %s: %d cases\n", st.Bold("route"), st.Info(r.Route), r.Cases)
	verdict := st.OK
	if r.MinAccuracy > 0 && r.Accuracy < r.MinAccuracy {
		verdict = st.Fail
	}
	fprintf(w, "accuracy  %s (%d/%d)", verdict(fmt.Sprintf("%.1f%%", r.Accuracy*100)), r.Correct, r.Cases)
	if r.FellBack > 0 {
		fprintf(w, "  %s", st.Warn(fmt.Sprintf("%d decided by a fallback router", r.FellBack)))
	}
	fprintln(w)
	fprintf(w, "latency   p50 %s  p95 %s  max %s\n\n", r.LatencyP50.Round(time.Millisecond), r.LatencyP95.Round(time.Millisecond), r.LatencyMax.Round(time.Millisecond))
	fprintf(w, "%s\n", st.Bold("per destination   precision  recall"))
	for _, d := range sortedKeys(r.PerDest) {
		c := r.PerDest[d]
		fprintf(w, "  %-16s %8s %7s\n", d, ratio(c["tp"], c["tp"]+c["fp"]), ratio(c["tp"], c["tp"]+c["fn"]))
	}
	labels := map[string]bool{}
	for e, row := range r.Confusion {
		labels[e] = true
		for g := range row {
			labels[g] = true
		}
	}
	cols := sortedKeys(labels)
	fprintf(w, "\n%s\n  %-16s", st.Bold("confusion (rows: expected, columns: got)"), "")
	for _, c := range cols {
		fprintf(w, " %10s", truncate(c, 10))
	}
	fprintln(w)
	for _, e := range cols {
		if _, ok := r.Confusion[e]; !ok {
			continue
		}
		fprintf(w, "  %-16s", truncate(e, 16))
		for _, c := range cols {
			fprintf(w, " %10d", r.Confusion[e][c])
		}
		fprintln(w)
	}
	fprintf(w, "\n%s %s\n", st.Bold("calibration (confidence → accuracy)"), st.Dim(fmt.Sprintf("ECE %.3f", r.ECE)))
	for _, b := range r.Bins {
		if b.N == 0 {
			continue
		}
		fprintf(w, "  %.1f-%.1f  n=%-3d confidence %.2f  accuracy %.2f\n", b.From, b.To, b.N, b.Confidence, b.Accuracy)
	}
	var wrong []AIResult
	for _, x := range r.Results {
		if !x.Correct {
			wrong = append(wrong, x)
		}
	}
	if len(wrong) > 0 {
		fprintf(w, "\n%s\n", st.Bold("misses"))
		for _, x := range wrong {
			got := strings.Join(x.Got, ", ")
			if x.Error != "" {
				got = "error: " + x.Error
			}
			fprintf(w, "  %s %s: expected %s, got %s %s\n", st.Fail("✗"), x.Name, strings.Join(x.Expect, ", "), got, st.Dim(fmt.Sprintf("(%s, p=%.2f)", x.Router, x.Confidence)))
		}
	}
}

func ratio(a, b int) string {
	if b == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", float64(a)/float64(b))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func (a *app) newChaosCmd() *cobra.Command {
	var dest, fail, data, errMode string
	var latency, retryAfter time.Duration
	var status, count int
	var seed int64
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "chaos",
		Short: "Inject faults into a destination to exercise retries and fallbacks",
		Long: "Inject faults into a destination: a share of attempts fail (--fail 50%), slow\n" +
			"down (--latency 2s), or time out. With --data, --count messages are pushed through\n" +
			"the destination's retry and fallback path and the outcome is summarised; without\n" +
			"it, the relay runs as with hikyaku run, the destination wrapped.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dest == "" {
				return usageError(fmt.Errorf("--destination is required"))
			}
			if a.deps.WrapFault == nil {
				return fmt.Errorf("this build has no fault injection (cmd/hikyaku links protocols/fault)")
			}
			f := config.Fault{Latency: config.Duration(latency), Status: status, RetryAfter: config.Duration(retryAfter), Error: errMode, Seed: seed}
			if fail != "" {
				p, err := config.ParsePercent(fail)
				if err != nil {
					return usageError(fmt.Errorf("--fail: %w", err))
				}
				f.Fail = config.Percent(p)
			}
			var wrapErr error
			wrap := func(d string, s plugin.Sink) plugin.Sink {
				if d != dest {
					return s
				}
				w, err := a.deps.WrapFault(s, f)
				if err != nil {
					wrapErr = err
					return s
				}
				return w
			}
			if data == "" {
				a.wrapSink = wrap
				return a.run(cmd.Context(), false, nil)
			}
			_, e, err := a.oneShot(cmd.Context(), wrap)
			if err != nil {
				return err
			}
			defer e.Close(context.Background())
			if wrapErr != nil {
				return usageError(wrapErr)
			}
			if !slices.Contains(e.DestinationNames(), dest) {
				return usageError(fmt.Errorf("destination %q is not declared", dest))
			}
			sum := chaosSummary{Destination: dest, Messages: count, ServedBy: map[string]int{}}
			var durs []time.Duration
			for i := 0; i < count; i++ {
				env, err := a.message("chaos", msgFlags{data: data})
				if err != nil {
					return err
				}
				start := time.Now()
				rep, err := e.DeliverTo(cmd.Context(), dest, env)
				durs = append(durs, time.Since(start))
				sum.Attempts += rep.Attempts
				switch {
				case err != nil:
					sum.Lost++
				case rep.OK() && !rep.ViaFallback():
					sum.Primary++
				case rep.OK():
					sum.Fallback++
					sum.ServedBy[rep.ServedBy]++
				case rep.DeadLettered:
					sum.DeadLettered++
				default:
					sum.Lost++
				}
			}
			sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
			sum.P50, sum.P95 = pct(durs, 0.5), pct(durs, 0.95)
			w := cmd.OutOrStdout()
			if asJSON {
				_ = printJSON(w, sum)
			} else {
				st := a.style(w)
				fprintf(w, "%s %d messages to %s, %d attempts (%.1f per message)\n", st.Bold("chaos"), count, st.Info(dest), sum.Attempts, float64(sum.Attempts)/float64(max(count, 1)))
				fprintf(w, "  delivered by %s     %d\n", dest, sum.Primary)
				for _, k := range sortedKeys(sum.ServedBy) {
					fprintf(w, "  delivered by fallback %s  %d\n", k, sum.ServedBy[k])
				}
				fprintf(w, "  dead-lettered          %d\n", sum.DeadLettered)
				lost := fmt.Sprintf("%d", sum.Lost)
				if sum.Lost > 0 {
					lost = st.Fail(lost)
				}
				fprintf(w, "  lost                   %s\n", lost)
				fprintf(w, "  latency p50 %s  p95 %s\n", sum.P50.Round(time.Millisecond), sum.P95.Round(time.Millisecond))
			}
			if sum.Lost > 0 {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&dest, "destination", "", "destination to inject faults into")
	f.StringVar(&fail, "fail", "", "share of attempts that fail, such as 50%")
	f.DurationVar(&latency, "latency", 0, "latency added to every attempt")
	f.IntVar(&status, "status", 503, "status of a failed attempt")
	f.StringVar(&errMode, "error", "status", "how an attempt fails: status, connect or timeout")
	f.DurationVar(&retryAfter, "retry-after", 0, "Retry-After of a failed attempt")
	f.Int64Var(&seed, "seed", 0, "random seed, for repeatable runs")
	f.StringVar(&data, "data", "", "push this message (@file or literal) instead of running the relay")
	f.IntVar(&count, "count", 100, "messages to push with --data")
	f.BoolVar(&asJSON, "json", false, "print the summary as JSON")
	return cmd
}

type chaosSummary struct {
	Destination  string         `json:"destination"`
	Messages     int            `json:"messages"`
	Attempts     int            `json:"attempts"`
	Primary      int            `json:"delivered_primary"`
	Fallback     int            `json:"delivered_fallback"`
	ServedBy     map[string]int `json:"served_by"`
	DeadLettered int            `json:"dead_lettered"`
	Lost         int            `json:"lost"`
	P50          time.Duration  `json:"p50_ns"`
	P95          time.Duration  `json:"p95_ns"`
}
