package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/cli"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
)

// sink is a test destination: it fails while fail is set.
type sink struct {
	mu   sync.Mutex
	got  []*envelope.Envelope
	fail bool
}

func (s *sink) Name() string { return "test" }
func (s *sink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, plugin.StatusError(503, 0, "down")
	}
	s.got = append(s.got, env)
	return &plugin.Result{Status: 202}, nil
}
func (s *sink) Health(context.Context) error { return nil }
func (s *sink) Close(context.Context) error  { return nil }

var (
	sinksMu sync.Mutex
	sinks   = map[string]*sink{}
)

func sinkNamed(n string) *sink {
	sinksMu.Lock()
	defer sinksMu.Unlock()
	if sinks[n] == nil {
		sinks[n] = &sink{}
	}
	return sinks[n]
}

func init() {
	plugin.RegisterSink("fault", func(_ context.Context, s plugin.Spec) (plugin.Sink, error) { return sinkNamed(s.Name), nil })
	plugin.RegisterSource("stdin", func(context.Context, plugin.Spec) (plugin.Source, error) { return nil, nil })
}

// fakeAI decides billing for anything about invoices, support otherwise.
type fakeAI struct{}

func (fakeAI) NewRouter(_ context.Context, req router.AIRequest) (plugin.Router, error) {
	return plugin.RouterFunc(func(_ context.Context, env *envelope.Envelope) (plugin.Decision, error) {
		dest, p := "support", 0.7
		if strings.Contains(env.BodyText(), "invoice") {
			dest, p = "billing", 0.92
		}
		return plugin.Decision{Destinations: []string{dest}, Router: "ai", Model: "fake/model", Confidence: p,
			Attributes: map[string]any{"ai.decision": map[string]any{"model": "fake/model", "answers": []store.Answer{{Candidate: dest, Probability: p, Chosen: true}}}}}, nil
	}), nil
}

func (fakeAI) Health(context.Context, []string) map[string]error { return nil }

func run(t *testing.T, dir string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	deps := cli.Deps{
		LookupEnv: func(string) (string, bool) { return "", false },
		NewAI: func(context.Context, *config.Spec, cli.AIOptions) (router.AIFactory, error) {
			return fakeAI{}, nil
		},
		WrapFault: func(s plugin.Sink, f config.Fault) (plugin.Sink, error) { return &failing{s}, nil },
	}
	root := cli.NewRoot(cli.BuildInfo{Version: "1.2.3", Commit: "abc"}, cli.Streams{Out: &out, Err: &errb}, deps)
	wd, _ := os.Getwd()
	if dir != "" {
		require.NoError(t, os.Chdir(dir))
		defer func() { _ = os.Chdir(wd) }()
	}
	code := cli.Execute(context.Background(), root, append(args, "--color", "never"))
	return code, out.String(), errb.String()
}

// failing is the chaos wrapper of these tests: every attempt fails.
type failing struct{ plugin.Sink }

func (f *failing) Deliver(context.Context, *envelope.Envelope) (*plugin.Result, error) {
	return nil, plugin.StatusError(503, 0, "chaos")
}

const spec = `
version: 1
name: cli
sources: [{ name: in, type: stdin }]
destinations:
  - { name: billing, type: fault, retry: { attempts: 2, backoff: 1ms }, fallback: [spool] }
  - { name: support, type: fault, retry: { attempts: 1 } }
  - { name: spool, type: fault }
dead_letter: { type: file, path: ./dlq.ndjson }
routes:
  - name: triage
    from: [in]
    router:
      type: ai
      model: fake/model
      candidates:
        - { name: billing, description: payments }
        - { name: support, description: bugs }
      fallback: [{ type: static, to: [support] }]
`

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hikyaku.yaml"), []byte(body), 0o644))
	return dir
}

func TestValidateExitCodes(t *testing.T) {
	dir := writeSpec(t, spec)
	code, out, _ := run(t, dir, "validate")
	assert.Equal(t, cli.ExitOK, code)
	assert.Contains(t, out, "valid (1 sources, 3 destinations, 1 routes)")

	bad := writeSpec(t, strings.Replace(spec, "to: [support] }", "to: [ghost] }", 1))
	code, _, errOut := run(t, bad, "validate")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, `destination "ghost" is not declared`)

	celBad := writeSpec(t, strings.Replace(spec, "fallback: [{ type: static, to: [support] }]", `fallback: [{ type: rules, rules: [{ when: "msg.((", to: [support] }] }]`, 1))
	code, _, errOut = run(t, celBad, "validate")
	assert.Equal(t, cli.ExitConfig, code, "CEL errors are caught by validate")
	assert.Contains(t, errOut, "rules[0].when")

	code, _, errOut = run(t, t.TempDir(), "validate")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, "hikyaku init")

	code, _, _ = run(t, dir, "validate", "--nope")
	assert.Equal(t, cli.ExitConfig, code, "a usage error")
}

func TestInitTemplatesValidate(t *testing.T) {
	for _, tmpl := range cli.Templates() {
		t.Run(tmpl, func(t *testing.T) {
			dir := t.TempDir()
			code, out, errOut := run(t, dir, "init", "--template", tmpl)
			require.Equal(t, 0, code, errOut)
			assert.Contains(t, out, "hikyaku.yaml")
			code, _, errOut = run(t, dir, "validate")
			assert.Equal(t, 0, code, "template %s must validate: %s", tmpl, errOut)
			code, _, _ = run(t, dir, "init", "--template", tmpl)
			assert.Equal(t, cli.ExitConfig, code, "init does not overwrite without --force")
			code, _, _ = run(t, dir, "init", "--template", tmpl, "--force")
			assert.Equal(t, 0, code)
		})
	}
	code, _, errOut := run(t, t.TempDir(), "init", "--template", "nope")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, "unknown template")
}

func TestSchemaAndVersion(t *testing.T) {
	code, out, _ := run(t, "", "schema")
	require.Equal(t, 0, code)
	var s map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &s))
	assert.Equal(t, config.SchemaBaseURL+"hikyaku.schema.json", s["$id"])

	code, out, _ = run(t, "", "version")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "hikyaku 1.2.3 (commit abc")
	code, out, _ = run(t, "", "version", "--json")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, `"version": "1.2.3"`)
}

func TestLintStrict(t *testing.T) {
	dir := writeSpec(t, spec)
	code, out, _ := run(t, dir, "lint")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "warning(s)")
	code, _, _ = run(t, dir, "lint", "--strict")
	assert.Equal(t, cli.ExitFailed, code)
}

func TestSendDeliversAndDeadLetters(t *testing.T) {
	dir := writeSpec(t, spec)
	sinkNamed("billing").fail = false
	code, out, errOut := run(t, dir, "send", "--to", "billing", "--data", `{"type":"invoice.paid"}`, "--header", "X-Test: 1")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "billing: delivered after 1 attempt(s)")

	sinkNamed("billing").fail = true
	defer func() { sinkNamed("billing").fail = false }()
	code, out, _ = run(t, dir, "send", "--to", "billing", "--data", `{"type":"invoice.paid"}`)
	assert.Equal(t, 0, code, "billing failed over to its fallback")
	assert.Contains(t, out, "via fallback spool")

	sinkNamed("support").fail = true
	defer func() { sinkNamed("support").fail = false }()
	code, out, _ = run(t, dir, "send", "--to", "support", "--data", "@-")
	assert.Equal(t, cli.ExitFailed, code, "a dead-lettered send exits 1")
	assert.Contains(t, out, "dead-lettered")

	code, _, errOut = run(t, dir, "send", "--to", "ghost", "--data", "{}")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, "ghost")
	code, _, _ = run(t, dir, "send", "--data", "{}")
	assert.Equal(t, cli.ExitConfig, code)
}

func TestRouteDryRunShowsDecisionAndPlans(t *testing.T) {
	dir := writeSpec(t, spec)
	before := len(sinkNamed("billing").got)
	code, out, errOut := run(t, dir, "route", "--dry-run", "--source", "in", "--data", `{"text":"my invoice is wrong"}`)
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "route triage: ai (fake/model) → billing")
	assert.Contains(t, out, "* billing")
	assert.Contains(t, out, "plan: billing → spool")
	assert.Equal(t, before, len(sinkNamed("billing").got), "a dry run delivers nothing")

	code, out, _ = run(t, dir, "route", "--source", "in", "--data", `{"text":"invoice"}`)
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "delivered")

	code, out, _ = run(t, dir, "route", "--dry-run", "--source", "in", "--data", `{"text":"x"}`, "--json")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, `"route": "triage"`)
}

// --subject and --attr set what a source would: rules can route on them.
func TestRouteSubjectAndAttributes(t *testing.T) {
	dir := writeSpec(t, `
version: 1
name: cli
sources: [{ name: in, type: stdin }]
destinations: [{ name: billing, type: fault }, { name: support, type: fault }]
routes:
  - name: r
    from: [in]
    router:
      type: rules
      rules:
        - { when: 'msg.subject == "orders.paid" && msg.attributes["region"] == "eu"', to: [billing] }
      default: [support]
`)
	code, out, errOut := run(t, dir, "route", "--dry-run", "--source", "in", "--data", `{}`, "--subject", "orders.paid", "--attr", "region=eu")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "rules → billing")
	code, out, _ = run(t, dir, "route", "--dry-run", "--source", "in", "--data", `{}`, "--subject", "orders.paid")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "→ support")
	code, _, errOut = run(t, dir, "route", "--dry-run", "--source", "in", "--data", `{}`, "--attr", "region")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, "want name=value")
}

func TestAITestReportsAccuracyAndCalibration(t *testing.T) {
	dir := writeSpec(t, spec)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cases.yaml"), []byte(`
cases:
  - { name: invoice, data: { text: "the invoice is wrong" }, expect: [billing] }
  - { name: crash, data: { text: "the app crashes" }, expect: [support] }
  - { name: refund, body: "I want a refund", expect: [billing] }
`), 0o644))
	code, out, errOut := run(t, dir, "ai", "test", "--route", "triage", "--cases", "cases.yaml", "--min-accuracy", "0.5")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "66.7% (2/3)")
	assert.Contains(t, out, "confusion")
	assert.Contains(t, out, "calibration")
	assert.Contains(t, out, "expected billing, got support")

	code, _, _ = run(t, dir, "ai", "test", "--route", "triage", "--cases", "cases.yaml", "--min-accuracy", "0.9")
	assert.Equal(t, cli.ExitFailed, code, "below --min-accuracy fails")

	code, out, _ = run(t, dir, "ai", "test", "--route", "triage", "--cases", "cases.yaml", "--json")
	assert.Equal(t, 0, code)
	var rep cli.AIReport
	require.NoError(t, json.Unmarshal([]byte(out), &rep))
	assert.Equal(t, 3, rep.Cases)
	assert.Equal(t, 1, rep.PerDest["billing"]["fn"])
	code, _, _ = run(t, dir, "ai", "test", "--route", "nope", "--cases", "cases.yaml")
	assert.Equal(t, cli.ExitConfig, code)
}

func TestDLQListAndReplay(t *testing.T) {
	dir := writeSpec(t, spec)
	sinkNamed("support").fail = true
	code, _, _ := run(t, dir, "send", "--to", "support", "--data", `{"n":1}`)
	require.Equal(t, cli.ExitFailed, code)
	code, out, _ := run(t, dir, "dlq", "list")
	require.Equal(t, 0, code)
	assert.Contains(t, out, "support")
	assert.Contains(t, out, "1 dead letter(s)")
	id := strings.Fields(out)[0]

	code, out, _ = run(t, dir, "dlq", "inspect", id)
	assert.Equal(t, 0, code)
	assert.Contains(t, out, `"dlq"`)

	code, out, _ = run(t, dir, "dlq", "replay", "--dry-run")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "would deliver "+id+" to support")

	sinkNamed("support").fail = false
	before := len(sinkNamed("support").got)
	code, out, _ = run(t, dir, "dlq", "replay", "--id", id)
	assert.Equal(t, 0, code, out)
	got := sinkNamed("support").got
	require.Len(t, got, before+1)
	assert.Equal(t, id, got[len(got)-1].ID, "the original idempotency key is kept")
	_, has := got[len(got)-1].Attribute("dlq")
	assert.False(t, has, "the dlq metadata is not delivered")
}

func TestChaosSummary(t *testing.T) {
	dir := writeSpec(t, spec)
	code, out, errOut := run(t, dir, "chaos", "--destination", "billing", "--data", `{"x":1}`, "--count", "3")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "3 messages to billing")
	assert.Contains(t, out, "delivered by fallback spool  3", "every attempt on billing failed; the fallback took them")
	code, _, _ = run(t, dir, "chaos", "--data", "{}")
	assert.Equal(t, cli.ExitConfig, code)
	code, _, _ = run(t, dir, "chaos", "--destination", "billing", "--fail", "300%", "--data", "{}")
	assert.Equal(t, cli.ExitConfig, code)
}

func TestHelpAndUnknownCommand(t *testing.T) {
	code, out, _ := run(t, "", "--help")
	assert.Equal(t, 0, code)
	assert.Contains(t, out, "Available Commands:")
	assert.Contains(t, out, "ai")
	code, _, errOut := run(t, "", "teleport")
	assert.Equal(t, cli.ExitConfig, code)
	assert.Contains(t, errOut, `unknown command "teleport"`)
}

func TestExitCodeMapping(t *testing.T) {
	assert.Equal(t, cli.ExitOK, cli.ExitCode(nil))
	assert.Equal(t, cli.ExitConfig, cli.ExitCode(config.Errors{{Msg: "x"}}))
	assert.Equal(t, cli.ExitInfra, cli.ExitCode(context.Canceled))
}
