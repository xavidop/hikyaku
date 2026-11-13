//go:build integration

package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm"
	"github.com/xavidop/hikyaku/store"
)

// Gemini Decision 1 through OpenRouter's decision server. The model that
// answers is named by its version, such as gemini-decision-1-20261111.
const (
	geminiDecision       = "openrouter-decisions/google/gemini-decision-1"
	geminiDecisionServed = "openrouter-decisions/gemini-decision-1"
)

// needOpenRouterKey skips a test unless the environment has an OpenRouter
// key (no CI job has one).
func needOpenRouterKey(t *testing.T) {
	t.Helper()
	if os.Getenv("OPENROUTER_API_KEY") == "" {
		t.Skip("set OPENROUTER_API_KEY to run the Gemini decision model tests through OpenRouter")
	}
}

// The ai-triage example's labeled cases on Gemini Decision 1, run by hand.
// The example's models are swapped for the Gemini one and its Redis store
// for an in-process one, so nothing else is needed.
func TestIntegrationOpenRouterGeminiEvals(t *testing.T) {
	needOpenRouterKey(t)
	raw, err := os.ReadFile(aiTriageExample + "hikyaku.yaml")
	require.NoError(t, err)
	spec := string(raw)
	for old, repl := range map[string]string{
		`(?m)^secrets:\n  TYPESAFE_API_KEY: env:TYPESAFE_API_KEY\n`: "",
		`servers: \[typesafe, ollama\]`:                             "servers: [openrouter]",
		`decision: typesafe/jev-1\.13\.0`:                           "decision: " + geminiDecision,
		`(?m)^  fallback:\n    decision: \[[^\]]*\][^\n]*\n`:        "",
		`(?s)stores:.*?\n(dead_letter:)`:                            "stores:\n  - { name: redis-main, type: memory }\n\n$1",
	} {
		re := regexp.MustCompile(old)
		require.True(t, re.MatchString(spec), "the example still has %s", old)
		spec = re.ReplaceAllString(spec, repl)
	}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hikyaku.yaml"), []byte(spec), 0o600))

	code, out, errOut := hk(t, "ai", "test", "--json", "-f", filepath.Join(dir, "hikyaku.yaml"),
		"--route", "triage", "--cases", aiTriageExample+"cases.yaml", "--min-accuracy", "0.8")
	require.Equal(t, 0, code, "%s\n%s", out, errOut)
	var rep struct {
		Accuracy float64 `json:"accuracy"`
		ECE      float64 `json:"ece"`
		Cases    int     `json:"cases"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &rep))
	assert.Equal(t, 5, rep.Cases)
	assert.GreaterOrEqual(t, rep.Accuracy, 0.8)
	assert.Less(t, rep.ECE, 0.2, "the example's thresholds stay calibrated on this model")
	t.Logf("accuracy %.0f%%, ECE %.3f", rep.Accuracy*100, rep.ECE)
}

// One choice between destinations whose names have a dot, and a follow-up
// of an earlier call, where the history goes to the model as turns.
func TestIntegrationOpenRouterGeminiDecisionModel(t *testing.T) {
	needOpenRouterKey(t)
	ctx := context.Background()
	doc := `
version: 1
name: openrouter-gemini-it
models: { decision: ` + geminiDecision + ` }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: team.billing, type: stdout }
  - { name: team.support, type: stdout }
stores: [{ name: mem, type: memory }]
routes:
  - name: triage
    from: [in]
    router:
      type: ai
      engine: decision
      instructions: Route customer events to the team that must see them.
      state: "{{ .bodyText }}"
      candidates:
        - { name: team.billing, description: "Payments, invoices, refunds, charges" }
        - { name: team.support, description: "Bugs, outages, errors, how-to questions" }
      %s
      timeout: 20s
      fallback: [{ type: static, to: [team.support] }]
`
	run := func(t *testing.T, extra, body string, past ...store.CallRecord) *engine.DecisionView {
		t.Helper()
		l, err := config.LoadBytes(ctx, "openrouter.yaml", []byte(strings.Replace(doc, "%s", extra, 1)), config.LoadOptions{})
		require.NoError(t, err)
		f, err := llm.New(ctx, l.Spec, llm.Options{})
		require.NoError(t, err)
		mem := store.NewMemory("mem")
		for _, rec := range past {
			require.NoError(t, mem.Append(ctx, rec.Route, rec.Key, rec, store.AppendOptions{TTL: time.Hour}))
		}
		e, err := engine.New(ctx, l.Spec, engine.Options{AI: f, NoSources: true, Stores: map[string]store.Store{"mem": mem}})
		require.NoError(t, err)
		defer e.Close(ctx)
		env := envelope.New("in", "stdin", []byte(body))
		env.ContentType = "application/json"
		reps, err := e.DryRun(ctx, "in", env)
		require.NoError(t, err)
		require.Len(t, reps, 1)
		require.NotNil(t, reps[0].Decision)
		return reps[0].Decision
	}

	t.Run("choice", func(t *testing.T) {
		d := run(t, "min_confidence: 0.5", `{"text":"I was charged twice for my invoice and want a refund"}`)
		assert.Contains(t, d.Model, geminiDecisionServed, "the decision model answered, not the fallback")
		assert.Equal(t, []string{"team.billing"}, d.Destinations)
		assert.Greater(t, d.Confidence, 0.5)
	})

	t.Run("follow-up", func(t *testing.T) {
		history := `history:
        store: mem
        key: "{{ .json.customer }}"
        ttl: 1h
        sticky: { enabled: true, mode: model, follow_up_threshold: 0.7 }`
		past := store.CallRecord{
			MsgID: "past-1", At: time.Now().Add(-time.Minute), Route: "triage", Key: "c-1",
			State:    `{"text":"I was charged twice for invoice 7 and want a refund","customer":"c-1"}`,
			Decision: []store.Answer{{Candidate: "team.billing", Probability: 0.97, Chosen: true}},
			Outcome:  store.OutcomeDelivered, DeliveredTo: []string{"team.billing"},
		}
		d := run(t, history, `{"text":"Any update on the refund for invoice 7? Still waiting.","customer":"c-1"}`, past)
		assert.Contains(t, d.Model, geminiDecisionServed)
		assert.Equal(t, []string{"team.billing"}, d.Destinations)
		ad, _ := d.Attributes["ai.decision"].(map[string]any)
		assert.Equal(t, true, ad["sticky"], "the model saw the earlier call and called this one a follow-up: %v", ad)
	})
}
