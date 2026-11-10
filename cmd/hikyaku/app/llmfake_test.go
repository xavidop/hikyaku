//go:build llmfake

package app

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/llm/llmfake"
)

const aiTriage = "../../../examples/ai-triage/"

// The ai-triage example, offline: the first decision model is down, the
// fallback model decides, and the double charge goes to billing.
func TestAITriageDryRun(t *testing.T) {
	code, out, errOut := hk(t, "route", "--dry-run", "-f", aiTriage+"hikyaku.llmfake.yaml", "--source", "webhooks", "--data", "@"+aiTriage+"msg.json")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "route triage: ai (llmfake-decisions/clef) → billing")
	assert.Contains(t, out, "model fallback from llmfake-decisions/down")

	code, out, _ = hk(t, "route", "--dry-run", "--json", "-f", aiTriage+"hikyaku.llmfake.yaml", "--source", "webhooks", "--data", `{"customer_id":"c1","text":"the app crashes with an error"}`)
	require.Equal(t, 0, code)
	var reps []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &reps))
	dec := reps[0]["decision"].(map[string]any)
	assert.Equal(t, []any{"support"}, dec["destinations"])
}

// The labeled cases score well enough to gate CI on.
func TestAITriageEvals(t *testing.T) {
	code, out, errOut := hk(t, "ai", "test", "--json", "-f", aiTriage+"hikyaku.llmfake.yaml", "--route", "triage", "--cases", aiTriage+"cases.yaml", "--min-accuracy", "0.6")
	require.Equal(t, 0, code, errOut)
	var rep map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &rep))
	assert.GreaterOrEqual(t, rep["accuracy"].(float64), 0.6)
	assert.Equal(t, 5.0, rep["cases"])
}

// doctor checks the fake models along with everything else.
func TestAITriageDoctor(t *testing.T) {
	code, out, _ := hk(t, "doctor", "-f", aiTriage+"hikyaku.llmfake.yaml")
	assert.Contains(t, out, "model llmfake-decisions/clef")
	assert.Contains(t, out, "✗ model llmfake-decisions/down", "a down model is reported")
	assert.Equal(t, 1, code)
}

// doctor on the LLM proxy: model destinations are not plugins (the AI
// factory builds them), their models are checked, and so is the DLQ.
func TestLLMProxyDoctor(t *testing.T) {
	code, out, _ := hk(t, "doctor", "-f", "../../../examples/llm-proxy/hikyaku.llmfake.yaml")
	assert.Contains(t, out, "✓ plugins")
	assert.Contains(t, out, "✓ destination fast")
	assert.Contains(t, out, "✓ model llmfake/chat")
	assert.Contains(t, out, "✗ model llmfake/down", "the always-down model is reported")
	assert.Contains(t, out, "✓ dead_letter")
	assert.Equal(t, 1, code)
}

// The AI router's when gate: a typed event without free text skips the
// model (no call, no spend) and the rules route it; a customer message
// still reaches the model.
func TestAITriageGateSkipsTheModel(t *testing.T) {
	before := llmfake.Calls.Load()
	code, out, errOut := hk(t, "route", "--dry-run", "-f", aiTriage+"hikyaku.llmfake.yaml", "--source", "webhooks",
		"--data", `{"customer_id":"c9","type":"invoice.paid","id":"in_1"}`)
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "route triage: rules → billing")
	assert.Contains(t, out, "ai: skipped (when is false)")
	assert.Equal(t, before, llmfake.Calls.Load(), "the model was never called")

	code, out, errOut = hk(t, "route", "--dry-run", "-f", aiTriage+"hikyaku.llmfake.yaml", "--source", "webhooks",
		"--data", `{"customer_id":"c9","text":"I was charged twice for my invoice"}`)
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "route triage: ai (llmfake-decisions/clef)")
	assert.Greater(t, llmfake.Calls.Load(), before, "a message with text reaches the model")
}
