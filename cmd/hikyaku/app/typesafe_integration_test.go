//go:build integration

package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const aiTriageExample = "../../../examples/ai-triage/"

// The ai-triage example's labeled cases on the real decision model, run
// by hand: it skips unless the environment has TYPESAFE_API_KEY (no CI job
// has one). The example's Redis store is swapped for an in-process one, so
// nothing else is needed.
func TestIntegrationTypeSafeEvals(t *testing.T) {
	if os.Getenv("TYPESAFE_API_KEY") == "" {
		t.Skip("set TYPESAFE_API_KEY to evaluate the ai-triage router on TypeSafe's decision models")
	}
	spec, err := os.ReadFile(aiTriageExample + "hikyaku.yaml")
	require.NoError(t, err)
	stores := regexp.MustCompile(`(?s)stores:.*?\n(dead_letter:)`)
	spec = stores.ReplaceAll(spec, []byte("stores:\n  - { name: redis-main, type: memory }\n\n$1"))
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hikyaku.yaml"), spec, 0o600))

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
	assert.Less(t, rep.ECE, 0.2, "the tuned thresholds stay calibrated")
	t.Logf("accuracy %.0f%%, ECE %.3f", rep.Accuracy*100, rep.ECE)
}
