package llm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/firebase/genkit/go/core/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

func TestRetryableAndReasons(t *testing.T) {
	for err, want := range map[error]bool{
		ErrBadOutput: true,
		status.Errorf(status.ErrUnavailable, "down"):            true,
		status.Errorf(status.ErrResourceExhausted, "slow"):      true,
		status.Errorf(status.ErrUnauthenticated, "key"):         false,
		status.Errorf(status.ErrInvalidArgument, "request"):     false,
		context.DeadlineExceeded:                                true,
		errors.New("upstream returned 503 Service Unavailable"): true,
		errors.New("no such model"):                             false,
	} {
		assert.Equal(t, want, retryable(err), "%v", err)
	}
	assert.False(t, retryable(nil))
	assert.Equal(t, "bad_output", reasonOf(ErrBadOutput))
	assert.Equal(t, "unavailable", reasonOf(status.Errorf(status.ErrUnavailable, "x")))
	assert.Equal(t, "rate_limited", reasonOf(status.Errorf(status.ErrResourceExhausted, "x")))
	assert.Equal(t, "auth", reasonOf(status.Errorf(status.ErrPermissionDenied, "x")))
	assert.Equal(t, "timeout", reasonOf(context.DeadlineExceeded))
	assert.Equal(t, "error", reasonOf(errors.New("x")))
}

func TestStateBuilderRedactsAndAddsHeaders(t *testing.T) {
	b, err := newStateBuilder(config.RouterSpec{
		StateJSON: true,
		Input:     &config.AIInput{Redact: []string{"$.card.number", "$.email"}, Headers: []string{"X-Plan"}},
	})
	require.NoError(t, err)
	env := envelope.New("s", "http", []byte(`{"email":"a@b.c","card":{"number":"4111","exp":"12/30"},"text":"refund please"}`))
	env.SetHeader("X-Plan", "gold")
	env.SetHeader("Authorization", "Bearer secret")
	s, err := b.build(env)
	require.NoError(t, err)
	assert.NotContains(t, s, "4111")
	assert.NotContains(t, s, "a@b.c")
	assert.Contains(t, s, `"exp":"12/30"`)
	assert.Contains(t, s, `"X-Plan":"gold"`)
	assert.NotContains(t, s, "Bearer", "only allowlisted headers reach the model")

	txt, err := newStateBuilder(config.RouterSpec{State: "{{ .json.text }}", Input: &config.AIInput{Headers: []string{"X-Plan"}}})
	require.NoError(t, err)
	s, err = txt.build(env)
	require.NoError(t, err)
	assert.Equal(t, "X-Plan: gold\n\nrefund please", s)
	assert.Equal(t, config.DefaultAIMaxBytes, txt.maxBytes)

	_, err = newStateBuilder(config.RouterSpec{Input: &config.AIInput{Redact: []string{"nope"}}})
	assert.Error(t, err)
}

func TestFlowInputRoundTrip(t *testing.T) {
	env := envelope.New("webhooks", "http", []byte(`{"a":1}`))
	env.Subject = "/ingest"
	env.SetHeader("X-A", "b")
	env.Attributes["k"] = "v"
	back := InputOf(env).Envelope()
	assert.Equal(t, env.ID, back.ID)
	assert.Equal(t, env.Body, back.Body)
	assert.Equal(t, "b", back.Header("X-A"))
	assert.Equal(t, "v", back.Attributes["k"])
	assert.Equal(t, "/ingest", back.Subject)
}

func TestPluginsForErrors(t *testing.T) {
	_, err := pluginsFor(&config.Spec{Models: config.Models{Decision: "mystery/model-1"}}, Options{})
	assert.ErrorContains(t, err, `unknown provider "mystery"`)

	t.Setenv("TYPESAFE_API_KEY", "")
	_, err = pluginsFor(&config.Spec{Models: config.Models{Decision: "typesafe/jev-1.13.0"}}, Options{})
	assert.ErrorContains(t, err, "needs TYPESAFE_API_KEY", "a missing key is an error, not the plugin's panic")

	ps, err := pluginsFor(&config.Spec{Models: config.Models{Decision: "typesafe/jev-1.13.0"}}, Options{Credentials: func(n string) (string, bool) {
		return "k", n == "TYPESAFE_API_KEY"
	}})
	require.NoError(t, err)
	assert.Len(t, ps, 1)

	t.Setenv("OPENROUTER_API_KEY", "")
	router := &config.Spec{Destinations: []config.Destination{{Name: "smart", Endpoint: config.Endpoint{Type: "model", Model: "openrouter/anthropic/claude-sonnet-4.5"}}}}
	_, err = pluginsFor(router, Options{})
	assert.ErrorContains(t, err, "openrouter needs OPENROUTER_API_KEY")
	ps, err = pluginsFor(router, Options{Credentials: func(n string) (string, bool) { return "k", n == "OPENROUTER_API_KEY" }})
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "openrouter", ps[0].Name(), "a generative OpenRouter model: openrouter/<vendor>/<model>")

	custom := &config.Spec{
		Genkit: config.Genkit{SystemOne: config.SystemOne{Custom: []config.SystemOneServer{{Provider: "corp-decisions", BaseURL: "https://d.internal", Headers: map[string]string{"X": "y"}, Images: true}}}},
		Models: config.Models{Decision: "corp-decisions/d1"},
	}
	ps, err = pluginsFor(custom, Options{})
	require.NoError(t, err)
	assert.Len(t, ps, 1)
	assert.True(t, isDecisionModel("corp-decisions/d1", custom.Genkit.SystemOne.Custom))
	assert.False(t, isDecisionModel("corp-decisions/d1", nil), "a custom provider is its spec's, not the process's")
	assert.True(t, isDecisionModel("ollama-decisions/clef", nil))
	assert.False(t, isDecisionModel("ollama/llama3.2", custom.Genkit.SystemOne.Custom))
}

func TestAllModels(t *testing.T) {
	spec := &config.Spec{
		Models: config.Models{Decision: "typesafe/jev-1.13.0", Fallback: config.ModelFallback{Decision: []string{"ollama-decisions/clef"}}},
		Routes: []config.Route{{Name: "r", Router: &config.RouterSpec{Type: "rules", Fallback: []config.RouterSpec{{Type: "ai", Engine: "generative", Model: "googleai/gemini-2.5-flash"}}}}},
	}
	assert.Equal(t, []string{"typesafe/jev-1.13.0", "ollama-decisions/clef", "googleai/gemini-2.5-flash"}, allModels(spec))
}

func TestSystemPrompt(t *testing.T) {
	assert.Equal(t, "route it", systemPrompt("  route it ", false))
	assert.True(t, strings.HasSuffix(systemPrompt("route it", true), store.HistoryPreamble))
	assert.Equal(t, store.HistoryPreamble, systemPrompt("", true))
	assert.NotEmpty(t, GenkitVersion())
}

func TestDecisionResultModelName(t *testing.T) {
	assert.Equal(t, "typesafe/jev-1.13.0", decisionResult{served: "jev-1.13.0"}.model("typesafe/jev-latest"))
	assert.Equal(t, "openrouter-decisions/d1", decisionResult{served: "liquid/d1"}.model("openrouter-decisions/liquid/d1"))
	assert.Equal(t, "llmfake-decisions/clef", decisionResult{}.model("llmfake-decisions/clef"))
}
