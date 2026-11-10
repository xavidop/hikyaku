//go:build integration

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm"
)

// ollamaServer is a running Ollama: HIKYAKU_OLLAMA_URL, else a container
// when HIKYAKU_OLLAMA_IT=1. Without either the Ollama tests are skipped:
// they pull models.
func ollamaServer(t *testing.T) string {
	t.Helper()
	if url := os.Getenv("HIKYAKU_OLLAMA_URL"); url != "" {
		return strings.TrimRight(url, "/")
	}
	if os.Getenv("HIKYAKU_OLLAMA_IT") != "1" {
		t.Skip("set HIKYAKU_OLLAMA_URL (a running Ollama) or HIKYAKU_OLLAMA_IT=1 (a container) to run the Ollama tests")
	}
	ctx := context.Background()
	ctr, err := tcollama.Run(ctx, "ollama/ollama:latest")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)
	return url
}

// ollamaPull pulls a model into the server (a no-op when it has it).
func ollamaPull(t *testing.T, url, model string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	body := strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":false}`, model))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/api/pull", body)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "ollama pull %s: %s", model, b)
}

// The local decision-model fallback on a real Ollama (design §13): a
// System One decision model served by Ollama decides a route.
// HIKYAKU_OLLAMA_DECISION_MODEL names it (clef, about 20 GB, so it is not
// pulled unless asked for).
func TestIntegrationOllamaDecisionModel(t *testing.T) {
	model := os.Getenv("HIKYAKU_OLLAMA_DECISION_MODEL")
	if model == "" {
		t.Skip("set HIKYAKU_OLLAMA_DECISION_MODEL (such as clef) to pull a decision model")
	}
	url := ollamaServer(t)
	ollamaPull(t, url, model)
	ctx := context.Background()

	doc := fmt.Sprintf(`
version: 1
name: ollama-it
genkit: { ollama: { url: %q } }
models: { decision: ollama-decisions/%s }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: billing, type: stdout }
  - { name: support, type: stdout }
routes:
  - name: triage
    from: [in]
    router:
      type: ai
      engine: decision
      instructions: Route customer events to the team that must see them.
      state: "{{ .bodyText }}"
      candidates:
        - { name: billing, description: "Payments, invoices, refunds, charges" }
        - { name: support, description: "Bugs, outages, errors, how-to questions" }
      timeout: 120s
      fallback: [{ type: static, to: [support] }]
`, url, model)
	l, err := config.LoadBytes(ctx, "ollama.yaml", []byte(doc), config.LoadOptions{})
	require.NoError(t, err)
	f, err := llm.New(ctx, l.Spec, llm.Options{})
	require.NoError(t, err)
	e, err := engine.New(ctx, l.Spec, engine.Options{AI: f, NoSources: true})
	require.NoError(t, err)
	defer e.Close(ctx)
	env := envelope.New("in", "stdin", []byte(`{"text":"I was charged twice for my invoice and want a refund"}`))
	env.ContentType = "application/json"
	reps, err := e.DryRun(ctx, "in", env)
	require.NoError(t, err)
	require.Len(t, reps, 1)
	d := reps[0].Decision
	require.NotNil(t, d)
	assert.Equal(t, "ollama-decisions/"+model, d.Model, "the local decision model answered, not the fallback")
	assert.Equal(t, []string{"billing"}, d.Destinations)
	assert.Greater(t, d.Confidence, 0.5)
}

// The LLM-proxy mode on a real local model: a model destination on an
// Ollama chat model (HIKYAKU_OLLAMA_CHAT_MODEL, default qwen2.5:0.5b, about
// 400 MB) answers a chat request, whole and streamed, with the
// destination's generation settings in the shape Ollama validates.
func TestIntegrationOllamaChatModel(t *testing.T) {
	url := ollamaServer(t)
	model := os.Getenv("HIKYAKU_OLLAMA_CHAT_MODEL")
	if model == "" {
		model = "qwen2.5:0.5b"
	}
	ollamaPull(t, url, model)
	ctx := context.Background()
	doc := fmt.Sprintf(`
version: 1
name: ollama-chat-it
genkit: { ollama: { url: %q } }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: local, type: model, model: "ollama/%s", timeout: 120s, generation: { max_tokens: 24, temperature: 0.1 } }
routes: [{ name: r, from: [in], to: [local] }]
`, url, model)
	l, err := config.LoadBytes(ctx, "ollama-chat.yaml", []byte(doc), config.LoadOptions{})
	require.NoError(t, err)
	f, err := llm.New(ctx, l.Spec, llm.Options{})
	require.NoError(t, err)
	e, err := engine.New(ctx, l.Spec, engine.Options{AI: f, NoSources: true})
	require.NoError(t, err)
	defer e.Close(ctx)

	env := envelope.New("in", "openai", []byte(`{"model":"x","messages":[{"role":"user","content":"Reply with one word: the color of the sky on a clear day."}]}`))
	env.ContentType = "application/json"
	rep, err := e.DeliverTo(ctx, "local", env)
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	var c struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(rep.Result.Body, &c))
	assert.Equal(t, "ollama/"+model, c.Model)
	assert.NotEmpty(t, strings.TrimSpace(c.Choices[0].Message.Content))
	assert.LessOrEqual(t, c.Usage.CompletionTokens, 24, "max_tokens reached Ollama as num_predict")

	env = envelope.New("in", "openai", []byte(`{"model":"x","stream":true,"messages":[{"role":"user","content":"Count from one to five in words."}]}`))
	st := &chunks{}
	env.SetStream(st)
	rep, err = e.DeliverTo(ctx, "local", env)
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	assert.Greater(t, st.n, 2, "the answer arrived in chunks")
}

// chunks is an envelope stream that counts what it gets.
type chunks struct{ n int }

func (c *chunks) Write([]byte) error { c.n++; return nil }
func (c *chunks) Committed() bool    { return c.n > 0 }
