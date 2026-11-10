//go:build llmfake

package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

// The LLM-proxy mode end to end: an OpenAI client talks to the openai
// source, a rules router picks the model destination by the requested
// model, and a model that is down falls back to another.
func TestLLMProxyEndToEnd(t *testing.T) {
	port := freePort(t)
	spec := fmt.Sprintf(`
version: 1
name: proxy
sources:
  - { name: api, type: openai, listen: "127.0.0.1:%d", models: [fast, smart, nowhere] }
destinations:
  - { name: fast, type: model, model: llmfake/chat }
  - { name: smart, type: model, model: llmfake/down, fallback: [fast], retry: { attempts: 1 } }
routes:
  - name: chat
    from: [api]
    router:
      type: rules
      rules:
        - { when: 'msg.json.model == "smart"', to: [smart] }
        - { when: 'msg.json.model == "fast"', to: [fast] }
dead_letter: { type: file, path: %q }
`, port, filepath.Join(t.TempDir(), "dlq.ndjson"))
	l, err := config.LoadBytes(context.Background(), "proxy.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	f, err := llm.New(context.Background(), l.Spec, llm.Options{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{AI: f})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	t.Cleanup(func() { cancel(); assert.NoError(t, <-done) })
	base := fmt.Sprintf("http://127.0.0.1:%d/v1", port)
	require.Eventually(t, func() bool {
		r, err := http.Get(base + "/models")
		if err != nil {
			return false
		}
		_ = r.Body.Close()
		return r.StatusCode == 200
	}, 5*time.Second, 20*time.Millisecond)

	chat := func(body string) *http.Response {
		r, err := http.Post(base+"/chat/completions", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Body.Close() })
		return r
	}

	r := chat(`{"model":"fast","messages":[{"role":"user","content":"ping"}]}`)
	require.Equal(t, 200, r.StatusCode)
	var c map[string]any
	require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
	msg := c["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	assert.Equal(t, "You said: ping", msg["content"])
	assert.Equal(t, "llmfake/chat", c["model"])

	r = chat(`{"model":"smart","messages":[{"role":"user","content":"hard question"}]}`)
	require.Equal(t, 200, r.StatusCode, "smart is down: the fallback model answers")
	require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
	assert.Equal(t, "llmfake/chat", c["model"])

	errCode := func(r *http.Response) string {
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&e))
		return e.Error.Code
	}
	r = chat(`{"model":"nowhere","messages":[{"role":"user","content":"hi"}]}`)
	assert.Equal(t, 404, r.StatusCode, "no rule routes this model")
	assert.Equal(t, "model_not_found", errCode(r))
	r = chat(`{"model":"fast","n":9,"messages":[{"role":"user","content":"hi"}]}`)
	assert.Equal(t, 400, r.StatusCode, "the model destination's bad request reaches the caller")
	assert.Equal(t, "bad_request", errCode(r))

	r = chat(`{"model":"fast","stream":true,"messages":[{"role":"user","content":"stream me"}]}`)
	require.Equal(t, 200, r.StatusCode)
	assert.Equal(t, "text/event-stream", r.Header.Get("Content-Type"))
	var text strings.Builder
	var events int
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		events++
		if data == "[DONE]" {
			break
		}
		var ch struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		require.NoError(t, json.Unmarshal([]byte(data), &ch))
		if len(ch.Choices) > 0 {
			text.WriteString(ch.Choices[0].Delta.Content)
		}
	}
	assert.Equal(t, "You said: stream me", text.String())
	assert.Equal(t, 4+1+1, events, "a chunk per word, the final chunk, [DONE]")
}
