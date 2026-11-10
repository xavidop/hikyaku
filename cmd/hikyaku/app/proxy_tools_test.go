//go:build llmfake

package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

// startProxy runs a relay from spec, whose %d is the openai source's port
// and %q the dead letter file, and returns the source's base URL.
func startProxy(t *testing.T, spec string) string {
	t.Helper()
	port := freePort(t)
	spec = fmt.Sprintf(spec, port, filepath.Join(t.TempDir(), "dlq.ndjson"))
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
	return base
}

// The OpenAI shapes a client reads.
type (
	oaiCall struct {
		Index    *int   `json:"index"`
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	oaiCompletion struct {
		Model   string `json:"model"`
		Choices []struct {
			Index   int `json:"index"`
			Message struct {
				Role      string    `json:"role"`
				Content   *string   `json:"content"`
				ToolCalls []oaiCall `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	oaiChunk struct {
		Choices []struct {
			Index int `json:"index"`
			Delta struct {
				Role      string    `json:"role"`
				Content   string    `json:"content"`
				ToolCalls []oaiCall `json:"tool_calls"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
)

const proxyToolsSpec = `
version: 1
name: proxy
sources:
  - { name: api, type: openai, listen: "127.0.0.1:%d", models: [fast, smart, local] }
destinations:
  - { name: fast, type: model, model: llmfake/chat }
  - { name: smart, type: model, model: llmfake/down, fallback: [fast], retry: { attempts: 1 } }
  - { name: local, type: model, model: llmfake/basic }
routes:
  - name: chat
    from: [api]
    router:
      type: rules
      rules:
        - { when: 'msg.json.model == "smart"', to: [smart] }
        - { when: 'msg.json.model == "local"', to: [local] }
      default: [fast]
dead_letter: { type: file, path: %q }
`

const weather = `{"type":"function","function":{"name":"get_weather","description":"The weather in a city","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`

// Function calling end to end: the model asks for a tool call, the caller
// runs it and answers under the call's ID, the model reads the answer.
func TestLLMProxyToolCallsEndToEnd(t *testing.T) {
	base := startProxy(t, proxyToolsSpec)
	chat := func(body string) *http.Response {
		r, err := http.Post(base+"/chat/completions", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Body.Close() })
		return r
	}
	decode := func(r *http.Response) oaiCompletion {
		require.Equal(t, 200, r.StatusCode)
		var c oaiCompletion
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		return c
	}

	// smart is down: its fallback answers, tool call included.
	c := decode(chat(`{"model":"smart","tools":[` + weather + `],"tool_choice":"auto","messages":[{"role":"user","content":"get_weather Paris"}]}`))
	require.Len(t, c.Choices, 1)
	ch := c.Choices[0]
	assert.Equal(t, "llmfake/chat", c.Model)
	assert.Equal(t, "tool_calls", ch.FinishReason)
	assert.Equal(t, "assistant", ch.Message.Role)
	assert.Nil(t, ch.Message.Content)
	require.Len(t, ch.Message.ToolCalls, 1)
	call := ch.Message.ToolCalls[0]
	assert.Regexp(t, `^call_`, call.ID)
	assert.Equal(t, "function", call.Type)
	assert.Equal(t, "get_weather", call.Function.Name)
	assert.JSONEq(t, `{"city":"get_weather Paris"}`, call.Function.Arguments)

	follow := fmt.Sprintf(`{"model":"fast","tools":[%s],"messages":[
		{"role":"user","content":"get_weather Paris"},
		{"role":"assistant","content":null,"tool_calls":[{"id":%q,"type":"function","function":{"name":"get_weather","arguments":%q}}]},
		{"role":"tool","tool_call_id":%q,"content":"{\"temp\":21,\"sky\":\"clear\"}"}]}`, weather, call.ID, call.Function.Arguments, call.ID)
	c = decode(chat(follow))
	assert.Equal(t, "stop", c.Choices[0].FinishReason)
	require.NotNil(t, c.Choices[0].Message.Content)
	assert.Equal(t, `Tools said: get_weather `+call.ID+` {"sky":"clear","temp":21}`, *c.Choices[0].Message.Content)

	// Streamed: the call is a delta.tool_calls entry, then the last chunk.
	r := chat(`{"model":"fast","stream":true,"stream_options":{"include_usage":true},"tools":[` + weather + `],"messages":[{"role":"user","content":"get_weather Rome"}]}`)
	require.Equal(t, 200, r.StatusCode)
	assert.Equal(t, "text/event-stream", r.Header.Get("Content-Type"))
	ev := events(t, r)
	require.Len(t, ev, 3+1, "the call, the last chunk, the usage, [DONE]")
	var first, last oaiChunk
	require.NoError(t, json.Unmarshal([]byte(ev[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(ev[1]), &last))
	d := first.Choices[0].Delta
	assert.Equal(t, "assistant", d.Role)
	require.Len(t, d.ToolCalls, 1)
	require.NotNil(t, d.ToolCalls[0].Index)
	assert.Equal(t, 0, *d.ToolCalls[0].Index)
	assert.Regexp(t, `^call_`, d.ToolCalls[0].ID)
	assert.Equal(t, "function", d.ToolCalls[0].Type)
	assert.Equal(t, "get_weather", d.ToolCalls[0].Function.Name)
	assert.JSONEq(t, `{"city":"get_weather Rome"}`, d.ToolCalls[0].Function.Arguments)
	require.NotNil(t, last.Choices[0].FinishReason)
	assert.Equal(t, "tool_calls", *last.Choices[0].FinishReason)
	assert.Contains(t, ev[2], `"usage"`)
	assert.Equal(t, "[DONE]", ev[3])

	// A model that cannot be made to call a tool says so with a 400.
	r = chat(`{"model":"local","tools":[` + weather + `],"tool_choice":"required","messages":[{"role":"user","content":"x"}]}`)
	assert.Equal(t, 400, r.StatusCode)
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&e))
	assert.Equal(t, "bad_request", e.Error.Code)
	assert.Contains(t, e.Error.Message, "does not support tool_choice")
}

// n choices end to end, whole and streamed, and media parts.
func TestLLMProxyChoicesAndMediaEndToEnd(t *testing.T) {
	base := startProxy(t, proxyToolsSpec)
	chat := func(body string) *http.Response {
		r, err := http.Post(base+"/chat/completions", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		t.Cleanup(func() { _ = r.Body.Close() })
		return r
	}

	r := chat(`{"model":"fast","n":2,"messages":[{"role":"user","content":"ping"}]}`)
	require.Equal(t, 200, r.StatusCode)
	var c oaiCompletion
	require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
	require.Len(t, c.Choices, 2)
	for i, ch := range c.Choices {
		assert.Equal(t, i, ch.Index)
		assert.Equal(t, "You said: ping", *ch.Message.Content)
	}
	assert.Equal(t, 2*3, c.Usage.CompletionTokens, "the usage of both generations")

	r = chat(`{"model":"fast","n":2,"stream":true,"messages":[{"role":"user","content":"stream me"}]}`)
	require.Equal(t, 200, r.StatusCode)
	text := map[int]string{}
	finish := map[int]string{}
	for _, data := range events(t, r) {
		if data == "[DONE]" {
			continue
		}
		var ch oaiChunk
		require.NoError(t, json.Unmarshal([]byte(data), &ch))
		require.Len(t, ch.Choices, 1)
		text[ch.Choices[0].Index] += ch.Choices[0].Delta.Content
		if f := ch.Choices[0].FinishReason; f != nil {
			finish[ch.Choices[0].Index] = *f
		}
	}
	assert.Equal(t, map[int]string{0: "You said: stream me", 1: "You said: stream me"}, text)
	assert.Equal(t, map[int]string{0: "stop", 1: "stop"}, finish)

	r = chat(`{"model":"fast","messages":[{"role":"user","content":[
		{"type":"text","text":"read and listen"},
		{"type":"file","file":{"file_data":"data:application/pdf;base64,JVBERi0=","filename":"a.pdf"}},
		{"type":"input_audio","input_audio":{"data":"UklGRg==","format":"wav"}}]}]}`)
	require.Equal(t, 200, r.StatusCode)
	require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
	assert.Equal(t, "You said: read and listen [application/pdf audio/wav]", *c.Choices[0].Message.Content)
}

// events reads server-sent events up to data: [DONE].
func events(t *testing.T, r *http.Response) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			out = append(out, data)
			if data == "[DONE]" {
				break
			}
		}
	}
	return out
}
