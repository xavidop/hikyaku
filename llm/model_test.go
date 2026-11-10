//go:build llmfake

package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm/llmfake"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

const proxySpec = `
version: 1
name: proxy
sources: [{ name: api, type: stdin }]
destinations:
  - { name: chat, type: model, model: llmfake/chat, generation: { system: "Be brief." } }
  - { name: down, type: model, model: llmfake/down, fallback: [chat], retry: { attempts: 2, backoff: 1ms } }
  - { name: denied, type: model, model: llmfake/denied }
routes: [{ name: r, from: [api], to: [chat] }]
`

func proxy(t *testing.T) (*Factory, *engine.Engine, *metrics.Metrics) {
	t.Helper()
	l, err := config.LoadBytes(context.Background(), "t.yaml", []byte(proxySpec), config.LoadOptions{})
	require.NoError(t, err)
	m := metrics.New()
	f, err := New(context.Background(), l.Spec, Options{Metrics: m, Plugins: llmfake.Plugins()})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{AI: f, NoSources: true, Metrics: m})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })
	return f, e, m
}

func chatEnv(body string) *envelope.Envelope {
	e := envelope.New("api", "openai", []byte(body))
	e.ContentType = "application/json"
	return e
}

func TestModelSinkAnswersAChatCompletion(t *testing.T) {
	_, e, _ := proxy(t)
	rep, err := e.DeliverTo(context.Background(), "chat", chatEnv(`{"model":"x","messages":[{"role":"user","content":"hello there"}]}`))
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	var c completion
	require.NoError(t, json.Unmarshal(rep.Result.Body, &c))
	assert.Equal(t, "chat.completion", c.Object)
	assert.Equal(t, "llmfake/chat", c.Model)
	require.NotNil(t, c.Choices[0].Message.Content)
	assert.Equal(t, "You said: hello there", *c.Choices[0].Message.Content)
	assert.Equal(t, "stop", c.Choices[0].FinishReason)
	assert.Equal(t, 4, c.Usage.CompletionTokens)
	assert.Equal(t, 2+2, c.Usage.PromptTokens, "the destination's system prompt counts too")
	assert.Equal(t, []string{"llmfake/chat"}, rep.Result.Headers["X-Hikyaku-Model"])
}

// A model that is down fails over to the next one (destination fallback).
func TestModelFallback(t *testing.T) {
	_, e, _ := proxy(t)
	rep, err := e.DeliverTo(context.Background(), "down", chatEnv(`{"model":"x","messages":[{"role":"user","content":"hi"}]}`))
	require.NoError(t, err)
	require.NoError(t, rep.Err)
	assert.Equal(t, "chat", rep.ServedBy)
	assert.Equal(t, 2, rep.Hops[0].Attempts, "down is retried (transient) before the fallback")
}

func TestModelErrorsAreClassified(t *testing.T) {
	f, _, _ := proxy(t)
	sink := func(model string) plugin.Sink {
		s, err := f.NewModelSink(context.Background(), plugin.Spec{Name: "m", Config: &config.Endpoint{Type: "model", Model: model}})
		require.NoError(t, err)
		return s
	}
	_, err := sink("llmfake/down").Deliver(context.Background(), chatEnv(`{"messages":[{"role":"user","content":"hi"}]}`))
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, plugin.Tags(err), "model:unavailable")

	_, err = sink("llmfake/denied").Deliver(context.Background(), chatEnv(`{"messages":[{"role":"user","content":"hi"}]}`))
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class, "a refused key is not retried")

	for _, body := range []string{
		`{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"retrieval"}]}`,
		`{"messages":[{"role":"user","content":"hi"}],"n":9}`,
		`{"messages":[{"role":"tool","content":"42"}]}`,
	} {
		_, err = sink("llmfake/chat").Deliver(context.Background(), chatEnv(body))
		class, _ = plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, body)
		assert.Contains(t, plugin.Tags(err), "model:bad_request", body)
	}

	assert.NoError(t, sink("llmfake/chat").Health(context.Background()))
	assert.ErrorContains(t, sink("nobody/none").Health(context.Background()), "is not available")
}

type recStream struct {
	mu     sync.Mutex
	chunks []string
	fail   bool
}

func (r *recStream) Write(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return assert.AnError
	}
	r.chunks = append(r.chunks, string(b))
	return nil
}

func (r *recStream) Committed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.chunks) > 0
}

func TestModelSinkStreams(t *testing.T) {
	f, _, m := proxy(t)
	s, err := f.NewModelSink(context.Background(), plugin.Spec{Name: "chat", Config: &config.Endpoint{Type: "model", Model: "llmfake/chat"}})
	require.NoError(t, err)
	env := chatEnv(`{"model":"x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"a b"}]}`)
	st := &recStream{}
	env.SetStream(st)
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	// "You said: a b" is four words: four content chunks, a final chunk,
	// a usage chunk.
	require.Len(t, st.chunks, 6)
	assert.Contains(t, st.chunks[0], `"role":"assistant"`)
	assert.Contains(t, st.chunks[0], `"content":"You"`)
	assert.Contains(t, st.chunks[1], `"content":" said:"`)
	assert.Contains(t, st.chunks[4], `"finish_reason":"stop"`)
	assert.Contains(t, st.chunks[5], `"usage"`)
	var text strings.Builder
	for _, c := range st.chunks[:4] {
		var ch completionChunk
		require.NoError(t, json.Unmarshal([]byte(c), &ch))
		text.WriteString(ch.Choices[0].Delta.Content)
	}
	assert.Equal(t, "You said: a b", text.String())
	assert.Contains(t, string(res.Body), `"chat.completion"`, "the result is the whole completion too")
	assert.Equal(t, 4.0, testutil.ToFloat64(m.ModelTokens.WithLabelValues("chat", "output")))

	// A stream that breaks is a permanent failure: the caller already has
	// part of the answer, so no other model may take over.
	env = chatEnv(`{"model":"x","stream":true,"messages":[{"role":"user","content":"a"}]}`)
	env.SetStream(&recStream{fail: true})
	_, err = s.Deliver(context.Background(), env)
	require.Error(t, err)
}

func sinkOf(t *testing.T, f *Factory, model string) plugin.Sink {
	t.Helper()
	s, err := f.NewModelSink(context.Background(), plugin.Spec{Name: "chat", Config: &config.Endpoint{Type: "model", Model: model}})
	require.NoError(t, err)
	return s
}

// A fallback after a model streamed part of the answer would write a second
// answer into the same stream: it fails instead, without a model call.
func TestModelSinkRefusesAStreamAnotherModelStarted(t *testing.T) {
	f, _, _ := proxy(t)
	env := chatEnv(`{"model":"x","stream":true,"messages":[{"role":"user","content":"a"}]}`)
	st := &recStream{}
	require.NoError(t, st.Write([]byte(`{"choices":[{"index":0,"delta":{"content":"par"}}]}`)))
	env.SetStream(st)
	before := llmfake.Calls.Load()
	_, err := sinkOf(t, f, "llmfake/chat").Deliver(context.Background(), env)
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, plugin.Tags(err), "model:stream_broken")
	assert.Equal(t, before, llmfake.Calls.Load())
	assert.Len(t, st.chunks, 1)
}

const timeTool = `{"type":"function","function":{"name":"get_time","parameters":{"type":"object","properties":{"zone":{"type":"string"}}}}}`

// ask delivers a chat request and decodes the completion.
func ask(t *testing.T, s plugin.Sink, body string) completion {
	t.Helper()
	res, err := s.Deliver(context.Background(), chatEnv(body))
	require.NoError(t, err)
	var c completion
	require.NoError(t, json.Unmarshal(res.Body, &c))
	return c
}

func TestModelSinkToolCallRoundTrip(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	c := ask(t, s, `{"model":"x","tools":[`+weatherTool+`],"messages":[{"role":"user","content":"get_weather Paris"}]}`)
	require.Len(t, c.Choices, 1)
	ch := c.Choices[0]
	assert.Equal(t, "tool_calls", ch.FinishReason)
	assert.Nil(t, ch.Message.Content, "a call and no text: content is null")
	require.Len(t, ch.Message.ToolCalls, 1)
	call := ch.Message.ToolCalls[0]
	assert.Regexp(t, `^call_[A-Z2-7]{26}$`, call.ID, "the fake, like Gemini, returns no ID: one is made")
	assert.Equal(t, "function", call.Type)
	assert.Equal(t, "get_weather", call.Function.Name)
	assert.JSONEq(t, `{"city":"get_weather Paris"}`, call.Function.Arguments)
	assert.Nil(t, call.Index, "index is for streamed deltas")

	// The caller runs the tool and answers; the model gets the answer under
	// the same ID.
	follow := fmt.Sprintf(`{"model":"x","tools":[%s],"messages":[
		{"role":"user","content":"get_weather Paris"},
		{"role":"assistant","content":null,"tool_calls":[{"id":%q,"type":"function","function":{"name":"get_weather","arguments":%q}}]},
		{"role":"tool","tool_call_id":%q,"content":"{\"temp\":21}"}]}`, weatherTool, call.ID, call.Function.Arguments, call.ID)
	c = ask(t, s, follow)
	ch = c.Choices[0]
	assert.Equal(t, "stop", ch.FinishReason)
	assert.Empty(t, ch.Message.ToolCalls)
	require.NotNil(t, ch.Message.Content)
	assert.Equal(t, `Tools said: get_weather `+call.ID+` {"temp":21}`, *ch.Message.Content)
}

func TestModelSinkParallelToolCalls(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	body := `{"model":"x","tools":[` + weatherTool + `,` + timeTool + `],"messages":[{"role":"user","content":"get_weather and get_time"}]}`
	calls := ask(t, s, body).Choices[0].Message.ToolCalls
	require.Len(t, calls, 2)
	assert.Equal(t, "get_weather", calls[0].Function.Name)
	assert.Equal(t, "get_time", calls[1].Function.Name)
	assert.NotEqual(t, calls[0].ID, calls[1].ID)

	one := ask(t, s, strings.Replace(body, `"model":"x"`, `"model":"x","parallel_tool_calls":false`, 1)).Choices[0].Message.ToolCalls
	require.Len(t, one, 1, "parallel_tool_calls: false keeps the first call")
	assert.Equal(t, "get_weather", one[0].Function.Name)

	// Both answers reach the model in one tool message.
	follow := fmt.Sprintf(`{"model":"x","tools":[%s,%s],"messages":[
		{"role":"user","content":"get_weather and get_time"},
		{"role":"assistant","content":null,"tool_calls":[
			{"id":"call_w","type":"function","function":{"name":"get_weather","arguments":"{}"}},
			{"id":"call_t","type":"function","function":{"name":"get_time","arguments":"{}"}}]},
		{"role":"tool","tool_call_id":"call_w","content":"sunny"},
		{"role":"tool","tool_call_id":"call_t","content":"noon"}]}`, weatherTool, timeTool)
	assert.Equal(t, `Tools said: get_weather call_w "sunny"; get_time call_t "noon"`, *ask(t, s, follow).Choices[0].Message.Content)
}

func TestModelSinkToolChoice(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	tools := `"tools":[` + weatherTool + `,` + timeTool + `]`

	ch := ask(t, s, `{"model":"x",`+tools+`,"tool_choice":"required","messages":[{"role":"user","content":"hello"}]}`).Choices[0]
	require.Len(t, ch.Message.ToolCalls, 1, "required: a call even when nothing asks for one")
	assert.Equal(t, "get_weather", ch.Message.ToolCalls[0].Function.Name)

	ch = ask(t, s, `{"model":"x",`+tools+`,"tool_choice":{"type":"function","function":{"name":"get_time"}},"messages":[{"role":"user","content":"get_weather please"}]}`).Choices[0]
	require.Len(t, ch.Message.ToolCalls, 1)
	assert.Equal(t, "get_time", ch.Message.ToolCalls[0].Function.Name, "the named function is the only one offered")

	ch = ask(t, s, `{"model":"x",`+tools+`,"tool_choice":"none","messages":[{"role":"user","content":"get_weather please"}]}`).Choices[0]
	assert.Empty(t, ch.Message.ToolCalls, "none: no call")
	assert.Equal(t, "You said: get_weather please", *ch.Message.Content)
}

func TestModelSinkDeprecatedFunctions(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	ch := ask(t, s, `{"model":"x","functions":[{"name":"get_time"}],"messages":[{"role":"user","content":"get_time"}]}`).Choices[0]
	assert.Equal(t, "function_call", ch.FinishReason)
	assert.Empty(t, ch.Message.ToolCalls)
	require.NotNil(t, ch.Message.FunctionCall)
	assert.Equal(t, functionOut{Name: "get_time", Arguments: "{}"}, *ch.Message.FunctionCall)

	ch = ask(t, s, `{"model":"x","functions":[{"name":"get_time"}],"messages":[
		{"role":"user","content":"get_time"},
		{"role":"assistant","content":null,"function_call":{"name":"get_time","arguments":"{}"}},
		{"role":"function","name":"get_time","content":"noon"}]}`).Choices[0]
	assert.Equal(t, `Tools said: get_time call_fn1 "noon"`, *ch.Message.Content)
}

func TestModelSinkStreamsToolCalls(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	env := chatEnv(`{"model":"x","stream":true,"stream_options":{"include_usage":true},"tools":[` + weatherTool + `],"messages":[{"role":"user","content":"get_weather Paris"}]}`)
	st := &recStream{}
	env.SetStream(st)
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	// The fake streams the call whole; it reaches the caller once.
	require.Len(t, st.chunks, 3, "the call, the last chunk, the usage")
	var first, last completionChunk
	require.NoError(t, json.Unmarshal([]byte(st.chunks[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(st.chunks[1]), &last))
	assert.Equal(t, "chat.completion.chunk", first.Object)
	d := first.Choices[0].Delta
	assert.Equal(t, "assistant", d.Role)
	require.Len(t, d.ToolCalls, 1)
	require.NotNil(t, d.ToolCalls[0].Index)
	assert.Equal(t, 0, *d.ToolCalls[0].Index)
	assert.Regexp(t, `^call_`, d.ToolCalls[0].ID)
	assert.Equal(t, "function", d.ToolCalls[0].Type)
	assert.Equal(t, "get_weather", d.ToolCalls[0].Function.Name)
	assert.JSONEq(t, `{"city":"get_weather Paris"}`, d.ToolCalls[0].Function.Arguments)
	require.NotNil(t, last.Choices[0].FinishReason)
	assert.Equal(t, "tool_calls", *last.Choices[0].FinishReason)
	assert.Contains(t, st.chunks[2], `"usage"`)

	var c completion
	require.NoError(t, json.Unmarshal(res.Body, &c))
	assert.Equal(t, d.ToolCalls[0].ID, c.Choices[0].Message.ToolCalls[0].ID, "the streamed and the whole answer agree")
}

// llmfake/basic declares tools but neither tool_choice nor media, as an
// Ollama model does.
func TestModelSinkChecksWhatTheModelSupports(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/basic")
	tools := `"tools":[` + weatherTool + `]`

	ch := ask(t, s, `{"model":"x",`+tools+`,"messages":[{"role":"user","content":"get_weather"}]}`).Choices[0]
	assert.Len(t, ch.Message.ToolCalls, 1, "tools work")
	ch = ask(t, s, `{"model":"x",`+tools+`,"tool_choice":"none","messages":[{"role":"user","content":"get_weather"}]}`).Choices[0]
	assert.Empty(t, ch.Message.ToolCalls, "none: the model is offered no tools")
	assert.Equal(t, "You said: get_weather", *ch.Message.Content)

	for body, msg := range map[string]string{
		`{"model":"x",` + tools + `,"tool_choice":"required","messages":[{"role":"user","content":"x"}]}`:                   "does not support tool_choice",
		`{"model":"x","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://x/y.png"}}]}]}`: "does not take media",
	} {
		_, err := s.Deliver(context.Background(), chatEnv(body))
		assert.ErrorContains(t, err, msg)
		assert.Contains(t, plugin.Tags(err), "model:bad_request")
	}
}

func TestModelSinkChoices(t *testing.T) {
	f, _, m := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	before := llmfake.Calls.Load()
	c := ask(t, s, `{"model":"x","n":3,"messages":[{"role":"user","content":"hi"}]}`)
	assert.Equal(t, int64(3), llmfake.Calls.Load()-before, "a generation per choice")
	require.Len(t, c.Choices, 3)
	for i, ch := range c.Choices {
		assert.Equal(t, i, ch.Index)
		assert.Equal(t, "You said: hi", *ch.Message.Content)
		assert.Equal(t, "stop", ch.FinishReason)
	}
	// "You said: hi" is three words, the prompt one: summed over the three.
	assert.Equal(t, usage{PromptTokens: 3, CompletionTokens: 9, TotalTokens: 12}, c.Usage)
	assert.Equal(t, 9.0, testutil.ToFloat64(m.ModelTokens.WithLabelValues("chat", "output")))

	// Streamed: each choice's chunks carry its index, the first its role,
	// the last its finish reason; the usage comes once, at the end.
	env := chatEnv(`{"model":"x","n":3,"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"a b"}]}`)
	st := &recStream{}
	env.SetStream(st)
	_, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	require.Len(t, st.chunks, 3*(4+1)+1, "four words and a last chunk per choice, then the usage")
	text := map[int]string{}
	roles := map[int]int{}
	finished := map[int]string{}
	for _, raw := range st.chunks[:len(st.chunks)-1] {
		var ch completionChunk
		require.NoError(t, json.Unmarshal([]byte(raw), &ch))
		require.Len(t, ch.Choices, 1)
		cc := ch.Choices[0]
		assert.Empty(t, finished[cc.Index], "nothing after a choice's last chunk")
		text[cc.Index] += cc.Delta.Content
		if cc.Delta.Role != "" {
			roles[cc.Index]++
		}
		if cc.FinishReason != nil {
			finished[cc.Index] = *cc.FinishReason
		}
	}
	for i := range 3 {
		assert.Equal(t, "You said: a b", text[i], i)
		assert.Equal(t, 1, roles[i], "the role once, on the first chunk")
		assert.Equal(t, "stop", finished[i])
	}
	var u completionChunk
	require.NoError(t, json.Unmarshal([]byte(st.chunks[len(st.chunks)-1]), &u))
	require.NotNil(t, u.Usage)
	assert.Equal(t, 3*4, u.Usage.CompletionTokens)
	assert.Empty(t, u.Choices)
}

func TestModelSinkChoicesStreamBreaks(t *testing.T) {
	f, _, _ := proxy(t)
	env := chatEnv(`{"model":"x","n":2,"stream":true,"messages":[{"role":"user","content":"a"}]}`)
	env.SetStream(&recStream{fail: true})
	_, err := sinkOf(t, f, "llmfake/chat").Deliver(context.Background(), env)
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Permanent, class)
}

func TestModelSinkMediaParts(t *testing.T) {
	f, _, _ := proxy(t)
	ch := ask(t, sinkOf(t, f, "llmfake/chat"), `{"model":"x","messages":[{"role":"user","content":[
		{"type":"text","text":"listen and read"},
		{"type":"file","file":{"file_data":"JVBERi0=","filename":"a.pdf"}},
		{"type":"input_audio","input_audio":{"data":"SUQz","format":"mp3"}}]}]}`).Choices[0]
	assert.Equal(t, "You said: listen and read [application/pdf audio/mp3]", *ch.Message.Content)
}

// response_format reaches the model through Genkit's output options, with
// tools and choices too.
func TestModelSinkResponseFormat(t *testing.T) {
	f, _, _ := proxy(t)
	s := sinkOf(t, f, "llmfake/chat")
	ch := ask(t, s, `{"model":"x","response_format":{"type":"json_object"},"messages":[{"role":"user","content":"hi"}]}`).Choices[0]
	assert.JSONEq(t, `{"said":"hi"}`, *ch.Message.Content)

	schema := `{"type":"json_schema","json_schema":{"name":"said","schema":{"type":"object","properties":{"said":{"type":"string"}},"required":["said"]}}}`
	c := ask(t, s, `{"model":"x","n":2,"tools":[`+weatherTool+`],"response_format":`+schema+`,"messages":[{"role":"user","content":"hi"}]}`)
	require.Len(t, c.Choices, 2)
	for _, ch := range c.Choices {
		assert.JSONEq(t, `{"said":"hi"}`, *ch.Message.Content)
	}

	// An answer that does not match the schema is bad output: retried.
	other := `{"type":"json_schema","json_schema":{"name":"x","schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}}}`
	_, err := s.Deliver(context.Background(), chatEnv(`{"model":"x","response_format":`+other+`,"messages":[{"role":"user","content":"hi"}]}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, plugin.Tags(err), "model:bad_output")
}
