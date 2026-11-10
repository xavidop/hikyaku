package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// parse reads a chat request the way Deliver does.
func parse(t *testing.T, body string) chatRequest {
	t.Helper()
	var r chatRequest
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	return r
}

func TestChatRequestConversion(t *testing.T) {
	var r chatRequest
	r.Messages = []chatMessage{
		{Role: "developer", Content: []byte(`"be terse"`)},
		{Role: "user", Content: []byte(`[{"type":"text","text":"what is this?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBOR"}}]`)},
		{Role: "assistant", Content: []byte(`"a cat"`)},
	}
	msgs, err := r.genkitMessages("ignored: the request has its own", true)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	assert.Equal(t, ai.RoleSystem, msgs[0].Role)
	assert.Equal(t, ai.RoleUser, msgs[1].Role)
	require.Len(t, msgs[1].Content, 2)
	assert.Equal(t, "image/png", msgs[1].Content[1].ContentType)
	assert.Equal(t, ai.RoleModel, msgs[2].Role)

	r.Messages = []chatMessage{{Role: "user", Content: []byte(`"hi"`)}}
	msgs, err = r.genkitMessages("You are a router.", true)
	require.NoError(t, err)
	assert.Equal(t, ai.RoleSystem, msgs[0].Role, "the destination's system prompt fills in")
	assert.Equal(t, "You are a router.", msgs[0].Text())
}

func TestChatRequestRejects(t *testing.T) {
	for name, body := range map[string]string{
		"no messages":         `{}`,
		"unknown role":        `{"messages":[{"role":"critic","content":"x"}]}`,
		"bad content":         `{"messages":[{"role":"user","content":{"x":1}}]}`,
		"bad part":            `{"messages":[{"role":"user","content":[{"type":"audio"}]}]}`,
		"tool without id":     `{"messages":[{"role":"user","content":"x"},{"role":"tool","content":"42"}]}`,
		"tool answers nobody": `{"messages":[{"role":"user","content":"x"},{"role":"tool","tool_call_id":"call_1","content":"42"}]}`,
		"function nobody":     `{"messages":[{"role":"user","content":"x"},{"role":"function","name":"f","content":"42"}]}`,
		"call without id":     `{"messages":[{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"f"}}]}]}`,
		"call without name":   `{"messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{}}]}]}`,
		"custom call":         `{"messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"custom","function":{"name":"f"}}]}]}`,
		"image in tool reply": `{"messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","function":{"name":"f"}}]},{"role":"tool","tool_call_id":"c","content":[{"type":"image_url","image_url":{"url":"x.png"}}]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, body).genkitMessages("", true)
			assert.Error(t, err)
		})
	}
}

func TestChoices(t *testing.T) {
	n, err := chatRequest{}.choices()
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	for _, ok := range []int{1, 3, maxChoices} {
		n, err := chatRequest{N: &ok}.choices()
		require.NoError(t, err)
		assert.Equal(t, ok, n)
	}
	for _, bad := range []int{0, -1, maxChoices + 1, 128} {
		_, err := chatRequest{N: &bad}.choices()
		assert.ErrorContains(t, err, "n must be between 1 and 8", "n=%d", bad)
	}
}

const weatherTool = `{"type":"function","function":{"name":"get_weather","description":"The weather in a city","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}`

func TestToolSet(t *testing.T) {
	ts, err := parse(t, `{"tools":[`+weatherTool+`,{"type":"function","function":{"name":"get_time","strict":true}}],"tool_choice":"auto"}`).toolSet()
	require.NoError(t, err)
	require.Len(t, ts.defs, 2)
	assert.Equal(t, "get_weather", ts.defs[0].name)
	assert.Equal(t, "The weather in a city", ts.defs[0].description)
	assert.JSONEq(t, `{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`, string(ts.defs[0].schema))
	assert.False(t, ts.defs[0].strict, "strict is off unless the function asks for it, as with OpenAI")
	assert.JSONEq(t, `{"type":"object","properties":{}}`, string(ts.defs[1].schema), "no parameters: an empty object")
	assert.True(t, ts.defs[1].strict)
	assert.Equal(t, ai.ToolChoiceAuto, ts.choice)
	assert.False(t, ts.single)
	assert.False(t, ts.legacy)

	for body, want := range map[string]ai.ToolChoice{
		`{"tools":[` + weatherTool + `]}`:                          "",
		`{"tools":[` + weatherTool + `],"tool_choice":"none"}`:     ai.ToolChoiceNone,
		`{"tools":[` + weatherTool + `],"tool_choice":"required"}`: ai.ToolChoiceRequired,
		`{"tool_choice":"none"}`:                                   "",
		`{"tool_choice":"auto","tools":[]}`:                        "",
	} {
		ts, err := parse(t, body).toolSet()
		require.NoError(t, err, body)
		assert.Equal(t, want, ts.choice, body)
	}

	// A named function is the only one offered, and it must be called.
	ts, err = parse(t, `{"tools":[`+weatherTool+`,{"type":"function","function":{"name":"get_time"}}],"tool_choice":{"type":"function","function":{"name":"get_time"}},"parallel_tool_calls":false}`).toolSet()
	require.NoError(t, err)
	require.Len(t, ts.defs, 1)
	assert.Equal(t, "get_time", ts.defs[0].name)
	assert.Equal(t, ai.ToolChoiceRequired, ts.choice)
	assert.True(t, ts.single, "parallel_tool_calls: false")

	// The deprecated functions and function_call.
	ts, err = parse(t, `{"functions":[{"name":"get_time"},{"name":"get_date"}],"function_call":{"name":"get_date"}}`).toolSet()
	require.NoError(t, err)
	assert.True(t, ts.legacy)
	assert.True(t, ts.single, "a function_call answer holds one call")
	require.Len(t, ts.defs, 1)
	assert.Equal(t, "get_date", ts.defs[0].name)
	assert.Equal(t, ai.ToolChoiceRequired, ts.choice)
	ts, err = parse(t, `{"functions":[{"name":"get_time"}],"function_call":"none"}`).toolSet()
	require.NoError(t, err)
	assert.Equal(t, ai.ToolChoiceNone, ts.choice)
}

func TestToolSetRejects(t *testing.T) {
	for body, msg := range map[string]string{
		`{"tools":[{"type":"retrieval"}]}`:                                                             `type "retrieval" is not supported`,
		`{"tools":[{"type":"function"}]}`:                                                              "function is required",
		`{"tools":[{"type":"function","function":{"name":"get weather"}}]}`:                            "must be 1 to 64 letters",
		`{"tools":[{"type":"function","function":{"name":"a/b"}}]}`:                                    "must be 1 to 64 letters",
		`{"tools":[{"type":"function","function":{"name":"f","parameters":[1]}}]}`:                     "parameters must be a JSON Schema object",
		`{"tools":[` + weatherTool + `,` + weatherTool + `]}`:                                          `declared twice`,
		`{"tools":[` + weatherTool + `],"functions":[{"name":"f"}]}`:                                   "cannot be used together",
		`{"tools":[` + weatherTool + `],"tool_choice":"always"}`:                                       `tool_choice: "always" is not a choice`,
		`{"tools":[` + weatherTool + `],"tool_choice":{"type":"function","function":{}}}`:              "must name a function",
		`{"tools":[` + weatherTool + `],"tool_choice":{"type":"function","function":{"name":"nope"}}}`: `names "nope", which is not one`,
		`{"tool_choice":"required"}`:                                                                   "the request has no tools",
		`{"functions":[{"name":"f"}],"function_call":{"name":"g"}}`:                                    `function_call names "g"`,
	} {
		_, err := parse(t, body).toolSet()
		assert.ErrorContains(t, err, msg, body)
	}
}

func TestToolDefinitionReachesGenkit(t *testing.T) {
	ts, err := parse(t, `{"tools":[`+weatherTool+`]}`).toolSet()
	require.NoError(t, err)
	tool, ok := ts.defs[0].tool().(ai.Tool)
	require.True(t, ok)
	d := tool.Definition()
	assert.Equal(t, "get_weather", d.Name)
	assert.Equal(t, "The weather in a city", d.Description)
	assert.Equal(t, map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}, "required": []any{"city"}}, d.InputSchema)
	assert.Equal(t, false, d.Metadata["strict"], "providers that are strict by default (Anthropic) are told not to be")
	_, err = tool.RunRaw(context.Background(), map[string]any{"city": "Paris"})
	assert.Error(t, err, "hikyaku never runs a tool")
}

func TestToolConversation(t *testing.T) {
	body := `{"messages":[
		{"role":"user","content":"weather in Paris and Rome?"},
		{"role":"assistant","content":null,"tool_calls":[
			{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\",\"days\":12345678901234567890}"}},
			{"id":"call_2","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Rome\"}"},"extra_content":{"google":{"thought_signature":"c2ln"}}}]},
		{"role":"tool","tool_call_id":"call_1","content":"{\"temp\":21}"},
		{"role":"tool","tool_call_id":"call_2","content":[{"type":"text","text":"sunny"}]},
		{"role":"assistant","content":"Paris 21, Rome sunny."}]}`
	r := parse(t, body)
	msgs, err := r.genkitMessages("", true)
	require.NoError(t, err)
	require.Len(t, msgs, 4, "the answers to one turn's calls share a tool message")

	model := msgs[1]
	assert.Equal(t, ai.RoleModel, model.Role)
	require.Len(t, model.Content, 2, "no empty text part next to the calls")
	call := model.Content[0].ToolRequest
	require.NotNil(t, call)
	assert.Equal(t, "get_weather", call.Name)
	assert.Equal(t, "call_1", call.Ref, "the caller's call ID is the Genkit ref")
	assert.Equal(t, map[string]any{"city": "Paris", "days": json.Number("12345678901234567890")}, call.Input)
	assert.Equal(t, "c2ln", model.Content[1].Metadata["signature"], "a Gemini thought signature goes back with its call")

	tool := msgs[2]
	assert.Equal(t, ai.RoleTool, tool.Role)
	require.Len(t, tool.Content, 2)
	first, second := tool.Content[0].ToolResponse, tool.Content[1].ToolResponse
	assert.Equal(t, "get_weather", first.Name, "the name comes from the call it answers")
	assert.Equal(t, "call_1", first.Ref)
	assert.Equal(t, map[string]any{"temp": json.Number("21")}, first.Output, "JSON content is the tool's output")
	assert.Equal(t, "call_2", second.Ref)
	assert.Equal(t, "sunny", second.Output, "plain text stays text")
	assert.Equal(t, ai.RoleModel, msgs[3].Role)

	// Ollama takes each answer as a message of its own.
	msgs, err = r.genkitMessages("", false)
	require.NoError(t, err)
	require.Len(t, msgs, 5)
	assert.Equal(t, ai.RoleTool, msgs[2].Role)
	assert.Equal(t, ai.RoleTool, msgs[3].Role)
}

func TestLegacyFunctionConversation(t *testing.T) {
	r := parse(t, `{"messages":[
		{"role":"user","content":"time?"},
		{"role":"assistant","content":null,"function_call":{"name":"get_time","arguments":"{}"}},
		{"role":"function","name":"get_time","content":"12:00"}]}`)
	msgs, err := r.genkitMessages("", true)
	require.NoError(t, err)
	require.Len(t, msgs, 3)
	call := msgs[1].Content[0].ToolRequest
	require.NotNil(t, call)
	assert.Equal(t, "get_time", call.Name)
	assert.Equal(t, "call_fn1", call.Ref, "a function_call has no ID: one is made up")
	answer := msgs[2].Content[0].ToolResponse
	require.NotNil(t, answer)
	assert.Equal(t, "call_fn1", answer.Ref, "the function message answers it by name")
	assert.Equal(t, "12:00", answer.Output)
}

func TestContentParts(t *testing.T) {
	r := parse(t, `{"messages":[{"role":"user","content":[
		{"type":"text","text":"read these"},
		{"type":"file","file":{"file_data":"data:application/pdf;base64,JVBERi0=","filename":"a.pdf"}},
		{"type":"file","file":{"file_data":"aGVsbG8=","filename":"notes.json"}},
		{"type":"file","file":{"file_data":"JVBERi0="}},
		{"type":"input_audio","input_audio":{"data":"UklGRg==","format":"wav"}},
		{"type":"input_audio","input_audio":{"data":"SUQz","format":"mp3"}},
		{"type":"image_url","image_url":{"url":"https://x/y.webp"}}]}]}`)
	msgs, err := r.genkitMessages("", true)
	require.NoError(t, err)
	parts := msgs[0].Content
	require.Len(t, parts, 7)
	want := []struct{ ct, url string }{
		{"application/pdf", "data:application/pdf;base64,JVBERi0="},
		{"application/json", "data:application/json;base64,aGVsbG8="},
		{"application/pdf", "data:application/pdf;base64,JVBERi0="},
		{"audio/wav", "data:audio/wav;base64,UklGRg=="},
		{"audio/mp3", "data:audio/mp3;base64,SUQz"},
		{"image/webp", "https://x/y.webp"},
	}
	for i, w := range want {
		p := parts[i+1]
		assert.True(t, p.IsMedia(), i)
		assert.Equal(t, w.ct, p.ContentType, i)
		assert.Equal(t, w.url, p.Text, i)
	}

	for part, msg := range map[string]string{
		`{"type":"file","file":{"file_id":"file-abc"}}`:                            "file_id is not supported",
		`{"type":"file","file":{}}`:                                                "needs file.file_data",
		`{"type":"file","file":{"file_data":"not base64!"}}`:                       "must be a data URL or base64",
		`{"type":"input_audio","input_audio":{"data":"UklGRg==","format":"flac"}}`: `"flac" is not supported (wav, mp3)`,
		`{"type":"input_audio","input_audio":{"data":"%%%","format":"wav"}}`:       "must be base64",
		`{"type":"input_audio","input_audio":{"format":"wav"}}`:                    "needs input_audio.data",
		`{"type":"image_url","image_url":{}}`:                                      "needs image_url.url",
	} {
		_, err := parse(t, `{"messages":[{"role":"user","content":[`+part+`]}]}`).genkitMessages("", true)
		assert.ErrorContains(t, err, msg, part)
	}
}

func TestGenerationConfigMerge(t *testing.T) {
	temp, top, max := 0.7, 0.9, 64
	s := &modelSink{gen: config.Generation{Temperature: &temp, MaxTokens: 256}}
	assert.Nil(t, (&modelSink{}).config(chatRequest{}), "nothing set: the model's defaults")
	c := s.config(chatRequest{TopP: &top, MaxTokens: &max, Stop: []byte(`"END"`)})
	require.NotNil(t, c)
	assert.Equal(t, 0.7, c.Temperature, "the destination's default")
	assert.Equal(t, 0.9, c.TopP)
	assert.Equal(t, 64, c.MaxOutputTokens, "the request wins")
	assert.Equal(t, []string{"END"}, c.StopSequences)
	c = s.config(chatRequest{Stop: []byte(`["a","b"]`)})
	assert.Equal(t, []string{"a", "b"}, c.StopSequences)
	assert.Equal(t, 256, c.MaxOutputTokens)
}

func TestMimeOf(t *testing.T) {
	assert.Equal(t, "image/webp", mimeOf("data:image/webp;base64,xx"))
	assert.Equal(t, "text/plain", mimeOf("data:text/plain,hello"))
	assert.Equal(t, "image/png", mimeOf("https://x/y.png"))
	assert.Equal(t, "image/jpeg", mimeOf("https://x/y"))
}

func TestArgumentsAndOutput(t *testing.T) {
	assert.Equal(t, map[string]any{}, arguments(nil))
	assert.Equal(t, map[string]any{}, arguments([]byte(`""`)))
	assert.Equal(t, map[string]any{"a": json.Number("1")}, arguments([]byte(`"{\"a\":1}"`)))
	assert.Equal(t, map[string]any{"a": json.Number("1")}, arguments([]byte(`{"a":1}`)), "an object instead of a string")
	assert.Equal(t, "{not json", arguments([]byte(`"{not json"`)), "kept as text")

	assert.Equal(t, "sunny", toolOutput("sunny"))
	assert.Equal(t, "42 apples", toolOutput("42 apples"))
	assert.Equal(t, json.Number("42"), toolOutput("42"))
	assert.Equal(t, []any{"a", "b"}, toolOutput(`["a","b"]`))
	assert.Equal(t, "null", toolOutput("null"))
	assert.Equal(t, "", toolOutput(""))

	assert.Equal(t, "{}", argumentsOf(nil))
	assert.Equal(t, `{"a":1}`, argumentsOf(map[string]any{"a": 1}))
	assert.Equal(t, `{"a":1}`, argumentsOf(`{"a":1}`), "a JSON string is the arguments")
	assert.Equal(t, `{"a":1}`, argumentsOf(json.RawMessage(`{"a":1}`)))
	assert.Equal(t, "{}", argumentsOf(json.RawMessage(nil)))
	assert.Equal(t, `"x y"`, argumentsOf("x y"))
}

func toolResponse(text string, calls ...*ai.ToolRequest) *ai.ModelResponse {
	var parts []*ai.Part
	if text != "" {
		parts = append(parts, ai.NewTextPart(text))
	}
	for _, c := range calls {
		parts = append(parts, ai.NewToolRequestPart(c))
	}
	return &ai.ModelResponse{Message: ai.NewModelMessage(parts...), FinishReason: ai.FinishReasonStop, Usage: &ai.GenerationUsage{InputTokens: 5, OutputTokens: 2}}
}

func TestCompletionOf(t *testing.T) {
	resp := &ai.ModelResponse{Message: ai.NewModelTextMessage("ok"), FinishReason: ai.FinishReasonLength,
		Usage: &ai.GenerationUsage{InputTokens: 3, OutputTokens: 1}}
	c := completionOf("chatcmpl-1", 1, "p/m", []*ai.ModelResponse{resp}, toolSet{})
	assert.Equal(t, "length", c.Choices[0].FinishReason)
	assert.Equal(t, 4, c.Usage.TotalTokens)
	require.NotNil(t, c.Choices[0].Message.Content)
	assert.Equal(t, "ok", *c.Choices[0].Message.Content)
	c = completionOf("chatcmpl-1", 1, "p/m", []*ai.ModelResponse{{Message: ai.NewModelTextMessage("x"), FinishReason: ai.FinishReasonBlocked}}, toolSet{})
	assert.Equal(t, "content_filter", c.Choices[0].FinishReason)

	// n choices: one per generation, in order, the usage summed.
	c = completionOf("chatcmpl-2", 1, "p/m", []*ai.ModelResponse{toolResponse("a"), toolResponse("b"), toolResponse("c")}, toolSet{})
	require.Len(t, c.Choices, 3)
	for i, text := range []string{"a", "b", "c"} {
		assert.Equal(t, i, c.Choices[i].Index)
		assert.Equal(t, text, *c.Choices[i].Message.Content)
	}
	assert.Equal(t, usage{PromptTokens: 15, CompletionTokens: 6, TotalTokens: 21}, c.Usage)
	b, err := json.Marshal(c)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "tool_calls", "no tool calls: no key")
}

func TestChoiceOfToolCalls(t *testing.T) {
	ch := choiceOf(0, toolResponse("", &ai.ToolRequest{Name: "get_weather", Ref: "toolu_1", Input: map[string]any{"city": "Paris"}}), toolSet{})
	assert.Equal(t, "tool_calls", ch.FinishReason)
	assert.Nil(t, ch.Message.Content, "no text: content is null")
	require.Len(t, ch.Message.ToolCalls, 1)
	tc := ch.Message.ToolCalls[0]
	assert.Equal(t, "toolu_1", tc.ID, "the provider's ID")
	assert.Equal(t, "function", tc.Type)
	assert.Equal(t, "get_weather", tc.Function.Name)
	assert.JSONEq(t, `{"city":"Paris"}`, tc.Function.Arguments)
	b, err := json.Marshal(ch)
	require.NoError(t, err)
	assert.JSONEq(t, `{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"toolu_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":"tool_calls"}`, string(b))

	// Text and two calls; the second without an ID (Gemini) gets one, kept.
	resp := toolResponse("Checking.", &ai.ToolRequest{Name: "a", Ref: "call_a"}, &ai.ToolRequest{Name: "b"})
	resp.Message.Content[2].Metadata = map[string]any{"signature": []byte("sig")}
	ch = choiceOf(1, resp, toolSet{})
	assert.Equal(t, 1, ch.Index)
	assert.Equal(t, "Checking.", *ch.Message.Content)
	require.Len(t, ch.Message.ToolCalls, 2)
	id := ch.Message.ToolCalls[1].ID
	assert.True(t, strings.HasPrefix(id, "call_"), id)
	assert.Equal(t, id, choiceOf(1, resp, toolSet{}).Message.ToolCalls[1].ID, "the same ID every time")
	assert.Equal(t, "{}", ch.Message.ToolCalls[0].Function.Arguments)
	require.NotNil(t, ch.Message.ToolCalls[1].ExtraContent)
	assert.Equal(t, "c2ln", ch.Message.ToolCalls[1].ExtraContent.Google.ThoughtSignature, "base64 of the signature")

	// parallel_tool_calls: false keeps the first call.
	ch = choiceOf(0, resp, toolSet{single: true})
	require.Len(t, ch.Message.ToolCalls, 1)
	assert.Equal(t, "a", ch.Message.ToolCalls[0].Function.Name)

	// A request with functions gets function_call.
	ch = choiceOf(0, toolResponse("", &ai.ToolRequest{Name: "get_time", Input: map[string]any{}}), toolSet{legacy: true, single: true})
	assert.Equal(t, "function_call", ch.FinishReason)
	assert.Empty(t, ch.Message.ToolCalls)
	require.NotNil(t, ch.Message.FunctionCall)
	assert.Equal(t, functionOut{Name: "get_time", Arguments: "{}"}, *ch.Message.FunctionCall)

	// A cut-off answer says so, calls or not.
	resp = toolResponse("", &ai.ToolRequest{Name: "a", Ref: "x"})
	resp.FinishReason = ai.FinishReasonLength
	assert.Equal(t, "length", choiceOf(0, resp, toolSet{}).FinishReason)
}

type recChunks struct {
	mu     sync.Mutex
	chunks []string
}

func (r *recChunks) Write(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks = append(r.chunks, string(b))
	return nil
}

func (r *recChunks) Committed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.chunks) > 0
}

func TestChoiceStream(t *testing.T) {
	rec := &recChunks{}
	var wrote atomic.Bool
	cs := &choiceStream{s: rec, id: "chatcmpl-1", created: 7, model: "p/m", index: 2, wrote: &wrote}
	require.NoError(t, cs.chunk(context.Background(), &ai.ModelResponseChunk{Content: []*ai.Part{ai.NewToolRequestPart(&ai.ToolRequest{Name: "a"})}}))
	assert.Empty(t, rec.chunks, "tool request chunks wait for the whole answer")
	require.NoError(t, cs.chunk(context.Background(), &ai.ModelResponseChunk{Content: []*ai.Part{ai.NewTextPart("Checking.")}}))
	assert.True(t, wrote.Load())
	ch := choiceOf(2, toolResponse("Checking.", &ai.ToolRequest{Name: "a", Ref: "call_a", Input: map[string]any{"x": 1}}, &ai.ToolRequest{Name: "b", Ref: "call_b"}), toolSet{})
	require.NoError(t, cs.finish(ch))
	require.Len(t, rec.chunks, 4, "the text, a chunk per call, the last chunk")
	assert.JSONEq(t, `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":7,"model":"p/m","choices":[{"index":2,"delta":{"role":"assistant","content":"Checking."},"finish_reason":null}]}`, rec.chunks[0])
	assert.JSONEq(t, `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":7,"model":"p/m","choices":[{"index":2,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"a","arguments":"{\"x\":1}"}}]},"finish_reason":null}]}`, rec.chunks[1])
	assert.JSONEq(t, `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":7,"model":"p/m","choices":[{"index":2,"delta":{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"b","arguments":"{}"}}]},"finish_reason":null}]}`, rec.chunks[2])
	assert.JSONEq(t, `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":7,"model":"p/m","choices":[{"index":2,"delta":{},"finish_reason":"tool_calls"}]}`, rec.chunks[3])

	// A model that does not stream: its text, then the last chunk; the role
	// on the first.
	rec = &recChunks{}
	cs = &choiceStream{s: rec, id: "c", model: "p/m", wrote: &wrote}
	require.NoError(t, cs.finish(choiceOf(0, toolResponse("whole"), toolSet{})))
	require.Len(t, rec.chunks, 2)
	assert.Contains(t, rec.chunks[0], `"delta":{"role":"assistant","content":"whole"}`)
	assert.Contains(t, rec.chunks[1], `"delta":{},"finish_reason":"stop"`)

	// The deprecated function_call streams as delta.function_call.
	rec = &recChunks{}
	cs = &choiceStream{s: rec, id: "c", model: "p/m", wrote: &wrote}
	require.NoError(t, cs.finish(choiceOf(0, toolResponse("", &ai.ToolRequest{Name: "get_time"}), toolSet{legacy: true, single: true})))
	require.Len(t, rec.chunks, 2)
	assert.Contains(t, rec.chunks[0], `"delta":{"role":"assistant","function_call":{"name":"get_time","arguments":"{}"}}`)
	assert.Contains(t, rec.chunks[1], `"finish_reason":"function_call"`)
}

func TestEachRunsConcurrentlyAndStopsOnTheFirstFailure(t *testing.T) {
	var started sync.WaitGroup
	started.Add(3)
	err := each(context.Background(), 3, func(ctx context.Context, i int) error {
		started.Done()
		started.Wait() // all three run at the same time
		return nil
	})
	require.NoError(t, err)

	boom := errors.New("boom")
	err = each(context.Background(), 3, func(ctx context.Context, i int) error {
		if i == 1 {
			return boom
		}
		<-ctx.Done() // the others are cancelled
		return ctx.Err()
	})
	assert.ErrorIs(t, err, boom)
}

func TestClassifyModelBadRequests(t *testing.T) {
	for _, err := range []error{
		status.Errorf(status.ErrInvalidArgument, "tool name must match"),
		status.Errorf(ai.ErrUnsupportedByModel, "model does not support media"),
		errors.New(`failed to send request: server returned non-200 status: 400, body: {"error":"does not support tools"}`),
	} {
		got := classifyModel("p/m", err)
		class, _ := plugin.Classify(got)
		assert.Equal(t, plugin.Permanent, class, "%v", err)
		assert.Contains(t, plugin.Tags(got), "model:bad_request", "%v", err)
	}
	got := classifyModel("p/m", status.Errorf(status.ErrResourceExhausted, "slow down"))
	class, _ := plugin.Classify(got)
	assert.Equal(t, plugin.RateLimited, class)
	assert.NotContains(t, plugin.Tags(got), "model:bad_request")
	got = classifyModel("p/m", status.Errorf(status.ErrUnauthenticated, "key"))
	assert.NotContains(t, plugin.Tags(got), "model:bad_request", "a refused key is not the caller's request")
}

func TestSupportsOf(t *testing.T) {
	model := func(s ai.ModelSupports) ai.Model {
		return ai.NewModelAction[any]("p/m", &ai.ModelOptions{Supports: &s}, func(context.Context, *ai.ModelRequest, any, ai.ModelStreamCallback) (*ai.ModelResponse, error) {
			return nil, nil
		})
	}
	assert.Equal(t, supports{tools: true, toolChoice: true, media: true}, supportsOf(model(ai.ModelSupports{Tools: true, ToolChoice: true, Media: true})))
	assert.Equal(t, supports{tools: true}, supportsOf(model(ai.ModelSupports{Tools: true})))
	assert.Equal(t, supports{tools: true, toolChoice: true, media: true}, supportsOf(nil), "unknown: Genkit decides")
}

func TestProviderConfig(t *testing.T) {
	c := &ai.GenerationCommonConfig{Temperature: 0.2, TopP: 0.9, MaxOutputTokens: 64, StopSequences: []string{"END"}}
	assert.Equal(t, map[string]any{"temperature": 0.2, "top_p": 0.9, "num_predict": 64, "stop": []string{"END"}}, providerConfig("ollama/qwen2.5:0.5b", c))
	assert.Equal(t, map[string]any{"temperature": 0.2, "top_p": 0.9, "max_tokens": 64, "stop_sequences": []string{"END"}}, providerConfig("anthropic/claude-sonnet-4-5", c))
	assert.Equal(t, map[string]any{"temperature": 0.2, "top_p": 0.9, "max_completion_tokens": 64, "stop": []string{"END"}}, providerConfig("openai/gpt-4o-mini", c))
	assert.Equal(t, map[string]any{"temperature": 0.2, "topP": 0.9, "maxOutputTokens": 64, "stopSequences": []string{"END"}}, providerConfig("openrouter/anthropic/claude-sonnet-4.5", c))
	assert.Same(t, c, providerConfig("googleai/gemini-2.5-flash", c), "Gemini takes the common names")
	assert.Equal(t, map[string]any{"num_predict": 8}, providerConfig("ollama/x", &ai.GenerationCommonConfig{MaxOutputTokens: 8}), "unset settings stay out")
}
