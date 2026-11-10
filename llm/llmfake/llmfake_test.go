package llmfake

import (
	"context"
	"testing"

	"github.com/firebase/genkit/go/ai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	weather = &ai.ToolDefinition{Name: "get_weather", InputSchema: map[string]any{"type": "object", "properties": map[string]any{
		"city": map[string]any{"type": "string"},
		"days": map[string]any{"type": "integer"},
	}}}
	clock = &ai.ToolDefinition{Name: "get_time"}
)

func chat(t *testing.T, req *ai.ModelRequest) (*ai.ModelResponse, []*ai.ModelResponseChunk) {
	t.Helper()
	var chunks []*ai.ModelResponseChunk
	resp, err := (&Generative{}).chat(context.Background(), req, nil, func(_ context.Context, c *ai.ModelResponseChunk) error {
		chunks = append(chunks, c)
		return nil
	})
	require.NoError(t, err)
	return resp, chunks
}

func TestChatCallsTheToolsTheMessageNames(t *testing.T) {
	req := &ai.ModelRequest{Tools: []*ai.ToolDefinition{weather, clock}, Messages: []*ai.Message{ai.NewUserTextMessage("get_weather in Paris")}}
	resp, chunks := chat(t, req)
	calls := resp.ToolRequests()
	require.Len(t, calls, 1)
	assert.Equal(t, "get_weather", calls[0].ToolRequest.Name)
	assert.Empty(t, calls[0].ToolRequest.Ref, "no call IDs, as with Gemini and Ollama")
	assert.Equal(t, map[string]any{"city": "get_weather in Paris"}, calls[0].ToolRequest.Input, "string properties get the message")
	require.Len(t, chunks, 1, "a stream sends each call whole")
	assert.Equal(t, "get_weather", chunks[0].Content[0].ToolRequest.Name)

	req.Messages = []*ai.Message{ai.NewUserTextMessage("get_weather and get_time")}
	resp, _ = chat(t, req)
	assert.Len(t, resp.ToolRequests(), 2, "parallel calls")

	req.ToolChoice = ai.ToolChoiceNone
	resp, _ = chat(t, req)
	assert.Empty(t, resp.ToolRequests())
	assert.Equal(t, "You said: get_weather and get_time", resp.Text())

	req.ToolChoice = ai.ToolChoiceRequired
	req.Messages = []*ai.Message{ai.NewUserTextMessage("hello")}
	resp, _ = chat(t, req)
	require.Len(t, resp.ToolRequests(), 1, "required: the first tool")
	assert.Equal(t, "get_weather", resp.ToolRequests()[0].ToolRequest.Name)
}

func TestChatSaysWhatTheToolsSaid(t *testing.T) {
	req := &ai.ModelRequest{Tools: []*ai.ToolDefinition{weather, clock}, Messages: []*ai.Message{
		ai.NewUserTextMessage("get_weather and get_time"),
		ai.NewModelMessage(
			ai.NewToolRequestPart(&ai.ToolRequest{Name: "get_weather", Ref: "call_1"}),
			ai.NewToolRequestPart(&ai.ToolRequest{Name: "get_time", Ref: "call_2"})),
		ai.NewMessage(ai.RoleTool, nil,
			ai.NewToolResponsePart(&ai.ToolResponse{Name: "get_weather", Ref: "call_1", Output: map[string]any{"temp": 21}}),
			ai.NewToolResponsePart(&ai.ToolResponse{Name: "get_time", Ref: "call_2", Output: "noon"})),
	}}
	resp, chunks := chat(t, req)
	assert.Empty(t, resp.ToolRequests(), "the tools answered: no new call")
	assert.Equal(t, `Tools said: get_weather call_1 {"temp":21}; get_time call_2 "noon"`, resp.Text())
	assert.Len(t, chunks, 8, "a chunk per word")
}

func TestChatListsTheMediaItGot(t *testing.T) {
	resp, _ := chat(t, &ai.ModelRequest{Messages: []*ai.Message{ai.NewUserMessage(
		ai.NewTextPart("look"), ai.NewMediaPart("image/png", "data:image/png;base64,iVBOR"), ai.NewMediaPart("audio/wav", "data:audio/wav;base64,UklGRg=="))}})
	assert.Equal(t, "You said: look [image/png audio/wav]", resp.Text())
	assert.Equal(t, 5, resp.Usage.OutputTokens, "words")
}

func TestChatAnswersJSONWhenAskedFor(t *testing.T) {
	instructions := ai.NewTextPart("Output should be in JSON format.")
	instructions.Metadata = map[string]any{"purpose": "output"}
	resp, _ := chat(t, &ai.ModelRequest{
		Messages: []*ai.Message{ai.NewUserMessage(ai.NewTextPart("hi"), instructions)},
		Output:   &ai.ModelOutputConfig{Format: ai.OutputFormatJSON},
	})
	assert.Equal(t, `{"said":"hi"}`, resp.Text(), "Genkit's format instructions are not the message")
}
