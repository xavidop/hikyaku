package llm

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"sync/atomic"

	"github.com/firebase/genkit/go/ai"

	"github.com/xavidop/hikyaku/envelope"
)

// The OpenAI chat completion response shapes, whole and streamed.

type completion struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
	Usage   usage    `json:"usage"`
}

type choice struct {
	Index        int           `json:"index"`
	Message      answerMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

// answerMessage is a choice's message. Content is null when the model
// answered with tool calls and no text.
type answerMessage struct {
	Role         string        `json:"role"`
	Content      *string       `json:"content"`
	ToolCalls    []toolCallOut `json:"tool_calls,omitempty"`
	FunctionCall *functionOut  `json:"function_call,omitempty"`
}

// toolCallOut is a tool call the model asks the caller to run. Index is
// set in a streamed delta only.
type toolCallOut struct {
	Index        *int          `json:"index,omitempty"`
	ID           string        `json:"id"`
	Type         string        `json:"type"`
	Function     functionOut   `json:"function"`
	ExtraContent *extraContent `json:"extra_content,omitempty"`
}

// functionOut is a call's function; Arguments is a JSON string.
type functionOut struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (u *usage) add(o usage) {
	u.PromptTokens += o.PromptTokens
	u.CompletionTokens += o.CompletionTokens
	u.TotalTokens += o.TotalTokens
}

type completionChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
	Usage   *usage        `json:"usage,omitempty"`
}

type chunkChoice struct {
	Index        int        `json:"index"`
	Delta        chunkDelta `json:"delta"`
	FinishReason *string    `json:"finish_reason"`
}

type chunkDelta struct {
	Role         string        `json:"role,omitempty"`
	Content      string        `json:"content,omitempty"`
	ToolCalls    []toolCallOut `json:"tool_calls,omitempty"`
	FunctionCall *functionOut  `json:"function_call,omitempty"`
}

// completionOf is the whole answer: one choice per generation, the usage
// summed.
func completionOf(id string, created int64, model string, resps []*ai.ModelResponse, ts toolSet) completion {
	c := completion{ID: id, Object: "chat.completion", Created: created, Model: model, Choices: make([]choice, len(resps))}
	for i, resp := range resps {
		c.Choices[i] = choiceOf(i, resp, ts)
		c.Usage.add(usageOf(resp))
	}
	return c
}

func usageOf(resp *ai.ModelResponse) usage {
	u := usage{}
	if resp.Usage != nil {
		u.PromptTokens, u.CompletionTokens, u.TotalTokens = resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.TotalTokens
		if u.TotalTokens == 0 {
			u.TotalTokens = u.PromptTokens + u.CompletionTokens
		}
	}
	return u
}

// choiceOf shapes one generation: its text, and the tool calls it asks
// for. A request that allows one call per answer keeps the first; one that
// used the deprecated functions gets it as function_call.
func choiceOf(index int, resp *ai.ModelResponse, ts toolSet) choice {
	text := resp.Text()
	var calls []toolCallOut
	if resp.Message != nil {
		for _, p := range resp.Message.Content {
			if p.IsToolRequest() && p.ToolRequest != nil {
				calls = append(calls, toolCallOf(p))
			}
		}
	}
	if ts.single && len(calls) > 1 {
		calls = calls[:1]
	}
	reason := "stop"
	switch resp.FinishReason {
	case ai.FinishReasonLength:
		reason = "length"
	case ai.FinishReasonBlocked:
		reason = "content_filter"
	}
	msg := answerMessage{Role: "assistant", Content: &text}
	if len(calls) > 0 {
		if text == "" {
			msg.Content = nil
		}
		if ts.legacy {
			msg.FunctionCall = &calls[0].Function
			if reason == "stop" {
				reason = "function_call"
			}
		} else {
			msg.ToolCalls = calls
			if reason == "stop" {
				reason = "tool_calls"
			}
		}
	}
	return choice{Index: index, Message: msg, FinishReason: reason}
}

// toolCallOf is a tool request as a tool call: its ID is the provider's
// (Genkit's Ref), and a Gemini thought signature rides in extra_content.
func toolCallOf(p *ai.Part) toolCallOut {
	tr := p.ToolRequest
	if tr.Ref == "" {
		// Kept on the request, so the streamed and the whole answer agree.
		tr.Ref = newCallID()
	}
	tc := toolCallOut{ID: tr.Ref, Type: "function", Function: functionOut{Name: tr.Name, Arguments: argumentsOf(tr.Input)}}
	var sig string
	switch v := p.Metadata["signature"].(type) {
	case []byte:
		sig = base64.StdEncoding.EncodeToString(v)
	case string:
		sig = v
	}
	if sig != "" {
		tc.ExtraContent = &extraContent{Google: &googleExtra{ThoughtSignature: sig}}
	}
	return tc
}

// argumentsOf is a tool request's input as function.arguments: a JSON
// string.
func argumentsOf(v any) string {
	switch x := v.(type) {
	case nil:
		return "{}"
	case string:
		if json.Valid([]byte(x)) {
			return x
		}
	case json.RawMessage:
		if len(x) == 0 {
			return "{}"
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// newCallID names a tool call the provider returned without an ID (Gemini,
// Ollama), so the caller can answer it.
func newCallID() string { return "call_" + rand.Text() }

// nameCalls gives every tool request the model returns without an ID one of
// its own, before Genkit fills in a UUID.
func nameCalls() ai.GenerateOption {
	return ai.WithUse(ai.MiddlewareFunc(func(context.Context) (*ai.Hooks, error) {
		return &ai.Hooks{WrapModel: func(ctx context.Context, p *ai.ModelParams, next ai.ModelNext) (*ai.ModelResponse, error) {
			resp, err := next(ctx, p)
			if resp != nil && resp.Message != nil {
				for _, part := range resp.Message.Content {
					if part.IsToolRequest() && part.ToolRequest != nil && part.ToolRequest.Ref == "" {
						part.ToolRequest.Ref = newCallID()
					}
				}
			}
			return resp, err
		}}, nil
	}))
}

// choiceStream writes one choice's chunks. A choice's first delta carries
// the role. wrote, shared by every choice of the answer, records that a
// chunk reached the caller.
type choiceStream struct {
	s       envelope.Stream
	id      string
	created int64
	model   string
	index   int
	started atomic.Bool
	text    atomic.Bool // a text chunk was written
	wrote   *atomic.Bool
}

func (c *choiceStream) write(d chunkDelta, finish *string) error {
	if !c.started.Swap(true) {
		d.Role = "assistant"
	}
	b, err := json.Marshal(completionChunk{ID: c.id, Object: "chat.completion.chunk", Created: c.created, Model: c.model,
		Choices: []chunkChoice{{Index: c.index, Delta: d, FinishReason: finish}}})
	if err != nil {
		return err
	}
	if err := c.s.Write(b); err != nil {
		return err
	}
	c.wrote.Store(true)
	return nil
}

// chunk is the streaming callback: the text of each chunk the model
// generates. Tool calls are written from the whole answer (finish): some
// providers stream them in fragments, others whole.
func (c *choiceStream) chunk(_ context.Context, ch *ai.ModelResponseChunk) error {
	text := ch.Text()
	if text == "" {
		return nil
	}
	if err := c.write(chunkDelta{Content: text}, nil); err != nil {
		return err
	}
	c.text.Store(true)
	return nil
}

// finish writes what the model did not stream: its text when it streamed
// none, each tool call, then the choice's last chunk.
func (c *choiceStream) finish(ch choice) error {
	if !c.text.Load() && ch.Message.Content != nil && *ch.Message.Content != "" {
		if err := c.write(chunkDelta{Content: *ch.Message.Content}, nil); err != nil {
			return err
		}
	}
	for k, tc := range ch.Message.ToolCalls {
		tc.Index = &k
		if err := c.write(chunkDelta{ToolCalls: []toolCallOut{tc}}, nil); err != nil {
			return err
		}
	}
	if fc := ch.Message.FunctionCall; fc != nil {
		if err := c.write(chunkDelta{FunctionCall: fc}, nil); err != nil {
			return err
		}
	}
	reason := ch.FinishReason
	return c.write(chunkDelta{}, &reason)
}
