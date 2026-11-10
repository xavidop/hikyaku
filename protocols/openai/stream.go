package openai

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

// sseStream is the envelope.Stream of a streaming chat request: each Write
// is one server-sent event. The first write sends the response headers.
type sseStream struct {
	mu        sync.Mutex
	w         http.ResponseWriter
	flusher   http.Flusher
	committed bool
	closed    bool
}

func newSSE(w http.ResponseWriter) *sseStream {
	f, _ := w.(http.Flusher)
	return &sseStream{w: w, flusher: f}
}

func (s *sseStream) commitLocked() {
	if s.committed {
		return
	}
	h := s.w.Header()
	h.Set("Content-Type", contentSSE)
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
	s.committed = true
}

// Write sends one event: data: <chunk>.
func (s *sseStream) Write(chunk []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("the stream is closed")
	}
	s.commitLocked()
	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", chunk); err != nil {
		return err
	}
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return nil
}

// Committed reports whether the response started.
func (s *sseStream) Committed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// close stops further writes without sending anything.
func (s *sseStream) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

// done ends the stream the OpenAI way.
func (s *sseStream) done() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.commitLocked()
	_, _ = fmt.Fprint(s.w, "data: [DONE]\n\n")
	if s.flusher != nil {
		s.flusher.Flush()
	}
	s.closed = true
}

// fail ends a stream with an error event.
func (s *sseStream) fail(code, msg string) {
	b, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": "server_error", "code": code}})
	_ = s.Write(b)
	s.done()
}

// raw passes an upstream's event stream through as is.
func (s *sseStream) raw(body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.commitLocked()
	_, _ = s.w.Write(body)
	if s.flusher != nil {
		s.flusher.Flush()
	}
	s.closed = true
}

// fromCompletion streams a whole chat completion (a destination that did
// not stream). Each choice is a content chunk, a chunk per tool call (or
// its function_call), and a final chunk.
func (s *sseStream) fromCompletion(body []byte) error {
	var c struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Index   int `json:"index"`
			Message struct {
				Content      string                       `json:"content"`
				ToolCalls    []map[string]json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage              `json:"function_call"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &c); err != nil || len(c.Choices) == 0 {
		return errors.New("the destination's reply is not a chat completion")
	}
	type delta struct {
		Role         string          `json:"role,omitempty"`
		Content      string          `json:"content,omitempty"`
		ToolCalls    []any           `json:"tool_calls,omitempty"`
		FunctionCall json.RawMessage `json:"function_call,omitempty"`
	}
	chunk := func(index int, d delta, finish *string) []byte {
		b, _ := json.Marshal(map[string]any{"id": c.ID, "object": "chat.completion.chunk", "created": c.Created, "model": c.Model,
			"choices": []any{map[string]any{"index": index, "delta": d, "finish_reason": finish}}})
		return b
	}
	for _, ch := range c.Choices {
		if err := s.Write(chunk(ch.Index, delta{Role: "assistant", Content: ch.Message.Content}, nil)); err != nil {
			return err
		}
		for k, tc := range ch.Message.ToolCalls {
			tc["index"], _ = json.Marshal(k)
			if err := s.Write(chunk(ch.Index, delta{ToolCalls: []any{tc}}, nil)); err != nil {
				return err
			}
		}
		if len(ch.Message.FunctionCall) > 0 && string(ch.Message.FunctionCall) != "null" {
			if err := s.Write(chunk(ch.Index, delta{FunctionCall: ch.Message.FunctionCall}, nil)); err != nil {
				return err
			}
		}
		reason := ch.FinishReason
		if reason == "" {
			reason = "stop"
		}
		if err := s.Write(chunk(ch.Index, delta{}, &reason)); err != nil {
			return err
		}
	}
	return nil
}
