package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// compatServer is an OpenAI-compatible server that records what it is sent.
type compatServer struct {
	*httptest.Server
	mu    sync.Mutex
	path  string
	auth  string
	model string
}

func newCompatServer(t *testing.T) *compatServer {
	t.Helper()
	s := &compatServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(body, &req)
		s.mu.Lock()
		s.path, s.auth, s.model = r.URL.Path, r.Header.Get("Authorization"), req.Model
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"`+req.Model+`",
			"choices":[{"index":0,"message":{"role":"assistant","content":"hi from the local server"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *compatServer) seen() (path, auth, model string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path, s.auth, s.model
}

// askOpenAI sends one chat completion through an openai/<model> destination
// and returns the answer's text.
func askOpenAI(t *testing.T, spec *config.Spec, o Options) string {
	t.Helper()
	f, err := New(context.Background(), spec, o)
	require.NoError(t, err)
	s, err := f.NewModelSink(context.Background(), plugin.Spec{Name: "local", Config: &spec.Destinations[0].Endpoint})
	require.NoError(t, err)
	env := envelope.New("api", "openai", []byte(`{"model":"local","messages":[{"role":"user","content":"hello"}]}`))
	env.ContentType = "application/json"
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	var c struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	require.NoError(t, json.Unmarshal(res.Body, &c))
	require.Len(t, c.Choices, 1)
	return c.Choices[0].Message.Content
}

func openAISpec(baseURL string) *config.Spec {
	return &config.Spec{
		Genkit:       config.Genkit{OpenAI: config.OpenAI{BaseURL: baseURL}},
		Destinations: []config.Destination{{Name: "local", Endpoint: config.Endpoint{Type: "model", Model: "openai/qwen2.5-7b-instruct"}}},
	}
}

func TestOpenAIBaseURL(t *testing.T) {
	srv := newCompatServer(t)
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI_BASE_URL", "")

	key := Options{Credentials: func(n string) (string, bool) { return "sk-local", n == "OPENAI_API_KEY" }}
	assert.Equal(t, "hi from the local server", askOpenAI(t, openAISpec(srv.URL+"/v1"), key))
	path, auth, model := srv.seen()
	assert.Equal(t, "/v1/chat/completions", path)
	assert.Equal(t, "Bearer sk-local", auth)
	assert.Equal(t, "qwen2.5-7b-instruct", model, "any model name resolves on a server of your own")

	// Without a key, a server other than OpenAI's is called with none.
	askOpenAI(t, openAISpec(srv.URL+"/v1"), Options{})
	_, auth, _ = srv.seen()
	assert.Empty(t, auth)

	// OPENAI_BASE_URL is the fallback for base_url.
	t.Setenv("OPENAI_BASE_URL", srv.URL+"/compat/v1")
	askOpenAI(t, openAISpec(""), key)
	path, _, _ = srv.seen()
	assert.Equal(t, "/compat/v1/chat/completions", path)
}

func TestOpenAIPluginKey(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI_BASE_URL", "")
	noKey := func(plugin string, envs ...string) (string, error) { return "", assert.AnError }

	_, err := openAIPlugin(config.OpenAI{}, noKey)
	require.ErrorIs(t, err, assert.AnError, "OpenAI's own API needs a key")
	_, err = openAIPlugin(config.OpenAI{BaseURL: DefaultOpenAIBaseURL + "/"}, noKey)
	require.ErrorIs(t, err, assert.AnError, "the default URL with a trailing slash too")

	p, err := openAIPlugin(config.OpenAI{BaseURL: "http://localhost:8000/v1"}, noKey)
	require.NoError(t, err)
	assert.Equal(t, "openai", p.Name())
}
