package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func start(t *testing.T, src config.Source, queue int) (string, chan *envelope.Envelope) {
	t.Helper()
	src.Listen = "127.0.0.1:0"
	s, err := newSource(context.Background(), plugin.Spec{Name: "api", Kind: "openai", Config: &src})
	require.NoError(t, err)
	out := make(chan *envelope.Envelope, queue)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	t.Cleanup(func() { cancel(); assert.NoError(t, <-done) })
	require.NotEmpty(t, s.Addr())
	require.NoError(t, s.Health(context.Background()))
	return "http://" + s.Addr(), out
}

func post(t *testing.T, url, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func decode(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))
	return m
}

const chatBody = `{"model":"fast","messages":[{"role":"user","content":"hi"}]}`

const completionBody = `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"llmfake/chat","choices":[{"index":0,"message":{"role":"assistant","content":"You said: hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":3,"total_tokens":4}}`

func reply(env *envelope.Envelope, dest, body string) {
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
		dest: {Status: 200, Body: []byte(body), ContentType: "application/json"},
	}})
}

func TestModels(t *testing.T) {
	url, _ := start(t, config.Source{Models: []string{"auto", "fast"}}, 1)
	resp, err := http.Get(url + "/v1/models")
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, 200, resp.StatusCode)
	m := decode(t, resp)
	assert.Equal(t, "list", m["object"])
	data := m["data"].([]any)
	require.Len(t, data, 2)
	assert.Equal(t, "auto", data[0].(map[string]any)["id"])
}

func TestChatRoundTrip(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	go func() {
		env := <-out
		reply(env, "fast", completionBody)
	}()
	resp := post(t, url+"/v1/chat/completions", chatBody, map[string]string{"Authorization": "Bearer sk-caller", "Idempotency-Key": "req-1", "X-Trace": "t"})
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	assert.Equal(t, "req-1", resp.Header.Get("X-Request-Id"))
	m := decode(t, resp)
	assert.Equal(t, "chat.completion", m["object"])
}

func TestChatEnvelope(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	got := make(chan *envelope.Envelope, 1)
	go func() {
		env := <-out
		got <- env.Clone()
		reply(env, "fast", completionBody)
	}()
	post(t, url+"/v1/chat/completions", chatBody, map[string]string{"Authorization": "Bearer sk-caller", "X-Trace": "t"})
	env := <-got
	assert.Equal(t, "openai", env.Protocol)
	assert.Equal(t, "api", env.Source)
	assert.Equal(t, "fast", env.Subject, "the requested model is the subject")
	assert.JSONEq(t, chatBody, string(env.Body))
	assert.Equal(t, "t", env.Header("X-Trace"))
	assert.Empty(t, env.Header("Authorization"), "the caller's key never reaches a destination")
	model, _ := env.Attribute("openai.model")
	assert.Equal(t, "fast", model)
	stream, _ := env.Attribute("openai.stream")
	assert.Equal(t, false, stream)
}

// readEvents reads server-sent events up to data: [DONE].
func readEvents(t *testing.T, resp *http.Response) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			out = append(out, data)
			if data == "[DONE]" {
				break
			}
		}
	}
	return out
}

// A model destination writes each chunk to the envelope's stream; the
// source ends it with [DONE].
func TestChatStreamsChunks(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	go func() {
		env := <-out
		st := env.Stream()
		_ = st.Write([]byte(`{"choices":[{"index":0,"delta":{"role":"assistant","content":"You"}}]}`))
		_ = st.Write([]byte(`{"choices":[{"index":0,"delta":{"content":" said"}}]}`))
		reply(env, "fast", completionBody)
	}()
	resp := post(t, url+"/v1/chat/completions", `{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	ev := readEvents(t, resp)
	require.Len(t, ev, 3)
	assert.Contains(t, ev[0], `"You"`)
	assert.Contains(t, ev[1], `" said"`)
	assert.Equal(t, "[DONE]", ev[2])
}

// A destination that does not stream is answered as one chunk.
func TestChatStreamFromAWholeCompletion(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	go func() { reply(<-out, "upstream", completionBody) }()
	resp := post(t, url+"/v1/chat/completions", `{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	ev := readEvents(t, resp)
	require.Len(t, ev, 3)
	assert.Contains(t, ev[0], `"content":"You said: hi"`)
	assert.Contains(t, ev[1], `"finish_reason":"stop"`)
	assert.Equal(t, "[DONE]", ev[2])
}

// Every choice of a whole completion is streamed, its tool calls as
// delta.tool_calls entries with their index.
func TestChatStreamFromAWholeCompletionWithToolCalls(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	body := `{"id":"chatcmpl-2","object":"chat.completion","created":1,"model":"m","choices":[
		{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[
			{"id":"call_a","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}},
			{"id":"call_b","type":"function","function":{"name":"get_time","arguments":"{}"}}]},"finish_reason":"tool_calls"},
		{"index":1,"message":{"role":"assistant","content":null,"function_call":{"name":"get_time","arguments":"{}"}},"finish_reason":"function_call"}]}`
	go func() { reply(<-out, "upstream", body) }()
	resp := post(t, url+"/v1/chat/completions", `{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	ev := readEvents(t, resp)
	require.Len(t, ev, 8)
	assert.JSONEq(t, `{"id":"chatcmpl-2","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`, ev[0])
	assert.JSONEq(t, `{"id":"chatcmpl-2","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]},"finish_reason":null}]}`, ev[1])
	chunk := func(index int, delta, finish string) string {
		return `{"id":"chatcmpl-2","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":` + strconv.Itoa(index) + `,"delta":` + delta + `,"finish_reason":` + finish + `}]}`
	}
	assert.JSONEq(t, chunk(0, `{"tool_calls":[{"index":1,"id":"call_b","type":"function","function":{"name":"get_time","arguments":"{}"}}]}`, "null"), ev[2])
	assert.JSONEq(t, chunk(0, `{}`, `"tool_calls"`), ev[3])
	assert.JSONEq(t, chunk(1, `{"role":"assistant"}`, "null"), ev[4])
	assert.JSONEq(t, chunk(1, `{"function_call":{"name":"get_time","arguments":"{}"}}`, "null"), ev[5])
	assert.JSONEq(t, chunk(1, `{}`, `"function_call"`), ev[6])
	assert.Equal(t, "[DONE]", ev[7])
}

func TestChatStreamPassesAnUpstreamStreamThrough(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	upstream := "data: {\"x\":1}\n\ndata: [DONE]\n\n"
	go func() {
		env := <-out
		env.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered, Replies: map[string]*envelope.Reply{
			"upstream": {Status: 200, Body: []byte(upstream), ContentType: "text/event-stream"},
		}})
	}()
	resp := post(t, url+"/v1/chat/completions", `{"model":"x","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, upstream, string(body))
}

func TestChatFailures(t *testing.T) {
	cases := []struct {
		name string
		o    envelope.Outcome
		code int
		typ  string
	}{
		{"unrouted", envelope.Outcome{Status: envelope.Unrouted}, 404, "model_not_found"},
		{"no route", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{plugin.TagNoRoute}, "no rule matched")}, 404, "model_not_found"},
		{"schema", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{plugin.TagSchema}, "schema: missing property 'model'")}, 400, "schema"},
		{"transform", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{plugin.TagTransform}, "jq failed")}, 500, "transform_failed"},
		{"rate limited", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.RateLimited, nil, "slow down")}, 429, "rate_limited"},
		{"bad request", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{"model:bad_request"}, "tools are not supported")}, 400, "bad_request"},
		{"timeout", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Timeout, nil, "slow")}, 504, "timeout"},
		{"down", envelope.Outcome{Status: envelope.Failed, Err: plugin.Errorf(plugin.Transient, nil, "every model is down")}, 503, "unavailable"},
		{"rejected", envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, nil, "no")}, 502, "upstream_error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			url, out := start(t, config.Source{}, 1)
			go func() { (<-out).Ack(context.Background(), c.o) }()
			resp := post(t, url+"/v1/chat/completions", chatBody, nil)
			assert.Equal(t, c.code, resp.StatusCode)
			m := decode(t, resp)
			assert.Equal(t, c.typ, m["error"].(map[string]any)["code"])
		})
	}
}

// A stream that already started ends with an error event when the request
// fails after all.
func TestChatStreamFailsAfterCommit(t *testing.T) {
	url, out := start(t, config.Source{}, 1)
	go func() {
		env := <-out
		_ = env.Stream().Write([]byte(`{"choices":[{"index":0,"delta":{"content":"par"}}]}`))
		env.Ack(context.Background(), envelope.Outcome{Status: envelope.DeadLettered, Err: plugin.Errorf(plugin.Permanent, []string{"model:stream_broken"}, "the model hung up")})
	}()
	resp := post(t, url+"/v1/chat/completions", `{"model":"x","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	assert.Equal(t, 200, resp.StatusCode)
	ev := readEvents(t, resp)
	require.Len(t, ev, 3)
	assert.Contains(t, ev[1], `"error"`)
	assert.Contains(t, ev[1], "the model hung up")
	assert.Equal(t, "[DONE]", ev[2])
}

func TestChatRejections(t *testing.T) {
	url, out := start(t, config.Source{Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"sk-relay"}}}}, 0)
	resp := post(t, url+"/v1/chat/completions", chatBody, nil)
	assert.Equal(t, 401, resp.StatusCode, "the relay's own key is required")
	resp = post(t, url+"/v1/chat/completions", `not json`, map[string]string{"Authorization": "Bearer sk-relay"})
	assert.Equal(t, 400, resp.StatusCode)
	resp = post(t, url+"/v1/chat/completions", `{"model":"x","messages":[]}`, map[string]string{"Authorization": "Bearer sk-relay"})
	assert.Equal(t, 400, resp.StatusCode)
	resp = post(t, url+"/v1/chat/completions", chatBody, map[string]string{"Authorization": "Bearer sk-relay"})
	assert.Equal(t, 503, resp.StatusCode, "a full queue pushes back")
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
	resp = post(t, url+"/v1/embeddings", `{}`, map[string]string{"Authorization": "Bearer sk-relay"})
	assert.Equal(t, 404, resp.StatusCode)
	r, err := http.Get(url + "/v1/chat/completions")
	require.NoError(t, err)
	_ = r.Body.Close()
	assert.Equal(t, 405, r.StatusCode)
	r, err = http.Get(url + "/v1/models")
	require.NoError(t, err)
	_ = r.Body.Close()
	assert.Equal(t, 401, r.StatusCode)
	assert.Len(t, out, 0)
}

func TestChatWaitRunsOut(t *testing.T) {
	url, out := start(t, config.Source{Timeout: config.Duration(50 * time.Millisecond)}, 1)
	go func() { <-out }() // never answered
	resp := post(t, url+"/v1/chat/completions", chatBody, nil)
	assert.Equal(t, 504, resp.StatusCode)
}

func TestNewSourceErrors(t *testing.T) {
	_, err := newSource(context.Background(), plugin.Spec{Name: "api", Config: &config.Source{}})
	assert.ErrorContains(t, err, "listen is required")
	_, err = newSource(context.Background(), plugin.Spec{Name: "api", Config: &config.Source{Listen: ":0", Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X-Sig", Secret: "s"}}}})
	assert.ErrorContains(t, err, "use auth.bearer")
	_, err = newSource(context.Background(), plugin.Spec{Name: "api"})
	assert.ErrorContains(t, err, "missing config")
}

func TestReplyOfPrefersTheCompletion(t *testing.T) {
	r := replyOf(envelope.Outcome{Replies: map[string]*envelope.Reply{
		"audit": {Body: []byte(`{"ok":true}`)},
		"model": {Body: []byte(completionBody)},
	}})
	require.NotNil(t, r)
	assert.Contains(t, string(r.Body), "chat.completion")
	assert.Nil(t, replyOf(envelope.Outcome{}))
}
