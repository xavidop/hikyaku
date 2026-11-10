package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/cmd/hikyaku/app"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

func init() { register() }

func cli(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := app.Main(append(args, "--color", "never"), app.WithStreams(strings.NewReader(""), &out, &errb),
		app.WithEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok }))
	return code, out.String(), errb.String()
}

func TestValidateKnowsThePluginType(t *testing.T) {
	code, out, errOut := cli(t, map[string]string{"CHAT_WEBHOOK_URL": "https://chat.example.com/hook"}, "validate", "-f", "hikyaku.yaml")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "valid")

	bad := strings.Replace(read(t, "hikyaku.yaml"), "username: hikyaku", "usrname: hikyaku", 1)
	f := writeTemp(t, bad)
	code, _, errOut = cli(t, nil, "validate", "-f", f)
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut, "usrname", "a typo in the plugin's options is caught by validate")
}

func TestSchemaDescribesThePluginOptions(t *testing.T) {
	code, out, _ := cli(t, nil, "schema")
	require.Equal(t, 0, code)
	assert.Contains(t, out, `"const": "chat"`)
	assert.Contains(t, out, "webhook_url")
}

func TestSendRetriesThroughThePlugin(t *testing.T) {
	var calls atomic.Int32
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503) // the engine retries the plugin's attempt
			return
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &got)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	spec := strings.Replace(read(t, "hikyaku.yaml"), `"{{secrets.CHAT_WEBHOOK_URL}}"`, srv.URL, 1)
	t.Setenv("CHAT_WEBHOOK_URL", "unused") // mamori's env: provider reads the process environment
	code, out, errOut := cli(t, nil, "send", "-f", writeTemp(t, spec), "--to", "chat", "--data", `{"severity":"high","summary":"disk full"}`)
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, "delivered after 2 attempt(s)")
	assert.Equal(t, "high: disk full", got["text"])
	assert.Equal(t, "hikyaku", got["username"])
}

func TestChatSink(t *testing.T) {
	_, err := newChat(context.Background(), plugin.Spec{Name: "c", Kind: "chat", Options: map[string]any{}})
	assert.ErrorContains(t, err, "webhook_url is required")
	_, err = newChat(context.Background(), plugin.Spec{Name: "c", Kind: "chat", Options: map[string]any{"webhook_url": "x", "text": "{{"}})
	assert.Error(t, err)
	s, err := newChat(context.Background(), plugin.Spec{Name: "c", Kind: "chat", Options: map[string]any{"webhook_url": "http://127.0.0.1:1/hook"}})
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), envelope.New("s", "t", []byte("x")))
	assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
	assert.NoError(t, s.Health(context.Background()))
	assert.NoError(t, s.Close(context.Background()))
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(body)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}
