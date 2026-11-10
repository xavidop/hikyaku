//go:build llmfake

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/llm/llmfake"
)

// readiness.models: the relay is not ready while every model of an AI
// route's chain fails (a chain whose fallback model answers is fine), and
// /readyz reuses a probe instead of calling the models each time.
func TestReadinessModels(t *testing.T) {
	dir := t.TempDir()
	src, adm := freeAddr(t), freeAddr(t)
	spec := filepath.Join(dir, "hikyaku.yaml")
	require.NoError(t, os.WriteFile(spec, []byte(fmt.Sprintf(`
version: 1
name: ready
genkit: { plugins: [llmfake] }
sources: [{ name: in, type: http, listen: %q }]
destinations: [{ name: a, type: stdout }, { name: b, type: stdout }]
routes:
  - name: answers
    from: [in]
    router:
      type: ai
      model: llmfake-decisions/down
      fallback_models: [llmfake-decisions/clef]
      candidates: [{ name: a, description: "Money" }, { name: b, description: "Bugs" }]
      fallback: [{ type: static, to: [a] }]
  - name: dark
    from: [in]
    router:
      type: ai
      model: llmfake-decisions/down
      candidates: [{ name: a, description: "Money" }, { name: b, description: "Bugs" }]
      fallback: [{ type: static, to: [a] }]
server:
  admin: { listen: %q }
  readiness: { models: true }
`, src, adm)), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	code := make(chan int, 1)
	go func() {
		code <- Main([]string{"run", "-f", spec, "--color", "never"}, WithContext(ctx), WithStreams(strings.NewReader(""), &out, &out))
	}()
	readyz := func() (int, map[string]any) {
		t.Helper()
		var status int
		var body map[string]any
		require.Eventually(t, func() bool {
			resp, err := http.Get("http://" + adm + "/readyz")
			if err != nil {
				return false
			}
			defer func() { _ = resp.Body.Close() }()
			status, body = resp.StatusCode, nil
			return json.NewDecoder(resp.Body).Decode(&body) == nil
		}, 15*time.Second, 50*time.Millisecond, out.String())
		return status, body
	}

	before := llmfake.Calls.Load()
	status, body := readyz()
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body["models"], "route dark: every model failed")
	assert.NotContains(t, body["models"], "route answers", "its fallback model answers")
	probed := llmfake.Calls.Load()
	assert.Greater(t, probed, before, "the first probe calls the models")

	status, _ = readyz()
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, probed, llmfake.Calls.Load(), "a second /readyz within 30s reuses the probe")

	cancel()
	select {
	case c := <-code:
		assert.Equal(t, 0, c, out.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
