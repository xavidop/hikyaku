//go:build llmfake

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/llm/llmfake"
)

// triageSpec is a relay whose route is static (model "") or decided by
// an AI router on model: the generative engine for llmfake/ models, the
// decision engine for the others.
func triageSpec(src, adm, a, b, model string) string {
	route := `routes: [{ name: triage, from: [in], to: [a] }]`
	genkit := ""
	if model != "" {
		engine := "decision"
		if strings.HasPrefix(model, "llmfake/") {
			engine = "generative"
		}
		genkit = "genkit: { plugins: [llmfake] }"
		route = fmt.Sprintf(`routes:
  - name: triage
    from: [in]
    router:
      type: ai
      engine: %s
      model: %s
      candidates:
        - { name: a, description: "Payments, invoices, refunds" }
        - { name: b, description: "Bugs, crashes, errors" }
      fallback: [{ type: static, to: [a] }]`, engine, model)
	}
	return fmt.Sprintf(`
version: 1
name: reload-ai
%s
sources: [{ name: in, type: http, listen: %q, ack: accepted }]
destinations: [{ name: a, type: http, url: %q }, { name: b, type: http, url: %q }]
%s
server: { admin: { listen: %q } }
`, genkit, src, a, b, route, adm)
}

// recorder is a destination that keeps what it got.
type recorder struct {
	mu   sync.Mutex
	got  []string
	srv  *httptest.Server
	name string
}

func newRecorder(t *testing.T, name string) *recorder {
	r := &recorder{name: name}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.got = append(r.got, string(b))
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *recorder) has(text string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, g := range r.got {
		if strings.Contains(g, text) {
			return true
		}
	}
	return false
}

// running is a relay of these tests: Main in the background.
type running struct {
	t      *testing.T
	out    *syncBuffer
	cancel context.CancelFunc
	code   chan int
}

func runRelay(t *testing.T, args ...string) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{t: t, out: &syncBuffer{}, cancel: cancel, code: make(chan int, 1)}
	go func() {
		r.code <- Main(append(args, "--color", "never"), WithContext(ctx), WithStreams(strings.NewReader(""), r.out, r.out))
	}()
	t.Cleanup(cancel)
	return r
}

// post sends a body and returns the status and answer, once the address
// takes connections.
func (r *running) post(url, body string) (int, string) {
	r.t.Helper()
	var status int
	var answer string
	require.Eventually(r.t, func() bool {
		resp, err := http.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		status, answer = resp.StatusCode, string(b)
		return true
	}, 15*time.Second, 50*time.Millisecond, "POST %s: %s", url, r.out.String())
	return status, answer
}

func (r *running) stop() int {
	r.cancel()
	select {
	case c := <-r.code:
		return c
	case <-time.After(30 * time.Second):
		r.t.Fatal("the relay did not stop")
		return -1
	}
}

// A reload that adds a model provider (the spec had no AI router) loads it
// without a restart: the new engine routes with a new Genkit instance and
// its model. A reload whose provider has no key is refused, and the relay
// keeps routing with the running one.
func TestReloadLoadsANewModelProvider(t *testing.T) {
	t.Setenv("TYPESAFE_API_KEY", "")
	a, b := newRecorder(t, "a"), newRecorder(t, "b")
	src, adm := freeAddr(t), freeAddr(t)
	spec := filepath.Join(t.TempDir(), "hikyaku.yaml")
	write := func(model string) {
		require.NoError(t, os.WriteFile(spec, []byte(triageSpec(src, adm, a.srv.URL, b.srv.URL, model)), 0o600))
	}
	write("")
	r := runRelay(t, "run", "-f", spec)

	status, _ := r.post("http://"+src+"/", `{"text":"the app crashes with an error","n":1}`)
	require.Equal(t, http.StatusAccepted, status)
	require.Eventually(t, func() bool { return a.has(`"n":1`) }, 10*time.Second, 20*time.Millisecond, "a static route: everything goes to a")

	write("llmfake-decisions/clef")
	before := llmfake.Calls.Load()
	status, answer := r.post("http://"+adm+"/reload", "")
	require.Equal(t, http.StatusOK, status, "a new model provider loads without a restart: %s", answer)
	status, _ = r.post("http://"+src+"/", `{"text":"the app crashes with an error","n":2}`)
	require.Equal(t, http.StatusAccepted, status)
	require.Eventually(t, func() bool { return b.has(`"n":2`) }, 10*time.Second, 20*time.Millisecond, r.out.String())
	assert.Greater(t, llmfake.Calls.Load(), before, "the new provider's model decided")

	write("typesafe/jev-1.13.0")
	status, answer = r.post("http://"+adm+"/reload", "")
	assert.Equal(t, http.StatusUnprocessableEntity, status)
	assert.Contains(t, answer, "needs TYPESAFE_API_KEY")
	status, _ = r.post("http://"+src+"/", `{"text":"the app crashes with an error","n":3}`)
	require.Equal(t, http.StatusAccepted, status)
	require.Eventually(t, func() bool { return b.has(`"n":3`) }, 10*time.Second, 20*time.Millisecond, "the running provider still routes")

	assert.Equal(t, 0, r.stop(), r.out.String())
}

// Under the Genkit Developer UI (hikyaku dev), a reload that changes the
// model providers starts a second Genkit instance next to the running
// one, with a reflection server and a runtime file of its own; once the
// old engine stops, the old instance's server stops and its file goes, so
// the Dev UI finds the new one. The new one serves the routes' flows.
func TestDevReloadMovesTheDevUIToTheNewGenkitInstance(t *testing.T) {
	for _, k := range []string{"GENKIT_ENV", "GENKIT_REFLECTION_ENABLED", "GENKIT_REFLECTION_PORT", "GENKIT_REFLECTION_V2_SERVER", "GENKIT_RUNTIME_ID"} {
		t.Setenv(k, "") // hikyaku dev sets GENKIT_ENV; this puts it back after
	}
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module devui\n"), 0o600))
	t.Chdir(root) // the reflection server writes .genkit/runtimes under the go.mod
	a, b := newRecorder(t, "a"), newRecorder(t, "b")
	src, adm := freeAddr(t), freeAddr(t)
	spec := filepath.Join(root, "hikyaku.yaml")
	write := func(model string) {
		require.NoError(t, os.WriteFile(spec, []byte(triageSpec(src, adm, a.srv.URL, b.srv.URL, model)), 0o600))
	}
	runtimes := func() map[string]string { // runtime file → reflection server URL
		out := map[string]string{}
		files, _ := filepath.Glob(filepath.Join(root, ".genkit", "runtimes", "*.json"))
		for _, f := range files {
			var rt struct {
				URL string `json:"reflectionServerUrl"`
			}
			if b, err := os.ReadFile(f); err == nil && json.Unmarshal(b, &rt) == nil {
				out[f] = rt.URL
			}
		}
		return out
	}
	only := func() string {
		t.Helper()
		var url string
		require.Eventually(t, func() bool {
			rts := runtimes()
			for _, u := range rts {
				url = u
			}
			return len(rts) == 1
		}, 15*time.Second, 50*time.Millisecond, "one runtime file")
		return url
	}

	write("llmfake/router")
	r := runRelay(t, "dev", "--watch=false", "-f", spec)
	first := only()
	actions := func(url string) string {
		resp, err := http.Get(url + "/api/actions")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	require.Eventually(t, func() bool {
		resp, err := http.Get(first + "/api/__health")
		if err == nil {
			_ = resp.Body.Close()
		}
		return err == nil
	}, 15*time.Second, 50*time.Millisecond)
	assert.Contains(t, actions(first), "/flow/route/triage")
	assert.Contains(t, actions(first), "llmfake/router")

	write("llmfake-decisions/clef")
	status, answer := r.post("http://"+adm+"/reload", "")
	require.Equal(t, http.StatusOK, status, answer)
	var second string
	require.Eventually(t, func() bool {
		second = only()
		return second != first
	}, 15*time.Second, 50*time.Millisecond, "the old instance's runtime file goes, the new one's stays")
	assert.Contains(t, actions(second), "/flow/route/triage", "the new instance serves the route's flow")
	require.Eventually(t, func() bool {
		resp, err := http.Get(first + "/api/__health")
		if err == nil {
			_ = resp.Body.Close()
		}
		return err != nil
	}, 15*time.Second, 50*time.Millisecond, "the old instance's reflection server stops")

	status, _ = r.post("http://"+src+"/", `{"text":"the app crashes with an error","n":1}`)
	require.Equal(t, http.StatusAccepted, status)
	require.Eventually(t, func() bool { return b.has(`"n":1`) }, 10*time.Second, 20*time.Millisecond, r.out.String())

	assert.Equal(t, 0, r.stop(), r.out.String())
	require.Eventually(t, func() bool { return len(runtimes()) == 0 }, 10*time.Second, 50*time.Millisecond, "the last runtime file goes at exit")
}
