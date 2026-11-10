package cli_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/cli"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/router"
)

// injected feeds the inject sources of a running relay.
var injected = make(chan *envelope.Envelope)

type injectSource struct{ name string }

func (s injectSource) Name() string { return s.name }

func (s injectSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case env := <-injected:
			select {
			case out <- env:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (injectSource) Health(context.Context) error { return nil }

func init() {
	plugin.RegisterSource("inject", func(_ context.Context, s plugin.Spec) (plugin.Source, error) { return injectSource{s.Name}, nil })
}

func inject(t *testing.T, body string) {
	t.Helper()
	select {
	case injected <- envelope.New("in", "test", []byte(body)):
	case <-time.After(5 * time.Second):
		t.Fatal("no source took the message")
	}
}

// genAI is an AI factory that knows which NewAI call built it; its
// decisions carry that number (the factory attribute).
type genAI struct {
	gen int
	ctx context.Context

	mu      sync.Mutex
	routers int
	probes  int
}

func (g *genAI) NewRouter(context.Context, router.AIRequest) (plugin.Router, error) {
	g.mu.Lock()
	g.routers++
	g.mu.Unlock()
	return plugin.RouterFunc(func(context.Context, *envelope.Envelope) (plugin.Decision, error) {
		return plugin.Decision{Destinations: []string{"reload-a"}, Router: "ai", Model: "fake", Confidence: 0.9,
			Attributes: map[string]any{"factory": g.gen}}, nil
	}), nil
}

func (g *genAI) Health(context.Context, []string) map[string]error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.probes++
	return nil
}

func (g *genAI) count() (routers, probes int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.routers, g.probes
}

// restartErr is what llm returns when only a new process can load a change.
type restartErr struct{ error }

func (restartErr) NeedsRestart() bool { return true }

// genAIs builds a genAI per NewAI call. A provider "broken" fails like a
// missing key, and "pinned" like a Dev UI port only a restart frees.
type genAIs struct {
	mu  sync.Mutex
	all []*genAI
}

func (f *genAIs) newAI(ctx context.Context, spec *config.Spec, _ cli.AIOptions) (router.AIFactory, error) {
	for _, r := range spec.Routes {
		if rs, ok := r.AIRouter(); ok {
			switch p, _, _ := strings.Cut(rs.Model, "/"); p {
			case "broken":
				return nil, errors.New("broken needs BROKEN_API_KEY: declare it under secrets: or export it")
			case "pinned":
				return nil, restartErr{errors.New("the reflection port is pinned")}
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	g := &genAI{gen: len(f.all) + 1, ctx: ctx}
	f.all = append(f.all, g)
	return g, nil
}

func (f *genAIs) built() []*genAI {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*genAI(nil), f.all...)
}

// syncBuffer is the relay's log, read while it runs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// relayRun is `hikyaku run` in the background.
type relayRun struct {
	t      *testing.T
	file   string
	log    *syncBuffer
	cancel context.CancelFunc
	code   chan int
}

func startRelay(t *testing.T, newAI func(context.Context, *config.Spec, cli.AIOptions) (router.AIFactory, error), spec string) *relayRun {
	t.Helper()
	r := &relayRun{t: t, file: filepath.Join(t.TempDir(), "hikyaku.yaml"), log: &syncBuffer{}, code: make(chan int, 1)}
	r.write(spec)
	deps := cli.Deps{LookupEnv: func(string) (string, bool) { return "", false }, NewAI: newAI}
	root := cli.NewRoot(cli.BuildInfo{Version: "1.2.3"}, cli.Streams{Out: io.Discard, Err: r.log}, deps)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { r.code <- cli.Execute(ctx, root, []string{"run", "-f", r.file, "--color", "never"}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.code:
		case <-time.After(30 * time.Second):
			t.Error("the relay did not stop")
		}
	})
	return r
}

func (r *relayRun) write(spec string) {
	require.NoError(r.t, os.WriteFile(r.file, []byte(spec), 0o600))
}

// stop stops the relay and returns its exit code.
func (r *relayRun) stop() int {
	r.cancel()
	select {
	case c := <-r.code:
		r.code <- c // for the cleanup
		return c
	case <-time.After(30 * time.Second):
		r.t.Fatal("the relay did not stop")
		return -1
	}
}

// call sends a request to the admin API, waiting for it to answer.
func (r *relayRun) call(method, url, token string) (int, string) {
	r.t.Helper()
	var code int
	var body string
	require.Eventually(r.t, func() bool {
		req, err := http.NewRequest(method, url, nil)
		require.NoError(r.t, err)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		code, body = resp.StatusCode, string(b)
		return true
	}, 15*time.Second, 20*time.Millisecond, "%s %s: %s", method, url, r.log.String())
	return code, body
}

const aiReloadSpec = `
version: 1
name: reload
sources: [{ name: in, type: inject }]
destinations: [{ name: reload-a, type: fault }]
routes:
  - name: triage
    from: [in]
    router:
      type: ai
      model: %s
      candidates: [{ name: reload-a, description: %q }]
      fallback: [{ type: static, to: [reload-a] }]
server:
  admin: { listen: %q }
  readiness: { models: true }
`

// delivered is how many messages a fault destination took.
func delivered(name string) int {
	s := sinkNamed(name)
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

// lastFactory waits for reload-a's nth message and returns the factory
// attribute of the last one.
func lastFactory(t *testing.T, n int) any {
	t.Helper()
	s := sinkNamed("reload-a")
	var v any
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.got) < n {
			return false
		}
		v, _ = s.got[len(s.got)-1].Attribute("factory")
		return true
	}, 5*time.Second, 10*time.Millisecond)
	return v
}

// A reload that changes the model providers builds a new AI factory (a new
// Genkit instance) for the new engine, without a restart; the old factory
// goes with the old engine. A reload that keeps them keeps the factory. A
// new spec whose provider fails to start is reported, and the relay keeps
// running the previous one.
func TestReloadBuildsANewAIFactoryWhenTheProvidersChange(t *testing.T) {
	ais := &genAIs{}
	adm := freeAddr(t)
	r := startRelay(t, ais.newAI, fmt.Sprintf(aiReloadSpec, "alpha/m1", "anything", adm))
	base := "http://" + adm
	code, _ := r.call("GET", base+"/readyz", "")
	require.Equal(t, 200, code)
	require.Len(t, ais.built(), 1)
	first := ais.built()[0]
	got := delivered("reload-a")
	inject(t, `{"m":1}`)
	got++
	assert.Equal(t, 1, lastFactory(t, got))

	r.write(fmt.Sprintf(aiReloadSpec, "beta/m1", "anything", adm))
	code, body := r.call("POST", base+"/reload", "")
	require.Equal(t, 200, code, body)
	require.Len(t, ais.built(), 2, "a new provider: a new factory")
	second := ais.built()[1]
	routers, _ := second.count()
	assert.Equal(t, 1, routers, "the new engine routes with the new factory")
	require.Eventually(t, func() bool { return first.ctx.Err() != nil }, 5*time.Second, 10*time.Millisecond,
		"the old factory's Genkit instance goes with the old engine")
	assert.NoError(t, second.ctx.Err())
	inject(t, `{"m":2}`)
	got++
	assert.Equal(t, 2, lastFactory(t, got), "the message is routed by the new provider")
	code, _ = r.call("GET", base+"/readyz", "")
	assert.Equal(t, 200, code)
	_, probes := second.count()
	assert.Equal(t, 1, probes, "readiness.models probes the new spec's models with the new factory")

	r.write(fmt.Sprintf(aiReloadSpec, "beta/m1", "everything", adm))
	code, body = r.call("POST", base+"/reload", "")
	require.Equal(t, 200, code, body)
	assert.Len(t, ais.built(), 2, "the same providers: the same factory")
	routers, _ = second.count()
	assert.Equal(t, 2, routers)
	assert.NoError(t, second.ctx.Err())

	r.write(fmt.Sprintf(aiReloadSpec, "broken/m1", "anything", adm))
	code, body = r.call("POST", base+"/reload", "")
	assert.Equal(t, 422, code)
	assert.Contains(t, body, "AI router: broken needs BROKEN_API_KEY")
	r.write(fmt.Sprintf(aiReloadSpec, "pinned/m1", "anything", adm))
	code, body = r.call("POST", base+"/reload", "")
	assert.Equal(t, 422, code)
	assert.Contains(t, body, "the reflection port is pinned")
	assert.Len(t, ais.built(), 2)
	assert.NoError(t, second.ctx.Err(), "the running factory stays")
	inject(t, `{"m":3}`)
	got++
	assert.Equal(t, 2, lastFactory(t, got), "the relay still runs the previous spec")
	_, cfg := r.call("GET", base+"/debug/config", "")
	assert.Contains(t, cfg, "beta/m1")

	_, metrics := r.call("GET", base+"/metrics", "")
	assert.Contains(t, metrics, `hikyaku_reloads_total{outcome="ok"} 2`)
	assert.Contains(t, metrics, `hikyaku_reloads_total{outcome="failed"} 1`)
	assert.Contains(t, metrics, `hikyaku_reloads_total{outcome="needs_restart"} 1`)

	r.write(fmt.Sprintf(`
version: 1
name: reload
sources: [{ name: in, type: inject }]
destinations: [{ name: reload-a, type: fault }]
routes: [{ name: triage, from: [in], to: [reload-a] }]
server: { admin: { listen: %q } }
`, adm))
	code, body = r.call("POST", base+"/reload", "")
	require.Equal(t, 200, code, body)
	require.Eventually(t, func() bool { return second.ctx.Err() != nil }, 5*time.Second, 10*time.Millisecond,
		"a spec without AI lets the factory go")
	assert.Equal(t, 0, r.stop(), r.log.String())
}

const adminReloadSpec = `
version: 1
name: reload
sources: [{ name: in, type: inject }]
destinations: [{ name: reload-b, type: fault }]
routes: [{ name: r, from: [in], to: [reload-b] }]
server: { admin: { listen: %q, token: %q } }
observability: { metrics: { prometheus: %t } }
`

// A reload that moves server.admin starts the admin API on the new address
// and shuts the old one down: the POST /reload that moved it gets its
// answer, the old address then refuses, and the metrics carry over. A
// reload that keeps the address applies the token and the metrics endpoint
// in place, and a tail open across it keeps its events. An address that is
// taken fails the reload, and the admin API stays where it was.
func TestReloadMovesTheAdminAPI(t *testing.T) {
	a, b := freeAddr(t), freeAddr(t)
	r := startRelay(t, nil, fmt.Sprintf(adminReloadSpec, a, "", true))
	code, _ := r.call("GET", "http://"+a+"/healthz", "")
	require.Equal(t, 200, code)
	oldTail, err := http.Get("http://" + a + "/v1/tail")
	require.NoError(t, err)
	defer func() { _ = oldTail.Body.Close() }()

	r.write(fmt.Sprintf(adminReloadSpec, b, "", true))
	code, body := r.call("POST", "http://"+a+"/reload", "")
	require.Equal(t, 200, code, "the POST /reload that moved the admin API gets its answer: %s", body)
	_, err = io.ReadAll(oldTail.Body)
	assert.NoError(t, err, "the old server's tail ends")
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + a + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
		}
		return err != nil
	}, 10*time.Second, 20*time.Millisecond, "the old address refuses")
	code, _ = r.call("GET", "http://"+b+"/healthz", "")
	assert.Equal(t, 200, code)
	_, metrics := r.call("GET", "http://"+b+"/metrics", "")
	assert.Contains(t, metrics, `hikyaku_reloads_total{outcome="ok"} 1`, "the metrics carry over")
	assert.Contains(t, r.log.String(), "admin API moved")

	tail, err := http.Get("http://" + b + "/v1/tail")
	require.NoError(t, err)
	defer func() { _ = tail.Body.Close() }()
	r.write(fmt.Sprintf(adminReloadSpec, b, "t0ken", false))
	code, body = r.call("POST", "http://"+b+"/reload", "")
	require.Equal(t, 200, code, body)
	code, _ = r.call("GET", "http://"+b+"/debug/config", "")
	assert.Equal(t, 401, code, "the new token applies")
	code, _ = r.call("GET", "http://"+b+"/debug/config", "t0ken")
	assert.Equal(t, 200, code)
	code, _ = r.call("GET", "http://"+b+"/metrics", "")
	assert.Equal(t, 404, code, "observability.metrics.prometheus: false")
	code, _ = r.call("POST", "http://"+b+"/reload", "")
	assert.Equal(t, 401, code, "reloading needs the token now")

	env := envelope.New("in", "test", []byte(`{"m":"after"}`))
	select {
	case injected <- env:
	case <-time.After(5 * time.Second):
		t.Fatal("no source took the message")
	}
	lines := bufio.NewScanner(tail.Body)
	found := false
	for !found && lines.Scan() {
		found = strings.HasPrefix(lines.Text(), "data: ") && strings.Contains(lines.Text(), env.ID)
	}
	assert.True(t, found, "a tail open across the reload keeps its events")

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = taken.Close() }()
	r.write(fmt.Sprintf(adminReloadSpec, taken.Addr().String(), "t0ken", true))
	code, body = r.call("POST", "http://"+b+"/reload", "t0ken")
	assert.Equal(t, 422, code)
	assert.Contains(t, body, "admin: listen "+taken.Addr().String())
	code, _ = r.call("GET", "http://"+b+"/debug/config", "")
	assert.Equal(t, 401, code, "the admin API stays where it was, with its token")
	code, _ = r.call("GET", "http://"+b+"/metrics", "")
	assert.Equal(t, 404, code, "and its options: prometheus stays off")
	assert.Equal(t, 0, r.stop(), r.log.String())
}
