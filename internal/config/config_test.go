package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadYAML(t *testing.T, yaml string) (*Loaded, error) {
	t.Helper()
	return LoadBytes(context.Background(), "hikyaku.yaml", []byte(yaml), LoadOptions{LookupEnv: func(string) (string, bool) { return "", false }})
}

func mustLoad(t *testing.T, yaml string) *Spec {
	t.Helper()
	l, err := loadYAML(t, yaml)
	require.NoError(t, err)
	return l.Spec
}

func errText(t *testing.T, err error) string {
	t.Helper()
	require.Error(t, err)
	var es Errors
	require.True(t, errors.As(err, &es), "want config.Errors, got %T: %v", err, err)
	return es.Error()
}

const minimal = `
version: 1
name: demo
sources:
  - { name: in, type: stdin }
destinations:
  - { name: out, type: stdout }
routes:
  - { name: all, from: [in], to: [out] }
`

func TestLoadMinimalAppliesDefaults(t *testing.T) {
	s := mustLoad(t, minimal)
	assert.Equal(t, "static", s.Routes[0].Router.Type)
	assert.Equal(t, []string{"out"}, s.Routes[0].Router.To)
	assert.Equal(t, "stderr", s.DeadLetter.Type)
	assert.Equal(t, "memory", s.Buffer.Type)
	assert.Equal(t, DefaultBufferSize, s.Buffer.Size)
	assert.Equal(t, DefaultAdminListen, s.Server.Admin.Listen)
	assert.Equal(t, DefaultWorkers, s.Server.Workers)
}

// The design's §5 example uses every section of the spec. It references
// two secrets it does not declare and hedges a group that is not marked
// idempotent, which validation rejects; the corrected copy loads cleanly.
func TestDesignExampleCoversEveryKey(t *testing.T) {
	data, err := os.ReadFile("testdata/design.yaml")
	require.NoError(t, err)
	_, err = LoadBytes(context.Background(), "design.yaml", data, LoadOptions{})
	text := errText(t, err)
	assert.NotContains(t, text, "unknown key", "every key of the design's example is in the structs")
	assert.Contains(t, text, "secret CORP_DECISIONS_KEY is used but not declared")
	assert.Contains(t, text, "secret REDIS_PASSWORD is used but not declared")

	l, err := Load(context.Background(), []string{"testdata/full.yaml"}, LoadOptions{})
	require.NoError(t, err)
	s := l.Spec
	assert.Equal(t, "support-intake", s.Name)
	assert.Len(t, s.Sources, 3)
	assert.Equal(t, "listen", s.Sources[0].Mode)
	assert.Equal(t, "subscribe", s.Sources[1].Mode)
	assert.Equal(t, 30*time.Second, s.Sources[2].Interval.Std())
	billing, _ := s.DestinationByName("billing")
	assert.Equal(t, []string{"billing-dr", "billing-queue"}, billing.Fallback)
	assert.Equal(t, 30*time.Second, billing.Deadline.Std())
	route, _ := s.RouteByName("smart-triage")
	ai, ok := route.AIRouter()
	require.True(t, ok)
	assert.Equal(t, int64(8<<10), int64(ai.Input.MaxBytes))
	assert.Equal(t, 7*24*time.Hour, ai.History.TTL.Std())
	assert.True(t, ai.History.Sticky.Enabled)
	assert.Equal(t, "rules", ai.Fallback[0].Type)
	assert.Equal(t, []string{"typesafe/jev-1.13.0", "ollama-decisions/clef"}, s.ModelChain(*ai))
	assert.Equal(t, Placeholder("REDIS_PASSWORD"), s.Stores[0].Password, "unresolved secrets are placeholders")
	assert.Equal(t, "env:WEBHOOK_SECRET", l.SecretRefs["WEBHOOK_SECRET"])
}

func TestLoadStrictUnknownKey(t *testing.T) {
	_, err := loadYAML(t, `
version: 1
name: demo
sources:
  - { name: in, type: stdin, colour: blue }
destinations:
  - name: out
    type: stdout
    retries: 3
routes:
  - { name: all, from: [in], to: [out] }
`)
	text := errText(t, err)
	assert.Contains(t, text, `hikyaku.yaml:5: unknown key "colour" in a source`)
	assert.Contains(t, text, `hikyaku.yaml:9: unknown key "retries" in a destination`)
}

func TestLoadTypeErrorsHaveLines(t *testing.T) {
	_, err := loadYAML(t, strings.Replace(minimal, "{ name: out, type: stdout }", "{ name: out, type: stdout, timeout: soon }", 1))
	text := errText(t, err)
	assert.Contains(t, text, "hikyaku.yaml:7")
	assert.Contains(t, text, `invalid duration "soon"`)
}

func TestInterpSecretsEnv(t *testing.T) {
	spec := `
version: 1
name: demo
secrets:
  TOKEN: env:STREAM_TOKEN
sources:
  - name: in
    type: wss
    url: "wss://{{env.STREAM_HOST}}/events"
    headers: { Authorization: "Bearer {{secrets.TOKEN}}" }
destinations:
  - { name: out, type: stdout }
routes:
  - name: all
    from: [in]
    router:
      type: rules
      rules:
        - { when: 'msg.json.id == "{{ .json.id }}"', to: [out] }
`
	l, err := LoadBytes(context.Background(), "s.yaml", []byte(spec), LoadOptions{
		LookupEnv: func(n string) (string, bool) { return map[string]string{"STREAM_HOST": "stream.example.com"}[n], true },
		ResolveSecrets: func(_ context.Context, refs map[string]string) (map[string]string, error) {
			assert.Equal(t, map[string]string{"TOKEN": "env:STREAM_TOKEN"}, refs)
			return map[string]string{"TOKEN": "t0ps3cret"}, nil
		},
	})
	require.NoError(t, err)
	src := l.Spec.Sources[0]
	assert.Equal(t, "wss://stream.example.com/events", src.URL)
	assert.Equal(t, "Bearer t0ps3cret", src.Headers["Authorization"])
	assert.Equal(t, `msg.json.id == "{{ .json.id }}"`, l.Spec.Routes[0].Router.Rules[0].When, "per-message templates are left alone")
	assert.Equal(t, map[string]string{"TOKEN": "t0ps3cret"}, l.SecretValues)
}

func TestUndeclaredSecret(t *testing.T) {
	_, err := loadYAML(t, strings.Replace(minimal, "type: stdout }", "type: http, url: \"https://x/{{secrets.NOPE}}\" }", 1))
	assert.Contains(t, errText(t, err), "hikyaku.yaml:7: secret NOPE is used but not declared under secrets:")
}

func TestResolveSecretsErrorStopsLoad(t *testing.T) {
	spec := "secrets: { A: env:A }\n" + minimal
	_, err := LoadBytes(context.Background(), "s.yaml", []byte(spec), LoadOptions{
		ResolveSecrets: func(context.Context, map[string]string) (map[string]string, error) {
			return nil, errors.New("vault unreachable")
		},
	})
	assert.EqualError(t, err, "vault unreachable")
}

func TestValidateDanglingRefs(t *testing.T) {
	_, err := loadYAML(t, `
version: 1
name: demo
sources:
  - { name: in, type: stdin }
destinations:
  - { name: out, type: stdout, fallback: [ghost] }
routes:
  - { name: a, from: [nowhere], to: [missing] }
  - name: b
    from: [in]
    router:
      type: ai
      model: typesafe/jev-1.13.0
      candidates:
        - { name: billing, description: payments }
      on_low_confidence: [support]
      history: { key: "{{ .json.customer }}", store: redis-main }
`)
	text := errText(t, err)
	for _, want := range []string{
		`destination "out".fallback[0]: destination "ghost" is not declared`,
		`route "a".from[0]: source "nowhere" is not declared`,
		`route "a".router.to[0]: destination "missing" is not declared`,
		`AI candidate "billing" is not a destination`,
		`route "b".router.on_low_confidence[0]: destination "support" is not declared`,
		`route "b".router.history.ttl: is required`,
		`route "b".router.history.store: store "redis-main" is not declared`,
	} {
		assert.Contains(t, text, want)
	}
}

func TestValidateFallbackCycle(t *testing.T) {
	_, err := loadYAML(t, `
version: 1
name: demo
sources: [{ name: in, type: stdin }]
destinations:
  - { name: a, type: stdout, fallback: [b] }
  - { name: b, type: stdout, fallback: [c] }
  - { name: c, type: stdout, fallback: [a] }
  - { name: d, type: stdout, fallback: [d] }
routes: [{ name: r, from: [in], to: [a] }]
`)
	text := errText(t, err)
	assert.Contains(t, text, "fallback cycle: a → b → c → a")
	assert.Equal(t, 1, strings.Count(text, "fallback cycle: a"), "a cycle is reported once")
	assert.Contains(t, text, "a destination cannot fall back to itself")
}

func TestValidateFallbackCycleAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}
	a := write("a.yaml", `
version: 1
name: demo
sources: [{ name: in, type: stdin }]
destinations: [{ name: a, type: stdout, fallback: [b] }]
routes: [{ name: r, from: [in], to: [a] }]
`)
	b := write("b.yaml", `
destinations: [{ name: b, type: stdout, fallback: [a] }]
`)
	_, err := Load(context.Background(), []string{a, b}, LoadOptions{})
	assert.Contains(t, errText(t, err), "fallback cycle: a → b → a")

	// The same files, loaded as a directory.
	_, err = Load(context.Background(), []string{dir}, LoadOptions{})
	assert.Contains(t, errText(t, err), "fallback cycle: a → b → a")
}

func TestMergeFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "10-base.yaml"), []byte(`
version: 1
name: demo
secrets: { A: env:A }
defaults: { timeout: 2s }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: stdout }]
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "20-routes.yml"), []byte(`
secrets: { B: env:B }
destinations: [{ name: archive, type: file, path: ./out.ndjson }]
routes: [{ name: r, from: [in], to: [out, archive] }]
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignored"), 0o644))
	l, err := Load(context.Background(), []string{dir}, LoadOptions{})
	require.NoError(t, err)
	assert.Len(t, l.Files, 2)
	assert.Equal(t, []string{"out", "archive"}, l.Spec.DestinationNames())
	assert.Equal(t, map[string]string{"A": "env:A", "B": "env:B"}, l.SecretRefs)
	assert.Equal(t, 2*time.Second, l.Spec.Defaults.Timeout.Std())
}

func TestMergeKeepsPlugins(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "plugins"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "10-base.yaml"), []byte(`
version: 1
name: demo
plugins: [{ name: onpath, command: hikyaku-plugin-x }]
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: stdout }]
routes: [{ name: r, from: [in], to: [out] }]
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugins", "echo.yaml"), []byte(`
plugins: [{ name: echo, command: ./hikyaku-plugin-echo }]
`), 0o644))
	l, err := Load(context.Background(), []string{filepath.Join(dir, "10-base.yaml"), filepath.Join(dir, "plugins", "echo.yaml")}, LoadOptions{})
	require.NoError(t, err)
	require.Len(t, l.Spec.Plugins, 2, "plugins from every file")
	assert.Equal(t, "hikyaku-plugin-x", l.Spec.Plugins[0].Command, "a name on PATH is kept")
	assert.Equal(t, filepath.Join(dir, "plugins", "hikyaku-plugin-echo"), l.Spec.Plugins[1].Command, "a path is relative to the file that declares it")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugins", "dup.yaml"), []byte(`
plugins: [{ name: echo, command: ./other }]
`), 0o644))
	_, err = Load(context.Background(), []string{filepath.Join(dir, "10-base.yaml"), filepath.Join(dir, "plugins", "echo.yaml"), filepath.Join(dir, "plugins", "dup.yaml")}, LoadOptions{})
	assert.Contains(t, errText(t, err), `plugin "echo": already declared in`)
}

func TestMergeDuplicateName(t *testing.T) {
	dir := t.TempDir()
	base := `
version: 1
name: demo
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: stdout }]
routes: [{ name: r, from: [in], to: [out] }]
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(base), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yaml"), []byte("destinations: [{ name: out, type: stderr }]\ndefaults: { timeout: 1s }\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "c.yaml"), []byte("name: other\ndefaults: { timeout: 3s }\n"), 0o644))
	_, err := Load(context.Background(), []string{dir}, LoadOptions{})
	text := errText(t, err)
	assert.Contains(t, text, `b.yaml: destination "out": already declared in `+filepath.Join(dir, "a.yaml"))
	assert.Contains(t, text, `c.yaml: defaults: also set in `+filepath.Join(dir, "b.yaml"))
	assert.Contains(t, text, `"other" differs from "demo"`)
}

func TestValidateHedgeNeedsIdempotent(t *testing.T) {
	_, err := loadYAML(t, `
version: 1
name: demo
sources: [{ name: in, type: stdin }]
destinations:
  - name: g
    type: group
    strategy: failover
    hedge: { after: 800ms }
    members:
      - { type: https, url: https://a.internal/hook }
      - { type: group }
routes: [{ name: r, from: [in], to: [g] }]
`)
	text := errText(t, err)
	assert.Contains(t, text, "set idempotent: true")
	assert.Contains(t, text, "groups do not nest")
}

func TestValidateProtocolRules(t *testing.T) {
	_, err := loadYAML(t, `
version: 1
name: demo
sources:
  - { name: hook, type: http }
  - { name: poll, type: https, mode: poll }
  - { name: ws, type: wss, mode: poll, url: wss://x }
  - { name: f, type: file }
  - { name: odd, type: carrier-pigeon }
destinations:
  - { name: h, type: https }
  - { name: w, type: ws, url: http://wrong }
  - { name: s, type: sse }
  - { name: k, type: kafka }
  - { name: x, type: smoke-signal }
routes: [{ name: r, from: [hook], to: [h] }]
`)
	text := errText(t, err)
	for _, want := range []string{
		`source "hook".listen: is required in listen mode`,
		`source "poll".url: is required in poll mode`,
		`source "ws".mode: wss sources run in listen or subscribe mode, not "poll"`,
		`source "f".path: is required for a file source`,
		`unknown source type "carrier-pigeon"`,
		`destination "h".url: is required`,
		`destination "w".url: "http://wrong" must start with ws:// or wss://`,
		`destination "s".listen: is required`,
		`destination "k".brokers: is required: the bootstrap brokers`,
		`unknown destination type "smoke-signal"`,
	} {
		assert.Contains(t, text, want)
	}
}

func TestValidateOpenAIBaseURL(t *testing.T) {
	for _, bad := range []string{"api.openai.com/v1", "ftp://models.internal", "https://"} {
		_, err := loadYAML(t, strings.Replace(minimal, "version: 1", "version: 1\ngenkit: { openai: { base_url: \""+bad+"\" } }", 1))
		assert.Contains(t, errText(t, err), "genkit.openai.base_url: must be an http:// or https:// URL", bad)
	}
	_, err := loadYAML(t, strings.Replace(minimal, "version: 1", "version: 1\ngenkit: { openai: { base_url: \"http://localhost:8000/v1\" } }", 1))
	require.NoError(t, err)
}

func TestValidateTransforms(t *testing.T) {
	_, err := loadYAML(t, strings.Replace(minimal, "to: [out] }", "to: [out], transforms: [{}, { jq: ., template: x }] }", 1))
	text := errText(t, err)
	assert.Contains(t, text, "set one of filter, jq, template")
	assert.Contains(t, text, "set only one of")
}

func TestValidateToAndRouterBoth(t *testing.T) {
	_, err := loadYAML(t, strings.Replace(minimal, "to: [out] }", "to: [out], router: { type: static, to: [out, out] } }", 1))
	assert.Contains(t, errText(t, err), "set to: or router:, not both")
}

func TestDefaultsInheritance(t *testing.T) {
	s := mustLoad(t, `
version: 1
name: demo
defaults:
  timeout: 2s
  retry: { attempts: 4, backoff: 100ms, retry_on: [5xx] }
  circuit_breaker: { failure_ratio: 0.3 }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: stdout, retry: { attempts: 1 } }
  - { name: plain, type: stdout }
routes: [{ name: r, from: [in], to: [out, plain] }]
`)
	out, _ := s.DestinationByName("out")
	p := ResolvePolicy(s.Defaults, out.Policy)
	assert.Equal(t, 1, p.Retry.Attempts, "the destination's own field wins")
	assert.Equal(t, 100*time.Millisecond, p.Retry.Backoff.Std(), "the rest is inherited from defaults")
	assert.Equal(t, TagList{"5xx"}, p.Retry.RetryOn)
	assert.Equal(t, TagList{"4xx"}, p.Retry.NeverOn, "then from the built-in defaults")
	assert.Equal(t, 2*time.Second, p.Timeout.Std())
	assert.Equal(t, 0.3, p.CircuitBreaker.FailureRatio)
	assert.Equal(t, DefaultBreakerMinReq, p.CircuitBreaker.MinRequests)
	assert.True(t, *p.Retry.RespectRetryAfter)
	assert.True(t, *p.RetryBudget.Enabled)

	plain, _ := s.DestinationByName("plain")
	p = ResolvePolicy(Policy{}, plain.Policy)
	assert.Equal(t, DefaultAttempts, p.Retry.Attempts)
	assert.Equal(t, DefaultTimeout, p.Timeout.Std())
	assert.Equal(t, DefaultRetryOn, p.Retry.RetryOn)
	assert.True(t, plain.IsRequired())
	assert.Equal(t, "Idempotency-Key", plain.IdempotencyHeaderName())
}

func TestDurationDays(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"7d": 7 * 24 * time.Hour, "1d12h": 36 * time.Hour, "500ms": 500 * time.Millisecond,
		"0": 0, "": 0, "1.5d": 36 * time.Hour,
	} {
		got, err := ParseDuration(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"soon", "5", "-1s", "d"} {
		_, err := ParseDuration(bad)
		assert.Error(t, err, bad)
	}
}

func TestSize8KiB(t *testing.T) {
	for in, want := range map[string]int64{"8KiB": 8192, "10MB": 10_000_000, "512": 512, "1mib": 1 << 20, "2 KB": 2000, "1.5K": 1536} {
		got, err := ParseSize(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := ParseSize("lots")
	assert.Error(t, err)
}

func TestParsePercent(t *testing.T) {
	for in, want := range map[string]float64{"50%": 0.5, "0.25": 0.25, "100%": 1, "0": 0} {
		got, err := ParsePercent(in)
		require.NoError(t, err, in)
		assert.InDelta(t, want, got, 1e-9, in)
	}
	for _, bad := range []string{"150%", "-1", "half"} {
		_, err := ParsePercent(bad)
		assert.Error(t, err, bad)
	}
}

func TestLint(t *testing.T) {
	s := mustLoad(t, `
version: 1
name: demo
models: { decision: typesafe/~jev-latest }
sources: [{ name: hook, type: http, listen: ":8080" }]
destinations:
  - { name: billing, type: https, url: https://billing.internal }
  - { name: support, type: https, url: https://support.internal }
routes:
  - name: triage
    from: [hook]
    router:
      type: ai
      candidates:
        - { name: billing, description: payments }
        - { name: support, description: bugs }
`)
	var msgs []string
	for _, w := range Lint(s) {
		msgs = append(msgs, w.String())
	}
	all := strings.Join(msgs, "\n")
	for _, want := range []string{
		`models.decision: decision model "typesafe/~jev-latest" is not pinned`,
		"an AI router without a router fallback",
		"min_confidence: is 0",
		"a single model",
		`destination "billing".fallback: required and without a fallback`,
		"dead letters go to stderr",
		"server.admin.token: unset",
		`source "hook".auth: a listen source without auth`,
	} {
		assert.Contains(t, all, want)
	}
}

func TestSchemaHasEveryTopLevelKey(t *testing.T) {
	s := Schema()
	assert.Equal(t, SchemaBaseURL+"hikyaku.schema.json", string(s.ID))
	for _, key := range []string{"version", "name", "secrets", "genkit", "models", "defaults", "sources", "destinations", "stores", "dead_letter", "routes", "server", "observability", "buffer", "egress"} {
		_, ok := s.Properties.Get(key)
		assert.True(t, ok, key)
	}
	b, err := SchemaJSON(s)
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(string(b), "}\n"))
	md := string(ReferenceMarkdown())
	assert.Contains(t, md, "## `RouterSpec`")
	assert.Contains(t, md, "`min_confidence`")
}

func TestExpandPathsErrors(t *testing.T) {
	_, err := ExpandPaths(nil)
	assert.Error(t, err)
	_, err = ExpandPaths([]string{"/nonexistent/hikyaku.yaml"})
	assert.Error(t, err)
	_, err = ExpandPaths([]string{t.TempDir()})
	assert.ErrorContains(t, err, "no .yaml files")
}

func TestLoadEmptyAndNonMapping(t *testing.T) {
	_, err := loadYAML(t, "")
	assert.Contains(t, errText(t, err), "the file is empty")
	_, err = loadYAML(t, "- a\n- b\n")
	assert.Contains(t, errText(t, err), "a spec is a mapping")
}

func TestInterpWholeValueTakesItsType(t *testing.T) {
	spec := strings.Replace(minimal, "routes:", `stores:
  - name: r
    type: redis
    addr: "{{env.ADDR}}"
    db: "{{env.DB}}"
    tls: { enabled: "{{env.TLS}}" }
    key_prefix: "pre-{{env.DB}}"
routes:`, 1)
	env := map[string]string{"ADDR": "redis:6379", "DB": "3", "TLS": "true"}
	l, err := LoadBytes(context.Background(), "s.yaml", []byte(spec), LoadOptions{LookupEnv: func(n string) (string, bool) { v, ok := env[n]; return v, ok }})
	require.NoError(t, err)
	st := l.Spec.Stores[0]
	assert.Equal(t, 3, st.DB, "a whole-value reference fills a number")
	assert.True(t, *st.TLS.Enabled, "and a boolean")
	assert.Equal(t, "redis:6379", st.Addr)
	assert.Equal(t, "pre-3", st.KeyPrefix, "a reference inside text stays text")

	// A secret stays a string even when it reads like null.
	sec := "secrets: { P: env:P }\n" + strings.Replace(minimal, "routes:", "stores: [{ name: r, type: redis, addr: x, password: \"{{secrets.P}}\" }]\nroutes:", 1)
	l, err = LoadBytes(context.Background(), "s.yaml", []byte(sec), LoadOptions{ResolveSecrets: func(context.Context, map[string]string) (map[string]string, error) {
		return map[string]string{"P": "null"}, nil
	}})
	require.NoError(t, err)
	assert.Equal(t, "null", l.Spec.Stores[0].Password)
}
