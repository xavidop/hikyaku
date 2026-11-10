package hikyaku_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

// memSink collects what it gets (a destination type only these tests have).
type memSink struct {
	mu   sync.Mutex
	got  []string
	name string
}

func (m *memSink) Name() string                 { return m.name }
func (m *memSink) Health(context.Context) error { return nil }
func (m *memSink) Close(context.Context) error  { return nil }
func (m *memSink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.got = append(m.got, string(env.Body))
	return &plugin.Result{}, nil
}

// listSource emits its bodies, then ends (like stdin at EOF).
type listSource struct {
	name   string
	bodies []string
}

func (l *listSource) Name() string                 { return l.name }
func (l *listSource) Health(context.Context) error { return nil }
func (l *listSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	for _, b := range l.bodies {
		select {
		case out <- envelope.New(l.name, "test", []byte(b)):
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

var (
	sink     = &memSink{name: "out"}
	register sync.Once
)

type memOptions struct {
	Prefix string `yaml:"prefix"`
}

func setup() {
	register.Do(func() {
		plugin.RegisterSink("lib-mem", func(context.Context, plugin.Spec) (plugin.Sink, error) { return sink, nil },
			plugin.WithOptions(memOptions{}))
		plugin.RegisterSource("lib-list", func(_ context.Context, s plugin.Spec) (plugin.Source, error) {
			return &listSource{name: s.Name, bodies: []string{`{"n":1}`, `{"n":2}`}}, nil
		})
	})
}

const spec = `
version: 1
name: library
secrets: { TOKEN: env:HIKYAKU_LIB_TOKEN }
sources: [{ name: in, type: lib-list }]
destinations:
  - name: out
    type: lib-mem
    options: { prefix: "{{secrets.TOKEN}}" }
routes: [{ name: r, from: [in], to: [out], filter: 'msg.json.n > 0' }]
`

func TestParseAndRun(t *testing.T) {
	setup()
	t.Setenv("HIKYAKU_LIB_TOKEN", "s3cret")
	s, err := hikyaku.Parse(context.Background(), "library.yaml", []byte(spec))
	require.NoError(t, err)
	assert.Equal(t, "library", s.Name)
	assert.Equal(t, "s3cret", s.Destinations[0].Options["prefix"], "secrets are resolved")
	require.NoError(t, hikyaku.Run(context.Background(), s, hikyaku.WithStore("default", store.NewMemory("default"))))
	sink.mu.Lock()
	defer sink.mu.Unlock()
	assert.ElementsMatch(t, []string{`{"n":1}`, `{"n":2}`}, sink.got)
}

func TestParseChecksLikeValidate(t *testing.T) {
	setup()
	_, err := hikyaku.Parse(context.Background(), "bad.yaml", []byte(`
version: 1
name: library
sources: [{ name: in, type: lib-list }]
destinations: [{ name: out, type: lib-mem, options: { nope: 1 } }]
routes: [{ name: r, from: [in], to: [out] }]
`), hikyaku.WithoutSecrets())
	assert.ErrorContains(t, err, "nope", "a plugin's options are checked")
	_, err = hikyaku.Parse(context.Background(), "bad.yaml", []byte(`
version: 1
name: library
sources: [{ name: in, type: lib-list }]
destinations: [{ name: out, type: lib-mem }]
routes: [{ name: r, from: [in], to: [out], filter: 'msg.json.n >' }]
`), hikyaku.WithoutSecrets())
	assert.ErrorContains(t, err, "filter", "CEL is compiled")
}

func TestRelaySendAndHealth(t *testing.T) {
	setup()
	s, err := hikyaku.Parse(context.Background(), "library.yaml", []byte(spec), hikyaku.WithoutSecrets(),
		hikyaku.WithEnvLookup(func(string) (string, bool) { return "", false }))
	require.NoError(t, err)
	r, err := hikyaku.New(context.Background(), s)
	require.NoError(t, err)
	defer r.Close(context.Background())
	rep := r.Send(context.Background(), envelope.New("in", "test", []byte(`{"n":3}`)))
	assert.Equal(t, "delivered", rep.Status)
	assert.True(t, r.Health(context.Background()).Ready)
}
