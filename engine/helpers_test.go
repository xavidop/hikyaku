package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

// scripted is a sink whose attempts fail as scripted: script[i] is the
// error of attempt i+1 (nil = success); after the script, final.
type scripted struct {
	name   string
	mu     sync.Mutex
	script []error
	final  error
	delay  time.Duration
	calls  []*envelope.Envelope
	result *plugin.Result
	health error
	closed bool
	// hook, when set, runs as each attempt starts (true) and ends (false).
	hook func(env *envelope.Envelope, start bool)
}

func (s *scripted) Name() string { return s.name }

func (s *scripted) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	s.mu.Lock()
	n := len(s.calls)
	s.calls = append(s.calls, env.Clone())
	err := s.final
	if n < len(s.script) {
		err = s.script[n]
	}
	delay := s.delay
	res := s.result
	hook := s.hook
	s.mu.Unlock()
	if hook != nil {
		hook(env, true)
		defer hook(env, false)
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if res == nil {
		res = &plugin.Result{Status: 200, Endpoint: s.name}
	}
	return res, nil
}

func (s *scripted) Health(context.Context) error { return s.health }
func (s *scripted) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *scripted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *scripted) call(i int) *envelope.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i]
}

var (
	sinksMu sync.Mutex
	sinks   = map[string]*scripted{}
)

// The engine tests stand in for the protocol modules: "fault" sinks are
// scripted sinks looked up by name, "stdin" sources are listSources.
func init() {
	plugin.RegisterSink("fault", func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		sinksMu.Lock()
		defer sinksMu.Unlock()
		s, ok := sinks[spec.Name]
		if !ok {
			s = &scripted{name: spec.Name}
			sinks[spec.Name] = s
		}
		return s, nil
	})
	plugin.RegisterSink("http", func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		sinksMu.Lock()
		defer sinksMu.Unlock()
		s, ok := sinks[spec.Name]
		if !ok {
			s = &scripted{name: spec.Name}
			sinks[spec.Name] = s
		}
		return s, nil
	})
	plugin.RegisterSource("stdin", func(_ context.Context, spec plugin.Spec) (plugin.Source, error) {
		sourcesMu.Lock()
		defer sourcesMu.Unlock()
		src, ok := sources[spec.Name]
		if !ok {
			return nil, errors.New("no test source " + spec.Name)
		}
		return src, nil
	})
}

var (
	sourcesMu sync.Mutex
	sources   = map[string]*listSource{}
)

// listSource emits its messages, then returns (like stdin at EOF), or
// blocks until ctx ends when hold is set.
type listSource struct {
	name    string
	msgs    []*envelope.Envelope
	hold    bool
	err     error
	health  error
	onClose func()
}

// Close makes listSource a plugin.Closer, like a broker consumer.
func (l *listSource) Close(context.Context) error {
	if l.onClose != nil {
		l.onClose()
	}
	return nil
}

func (l *listSource) Name() string                 { return l.name }
func (l *listSource) Health(context.Context) error { return l.health }
func (l *listSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	for _, m := range l.msgs {
		select {
		case out <- m:
		case <-ctx.Done():
			return nil
		}
	}
	if l.err != nil {
		return l.err
	}
	if l.hold {
		<-ctx.Done()
	}
	return nil
}

// setSinks installs this test's scripted sinks.
func setSinks(t *testing.T, ss ...*scripted) {
	t.Helper()
	sinksMu.Lock()
	defer sinksMu.Unlock()
	sinks = map[string]*scripted{}
	for _, s := range ss {
		sinks[s.name] = s
	}
}

func setSources(t *testing.T, ls ...*listSource) {
	t.Helper()
	sourcesMu.Lock()
	defer sourcesMu.Unlock()
	sources = map[string]*listSource{}
	for _, l := range ls {
		sources[l.name] = l
	}
}

func spec(t *testing.T, yaml string) *config.Spec {
	t.Helper()
	l, err := config.LoadBytes(context.Background(), "test.yaml", []byte(yaml), config.LoadOptions{LookupEnv: func(string) (string, bool) { return "", false }})
	require.NoError(t, err)
	require.Empty(t, Check(l.Spec))
	return l.Spec
}

func build(t *testing.T, yaml string, o Options) *Engine {
	t.Helper()
	if o.Metrics == nil {
		o.Metrics = metrics.New()
	}
	if o.Rand == nil {
		o.Rand = func() float64 { return 0 } // no waits: retries are instant
	}
	if !o.NoSources {
		o.NoSources = true
	}
	e, err := New(context.Background(), spec(t, yaml), o)
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })
	return e
}

func msg(source, body string) *envelope.Envelope {
	e := envelope.New(source, "test", []byte(body))
	e.ContentType = "application/json"
	return e
}

var (
	err503 = plugin.StatusError(503, 0, "")
	err400 = plugin.StatusError(400, 0, "bad payload")
)

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
