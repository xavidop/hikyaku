package grpcplugin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// echoBin is the example plugin, built once for the whole run.
var echoBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "grpcplugin-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	echoBin = filepath.Join(dir, "hikyaku-plugin-echo")
	build := exec.Command("go", "build", "-o", echoBin, "./example/hikyaku-plugin-echo")
	build.Env = append(os.Environ(), "GOWORK=off")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building the example plugin: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	if leaked := leakedProcesses(dir); code == 0 && leaked != "" {
		fmt.Fprintf(os.Stderr, "plugin processes left running:\n%s\n", leaked)
		code = 1
	}
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// leakedProcesses lists the processes still running a binary under dir.
func leakedProcesses(dir string) string {
	out, err := exec.Command("pgrep", "-fl", dir).Output()
	if err != nil {
		return "" // pgrep exits 1 when nothing matches (or is missing)
	}
	return strings.TrimSpace(string(out))
}

// logBuffer collects a test's logs (JSON lines) for assertions, and prints
// them when the test fails. Plugin goroutines may log after the test ends,
// so it never writes to t directly.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// records are the logged records with msg.
func (b *logBuffer) records(msg string) []map[string]any {
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(b.String()))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r map[string]any
		if json.Unmarshal(sc.Bytes(), &r) == nil && r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

func newLogs(t *testing.T) (*logBuffer, *slog.Logger) {
	t.Helper()
	b := &logBuffer{}
	t.Cleanup(func() {
		if t.Failed() || os.Getenv("GRPCPLUGIN_TEST_LOGS") != "" {
			t.Logf("logs:\n%s", b.String())
		}
	})
	return b, slog.New(slog.NewJSONHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// launchEcho launches the example plugin.
func launchEcho(t *testing.T, name string, edit ...func(*config.PluginBinary)) (*Plugin, *logBuffer) {
	t.Helper()
	logs, log := newLogs(t)
	cfg := config.PluginBinary{Name: name, Command: echoBin}
	for _, e := range edit {
		e(&cfg)
	}
	p, err := Launch(context.Background(), cfg, "", log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p, logs
}

// openEcho opens an echo destination writing to a new file, with the test
// hooks on.
func openEcho(t *testing.T, p *Plugin, name string) (plugin.Sink, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), name+".ndjson")
	sink, err := p.sinkFactory("echo")(context.Background(), plugin.Spec{
		Name: name, Kind: "echo",
		Options: map[string]any{"file": file, "test_hooks": true},
		Env:     plugin.Env{Relay: "test"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	return sink, file
}

// openTicker opens a ticker source.
func openTicker(t *testing.T, p *Plugin, name string, options map[string]any, src *config.Source) plugin.Source {
	t.Helper()
	if src == nil {
		src = &config.Source{}
	}
	src.Name, src.Type = name, "ticker"
	s, err := p.sourceFactory("ticker")(context.Background(), plugin.Spec{
		Name: name, Kind: "ticker", Options: options, Config: src, Env: plugin.Env{Relay: "test"},
	})
	require.NoError(t, err)
	return s
}

// runSource starts a source; the returned stop ends it and returns what
// Start returned.
func runSource(t *testing.T, s plugin.Source) (<-chan *envelope.Envelope, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				err = fmt.Errorf("the source did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return out, stop
}

// recv waits for the next message.
func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(10 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

// lines reads a file's lines (none when it does not exist yet).
func lines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	s := strings.TrimSuffix(string(b), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// msg is an envelope with headers.
func msg(body string, headers ...string) *envelope.Envelope {
	env := envelope.New("in", "http", []byte(body))
	for i := 0; i+1 < len(headers); i += 2 {
		env.SetHeader(headers[i], headers[i+1])
	}
	return env
}
