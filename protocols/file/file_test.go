package file

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func init() { Register() }

func source(t *testing.T, kind string, cfg config.Source) plugin.Source {
	t.Helper()
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg})
	require.NoError(t, err)
	return s
}

func drain(t *testing.T, s plugin.Source) []*envelope.Envelope {
	t.Helper()
	out := make(chan *envelope.Envelope, 100)
	require.NoError(t, s.Start(context.Background(), out))
	close(out)
	var got []*envelope.Envelope
	for e := range out {
		got = append(got, e)
	}
	return got
}

func TestStdinLinesUntilEOF(t *testing.T) {
	Stdin = strings.NewReader("{\"a\":1}\n\nplain text\r\n{\"b\":2}")
	got := drain(t, source(t, "stdin", config.Source{}))
	require.Len(t, got, 3, "blank lines are skipped; the last line needs no newline")
	assert.Equal(t, `{"a":1}`, got[0].BodyText())
	assert.Equal(t, "application/json", got[0].ContentType)
	assert.Equal(t, "plain text", got[1].BodyText())
	assert.Equal(t, "text/plain; charset=utf-8", got[1].ContentType)
	assert.Equal(t, "in", got[2].Source)
	assert.Equal(t, "stdin", got[2].Protocol)
	assert.Equal(t, 4, got[2].Attributes["line"])
	assert.NoError(t, source(t, "stdin", config.Source{}).Health(context.Background()))
}

func TestStdinEnvelopeFormat(t *testing.T) {
	orig := envelope.New("webhooks", "http", []byte(`{"x":1}`))
	b, _ := json.Marshal(orig)
	Stdin = bytes.NewReader(append(b, '\n'))
	got := drain(t, source(t, "stdin", config.Source{Format: "envelope"}))
	require.Len(t, got, 1)
	assert.Equal(t, orig.ID, got[0].ID, "the idempotency key survives a replay")
	assert.Equal(t, "in", got[0].Source, "it now comes from this source")

	Stdin = strings.NewReader("not an envelope\n")
	s := source(t, "stdin", config.Source{Format: "envelope"})
	err := s.Start(context.Background(), make(chan *envelope.Envelope, 1))
	assert.ErrorContains(t, err, "line 1 is not an envelope")
	assert.Error(t, s.Health(context.Background()))
}

func TestFileFollow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "in.ndjson")
	require.NoError(t, os.WriteFile(path, []byte("one\n"), 0o644))
	s := source(t, "file", config.Source{Path: path, Follow: true})
	s.(*Source).poll = 10 * time.Millisecond
	out := make(chan *envelope.Envelope, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Start(ctx, out) }()
	assert.Equal(t, "one", (<-out).BodyText())
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, _ = f.WriteString("two\n")
	_ = f.Close()
	select {
	case e := <-out:
		assert.Equal(t, "two", e.BodyText())
	case <-time.After(2 * time.Second):
		t.Fatal("appended line not read")
	}
	cancel()
	assert.NoError(t, <-done)
}

func TestFileSourceErrors(t *testing.T) {
	_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "f", Kind: "file", Config: &config.Source{}})
	assert.ErrorContains(t, err, "path is required")
	_, err = plugin.NewSource(context.Background(), plugin.Spec{Name: "f", Kind: "stdin", Config: &config.Source{Format: "xml"}})
	assert.Error(t, err)
	_, err = plugin.NewSource(context.Background(), plugin.Spec{Name: "f", Kind: "stdin"})
	assert.Error(t, err)
	s := source(t, "file", config.Source{Path: filepath.Join(t.TempDir(), "missing")})
	assert.Error(t, s.Start(context.Background(), make(chan *envelope.Envelope)))
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func TestStdoutFormats(t *testing.T) {
	buf := &syncBuf{}
	Stdout = buf
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: "stdout", Config: &config.Endpoint{Type: "stdout", Format: "body"}})
	require.NoError(t, err)
	r, err := sink.Deliver(context.Background(), envelope.New("s", "t", []byte("hello\n")))
	require.NoError(t, err)
	assert.Equal(t, "stdout", r.Endpoint)
	assert.Equal(t, "hello\n", buf.b.String())

	Stderr = buf
	buf.b.Reset()
	sink, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "err", Kind: "stderr", Config: &config.Endpoint{Type: "stderr"}})
	require.NoError(t, err)
	env := envelope.New("s", "t", []byte(`{"k":"v"}`))
	_, err = sink.Deliver(context.Background(), env)
	require.NoError(t, err)
	var back envelope.Envelope
	require.NoError(t, json.Unmarshal(buf.b.Bytes(), &back))
	assert.Equal(t, env.ID, back.ID)
	assert.NoError(t, sink.Close(context.Background()))
}

func TestFileSinkConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out", "archive.ndjson")
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "archive", Kind: "file", Config: &config.Endpoint{Type: "file", Path: path}})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sink.Deliver(context.Background(), envelope.New("s", "t", []byte(`{"n":1}`)))
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	require.NoError(t, sink.Health(context.Background()))
	require.NoError(t, sink.Close(context.Background()))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	assert.Len(t, lines, 50, "every line is whole")
	for _, l := range lines {
		assert.True(t, json.Valid([]byte(l)))
	}
	_, err = sink.Deliver(context.Background(), envelope.New("s", "t", nil))
	assert.Error(t, err, "closed")
	assert.Error(t, sink.Health(context.Background()))

	_, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "x", Kind: "file", Config: &config.Endpoint{Type: "file"}})
	assert.ErrorContains(t, err, "path is required")
	_, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "x", Kind: "stdout", Config: &config.Endpoint{Format: "csv"}})
	assert.Error(t, err)
}
