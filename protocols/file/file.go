// Package file is hikyaku's stdin, stdout, stderr and file protocol: NDJSON
// in and out, for local runs, piping and archives.
//
// Sources read one message per line: the line is the body (format: body,
// the default) or a wire envelope (format: envelope, as an archive or a DLQ
// holds them). stdin ends at EOF; a file source with follow: true keeps
// reading what is appended. Sinks write one line per message: the wire
// envelope (format: envelope, the default) or the body.
package file

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Register makes stdin, file, stdout and stderr available.
func Register() {
	plugin.RegisterSource("stdin", func(_ context.Context, s plugin.Spec) (plugin.Source, error) {
		cfg, err := sourceConfig(s)
		if err != nil {
			return nil, err
		}
		return &Source{name: s.Name, kind: "stdin", format: cfg.Format, open: func() (io.ReadCloser, error) { return io.NopCloser(Stdin), nil }}, nil
	})
	plugin.RegisterSource("file", func(_ context.Context, s plugin.Spec) (plugin.Source, error) {
		cfg, err := sourceConfig(s)
		if err != nil {
			return nil, err
		}
		if cfg.Path == "" {
			return nil, fmt.Errorf("source %s: path is required", s.Name)
		}
		return &Source{name: s.Name, kind: "file", format: cfg.Format, follow: cfg.Follow, path: cfg.Path, poll: 250 * time.Millisecond,
			open: func() (io.ReadCloser, error) { return os.Open(cfg.Path) }}, nil
	})
	plugin.RegisterSink("stdout", func(_ context.Context, s plugin.Spec) (plugin.Sink, error) {
		ep, err := sinkConfig(s)
		if err != nil {
			return nil, err
		}
		return &Sink{name: s.Name, label: "stdout", w: Stdout, body: ep.Format == "body"}, nil
	})
	plugin.RegisterSink("stderr", func(_ context.Context, s plugin.Spec) (plugin.Sink, error) {
		ep, err := sinkConfig(s)
		if err != nil {
			return nil, err
		}
		return &Sink{name: s.Name, label: "stderr", w: Stderr, body: ep.Format == "body"}, nil
	})
	plugin.RegisterSink("file", func(_ context.Context, s plugin.Spec) (plugin.Sink, error) {
		ep, err := sinkConfig(s)
		if err != nil {
			return nil, err
		}
		return OpenFileSink(s.Name, ep.Path, ep.Format == "body")
	})
}

// Stdin, Stdout and Stderr are the standard streams; tests replace them.
var (
	Stdin  io.Reader = os.Stdin
	Stdout io.Writer = os.Stdout
	Stderr io.Writer = os.Stderr
)

// MaxLine is the longest line a source reads (a larger one is an error).
const MaxLine = 16 << 20

func sourceConfig(s plugin.Spec) (*config.Source, error) {
	cfg, ok := s.Config.(*config.Source)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("source %s: missing config", s.Name)
	}
	if cfg.Format != "" && cfg.Format != "body" && cfg.Format != "envelope" {
		return nil, fmt.Errorf("source %s: format must be body or envelope", s.Name)
	}
	return cfg, nil
}

func sinkConfig(s plugin.Spec) (*config.Endpoint, error) {
	cfg, ok := s.Config.(*config.Endpoint)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("destination %s: missing config", s.Name)
	}
	if cfg.Format != "" && cfg.Format != "body" && cfg.Format != "envelope" {
		return nil, fmt.Errorf("destination %s: format must be envelope or body", s.Name)
	}
	return cfg, nil
}

// Source reads NDJSON lines.
type Source struct {
	name   string
	kind   string
	format string
	follow bool
	path   string
	poll   time.Duration
	open   func() (io.ReadCloser, error)

	mu      sync.Mutex
	lastErr error
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health reports the last read error.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Start reads until EOF (or, following a file, until ctx ends).
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	rc, err := s.open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	r := bufio.NewReaderSize(rc, 64<<10)
	var offset int64
	for n := 1; ; n++ {
		line, err := readLine(r)
		offset += int64(len(line))
		if len(bytes.TrimSpace(line)) > 0 {
			env, perr := s.parse(bytes.TrimRight(line, "\r\n"), n)
			if perr != nil {
				s.setErr(perr)
				return perr
			}
			select {
			case out <- env:
			case <-ctx.Done():
				return nil
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, io.EOF) {
			s.setErr(err)
			return fmt.Errorf("source %s: %w", s.name, err)
		}
		if !s.follow {
			return nil
		}
		// Following: wait for more, and start over if the file was truncated.
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.poll):
		}
		if fi, serr := os.Stat(s.path); serr == nil && fi.Size() < offset {
			_ = rc.Close()
			if rc, err = s.open(); err != nil {
				return err
			}
			r.Reset(rc)
			offset = 0
		}
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > MaxLine {
			return nil, fmt.Errorf("a line is longer than %d bytes", MaxLine)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func (s *Source) parse(line []byte, n int) (*envelope.Envelope, error) {
	if s.format == "envelope" {
		var env envelope.Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			return nil, fmt.Errorf("source %s: line %d is not an envelope: %w", s.name, n, err)
		}
		env.Source = s.name
		if env.ReceivedAt.IsZero() {
			env.ReceivedAt = time.Now().UTC()
		}
		return &env, nil
	}
	body := append([]byte(nil), line...)
	env := envelope.New(s.name, s.kind, body)
	env.Subject = s.path
	if json.Valid(body) {
		env.ContentType = "application/json"
	} else {
		env.ContentType = "text/plain; charset=utf-8"
	}
	env.Attributes["line"] = n
	return env, nil
}

func (s *Source) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr = err
}

// Sink writes NDJSON lines.
type Sink struct {
	name  string
	label string
	body  bool

	mu     sync.Mutex
	w      io.Writer
	closer io.Closer
}

// OpenFileSink appends to path, creating it and its directory.
func OpenFileSink(name, path string, body bool) (*Sink, error) {
	if path == "" {
		return nil, fmt.Errorf("destination %s: path is required", name)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("destination %s: %w", name, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", name, err)
	}
	return &Sink{name: name, label: path, w: f, closer: f, body: body}, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver writes one line.
func (s *Sink) Deliver(_ context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	var line []byte
	if s.body {
		line = bytes.TrimRight(env.Body, "\n")
		line = append(append([]byte(nil), line...), '\n')
	} else {
		b, err := json.Marshal(env)
		if err != nil {
			return nil, plugin.Permanentf("encode: %v", err)
		}
		line = append(b, '\n')
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return nil, plugin.Errorf(plugin.Transient, []string{"io"}, "%s is closed", s.label)
	}
	if _, err := s.w.Write(line); err != nil {
		return nil, plugin.Wrap(plugin.Transient, []string{"io"}, err)
	}
	return &plugin.Result{Endpoint: s.label}, nil
}

// Health is always fine for an open sink.
func (s *Sink) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.w == nil {
		return errors.New("closed")
	}
	return nil
}

// Close closes a file sink.
func (s *Sink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.closer != nil {
		err = s.closer.Close()
		s.w, s.closer = nil, nil
	}
	return err
}
