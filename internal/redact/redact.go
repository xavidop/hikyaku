// Package redact replaces resolved secret values with ‹secret:NAME› in every
// string hikyaku writes: transcripts, HARs, logs, reports and errors. Each
// value is matched in its plain, base64, URL-encoded and JSON-escaped forms.
package redact

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// MinLength is the shortest value the redactor replaces. A one- or two-byte
// secret (a PIN digit, "ok") would rewrite ordinary text everywhere, so such
// values are ignored and Add reports it.
const MinLength = 3

// envSecretRe matches environment variable names that conventionally hold
// secrets; AddEnv registers only those.
var envSecretRe = regexp.MustCompile(`(?i)(_KEY|_SECRET|_TOKEN)$|PASSWORD`)

// Token renders the placeholder written in place of the secret name.
func Token(name string) string { return "‹secret:" + name + "›" }

type needle struct {
	value string
	token string
}

// Redactor holds the values to hide. It is safe for concurrent use.
type Redactor struct {
	mu      sync.RWMutex
	needles []needle
	names   map[string]bool
}

// New returns an empty Redactor.
func New() *Redactor { return &Redactor{names: map[string]bool{}} }

// Add registers value under name, in its plain, base64 (standard and raw
// URL), URL-encoded and JSON-escaped forms. It returns false, and registers
// nothing, when value is shorter than MinLength.
func (r *Redactor) Add(name, value string) bool {
	if len(value) < MinLength {
		return false
	}
	token := Token(name)
	forms := []string{
		value,
		base64.StdEncoding.EncodeToString([]byte(value)),
		base64.RawURLEncoding.EncodeToString([]byte(value)),
		url.QueryEscape(value),
	}
	if b, err := json.Marshal(value); err == nil {
		forms = append(forms, string(b[1:len(b)-1]))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names[name] = true
	for _, f := range forms {
		if len(f) < MinLength {
			continue
		}
		dup := false
		for _, n := range r.needles {
			if n.value == f {
				dup = true
				break
			}
		}
		if !dup {
			r.needles = append(r.needles, needle{value: f, token: token})
		}
	}
	// Longest first, so a value that contains another is replaced whole.
	sort.SliceStable(r.needles, func(i, j int) bool { return len(r.needles[i].value) > len(r.needles[j].value) })
	return true
}

// AddEnv registers an environment variable's value when its name looks like a
// secret (*_KEY, *_SECRET, *_TOKEN or *PASSWORD*). It is the second line of
// defence for ${VAR} values that should have been secrets.
func (r *Redactor) AddEnv(name, value string) bool {
	if !envSecretRe.MatchString(name) {
		return false
	}
	return r.Add(name, value)
}

// Names lists the registered secret names, sorted.
func (r *Redactor) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.names))
	for n := range r.names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Redact replaces every registered value in s with its token.
func (r *Redactor) Redact(s string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, n := range r.needles {
		if strings.Contains(s, n.value) {
			s = strings.ReplaceAll(s, n.value, n.token)
		}
	}
	return s
}

// RedactBytes is Redact for bytes.
func (r *Redactor) RedactBytes(b []byte) []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, n := range r.needles {
		if bytes.Contains(b, []byte(n.value)) {
			b = bytes.ReplaceAll(b, []byte(n.value), []byte(n.token))
		}
	}
	return b
}

// Writer wraps w so that every line written through it is redacted. Output is
// buffered until a newline (a secret split across two writes is still
// caught); Close flushes a trailing partial line.
func (r *Redactor) Writer(w io.Writer) io.WriteCloser { return &writer{w: w, r: r} }

type writer struct {
	mu  sync.Mutex
	w   io.Writer
	r   *Redactor
	buf []byte
}

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := w.buf[:i+1]
		w.buf = w.buf[i+1:]
		if _, err := w.w.Write(w.r.RedactBytes(line)); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// Close writes a trailing partial line.
func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.w.Write(w.r.RedactBytes(w.buf))
	w.buf = nil
	return err
}

// Handler wraps a slog.Handler so that the message and every string attribute
// are redacted before h sees them.
func (r *Redactor) Handler(h slog.Handler) slog.Handler { return &handler{h: h, r: r} }

type handler struct {
	h slog.Handler
	r *Redactor
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool { return h.h.Enabled(ctx, l) }

func (h *handler) Handle(ctx context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, h.r.Redact(rec.Message), rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.attr(a))
		return true
	})
	return h.h.Handle(ctx, out)
}

func (h *handler) attr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return slog.String(a.Key, h.r.Redact(v.String()))
	case slog.KindGroup:
		attrs := v.Group()
		out := make([]any, 0, len(attrs))
		for _, g := range attrs {
			out = append(out, h.attr(g))
		}
		return slog.Group(a.Key, out...)
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			return slog.String(a.Key, h.r.Redact(err.Error()))
		}
		return slog.String(a.Key, h.r.Redact(v.String()))
	default:
		return a
	}
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = h.attr(a)
	}
	return &handler{h: h.h.WithAttrs(out), r: h.r}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{h: h.h.WithGroup(name), r: h.r}
}
