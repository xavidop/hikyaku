package grpcplugin

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

// levelTrace is hclog's trace level in slog terms: below debug.
const levelTrace = slog.LevelDebug - 4

func slogLevel(l hclog.Level) slog.Level {
	switch l {
	case hclog.Trace:
		return levelTrace
	case hclog.Debug, hclog.NoLevel:
		return slog.LevelDebug
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error:
		return slog.LevelError
	}
	return slog.LevelInfo
}

// levelName is the hclog name of a slog level, as a plugin writes it in
// its JSON log lines and as the host passes it in logLevelEnv.
func levelName(l slog.Level) string {
	switch {
	case l < slog.LevelDebug:
		return "trace"
	case l < slog.LevelInfo:
		return "debug"
	case l < slog.LevelWarn:
		return "info"
	case l < slog.LevelError:
		return "warn"
	}
	return "error"
}

// parseLevel reads logLevelEnv; anything else is debug.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return levelTrace
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelDebug
}

// enabledLevel is the most verbose level a logger records.
func enabledLevel(l *slog.Logger) slog.Level {
	for _, lvl := range []slog.Level{levelTrace, slog.LevelDebug, slog.LevelInfo, slog.LevelWarn} {
		if l.Enabled(context.Background(), lvl) {
			return lvl
		}
	}
	return slog.LevelError
}

// hclogAdapter is the hclog.Logger go-plugin logs through on the host: its
// own messages and every line of the plugin's stderr, which it parses
// (hclog JSON lines keep their level and fields). Both reach the host's
// slog logger.
type hclogAdapter struct {
	log     *slog.Logger
	name    string
	implied []any
}

var _ hclog.Logger = (*hclogAdapter)(nil)

func newHCLogAdapter(l *slog.Logger) *hclogAdapter { return &hclogAdapter{log: l} }

func (a *hclogAdapter) Log(level hclog.Level, msg string, args ...any) {
	if level == hclog.Off {
		return
	}
	lvl := slogLevel(level)
	if !a.log.Enabled(context.Background(), lvl) {
		return
	}
	// go-plugin appends the plugin's own timestamp to every parsed line;
	// the record has one already.
	if n := len(args); n >= 2 {
		if k, ok := args[n-2].(string); ok && k == "timestamp" {
			args = args[:n-2]
		}
	}
	a.log.Log(context.Background(), lvl, msg, args...)
}

func (a *hclogAdapter) Trace(msg string, args ...any) { a.Log(hclog.Trace, msg, args...) }
func (a *hclogAdapter) Debug(msg string, args ...any) { a.Log(hclog.Debug, msg, args...) }
func (a *hclogAdapter) Info(msg string, args ...any)  { a.Log(hclog.Info, msg, args...) }
func (a *hclogAdapter) Warn(msg string, args ...any)  { a.Log(hclog.Warn, msg, args...) }
func (a *hclogAdapter) Error(msg string, args ...any) { a.Log(hclog.Error, msg, args...) }

func (a *hclogAdapter) enabled(l hclog.Level) bool {
	return a.log.Enabled(context.Background(), slogLevel(l))
}

func (a *hclogAdapter) IsTrace() bool { return a.enabled(hclog.Trace) }
func (a *hclogAdapter) IsDebug() bool { return a.enabled(hclog.Debug) }
func (a *hclogAdapter) IsInfo() bool  { return a.enabled(hclog.Info) }
func (a *hclogAdapter) IsWarn() bool  { return a.enabled(hclog.Warn) }
func (a *hclogAdapter) IsError() bool { return a.enabled(hclog.Error) }

func (a *hclogAdapter) ImpliedArgs() []any { return a.implied }

func (a *hclogAdapter) With(args ...any) hclog.Logger {
	return &hclogAdapter{log: a.log.With(args...), name: a.name, implied: append(append([]any(nil), a.implied...), args...)}
}

func (a *hclogAdapter) Name() string { return a.name }

func (a *hclogAdapter) Named(name string) hclog.Logger {
	if a.name != "" {
		name = a.name + "." + name
	}
	return a.ResetNamed(name)
}

func (a *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{log: a.log, name: name, implied: a.implied}
}

// SetLevel is a no-op: the slog handler decides.
func (a *hclogAdapter) SetLevel(hclog.Level) {}

func (a *hclogAdapter) GetLevel() hclog.Level {
	for _, l := range []hclog.Level{hclog.Trace, hclog.Debug, hclog.Info, hclog.Warn, hclog.Error} {
		if a.enabled(l) {
			return l
		}
	}
	return hclog.Off
}

func (a *hclogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	level := slog.LevelInfo
	if opts != nil && opts.ForceLevel != hclog.NoLevel {
		level = slogLevel(opts.ForceLevel)
	}
	return slog.NewLogLogger(a.log.Handler(), level)
}

func (a *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return a.StandardLogger(opts).Writer()
}

// hclogTime is the timestamp layout go-plugin parses in a plugin's JSON log
// lines; a line with any other layout is not parsed at all.
const hclogTime = "2006-01-02T15:04:05.000000Z07:00"

// newPluginLogger is a plugin's logger: hclog JSON lines on the plugin's
// own stderr, which go-plugin parses on the host, so every record keeps its
// level and attributes in the host's log.
func newPluginLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String("@timestamp", t.Format(hclogTime))
				}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String("@level", levelName(l))
				}
			case slog.MessageKey:
				return slog.String("@message", a.Value.String())
			}
			return a
		},
	}))
}

// lineWriter logs what a plugin writes to its synced stdout or stderr
// (os.Stdout and os.Stderr once it serves), one record per line.
type lineWriter struct {
	mu   sync.Mutex
	buf  []byte
	emit func(line string)
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		if line := strings.TrimRight(string(w.buf[:i]), "\r"); line != "" {
			w.emit(line)
		}
		w.buf = w.buf[i+1:]
	}
	if len(w.buf) > 64<<10 { // a very long line without a newline
		w.emit(string(w.buf))
		w.buf = nil
	}
	return len(p), nil
}

func stdioWriter(l *slog.Logger, stream string) io.Writer {
	return &lineWriter{emit: func(line string) { l.Info(line, "stream", stream) }}
}
