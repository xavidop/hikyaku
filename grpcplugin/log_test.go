package grpcplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPluginLoggerWritesHCLogJSON(t *testing.T) {
	var b bytes.Buffer
	log := newPluginLogger(&b, slog.LevelDebug)
	log.Warn("disk almost full", "free", 3, slog.Group("g", "k", "v"))
	log.Log(context.Background(), levelTrace, "dropped below debug")

	var line map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(b.Bytes()), &line), "one line, the trace record dropped")
	assert.Equal(t, "warn", line["@level"])
	assert.Equal(t, "disk almost full", line["@message"])
	assert.Equal(t, float64(3), line["free"])
	assert.Equal(t, map[string]any{"k": "v"}, line["g"])
	ts, ok := line["@timestamp"].(string)
	require.True(t, ok)
	_, err := time.Parse(hclogTime, ts)
	assert.NoError(t, err, "go-plugin only parses lines with its timestamp layout")
	assert.NotContains(t, line, "level")
	assert.NotContains(t, line, "msg")
}

func TestLevels(t *testing.T) {
	for l, name := range map[slog.Level]string{levelTrace: "trace", slog.LevelDebug: "debug", slog.LevelInfo: "info", slog.LevelWarn: "warn", slog.LevelError: "error", slog.LevelError + 4: "error"} {
		assert.Equal(t, name, levelName(l))
		if l <= slog.LevelError {
			assert.Equal(t, l, parseLevel(strings.ToUpper(name)))
		}
	}
	assert.Equal(t, slog.LevelDebug, parseLevel(""))
	assert.Equal(t, slog.LevelDebug, parseLevel("verbose"))
	for hl, sl := range map[hclog.Level]slog.Level{hclog.Trace: levelTrace, hclog.Debug: slog.LevelDebug, hclog.NoLevel: slog.LevelDebug, hclog.Info: slog.LevelInfo, hclog.Warn: slog.LevelWarn, hclog.Error: slog.LevelError} {
		assert.Equal(t, sl, slogLevel(hl))
	}
	info := slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelInfo}))
	assert.Equal(t, slog.LevelInfo, enabledLevel(info))
	off := slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
	assert.Equal(t, slog.LevelError, enabledLevel(off))
}

func TestHCLogAdapter(t *testing.T) {
	var b bytes.Buffer
	a := newHCLogAdapter(slog.New(slog.NewJSONHandler(&b, &slog.HandlerOptions{Level: slog.LevelDebug})))
	assert.Equal(t, hclog.Debug, a.GetLevel())
	assert.False(t, a.IsTrace())
	assert.True(t, a.IsDebug())
	assert.True(t, a.IsInfo())
	assert.True(t, a.IsWarn())
	assert.True(t, a.IsError())

	l := a.Named("plugin").Named("echo").With("k", "v")
	assert.Equal(t, "plugin.echo", l.Name())
	assert.Equal(t, []any{"k", "v"}, l.ImpliedArgs())
	assert.Equal(t, "other", l.ResetNamed("other").Name())
	l.SetLevel(hclog.Error) // the slog handler decides
	l.Info("from the plugin", "file", "x", "timestamp", "2026-01-01T00:00:00.000Z")
	l.Trace("too verbose")
	l.Log(hclog.Off, "never")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(b.Bytes()), &rec))
	assert.Equal(t, "INFO", rec["level"])
	assert.Equal(t, "from the plugin", rec["msg"])
	assert.Equal(t, "v", rec["k"])
	assert.Equal(t, "x", rec["file"])
	assert.NotContains(t, rec, "timestamp", "go-plugin's copy of the plugin's timestamp is dropped")

	b.Reset()
	l.Warn("w")
	l.Error("e")
	l.Debug("d")
	assert.Equal(t, 3, strings.Count(b.String(), "\n"))

	b.Reset()
	l.StandardLogger(&hclog.StandardLoggerOptions{ForceLevel: hclog.Warn}).Print("std")
	assert.Contains(t, b.String(), `"level":"WARN"`)
	b.Reset()
	_, _ = l.StandardWriter(nil).Write([]byte("written\n"))
	assert.Contains(t, b.String(), `"msg":"written"`)

	quiet := newHCLogAdapter(slog.New(slog.NewJSONHandler(&b, &slog.HandlerOptions{Level: slog.LevelError + 1})))
	assert.Equal(t, hclog.Off, quiet.GetLevel())
}

func TestLineWriter(t *testing.T) {
	var got []string
	w := &lineWriter{emit: func(l string) { got = append(got, l) }}
	_, _ = w.Write([]byte("one\r\ntw"))
	_, _ = w.Write([]byte("o\n\nthree"))
	assert.Equal(t, []string{"one", "two"}, got)
	_, _ = w.Write(bytes.Repeat([]byte("x"), 70<<10))
	require.Len(t, got, 3, "a very long line without a newline is not held forever")
	assert.True(t, strings.HasPrefix(got[2], "three"))

	var b bytes.Buffer
	sw := stdioWriter(slog.New(slog.NewJSONHandler(&b, nil)), "stdout")
	_, _ = sw.Write([]byte("hello\n"))
	assert.Contains(t, b.String(), `"msg":"hello","stream":"stdout"`)
}
