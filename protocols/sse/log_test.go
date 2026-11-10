package sse

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// records keeps what a source logs.
type records struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *records) Enabled(context.Context, slog.Level) bool { return true }
func (r *records) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *records) WithGroup(string) slog.Handler            { return r }
func (r *records) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec.Clone())
	return nil
}

func (r *records) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level == level && rec.Message == msg {
			n++
		}
	}
	return n
}

func TestSubscribeLogsReconnectsAtWarn(t *testing.T) {
	logs := &records{}
	cfg := &config.Source{Name: "in", Type: "sse", URL: "http://" + closedAddr(t) + "/",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 3}}
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "sse", Config: cfg, Env: plugin.Env{Logger: slog.New(logs)}})
	require.NoError(t, err)
	require.Error(t, s.Start(context.Background(), make(chan *envelope.Envelope)))
	assert.Equal(t, 2, logs.count(slog.LevelWarn, "sse source reconnecting"), "one warning per reconnect; none once it gives up")
}
