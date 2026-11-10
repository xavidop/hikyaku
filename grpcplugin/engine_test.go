package grpcplugin

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
)

// TestRelayEndToEnd runs a relay whose source and destination are both
// served by the example plugin, wired the way the CLI wires plugins: read
// plugins:, launch them, register their kinds, then load and validate the
// spec with their types.
func TestRelayEndToEnd(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.ndjson")
	settles := filepath.Join(dir, "settle.log")
	spec := fmt.Sprintf(`
version: 1
name: e2e
plugins:
  - { name: echo, command: ./%s }
sources:
  - { name: ticks, type: ticker, options: { interval: 10ms, count: 3, settle_log: %q } }
destinations:
  - { name: out, type: echo, options: { file: %q } }
routes:
  - { name: r, from: [ticks], to: [out] }
`, filepath.Base(echoBin), settles, out)
	ctx := context.Background()
	logs, log := newLogs(t)

	raw, err := config.LoadBytes(ctx, "s.yaml", []byte(spec), config.LoadOptions{SkipValidate: true})
	require.NoError(t, err)
	set, err := LoadAll(ctx, raw.Spec.Plugins, filepath.Dir(echoBin), log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	set.Register()
	loaded, err := config.LoadBytes(ctx, "s.yaml", []byte(spec), config.LoadOptions{Extensions: set.Extensions()})
	require.NoError(t, err)

	e, err := engine.New(ctx, loaded.Spec, engine.Options{Logger: log})
	require.NoError(t, err)
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, e.Run(runCtx), "the relay stops once the ticker has run out")
	require.NoError(t, runCtx.Err())

	assert.ElementsMatch(t, []string{`{"n":0}`, `{"n":1}`, `{"n":2}`}, lines(t, out))
	settled := lines(t, settles)
	assert.Len(t, settled, 3, "every message settled once")
	for _, l := range settled {
		assert.Regexp(t, `^\d [0-9A-Z]{26} true$`, l)
	}
	assert.NotEmpty(t, logs.records("echo closed"), "the engine closed the destination")
}
