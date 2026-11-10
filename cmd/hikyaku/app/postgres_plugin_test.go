//go:build !lite

package app

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildPostgresPlugin compiles examples/postgres-plugin into dir/bin and
// copies its specs next to it.
func buildPostgresPlugin(t *testing.T, dir string) (example, bin string) {
	t.Helper()
	example, err := filepath.Abs("../../../examples/postgres-plugin")
	require.NoError(t, err)
	bin = filepath.Join(dir, "bin", "hikyaku-postgres")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = example
	build.Env = append(os.Environ(), "GOWORK=off")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	for _, f := range []string{"hikyaku.yaml", "hikyaku.llmfake.yaml"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte(mustRead(t, filepath.Join(example, f))), 0o600))
	}
	return example, bin
}

// examples/postgres-plugin through the CLI: a plugin that serves a source
// type and a destination type of the same name. validate starts it and
// checks both options: blocks against the schemas it declares. No database
// is needed for that: the plugin connects on first use.
func TestPostgresPluginExample(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the example plugin")
	}
	t.Setenv("DATABASE_URL", "postgres://hikyaku:secret@127.0.0.1:1/relay")
	dir := t.TempDir()
	buildPostgresPlugin(t, dir)

	code, stdout, stderr := hk(t, "validate", "-f", filepath.Join(dir, "hikyaku.yaml"))
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "valid")
	code, _, stderr = hk(t, "validate", "-f", filepath.Join(dir, "hikyaku.llmfake.yaml"))
	require.Equal(t, 0, code, stderr)

	write := func(name, spec string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(spec), 0o600))
		return p
	}
	twin := mustRead(t, filepath.Join(dir, "hikyaku.llmfake.yaml"))
	for _, c := range []struct{ name, from, to, want string }{
		{"sink-typo", "table: events, create_table", "tabel: events, create_table", "tabel"},
		{"sink-pattern", "table: events,", "table: drop table;,", "table"},
		{"sink-enum", "table: events, create_table: true", "table: events, on_conflict: update", "on_conflict"},
		{"source-typo", "table: outbox, interval", "table: outbox, intervall", "intervall"},
		{"source-duration", "interval: 200ms", "interval: soon", "interval"},
		{"source-enum", "claim_ttl: 30s", "claim_ttl: 30s, on_delivered: archive", "on_delivered"},
	} {
		spec := strings.Replace(twin, c.from, c.to, 1)
		require.NotEqual(t, twin, spec, c.name)
		code, _, stderr := hk(t, "validate", "-f", write(c.name+".yaml", spec))
		assert.Equal(t, 2, code, c.name)
		assert.Contains(t, stderr, c.want, "%s: the plugin's options schema is checked", c.name)
	}
}
