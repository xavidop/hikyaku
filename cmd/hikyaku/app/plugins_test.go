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

// An out-of-process plugin through the CLI: hikyaku starts the binary a
// spec lists under plugins:, validates the spec with the plugin's types and
// its options schema, and delivers through the plugin's sink.
func TestOutOfProcessPluginThroughTheCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the example plugin")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "hikyaku-plugin-echo")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "../../../grpcplugin/example/hikyaku-plugin-echo"
	build.Env = append(os.Environ(), "GOWORK=off")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)

	echoed := filepath.Join(dir, "echo.ndjson")
	spec := filepath.Join(dir, "hikyaku.yaml")
	require.NoError(t, os.WriteFile(spec, []byte(`
version: 1
name: plugins
plugins:
  - { name: echo, command: ./hikyaku-plugin-echo }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: echo, options: { file: `+echoed+` } }
routes: [{ name: r, from: [in], to: [out] }]
`), 0o600))

	// First, while nothing registered the plugin's types in this process.
	code, stdout, stderr := hk(t, "schema", "-f", spec)
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, `"echo"`, "the schema knows the spec's plugin types")

	code, stdout, stderr = hk(t, "validate", "-f", spec)
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "valid")

	code, stdout, stderr = hk(t, "send", "-f", spec, "--to", "out", "--data", `{"hello":"plugin"}`)
	require.Equal(t, 0, code, "%s\n%s", stdout, stderr)
	b, err := os.ReadFile(echoed)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"hello":"plugin"`, "the plugin process got the message")

	code, stdout, stderr = hk(t, "doctor", "-f", spec)
	require.Equal(t, 0, code, "%s\n%s", stdout, stderr)
	assert.Contains(t, stdout, "✓ plugins")
	assert.Contains(t, stdout, "✓ destination out", "doctor checks the plugin's sink, on the one plugin process it started")

	bad := strings.Replace(mustRead(t, spec), "options: { file: ", "options: { nope: 1, file: ", 1)
	require.NoError(t, os.WriteFile(spec, []byte(bad), 0o600))
	code, _, stderr = hk(t, "validate", "-f", spec)
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "nope", "the plugin's options schema is checked")
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}
