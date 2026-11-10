//go:build !lite

package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// examples/crm-plugin through the CLI: its crm destination type is served by
// an out-of-process Go plugin. validate starts the plugin (checking a sha256
// pin) and checks options: against the schema it declares; run delivers a
// lead to the CRM's API and dead-letters a message that is not one.
func TestCRMPluginExample(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the example plugin")
	}
	t.Setenv("CRM_TOKEN", "dev-token") // mamori's env: provider reads the process environment
	example, err := filepath.Abs("../../../examples/crm-plugin")
	require.NoError(t, err)
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin", "hikyaku-crm")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = example
	build.Env = append(os.Environ(), "GOWORK=off")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	for _, f := range []string{"hikyaku.yaml", "hikyaku.llmfake.yaml"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte(mustRead(t, filepath.Join(example, f))), 0o600))
	}

	code, stdout, stderr := hk(t, "validate", "-f", filepath.Join(dir, "hikyaku.yaml"))
	require.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "valid")

	write := func(name, spec string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(spec), 0o600))
		return p
	}
	twin := mustRead(t, filepath.Join(dir, "hikyaku.llmfake.yaml"))
	code, _, stderr = hk(t, "validate", "-f", write("typo.yaml", strings.Replace(twin, "pipeline: inbound", "pipline: inbound", 1)))
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "pipline", "the plugin's options schema is checked")
	code, _, stderr = hk(t, "validate", "-f", write("pattern.yaml", strings.Replace(twin, "pipeline: inbound", "pipeline: Inbound!", 1)))
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "pipeline", "the schema's pattern is checked too")

	sum := sha256.Sum256([]byte(mustRead(t, bin)))
	pin := func(hash string) string {
		return strings.Replace(twin, "env: { CRM_TOKEN", `sha256: "`+hash+`"`+"\n    env: { CRM_TOKEN", 1)
	}
	code, _, stderr = hk(t, "validate", "-f", write("pinned.yaml", pin(hex.EncodeToString(sum[:]))))
	assert.Equal(t, 0, code, stderr)
	code, _, stderr = hk(t, "validate", "-f", write("other.yaml", pin(strings.Repeat("ab", 32))))
	assert.NotEqual(t, 0, code)
	assert.Contains(t, stderr, "does not match its sha256", "another binary is refused")

	// The offline twin, on free ports, run from its directory.
	forms, api, admin := freeAddr(t), freeAddr(t), freeAddr(t)
	spec := strings.ReplaceAll(twin, `":8080"`, `"`+forms+`"`)
	spec = strings.ReplaceAll(spec, "127.0.0.1:8082", api)
	spec = strings.ReplaceAll(spec, `":9090"`, `"`+admin+`"`)
	write("run.yaml", spec)
	t.Chdir(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- Main([]string{"run", "-f", "run.yaml", "--color", "never"}, WithContext(ctx), WithStreams(strings.NewReader(""), &logs, &logs))
	}()
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + admin + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 20*time.Second, 50*time.Millisecond, "the relay did not come up: %s", logs.String())

	post := func(body string) int {
		t.Helper()
		resp, err := http.Post("http://"+forms+"/leads", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	assert.Equal(t, http.StatusOK, post(`{"email":"ada@example.com","name":"Ada"}`), "answered once the CRM has the lead: %s", logs.String())
	assert.Contains(t, mustRead(t, filepath.Join(dir, "out", "crm.ndjson")), `"email":"ada@example.com"`, "posted with the plugin's bearer token")

	assert.Equal(t, http.StatusAccepted, post(`{"name":"Bob"}`), "not a lead: dead-lettered")
	dlq := mustRead(t, filepath.Join(dir, "out", "dlq.ndjson"))
	assert.Contains(t, dlq, "not a lead: it has no email")
	assert.Contains(t, dlq, "Bob")

	cancel()
	select {
	case c := <-done:
		assert.Equal(t, 0, c, logs.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
