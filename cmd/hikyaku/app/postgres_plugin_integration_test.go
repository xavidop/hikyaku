//go:build integration && !lite

package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// examples/postgres-plugin's local twin against a PostgreSQL container: a
// webhook lands in the events table, and a row of the outbox table is
// delivered to the file and marked delivered.
func TestPostgresPluginExampleRuns(t *testing.T) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("relay"), tcpostgres.WithUsername("hikyaku"), tcpostgres.WithPassword("secret"),
		tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	t.Setenv("DATABASE_URL", url)
	psql := func(sql string) string {
		t.Helper()
		code, out, err := ctr.Exec(ctx, []string{"psql", "-U", "hikyaku", "-d", "relay", "-At", "-c", sql}, tcexec.Multiplexed())
		require.NoError(t, err)
		b, err := io.ReadAll(out)
		require.NoError(t, err)
		require.Equal(t, 0, code, "%s: %s", sql, b)
		return strings.TrimSpace(string(b))
	}

	dir := t.TempDir()
	buildPostgresPlugin(t, dir)
	twin := mustRead(t, filepath.Join(dir, "hikyaku.llmfake.yaml"))
	webhooks, admin := freeAddr(t), freeAddr(t)
	spec := strings.ReplaceAll(twin, `":8080"`, `"`+webhooks+`"`)
	spec = strings.ReplaceAll(spec, `":9090"`, `"`+admin+`"`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "run.yaml"), []byte(spec), 0o600))
	t.Chdir(dir)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- Main([]string{"run", "-f", "run.yaml", "--color", "never"}, WithContext(runCtx), WithStreams(strings.NewReader(""), &logs, &logs))
	}()
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + admin + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 50*time.Millisecond, "the relay did not come up: %s", logs.String())

	resp, err := http.Post("http://"+webhooks+"/ingest", "application/json", strings.NewReader(`{"type":"invoice.paid","amount":12}`))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "answered once the row is in the table: %s", logs.String())
	assert.Equal(t, "invoice.paid", psql(`SELECT body->>'type' FROM events`), "one row, with the body as jsonb")

	// The table is created by the source's first poll.
	require.Eventually(t, func() bool { return psql(`SELECT to_regclass('outbox') IS NOT NULL`) == "t" }, 10*time.Second, 100*time.Millisecond)
	psql(`INSERT INTO outbox (subject, payload, headers) VALUES ('order-1', '{"n":1}', '{"X-Event":"order.paid"}')`)
	outbox := filepath.Join(dir, "out", "outbox.ndjson")
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(outbox)
		return err == nil && strings.Contains(string(b), `"order-1"`)
	}, 10*time.Second, 100*time.Millisecond, "the row was not delivered: %s", logs.String())
	var line struct {
		ID      string              `json:"id"`
		Subject string              `json:"subject"`
		Headers map[string][]string `json:"headers"`
		Body    map[string]any      `json:"body"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.SplitN(mustRead(t, outbox), "\n", 2)[0]), &line))
	assert.Equal(t, "outbox-1", line.ID, "the row's ID")
	assert.Equal(t, "order-1", line.Subject, "the subject travels with the message")
	assert.Equal(t, []string{"order.paid"}, line.Headers["X-Event"], "the headers too")
	assert.Equal(t, float64(1), line.Body["n"])
	require.Eventually(t, func() bool { return psql(`SELECT count(*) FROM outbox WHERE delivered_at IS NOT NULL`) == "1" }, 10*time.Second, 100*time.Millisecond, "marked delivered")

	cancel()
	select {
	case c := <-done:
		assert.Equal(t, 0, c, logs.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
