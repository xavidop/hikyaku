//go:build integration

package app

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/llm"
	"github.com/xavidop/hikyaku/llm/llmfake"
	"github.com/xavidop/hikyaku/store"
)

// The history-aware AI router on a real Redis (design §13): routing keeps
// working while Redis is dead, and two relays sharing one Redis see each
// other's calls.

func redisOnFixedPort(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, ln.Close())
	ctr, err := tcredis.Run(context.Background(), "redis:7-alpine", testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		hc.PortBindings = network.PortMap{network.MustParsePort("6379/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
	}))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	return ctr, "127.0.0.1:" + port
}

func triageSpec(t *testing.T, addr string) *config.Spec {
	t.Helper()
	dir := t.TempDir()
	doc := fmt.Sprintf(`
version: 1
name: triage-it
models:
  decision: llmfake-decisions/clef
sources: [{ name: webhooks, type: stdin }]
destinations:
  - { name: billing, type: file, path: %[1]q }
  - { name: support, type: file, path: %[2]q }
stores:
  - name: shared
    type: redis
    addr: %[3]q
    timeout: 300ms
    circuit_breaker: { open_for: 1s, min_requests: 2 }
routes:
  - name: triage
    from: [webhooks]
    router:
      type: ai
      engine: decision
      instructions: Route customer events to the team that must see them.
      state: "{{ .bodyText }}"
      candidates:
        - { name: billing, description: "Payments, invoices, refunds, charges" }
        - { name: support, description: "Bugs, outages, errors, how-to questions" }
      min_confidence: 0.3
      history:
        store: shared
        key: "{{ .json.customer_id }}"
        window: { max_entries: 10, max_age: 24h }
        ttl: 1d
        sticky: { enabled: true, follow_up_threshold: 0.8 }
      fallback:
        - { type: static, to: [support] }
`, filepath.Join(dir, "billing.ndjson"), filepath.Join(dir, "support.ndjson"), addr)
	l, err := config.LoadBytes(context.Background(), "triage.yaml", []byte(doc), config.LoadOptions{})
	require.NoError(t, err)
	return l.Spec
}

func relay(t *testing.T, spec *config.Spec) *engine.Engine {
	t.Helper()
	f, err := llm.New(context.Background(), spec, llm.Options{Plugins: llmfake.Plugins()})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), spec, engine.Options{AI: f, NoSources: true, StoreFactories: Deps(os.LookupEnv).StoreFactories})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })
	return e
}

func event(customer, text string) *envelope.Envelope {
	env := envelope.New("webhooks", "stdin", []byte(fmt.Sprintf(`{"customer_id":%q,"text":%q}`, customer, text)))
	env.ContentType = "application/json"
	return env
}

func routedTo(t *testing.T, rep *engine.Report) []string {
	t.Helper()
	require.Len(t, rep.Routes, 1)
	require.NotNil(t, rep.Routes[0].Decision)
	return rep.Routes[0].Decision.Destinations
}

func history(t *testing.T, e *engine.Engine, key string) int {
	t.Helper()
	recs, err := e.Stores()["shared"].Recent(context.Background(), "triage", key, store.Window{MaxEntries: 10})
	if err != nil {
		return -1
	}
	return len(recs)
}

func TestIntegrationRoutingSurvivesRedisKilledMidRun(t *testing.T) {
	ctr, addr := redisOnFixedPort(t)
	e := relay(t, triageSpec(t, addr))
	ctx := context.Background()

	rep := e.Inject(ctx, event("c1", "I was charged twice for my invoice"))
	assert.Equal(t, "delivered", rep.Status)
	assert.Equal(t, []string{"billing"}, routedTo(t, rep))
	require.Eventually(t, func() bool { return history(t, e, "c1") == 1 }, 5*time.Second, 50*time.Millisecond)

	stop := 2 * time.Second
	require.NoError(t, ctr.Stop(ctx, &stop))
	for i := 0; i < 4; i++ {
		start := time.Now()
		rep = e.Inject(ctx, event("c1", "The app crashes with an error on login"))
		assert.Equal(t, "delivered", rep.Status, "routing does not need Redis")
		assert.Equal(t, []string{"support"}, routedTo(t, rep), "the model still decides, without history")
		assert.Less(t, time.Since(start), 3*time.Second, "a dead Redis does not hold routing up")
	}

	require.NoError(t, ctr.Start(ctx))
	probe := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = probe.Close() })
	require.Eventually(t, func() bool { return probe.Ping(ctx).Err() == nil }, 30*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		e.Inject(ctx, event("c3", "Please refund my last invoice"))
		return history(t, e, "c3") >= 1
	}, 20*time.Second, 300*time.Millisecond, "history is recorded again once Redis is back")
}

// Two replicas of one relay share a Redis: the second one sees the first
// one's call and keeps a follow-up where the first sent it (sticky).
func TestIntegrationTwoRelaysShareRedis(t *testing.T) {
	_, addr := redisOnFixedPort(t)
	spec := triageSpec(t, addr)
	a, b := relay(t, spec), relay(t, triageSpec(t, addr))
	ctx := context.Background()

	rep := a.Inject(ctx, event("c2", "I was charged twice for my invoice"))
	require.Equal(t, []string{"billing"}, routedTo(t, rep))
	require.Eventually(t, func() bool { return history(t, b, "c2") == 1 }, 5*time.Second, 50*time.Millisecond,
		"replica b reads the call replica a recorded")

	rep = b.Inject(ctx, event("c2", "Still no refund, following up again on the same invoice."))
	assert.Equal(t, []string{"billing"}, routedTo(t, rep), "the follow-up sticks to billing on the other replica")
	assert.Contains(t, rep.Routes[0].Decision.Reason, "a follow-up of the last call of c2", "decided by sticky routing, not by asking the model again")
}
