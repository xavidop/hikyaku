package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
)

// A Kafka-to-Kafka bridge through the engine: each record is produced to the
// topic its body names, keyed by its id; the one whose topic does not exist
// is dead-lettered to another topic. Both are final, so the source commits
// past them, and every copy carries the source record's stable ID.
func TestEndToEndKafkaBridge(t *testing.T) {
	Register()
	c := cluster(t, 1, "orders", "orders-eu", "dlq")
	cl := client(t, c)
	spec := fmt.Sprintf(`
version: 1
name: bridge
sources:
  - { name: in, type: kafka, brokers: ["%[1]s"], topic: orders, group: bridge, start: earliest }
destinations:
  - name: out
    type: kafka
    brokers: ["%[1]s"]
    topic: "orders-{{ .json.region }}"
    key: "{{ .json.id }}"
    retry: { attempts: 1 }
    timeout: 10s
dead_letter: { type: kafka, brokers: ["%[1]s"], topic: dlq }
routes:
  - { name: r, from: [in], to: [out] }
server: { admin: { disabled: true }, shutdown_grace: 5s }
`, c.ListenAddrs()[0])
	l, err := config.LoadBytes(context.Background(), "bridge.yaml", []byte(spec), config.LoadOptions{})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	produce(t, cl, record("orders", 0, `{"region":"eu","id":"o-1"}`), record("orders", 0, `{"region":"xx","id":"o-2"}`))

	out := consumed(t, c, "orders-eu", 1)[0]
	assert.Equal(t, "o-1", string(out.Key))
	assert.Equal(t, []string{"orders-0-0"}, headersOf(out)["Idempotency-Key"])
	assert.Equal(t, []string{"application/json"}, headersOf(out)["content-type"])

	dead := consumed(t, c, "dlq", 1)[0]
	assert.JSONEq(t, `{"region":"xx","id":"o-2"}`, string(dead.Value))
	assert.Equal(t, []string{"orders-0-1"}, headersOf(dead)["Idempotency-Key"])

	eventuallyCommitted(t, cl, "bridge", "orders", 0, 2)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(wait):
		t.Fatal("the relay did not stop")
	}
}
