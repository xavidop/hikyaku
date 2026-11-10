package engine

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// The engine's throughput end to end: a source producing b.N messages,
// the routes, workers and delivery to a sink that answers at once. It
// measures hikyaku's own overhead per message (make bench).

func benchRun(b *testing.B, yaml string) { benchRunCount(b, yaml, nil) }

// benchRunCount counts what the sink got with count (default: one per
// call; a batch is one call holding many messages).
func benchRunCount(b *testing.B, yaml string, count func(*scripted) int) {
	b.Helper()
	msgs := make([]*envelope.Envelope, b.N)
	for i := range msgs {
		msgs[i] = msg("in", fmt.Sprintf(`{"n":%d,"customer_id":"c%d","type":"invoice.paid"}`, i, i%64))
	}
	sinksMu.Lock()
	sinks = map[string]*scripted{"out": {name: "out"}}
	sinksMu.Unlock()
	sourcesMu.Lock()
	sources = map[string]*listSource{"in": {name: "in", msgs: msgs}}
	sourcesMu.Unlock()
	l, err := config.LoadBytes(context.Background(), "bench.yaml", []byte(yaml), config.LoadOptions{})
	if err != nil {
		b.Fatal(err)
	}
	e, err := New(context.Background(), l.Spec, Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	if err := e.Run(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if count == nil {
		count = func(s *scripted) int { return s.count() }
	}
	if got := count(sinks["out"]); got != b.N {
		b.Fatalf("delivered %d of %d", got, b.N)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
}

const benchBase = `
version: 1
name: bench
server: { workers: 64 }
sources: [{ name: in, type: stdin%s }]
destinations: [{ name: out, type: fault%s }]
routes: [{ name: r, from: [in], to: [out]%s }]
`

func BenchmarkRelayMemory(b *testing.B) {
	benchRun(b, fmt.Sprintf(benchBase, "", "", ""))
}

func BenchmarkRelayFilterAndTemplate(b *testing.B) {
	benchRun(b, fmt.Sprintf(benchBase, "", ", transforms: [{ template: '{\"id\":\"{{ .json.n }}\",\"kind\":\"{{ .json.type }}\"}' }]",
		", filter: 'msg.json.type.startsWith(\"invoice.\")'"))
}

func BenchmarkRelayRules(b *testing.B) {
	benchRun(b, `
version: 1
name: bench
server: { workers: 64 }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes:
  - name: r
    from: [in]
    router:
      type: rules
      rules:
        - { when: 'msg.json.type == "refund"', to: [out] }
        - { when: 'msg.json.type.startsWith("invoice.")', to: [out] }
`)
}

func BenchmarkRelayOrdered(b *testing.B) {
	benchRun(b, fmt.Sprintf(benchBase, `, ordering: { key: "{{ .json.customer_id }}" }`, "", ""))
}

func BenchmarkRelayBatched(b *testing.B) {
	benchRunCount(b, fmt.Sprintf(benchBase, "", ", batch: { max_messages: 500, max_wait: 5ms }", ""), func(s *scripted) int {
		n := 0
		for i := 0; i < s.count(); i++ {
			n += bytes.Count(s.call(i).Body, []byte("\n"))
		}
		return n
	})
}

func BenchmarkRelayDiskBuffer(b *testing.B) {
	path := filepath.Join(b.TempDir(), "buffer.db")
	benchRun(b, fmt.Sprintf(`
version: 1
name: bench
server: { workers: 64 }
buffer: { type: disk, path: %q, size: 100000 }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`, path))
}

func BenchmarkRelayDiskBufferNoSync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "buffer.db")
	benchRun(b, fmt.Sprintf(`
version: 1
name: bench
server: { workers: 64 }
buffer: { type: disk, path: %q, size: 100000, sync: false }
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`, path))
}
