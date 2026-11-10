package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/wal"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

// batchSink is a scripted sink with a native batch call (registered as the
// sqs type in these tests).
type batchSink struct {
	scripted
	bmu     sync.Mutex
	batches [][]*envelope.Envelope
	// handBack answers every batch with plugin.ErrNoBatch.
	handBack bool
}

func (b *batchSink) DeliverBatch(_ context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	b.bmu.Lock()
	defer b.bmu.Unlock()
	if b.handBack {
		return nil, plugin.ErrNoBatch
	}
	b.batches = append(b.batches, envs)
	return &plugin.Result{Status: 200, Endpoint: b.name}, nil
}

var (
	batchSinksMu sync.Mutex
	batchSinks   = map[string]*batchSink{}
)

func init() {
	plugin.RegisterSink("sqs", func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		batchSinksMu.Lock()
		defer batchSinksMu.Unlock()
		b, ok := batchSinks[spec.Name]
		if !ok {
			b = &batchSink{scripted: scripted{name: spec.Name}}
			batchSinks[spec.Name] = b
		}
		return b, nil
	})
}

// acks counts outcomes, safely across workers.
type acks struct {
	mu sync.Mutex
	n  map[envelope.AckStatus]int
}

func (a *acks) watch(m *envelope.Envelope) *envelope.Envelope {
	m.OnAck(func(_ context.Context, o envelope.Outcome) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.n == nil {
			a.n = map[envelope.AckStatus]int{}
		}
		a.n[o.Status]++
	})
	return m
}

func (a *acks) get(s envelope.AckStatus) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n[s]
}

func msgs(t *testing.T, a *acks, n int, body func(i int) string) []*envelope.Envelope {
	t.Helper()
	out := make([]*envelope.Envelope, n)
	for i := range out {
		out[i] = a.watch(msg("in", body(i)))
	}
	return out
}

func run(t *testing.T, yaml string, src *listSource) *Engine {
	t.Helper()
	setSources(t, src)
	e, err := New(context.Background(), spec(t, yaml), Options{Metrics: metrics.New()})
	require.NoError(t, err)
	require.NoError(t, e.Run(context.Background()))
	return e
}

// Six messages, batches of three, two workers: each batch holds more
// messages than there are workers (a waiting member gives its slot back),
// and a plain sink gets one NDJSON envelope per batch.
func TestBatchCombinesForPlainSinks(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	var a acks
	run(t, `
version: 1
name: relay
server: { workers: 2 }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: fault, batch: { max_messages: 3, max_wait: 5s } }
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &a, 6, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })})
	require.Equal(t, 2, out.count(), "two batches of three")
	for i := 0; i < 2; i++ {
		b := out.call(i)
		assert.Equal(t, "application/x-ndjson", b.ContentType)
		assert.Equal(t, 3, strings.Count(string(b.Body), "\n"))
		assert.Equal(t, b.ID, b.Header("Idempotency-Key"), "the batch has its own idempotency key")
	}
	assert.Equal(t, 6, a.get(envelope.Delivered))
}

// DeliverTo (hikyaku send, dlq replay --to) hands a batching destination
// one message as a batch of its own, at once, in the batch format.
func TestDeliverToABatchingDestination(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	e := build(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: fault, batch: { max_messages: 100, max_wait: 1h } }
routes: [{ name: r, from: [in], to: [out] }]
`, Options{})
	defer e.Close(context.Background())
	rep, err := e.DeliverTo(context.Background(), "out", msg("in", `{"n":1}`))
	require.NoError(t, err)
	assert.True(t, rep.OK())
	require.Equal(t, 1, out.count(), "delivered at once, not after max_wait")
	assert.Equal(t, "application/x-ndjson", out.call(0).ContentType)
	assert.Equal(t, "{\"n\":1}\n", string(out.call(0).Body))
}

// A batch never takes more than max_messages, however many workers submit
// at once: a full batch closes to new members before it is flushed.
func TestBatchNeverExceedsMaxMessages(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	var a acks
	const n = 600
	run(t, `
version: 1
name: relay
server: { workers: 64 }
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: fault, batch: { max_messages: 3, max_wait: 1h } }
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &a, n, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })})
	total := 0
	for i := 0; i < out.count(); i++ {
		lines := strings.Count(string(out.call(i).Body), "\n")
		assert.LessOrEqual(t, lines, 3, "batch %d", i)
		total += lines
	}
	assert.Equal(t, n, total)
	assert.Equal(t, n, a.get(envelope.Delivered))
}

func TestBatchFlushesAfterMaxWait(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	var a acks
	start := time.Now()
	run(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: out, type: fault, batch: { max_messages: 100, max_wait: 60ms, format: json_array } }
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &a, 2, func(i int) string {
		if i == 0 {
			return `{"n":0}`
		}
		return "plain text"
	})})
	require.Equal(t, 1, out.count())
	var got []any
	require.NoError(t, json.Unmarshal(out.call(0).Body, &got))
	assert.ElementsMatch(t, []any{map[string]any{"n": 0.0}, "plain text"}, got, "json_array keeps JSON bodies and quotes the rest (in the order the members joined)")
	assert.GreaterOrEqual(t, time.Since(start), 60*time.Millisecond)
	assert.Equal(t, 2, a.get(envelope.Delivered))
}

// A sink with a native batch call gets the members, each with its own
// idempotency key; destination transforms apply to each member.
func TestBatchNativeSinkAndPerMemberTransforms(t *testing.T) {
	q := &batchSink{scripted: scripted{name: "q"}}
	batchSinksMu.Lock()
	batchSinks["q"] = q
	batchSinksMu.Unlock()
	t.Cleanup(func() { batchSinksMu.Lock(); delete(batchSinks, "q"); batchSinksMu.Unlock() })
	setSinks(t)
	var a acks
	run(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: q
    type: sqs
    queue_url: https://sqs.example/1/q
    batch: { max_messages: 4, max_wait: 5s }
    transforms:
      - filter: 'msg.json.keep'
routes: [{ name: r, from: [in], to: [q] }]
`, &listSource{name: "in", msgs: msgs(t, &a, 4, func(i int) string { return fmt.Sprintf(`{"keep":%t}`, i%2 == 0) })})
	require.Len(t, q.batches, 1)
	require.Len(t, q.batches[0], 2, "the filtered members are not sent")
	for _, m := range q.batches[0] {
		assert.NotEmpty(t, m.Header("Idempotency-Key"))
		assert.NotEqual(t, q.batches[0][0].Header("Idempotency-Key"), "", "each member keeps its key")
	}
	assert.Equal(t, 0, q.count(), "Deliver is not used for a batch")
	assert.Equal(t, 4, a.get(envelope.Delivered), "filtered members count as handled")
}

// A sink with a batch call can hand a batch back (plugin.ErrNoBatch): it
// then gets the combined envelope, as a sink without one does.
func TestBatchSinkHandsTheBatchBack(t *testing.T) {
	q := &batchSink{scripted: scripted{name: "q"}, handBack: true}
	batchSinksMu.Lock()
	batchSinks["q"] = q
	batchSinksMu.Unlock()
	t.Cleanup(func() { batchSinksMu.Lock(); delete(batchSinks, "q"); batchSinksMu.Unlock() })
	setSinks(t)
	var a acks
	run(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: q
    type: sqs
    queue_url: https://sqs.example/1/q
    batch: { max_messages: 3, max_wait: 5s, format: json_array }
routes: [{ name: r, from: [in], to: [q] }]
`, &listSource{name: "in", msgs: msgs(t, &a, 3, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })})
	assert.Empty(t, q.batches)
	require.Equal(t, 1, q.count(), "one delivery of the combined envelope")
	b := q.call(0)
	assert.Equal(t, "application/json", b.ContentType)
	var got []any
	require.NoError(t, json.Unmarshal(b.Body, &got))
	assert.ElementsMatch(t, []any{map[string]any{"n": 0.0}, map[string]any{"n": 1.0}, map[string]any{"n": 2.0}}, got)
	assert.Equal(t, b.ID, b.Header("Idempotency-Key"), "the batch's own idempotency key")
	assert.Equal(t, 3, a.get(envelope.Delivered))
}

// A failing batch falls back as a whole; with no fallback left, every
// member is dead-lettered on its own.
func TestBatchFallbackAndPerMemberDeadLetters(t *testing.T) {
	out := &scripted{name: "out", final: plugin.Permanentf("rejected")}
	spool := &scripted{name: "spool"}
	dlq := &scripted{name: "dead_letter"}
	setSinks(t, out, spool, dlq)
	var a acks
	run(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
dead_letter: { type: fault }
destinations:
  - { name: out, type: fault, fallback: [spool], batch: { max_messages: 3, max_wait: 5s } }
  - { name: spool, type: fault }
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &a, 3, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })})
	require.Equal(t, 1, spool.count(), "the fallback gets the batch")
	assert.Equal(t, 3, strings.Count(string(spool.call(0).Body), "\n"))
	assert.Equal(t, 3, a.get(envelope.Delivered))

	out2 := &scripted{name: "out", final: plugin.Permanentf("rejected")}
	dlq2 := &scripted{name: "dead_letter"}
	setSinks(t, out2, dlq2)
	var b acks
	run(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
dead_letter: { type: fault }
destinations:
  - { name: out, type: fault, batch: { max_messages: 3, max_wait: 5s } }
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &b, 3, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })})
	assert.Equal(t, 3, dlq2.count(), "one dead letter per member")
	assert.Equal(t, 3, b.get(envelope.DeadLettered))
}

// Messages with the same key are processed one at a time and in order;
// other keys run alongside.
func TestOrderingPerKey(t *testing.T) {
	var mu sync.Mutex
	active := map[string]int{}
	maxSame, maxAll, all := 0, 0, 0
	out := &scripted{name: "out", delay: 10 * time.Millisecond, hook: func(env *envelope.Envelope, start bool) {
		j, _ := env.JSON()
		k := j.(map[string]any)["k"].(string)
		mu.Lock()
		defer mu.Unlock()
		d := 1
		if !start {
			d = -1
		}
		active[k] += d
		all += d
		maxSame = max(maxSame, active[k])
		maxAll = max(maxAll, all)
	}}
	setSinks(t, out)
	var a acks
	keys := []string{"a", "b", "a", "b", "a", "b", "a", "b"}
	run(t, `
version: 1
name: relay
server: { workers: 8 }
sources: [{ name: in, type: stdin, ordering: { key: "{{ .json.k }}" } }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`, &listSource{name: "in", msgs: msgs(t, &a, len(keys), func(i int) string { return fmt.Sprintf(`{"k":%q,"n":%d}`, keys[i], i) })})
	require.Equal(t, 8, out.count())
	last := map[string]int{"a": -1, "b": -1}
	for i := 0; i < out.count(); i++ {
		j, _ := out.call(i).JSON()
		m := j.(map[string]any)
		k, n := m["k"].(string), int(m["n"].(float64))
		assert.Greater(t, n, last[k], "key %s delivered in order", k)
		last[k] = n
	}
	assert.Equal(t, 1, maxSame, "one message of a key at a time")
	assert.Equal(t, 2, maxAll, "the two keys run in parallel")
	assert.Equal(t, 8, a.get(envelope.Delivered))
}

// An empty key template uses the source's natural order unit (the
// ordering_key attribute); a message without one is not ordered.
func TestOrderingNaturalKey(t *testing.T) {
	out := &scripted{name: "out"}
	setSinks(t, out)
	setSources(t, &listSource{name: "in"})
	e, err := New(context.Background(), spec(t, `
version: 1
name: relay
sources: [{ name: in, type: stdin, ordering: {} }]
destinations: [{ name: out, type: fault }]
routes: [{ name: r, from: [in], to: [out] }]
`), Options{})
	require.NoError(t, err)
	defer e.Close(context.Background())
	m := msg("in", `{}`)
	m.SetAttribute("ordering_key", "topic/3")
	assert.Equal(t, "topic/3", e.orderer.key(m))
	assert.Equal(t, "", e.orderer.key(msg("in", `{}`)))
	assert.Equal(t, "", e.orderer.key(msg("other", `{}`)), "only the sources with ordering")
}

func diskSpec(path string, extra string) string {
	return fmt.Sprintf(`
version: 1
name: relay
server: { shutdown_grace: 2s }
buffer: { type: disk, path: %q, size: 100 }
sources: [{ name: in, type: stdin%s }]
destinations: [{ name: out, type: fault, retry: { attempts: 1 } }]
routes: [{ name: r, from: [in], to: [out] }]
`, path, extra)
}

// With a disk buffer every message is written before it counts as queued
// and deleted once delivered.
func TestDiskBufferDeliversAndEmpties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	out := &scripted{name: "out"}
	setSinks(t, out)
	var a acks
	var queuedOnDisk atomic.Int32
	ms := msgs(t, &a, 20, func(i int) string { return fmt.Sprintf(`{"n":%d}`, i) })
	for _, m := range ms {
		m.OnQueued(func() { queuedOnDisk.Add(1) })
	}
	run(t, diskSpec(path, ""), &listSource{name: "in", msgs: ms})
	assert.Equal(t, 20, out.count())
	assert.Equal(t, 20, a.get(envelope.Delivered))
	assert.Equal(t, int32(20), queuedOnDisk.Load())
	l, err := wal.Open(path, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	assert.Equal(t, 0, l.Len(), "delivered messages leave the log")
}

// A message the relay could not deliver before shutting down stays on disk
// and is delivered by the next run, with its ID (so destinations dedupe).
func TestDiskBufferKeepsUndeliveredForTheNextRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	down := &scripted{name: "out", final: plugin.Errorf(plugin.Transient, nil, "down")}
	setSinks(t, down, &scripted{name: "dead_letter", final: plugin.Errorf(plugin.Transient, nil, "dlq down too")})
	setSources(t, &listSource{name: "in", hold: true})
	e, err := New(context.Background(), spec(t, diskSpec(path, "")+"dead_letter: { type: fault }\n"), Options{})
	require.NoError(t, err)
	m := msg("in", `{"keep":"me"}`)
	id := m.ID
	var nacked atomic.Bool
	m.OnAck(func(_ context.Context, o envelope.Outcome) { nacked.Store(o.Status == envelope.Failed) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	e.Buffer() <- m
	require.Eventually(t, func() bool { return down.count() >= 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	assert.True(t, nacked.Load(), "the source hears the message failed")

	up := &scripted{name: "out"}
	setSinks(t, up)
	run(t, diskSpec(path, ""), &listSource{name: "in"})
	require.Equal(t, 1, up.count(), "the next run delivers what the last one left")
	assert.Equal(t, id, up.call(0).ID)
	assert.Equal(t, `{"keep":"me"}`, string(up.call(0).Body))
}

// While the relay runs, a message that failed (destination and DLQ down)
// is retried from the buffer instead of waiting for a restart.
func TestDiskBufferRetriesFailedMessages(t *testing.T) {
	base := diskRetryBase
	diskRetryBase = 20 * time.Millisecond
	t.Cleanup(func() { diskRetryBase = base })
	path := filepath.Join(t.TempDir(), "buffer.db")
	flaky := &scripted{name: "out", script: []error{plugin.Errorf(plugin.Transient, nil, "down")}}
	setSinks(t, flaky, &scripted{name: "dead_letter", final: plugin.Errorf(plugin.Transient, nil, "dlq down")})
	setSources(t, &listSource{name: "in", hold: true})
	e, err := New(context.Background(), spec(t, diskSpec(path, "")+"dead_letter: { type: fault }\n"), Options{Rand: func() float64 { return 0 }})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	e.Buffer() <- msg("in", `{}`)
	require.Eventually(t, func() bool { return flaky.count() == 2 }, 10*time.Second, 10*time.Millisecond, "retried from the buffer")
	cancel()
	require.NoError(t, <-done)
	l, err := wal.Open(path, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	assert.Equal(t, 0, l.Len())
}

// An ordered message that failed (destination and DLQ down) is retried
// where it stands: the messages behind it on its key wait for it.
func TestDiskBufferRetriesAnOrderedMessageInPlace(t *testing.T) {
	base := diskRetryBase
	diskRetryBase = 20 * time.Millisecond
	t.Cleanup(func() { diskRetryBase = base })
	path := filepath.Join(t.TempDir(), "buffer.db")
	out := &scripted{name: "out", script: []error{plugin.Errorf(plugin.Transient, nil, "down")}}
	setSinks(t, out, &scripted{name: "dead_letter", final: plugin.Errorf(plugin.Transient, nil, "dlq down")})
	setSources(t, &listSource{name: "in", hold: true})
	e, err := New(context.Background(), spec(t, diskSpec(path, `, ordering: { key: "{{ .json.k }}" }`)+"dead_letter: { type: fault }\n"), Options{Rand: func() float64 { return 0 }})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	for i := range 3 {
		e.Buffer() <- msg("in", fmt.Sprintf(`{"k":"a","n":%d}`, i))
	}
	require.Eventually(t, func() bool { return out.count() == 4 }, 10*time.Second, 10*time.Millisecond)
	var ns []int
	for i := range 4 {
		j, _ := out.call(i).JSON()
		ns = append(ns, int(j.(map[string]any)["n"].(float64)))
	}
	assert.Equal(t, []int{0, 0, 1, 2}, ns, "the failed message is retried before the ones behind it")
	cancel()
	require.NoError(t, <-done)
}

// When the relay stops while an ordered message waits for its retry, it
// stays in the buffer, and so do the messages behind it on its key: the
// next start delivers them all, in order.
func TestDiskBufferLeavesAnOrderedKeyInOrder(t *testing.T) {
	base := diskRetryBase
	diskRetryBase = time.Minute // still waiting when the relay stops
	t.Cleanup(func() { diskRetryBase = base })
	path := filepath.Join(t.TempDir(), "buffer.db")
	ordered := `, ordering: { key: "{{ .json.k }}" }`
	down := &scripted{name: "out", final: plugin.Errorf(plugin.Transient, nil, "down")}
	setSinks(t, down, &scripted{name: "dead_letter", final: plugin.Errorf(plugin.Transient, nil, "dlq down")})
	setSources(t, &listSource{name: "in", hold: true})
	e, err := New(context.Background(), spec(t, diskSpec(path, ordered)+"dead_letter: { type: fault }\n"), Options{Rand: func() float64 { return 0 }})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	var nacked atomic.Int32
	for i := range 3 {
		m := msg("in", fmt.Sprintf(`{"k":"a","n":%d}`, i))
		m.OnAck(func(_ context.Context, o envelope.Outcome) {
			if o.Status == envelope.Failed {
				nacked.Add(1)
			}
		})
		e.Buffer() <- m
	}
	require.Eventually(t, func() bool {
		e.orderer.mu.Lock()
		defer e.orderer.mu.Unlock()
		return down.count() == 1 && e.orderer.waiting == 2
	}, 5*time.Second, 5*time.Millisecond, "the first message waits for its retry, the others behind it")
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, int32(3), nacked.Load(), "every source hears its message was not delivered")
	assert.Equal(t, 1, down.count(), "nothing behind the failed message was delivered")

	up := &scripted{name: "out"}
	setSinks(t, up)
	run(t, diskSpec(path, ordered), &listSource{name: "in"})
	require.Equal(t, 3, up.count(), "the next run delivers the whole key")
	for i := range 3 {
		j, _ := up.call(i).JSON()
		assert.Equal(t, float64(i), j.(map[string]any)["n"], "in order")
	}
}

func TestDiskBufferLockedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "buffer.db")
	l, err := wal.Open(path, true)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	setSinks(t, &scripted{name: "out"})
	setSources(t, &listSource{name: "in"})
	e, err := New(context.Background(), spec(t, diskSpec(path, "")), Options{})
	require.NoError(t, err, "New does not open the buffer: one-shot commands never touch it")
	assert.ErrorContains(t, e.Run(context.Background()), "is locked by another process")
}
