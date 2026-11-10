package store_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/store/storetest"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestMemoryConformance(t *testing.T) {
	c := &clock{now: time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)}
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			m := store.NewMemory("mem")
			m.SetClock(c.Now)
			return m
		},
		Advance: c.Advance,
		Now:     c.Now,
	})
}

func TestMemoryInjectedError(t *testing.T) {
	m := store.NewMemory("mem")
	down := errors.New("connection refused")
	m.SetError(down)
	ctx := context.Background()
	_, err := m.Recent(ctx, "r", "k", store.Window{})
	assert.ErrorIs(t, err, down)
	assert.ErrorIs(t, m.Append(ctx, "r", "k", store.CallRecord{}, store.AppendOptions{TTL: time.Hour}), down)
	_, _, err = m.Get(ctx, "k")
	assert.ErrorIs(t, err, down)
	assert.ErrorIs(t, m.Health(ctx), down)
	m.SetError(nil)
	assert.NoError(t, m.Health(ctx))
	assert.NoError(t, m.Close())
}

func TestLRUEvictsAndExpires(t *testing.T) {
	c := store.NewLRU(2)
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		ok, err := c.SetNX(ctx, k, []byte(k), time.Minute)
		require.NoError(t, err)
		assert.True(t, ok)
	}
	assert.Equal(t, 2, c.Len())
	_, ok, _ := c.Get(ctx, "a")
	assert.False(t, ok, "the least recently used entry is evicted")
	v, ok, _ := c.Get(ctx, "c")
	assert.True(t, ok)
	assert.Equal(t, "c", string(v))

	n, _ := c.Incr(ctx, "n", 0)
	assert.Equal(t, int64(1), n)
	n, _ = c.Incr(ctx, "n", 0)
	assert.Equal(t, int64(2), n)

	short := store.NewLRU(0)
	_, _ = short.SetNX(ctx, "k", []byte("v"), time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	_, ok, _ = short.Get(ctx, "k")
	assert.False(t, ok)
}

type slowStore struct {
	store.HistoryStore
	calls atomic.Int32
	fail  int32
	block chan struct{}
}

func (s *slowStore) Append(ctx context.Context, route, key string, rec store.CallRecord, o store.AppendOptions) error {
	n := s.calls.Add(1)
	if s.block != nil {
		<-s.block
	}
	if n <= s.fail {
		return errors.New("transient")
	}
	return nil
}

func TestRecorderRetries(t *testing.T) {
	s := &slowStore{fail: 2}
	var errs []string
	r := store.NewRecorder(store.RecorderOptions{Backoff: time.Millisecond, OnError: func(op string, _ error) { errs = append(errs, op) }})
	r.Start()
	assert.True(t, r.Record(s, "r", "k", store.CallRecord{MsgID: "m"}, store.AppendOptions{TTL: time.Hour}))
	require.NoError(t, r.Close(context.Background()))
	assert.Equal(t, int32(3), s.calls.Load(), "two failures, then success")
	assert.Empty(t, errs)
	assert.False(t, r.Record(s, "r", "k", store.CallRecord{}, store.AppendOptions{}), "closed recorders drop")
}

func TestRecorderNeverBlocks(t *testing.T) {
	s := &slowStore{block: make(chan struct{})}
	var full atomic.Int32
	r := store.NewRecorder(store.RecorderOptions{Size: 2, OnError: func(op string, _ error) {
		if op == "queue_full" {
			full.Add(1)
		}
	}})
	r.Start()
	start := time.Now()
	accepted := 0
	for i := 0; i < 10; i++ {
		if r.Record(s, "r", "k", store.CallRecord{}, store.AppendOptions{TTL: time.Hour}) {
			accepted++
		}
	}
	assert.Less(t, time.Since(start), 100*time.Millisecond, "Record returns at once while the store hangs")
	assert.LessOrEqual(t, accepted, 3, "one in flight plus the queue")
	assert.Equal(t, int32(10-accepted), full.Load())
	close(s.block)
	require.NoError(t, r.Close(context.Background()))
}

func TestRecorderReportsFinalFailure(t *testing.T) {
	s := &slowStore{fail: 100}
	var ops []string
	r := store.NewRecorder(store.RecorderOptions{Attempts: 2, Backoff: time.Millisecond, OnError: func(op string, _ error) { ops = append(ops, op) }})
	assert.True(t, r.Record(s, "r", "k", store.CallRecord{}, store.AppendOptions{TTL: time.Hour}), "records queue before Start")
	require.NoError(t, r.Close(context.Background()), "Close drains even when never started")
	assert.Equal(t, []string{"append"}, ops)
}

func history(n int) []store.CallRecord {
	var out []store.CallRecord
	for i := 0; i < n; i++ {
		out = append(out, store.CallRecord{
			MsgID:    string(rune('a' + i)),
			State:    strings.Repeat(string(rune('a'+i)), 10),
			Decision: []store.Answer{{Candidate: "support", Probability: 0.91, Chosen: true}},
			Outcome:  store.OutcomeDelivered,
		})
	}
	return out
}

func TestBuildTurnsShape(t *testing.T) {
	recs := history(2)
	recs[0].Feedback = &store.Feedback{Correct: []string{"billing"}}
	got := store.BuildTurns(recs, "current message", 0)
	require.Len(t, got.Turns, 5)
	assert.Equal(t, store.Turn{Role: "user", Content: "aaaaaaaaaa"}, got.Turns[0])
	assert.Equal(t, store.Turn{Role: "model", Content: "routed to support (p=0.91), outcome: delivered, correct destination: billing"}, got.Turns[1])
	assert.Equal(t, "routed to support (p=0.91), outcome: delivered", got.Turns[3].Content)
	assert.Equal(t, store.Turn{Role: "user", Content: "current message"}, got.Turns[4])
	assert.Zero(t, got.Dropped)
	assert.False(t, got.Truncated)
}

func TestBuildTurnsDropsOldestFirst(t *testing.T) {
	recs := history(3)
	full := store.BuildTurns(recs, "now", 0)
	budget := full.Bytes() - 1 // one byte too many: the oldest pair must go
	got := store.BuildTurns(recs, "now", budget)
	assert.Equal(t, 1, got.Dropped)
	assert.LessOrEqual(t, got.Bytes(), budget)
	assert.Equal(t, "bbbbbbbbbb", got.Turns[0].Content, "the oldest pair (a) was dropped")
	assert.Equal(t, "now", got.Turns[len(got.Turns)-1].Content)
	assert.False(t, got.Truncated)
}

func TestBuildTurnsCurrentTruncatedLast(t *testing.T) {
	current := strings.Repeat("é", 10) // 20 bytes
	got := store.BuildTurns(history(2), current, 15)
	assert.Equal(t, 2, got.Dropped, "all history goes before the current message is cut")
	require.Len(t, got.Turns, 1)
	assert.True(t, got.Truncated)
	assert.LessOrEqual(t, len(got.Turns[0].Content), 15)
	assert.Equal(t, strings.Repeat("é", 7), got.Turns[0].Content, "cut on a character boundary")
}

func TestModelTurnTextVariants(t *testing.T) {
	multi := store.CallRecord{Decision: []store.Answer{
		{Candidate: "billing", Probability: 0.88, Chosen: true},
		{Candidate: "support", Probability: 0.71, Chosen: true},
		{Candidate: "dashboard", Probability: 0.12},
	}, Outcome: store.OutcomeFallback}
	assert.Equal(t, "routed to billing (p=0.88), support (p=0.71), outcome: fallback", store.ModelTurnText(multi))
	low := store.CallRecord{DeliveredTo: []string{"support"}, Outcome: store.OutcomeLowConfidence}
	assert.Equal(t, "routed to support, outcome: low_confidence", store.ModelTurnText(low))
	assert.Equal(t, "routed to no destination, outcome: dlq", store.ModelTurnText(store.CallRecord{Outcome: store.OutcomeDLQ}))
	assert.Equal(t, "(message not recorded)", store.UserTurnText(store.CallRecord{}))
	assert.True(t, store.CallRecord{Outcome: store.OutcomeFallback}.Delivered())
	assert.False(t, store.CallRecord{Outcome: store.OutcomeDLQ}.Delivered())
	assert.Equal(t, 10, store.Window{}.Entries())
	assert.Equal(t, 20, store.AppendOptions{}.KeepOrDefault())
}
