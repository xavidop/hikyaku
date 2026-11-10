package azure

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/store"
)

func TestWatermark(t *testing.T) {
	w := newWatermark(-1)
	evs := map[int64]*wmEvent{}
	for seq := range int64(6) {
		evs[seq] = w.add(seq)
	}
	assert.Equal(t, int64(-1), w.value())
	w.settle(evs[1], true)
	assert.Equal(t, int64(-1), w.value(), "event 0 is still in flight")
	w.settle(evs[0], true)
	assert.Equal(t, int64(1), w.value(), "0 and 1 are final")
	w.settle(evs[3], true)
	assert.Equal(t, int64(1), w.value())
	w.settle(evs[2], true)
	assert.Equal(t, int64(3), w.value())
	assert.False(t, w.idle())

	assert.True(t, w.settle(evs[5], false), "a failed event holds the watermark")
	assert.False(t, w.settle(evs[5], false), "settled once")
	assert.Nil(t, w.add(6), "nothing after the hold can move the watermark")
	w.settle(evs[4], true)
	assert.Equal(t, int64(4), w.value(), "the watermark still reaches the event before the failed one")
	assert.True(t, w.idle())
	w.settle(nil, true) // an untracked event

	w = newWatermark(10)
	assert.Nil(t, w.add(10), "an event the checkpoint passed already")
	ev := w.add(11)
	w.settle(ev, true)
	assert.Equal(t, int64(11), w.value())
	w.settle(ev, false)
	assert.Equal(t, int64(11), w.value(), "a second outcome is ignored")
}

func newCheckpoints(kv store.KV, now func() time.Time) *checkpoints {
	return &checkpoints{kv: kv, prefix: "eventhubs:ns:hub:$Default", now: now}
}

// read is the checkpoint a new owner of the partition would read.
func read(t *testing.T, c *checkpoints, partition string) (int64, bool) {
	t.Helper()
	ctx := context.Background()
	raw, ok, err := c.kv.Get(ctx, c.key(partition, "gen"))
	require.NoError(t, err)
	if !ok {
		return 0, false
	}
	gen, err := parseCounter(raw)
	require.NoError(t, err)
	v, found, err := c.latest(ctx, partition, gen+1)
	require.NoError(t, err)
	return v, found
}

func TestCheckpointRegister(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory("m")
	c := newCheckpoints(mem, time.Now)

	s, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	assert.Equal(t, int64(1), s.gen)
	_, found, err := c.latest(ctx, "0", s.gen)
	require.NoError(t, err)
	assert.False(t, found, "a new partition has no checkpoint")

	require.NoError(t, s.begin(ctx, -1))
	v, found := read(t, c, "0")
	require.True(t, found, "the start is written at once")
	assert.Equal(t, int64(-1), v)

	prev := int64(-1)
	for _, w := range []int64{0, 5, 5, 4, 1000, 123456789} {
		require.NoError(t, s.commit(ctx, w, false))
		v, _ := read(t, c, "0")
		assert.LessOrEqual(t, v, max(w, prev), "never past the watermark")
		assert.GreaterOrEqual(t, v, prev+(max(w, prev)-prev)/2, "at least half the way")
		prev = v
	}
	for range 40 {
		require.NoError(t, s.commit(ctx, 123456789, false))
	}
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(123456788), v, "the flushes close in on the watermark")
	require.NoError(t, s.commit(ctx, 123456789, true))
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(123456789), v, "a final write is exact")

	// A new owner reads it and starts its own generation there.
	s2, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	assert.Greater(t, s2.gen, s.lastGen, "generations only grow: they are owner levels")
	v, found, err = c.latest(ctx, "0", s2.gen)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, s2.begin(ctx, v))
	require.NoError(t, s2.commit(ctx, v+8, false))
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(123456793), v)

	// The old owner's late writes do not hide the new owner's checkpoint,
	// not even its final one.
	s.fixed = true
	require.NoError(t, s.commit(ctx, 999999999, false))
	require.NoError(t, s.commit(ctx, 999999999, true))
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(123456793), v)

	// Generations without a base (owners that stopped first) are skipped.
	for range 5 {
		_, err := c.alloc(ctx, "0")
		require.NoError(t, err)
	}
	v, found = read(t, c, "0")
	assert.True(t, found)
	assert.Equal(t, int64(123456793), v)

	_, found = read(t, c, "1")
	assert.False(t, found, "partitions are independent")
}

func TestCheckpointRebases(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	clock := func() time.Time { return now }
	c := newCheckpoints(store.NewMemory("m"), clock)
	s, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	require.NoError(t, s.begin(ctx, 10))
	require.NoError(t, s.commit(ctx, 20, false))
	assert.Equal(t, s.gen, s.wgen)

	now = now.Add(cpRebase)
	require.NoError(t, s.commit(ctx, 30, false))
	assert.Greater(t, s.wgen, s.gen, "an owner starts a new generation every cpRebase")
	v, _ := read(t, c, "0")
	assert.Equal(t, int64(30), v, "a new generation starts exactly at the watermark")

	g := s.wgen
	require.NoError(t, s.commit(ctx, 31+1<<cpBits, false))
	assert.Greater(t, s.wgen, g, "so does a jump too large for the counters")

	// A stopped owner keeps writing its generation, but starts none.
	s.fixed = true
	now = now.Add(2 * cpRebase)
	g = s.wgen
	require.NoError(t, s.commit(ctx, 41+1<<cpBits, false))
	assert.Equal(t, g, s.wgen)
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(36+1<<cpBits), v)
	s.broken = true
	require.NoError(t, s.commit(ctx, 50+1<<cpBits, false))
	assert.Equal(t, g, s.wgen, "nor when it is broken")
	require.NoError(t, s.commit(ctx, 50+1<<cpBits, true))
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(50+1<<cpBits), v, "but on Close, as the newest owner, it writes the watermark")
}

// flakyKV misbehaves on chosen Incr calls: it fails before or after applying
// them, or applies them several times (a retried command whose reply was
// lost).
type flakyKV struct {
	store.KV
	mu        sync.Mutex
	failIncr  int // the n-th next Incr (1-based) misbehaves; 0 = none
	applied   int // how many times the misbehaving Incr runs
	silent    bool
	failSetNX bool
}

var errFlaky = errors.New("store timeout")

func (f *flakyKV) Incr(ctx context.Context, k string, ttl time.Duration) (int64, error) {
	f.mu.Lock()
	odd := false
	if f.failIncr > 0 {
		f.failIncr--
		odd = f.failIncr == 0
	}
	applied, silent := f.applied, f.silent
	f.mu.Unlock()
	if !odd {
		return f.KV.Incr(ctx, k, ttl)
	}
	var (
		n   int64
		err error
	)
	for range applied {
		n, err = f.KV.Incr(ctx, k, ttl)
	}
	if silent {
		return n, err // ran several times, answered once
	}
	return 0, errFlaky
}

func (f *flakyKV) SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	fail := f.failSetNX
	f.failSetNX = false
	f.mu.Unlock()
	if fail {
		return false, errFlaky
	}
	return f.KV.SetNX(ctx, k, v, ttl)
}

func TestCheckpointNeverRunsAhead(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		applied int
		silent  bool
		broken  bool
	}{
		"an error before the write": {applied: 0},
		"an error after the write":  {applied: 1},
		"an error after two writes": {applied: 2},
		"a write that ran twice":    {applied: 2, silent: true},
		"a write that ran 3 times":  {applied: 3, silent: true, broken: true},
	} {
		t.Run(name, func(t *testing.T) {
			kv := &flakyKV{KV: store.NewMemory("m")}
			c := newCheckpoints(kv, time.Now)
			s, err := c.alloc(ctx, "0")
			require.NoError(t, err)
			require.NoError(t, s.begin(ctx, 0))

			// Half of the gap is 0b1111: four counters; the second misbehaves.
			kv.failIncr, kv.applied, kv.silent = 2, tc.applied, tc.silent
			err = s.commit(ctx, 31, false)
			v, _ := read(t, c, "0")
			assert.LessOrEqual(t, v, int64(31), "never past the watermark")
			if tc.silent && !tc.broken {
				require.NoError(t, err, "a doubled write is accounted for")
				assert.Equal(t, s.gen, s.wgen)
				assert.Equal(t, int64(19), v, "half the gap, and the doubled step: 8+4+4+2+1")
			} else {
				assert.Error(t, err)
				assert.Equal(t, int64(31), v, "the session starts over at the watermark at once")
				assert.Greater(t, s.wgen, s.gen)
			}
			require.NoError(t, s.commit(ctx, 31, true))
			v, _ = read(t, c, "0")
			assert.Equal(t, int64(31), v)
		})
	}

	// A failed base write leaves the session broken; the next commit rebases.
	kv := &flakyKV{KV: store.NewMemory("m"), failSetNX: true}
	c := newCheckpoints(kv, time.Now)
	s, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	require.ErrorIs(t, s.begin(ctx, 5), errFlaky)
	assert.True(t, s.broken)
	require.NoError(t, s.commit(ctx, 6, false))
	v, found := read(t, c, "0")
	assert.True(t, found)
	assert.Equal(t, int64(6), v)

	// A failed allocation is reported, and the next commit tries again.
	kv.failIncr, kv.applied = 1, 0
	s.broken = true
	require.ErrorIs(t, s.commit(ctx, 8, false), errFlaky)
	require.NoError(t, s.commit(ctx, 8, false))
	v, _ = read(t, c, "0")
	assert.Equal(t, int64(8), v)
}

func TestCheckpointDetectsAForeignWriter(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemory("m")
	c := newCheckpoints(mem, time.Now)
	s, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	require.NoError(t, s.begin(ctx, 0))
	for range 2 {
		_, err = mem.Incr(ctx, c.counterKey("0", s.gen, 0), cpTTL)
		require.NoError(t, err)
	}
	assert.ErrorContains(t, s.commit(ctx, 2, false), "moved by 3")
	v, _ := read(t, c, "0")
	assert.Equal(t, int64(2), v, "it starts over at the watermark")

	// A base that already exists is refused.
	s2 := &cpSession{c: c, partition: "0", gen: s.gen, wgen: s.gen}
	assert.ErrorContains(t, s2.begin(ctx, 3), "already has a base")
}

// decimalKV keeps Incr counters as decimal text, as Redis does.
type decimalKV struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (d *decimalKV) Get(_ context.Context, k string) ([]byte, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.m[k]
	return v, ok, nil
}

func (d *decimalKV) SetNX(_ context.Context, k string, v []byte, _ time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.m[k]; ok {
		return false, nil
	}
	d.m[k] = v
	return true, nil
}

func (d *decimalKV) Incr(_ context.Context, k string, _ time.Duration) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n, _ := strconv.ParseInt(string(d.m[k]), 10, 64)
	n++
	d.m[k] = []byte(strconv.FormatInt(n, 10))
	return n, nil
}

func TestCheckpointCounterEncodings(t *testing.T) {
	ctx := context.Background()
	kv := &decimalKV{m: map[string][]byte{}}
	c := newCheckpoints(kv, time.Now)
	s, err := c.alloc(ctx, "3")
	require.NoError(t, err)
	require.NoError(t, s.begin(ctx, 41))
	for range 30 {
		require.NoError(t, s.commit(ctx, 300, false))
	}
	v, found := read(t, c, "3")
	require.True(t, found)
	assert.Equal(t, int64(299), v)

	n, err := parseCounter([]byte("1234"))
	require.NoError(t, err)
	assert.Equal(t, int64(1234), n)
	n, err = parseCounter(binary.BigEndian.AppendUint64(nil, 77))
	require.NoError(t, err)
	assert.Equal(t, int64(77), n)
	_, err = parseCounter([]byte("1x"))
	assert.Error(t, err)

	kv.m[c.baseKey("3", 1)] = []byte("nope")
	_, _, err = c.latest(ctx, "3", 2)
	assert.ErrorContains(t, err, "is not a number")
	kv.m[c.baseKey("3", 1)] = []byte("1")
	kv.m[c.counterKey("3", 1, 0)] = []byte("abc")
	_, _, err = c.latest(ctx, "3", 2)
	assert.ErrorContains(t, err, "counter 0")
	kv.m[c.counterKey("3", 1, 0)] = []byte("1")
	kv.m[c.counterKey("3", 1, 40)] = []byte(strconv.FormatInt(1<<40, 10))
	_, _, err = c.latest(ctx, "3", 2)
	assert.ErrorContains(t, err, "overflows")
}

func TestCheckpointReadersSeeOnlySettledValues(t *testing.T) {
	ctx := context.Background()
	c := newCheckpoints(store.NewMemory("m"), time.Now)
	s, err := c.alloc(ctx, "0")
	require.NoError(t, err)
	require.NoError(t, s.begin(ctx, -1))
	var (
		mu   sync.Mutex
		high int64 = -1 // the highest value a commit has started writing
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := int64(0); v < 3000; v += 13 {
			mu.Lock()
			high = v
			mu.Unlock()
			_ = s.commit(ctx, v, false)
		}
	}()
	last := int64(-1)
	for {
		v, found := read(t, c, "0")
		require.True(t, found)
		mu.Lock()
		h := high
		mu.Unlock()
		assert.LessOrEqual(t, v, h, "a reader never sees more than was committed")
		assert.GreaterOrEqual(t, v, last, "nor less than it saw before")
		last = v
		select {
		case <-done:
			return
		default:
		}
	}
}
