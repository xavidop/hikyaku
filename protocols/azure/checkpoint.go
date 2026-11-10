package azure

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/store"
)

// Event Hubs checkpoints in a hikyaku store.
//
// A store's KV surface has Get, SetNX and Incr: a key written with SetNX
// cannot be overwritten, and only Incr changes a value in place. A
// partition's checkpoint is therefore a monotonic register built from them,
// under the prefix eventhubs:<namespace>:<hub>:<group>:<partition>:
//
//	...:gen          Incr counter, no TTL: allocates generations 1, 2, 3, ...
//	...:g<N>:base    SetNX, TTL: the sequence number generation N started at
//	...:g<N>:b<k>    Incr counters, TTL: how many times 2^k was added to it
//
// A generation's value is base + Σ count_k·2^k: the sequence number of the
// last event of an unbroken run of events settled final (-1: before the
// first event). Every owner of a partition allocates its own generation
// (whose number is also its owner level), so each generation has one
// writer, and a reader takes the newest generation that has a base. A
// value only grows, and a reader that reads while it grows gets a value it
// had.
//
// A checkpoint must never run ahead of what was settled, and an Incr is not
// idempotent: a store may run one twice (go-redis retries a command whose
// reply was lost). So a write moves a generation by the binary digits of
// half the gap to the watermark, one Incr each: even an Incr run twice
// leaves it at or below the watermark. Incr's answer says how far the
// counter moved, so a doubled Incr is accounted for; anything stranger, or
// an error (the outcome is unknown and never repeated), starts a new
// generation at the watermark, written with SetNX, which is idempotent.
// The gap halves at every flush, so a crash reads about one flush of events
// again; Close writes the watermark exactly.
//
// Each generation's keys expire after cpTTL, and a long-lived owner starts
// a new generation every cpRebase, so the keys do not pile up.

const (
	// cpBits is the number of counters of a generation: it absorbs 2^48
	// events before it starts a new one.
	cpBits = 48
	// cpTTL is how long a generation's keys live: a relay stopped for
	// longer than cpTTL-cpRebase restarts from start:.
	cpTTL = 30 * 24 * time.Hour
	// cpRebase is how often an owner starts a new generation.
	cpRebase = 7 * 24 * time.Hour
	// cpProbe is how many generations a reader looks back for the newest
	// with a base (the others were allocated by owners that stopped before
	// they wrote one).
	cpProbe = 256
)

// checkpoints is the checkpoint register of an event hub's partitions in
// one consumer group.
type checkpoints struct {
	kv     store.KV
	prefix string // eventhubs:<namespace>:<hub>:<group>
	now    func() time.Time
}

func (c *checkpoints) key(partition, suffix string) string {
	return c.prefix + ":" + partition + ":" + suffix
}

func genPart(g int64) string { return "g" + strconv.FormatInt(g, 10) }

func (c *checkpoints) baseKey(partition string, g int64) string {
	return c.key(partition, genPart(g)+":base")
}

func (c *checkpoints) counterKey(partition string, g int64, k int) string {
	return c.key(partition, genPart(g)+":b"+strconv.Itoa(k))
}

// alloc allocates a generation for a new owner of a partition.
func (c *checkpoints) alloc(ctx context.Context, partition string) (*cpSession, error) {
	g, err := c.kv.Incr(ctx, c.key(partition, "gen"), 0)
	if err != nil {
		return nil, fmt.Errorf("allocating a checkpoint generation: %w", err)
	}
	return &cpSession{c: c, partition: partition, gen: g, wgen: g, lastGen: g}, nil
}

// latest reads the newest checkpoint written before generation below:
// found is false when there is none.
func (c *checkpoints) latest(ctx context.Context, partition string, below int64) (value int64, found bool, err error) {
	for g := below - 1; g >= 1 && g >= below-cpProbe; g-- {
		v, ok, err := c.read(ctx, partition, g)
		if err != nil || ok {
			return v, ok, err
		}
	}
	return 0, false, nil
}

// read returns generation g's value; ok is false when it has no base.
func (c *checkpoints) read(ctx context.Context, partition string, g int64) (int64, bool, error) {
	raw, ok, err := c.kv.Get(ctx, c.baseKey(partition, g))
	if err != nil || !ok {
		return 0, false, err
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("checkpoint of partition %s, generation %d: base %q is not a number", partition, g, raw)
	}
	for k := range cpBits {
		raw, ok, err := c.kv.Get(ctx, c.counterKey(partition, g, k))
		if err != nil {
			return 0, false, err
		}
		if !ok {
			continue
		}
		n, err := parseCounter(raw)
		if err != nil {
			return 0, false, fmt.Errorf("checkpoint of partition %s, generation %d, counter %d: %w", partition, g, k, err)
		}
		if n < 0 || n > (math.MaxInt64-max(v, 0))>>k {
			return 0, false, fmt.Errorf("checkpoint of partition %s, generation %d, counter %d: %d overflows", partition, g, k, n)
		}
		v += n << k
	}
	return v, true, nil
}

// parseCounter reads an Incr counter as a store keeps it: Redis as
// decimal text, the memory store as 8 big-endian bytes.
func parseCounter(b []byte) (int64, error) {
	digits := len(b) > 0
	for _, c := range b {
		if c < '0' || c > '9' {
			digits = false
			break
		}
	}
	switch {
	case digits:
		return strconv.ParseInt(string(b), 10, 64)
	case len(b) == 8:
		return int64(binary.BigEndian.Uint64(b)), nil
	}
	return 0, fmt.Errorf("counter of %d bytes is neither decimal nor 8 big-endian bytes", len(b))
}

// errNoGeneration: every generation tried already had a base.
var errNoGeneration = errors.New("no free checkpoint generation")

// cpSession is one owner's checkpoint writer for a partition. Only one
// goroutine at a time uses it (the reader, then the flusher).
type cpSession struct {
	c         *checkpoints
	partition string
	gen       int64 // the generation alloc gave: the owner level

	wgen      int64 // the generation written to (a newer one after a rebase)
	lastGen   int64 // the newest generation this session allocated
	based     bool  // wgen has a base
	broken    bool  // a write's outcome is unknown: rebase before the next
	fixed     bool  // the owner stopped: no new generation, but on Close
	committed int64 // wgen's value
	counts    [cpBits]int64
	rebaseAt  time.Time
}

// begin writes the base of the allocated generation: where the owner
// starts. A failed write leaves the session broken; the next commit then
// starts a new generation.
func (s *cpSession) begin(ctx context.Context, base int64) error {
	s.committed = base
	ok, err := s.c.kv.SetNX(ctx, s.c.baseKey(s.partition, s.gen), []byte(strconv.FormatInt(base, 10)), cpTTL)
	switch {
	case err != nil:
		s.broken = true
		return fmt.Errorf("writing the checkpoint base: %w", err)
	case !ok:
		s.broken = true
		return fmt.Errorf("checkpoint generation %d of partition %s already has a base", s.gen, s.partition)
	}
	s.based, s.rebaseAt = true, s.c.now().Add(cpRebase)
	return nil
}

// commit records that every event up to v is final: it moves the
// checkpoint toward v (see above). final writes v exactly; Close asks for
// it.
func (s *cpSession) commit(ctx context.Context, v int64, final bool) error {
	if s.based && !s.broken && v <= s.committed {
		return nil
	}
	rebase := !s.based || s.broken || v-s.committed >= 1<<cpBits
	if !rebase && !s.fixed && !s.c.now().Before(s.rebaseAt) {
		rebase = true // a fixed session keeps its generation, which outlives it
	}
	if rebase {
		switch {
		case !s.fixed:
			return s.rebase(ctx, v)
		case final:
			return s.closeGap(ctx, v)
		}
		return nil // a stopped owner does not start a generation
	}
	if err := s.add(ctx, (v-s.committed)/2); err != nil {
		if s.broken && !s.fixed {
			// Start over at v at once: the value may be past it.
			return errors.Join(err, s.rebase(ctx, v))
		}
		return err
	}
	if final && s.committed < v {
		return s.closeGap(ctx, v)
	}
	return nil
}

// add adds h to the generation: one Incr per binary digit, highest first.
// Each Incr's answer says how many times it ran; twice is accounted for
// (the steps are at most half the gap), anything else breaks the session.
func (s *cpSession) add(ctx context.Context, h int64) error {
	for k := cpBits - 1; k >= 0; k-- {
		if h&(1<<k) == 0 {
			continue
		}
		n, err := s.c.kv.Incr(ctx, s.c.counterKey(s.partition, s.wgen, k), cpTTL)
		if err != nil {
			s.broken = true
			return fmt.Errorf("writing the checkpoint: %w", err)
		}
		moved := n - s.counts[k]
		s.counts[k] = n
		if moved != 1 && moved != 2 {
			s.broken = true
			return fmt.Errorf("checkpoint counter %d of generation %d of partition %s moved by %d under us", k, s.wgen, s.partition, moved)
		}
		s.committed += moved << k
	}
	return nil
}

// closeGap writes v exactly, as the base of a new generation, unless
// another owner allocated a generation since this session's last one: a
// stopped owner must not hide a newer owner's checkpoint.
func (s *cpSession) closeGap(ctx context.Context, v int64) error {
	g, err := s.c.kv.Incr(ctx, s.c.key(s.partition, "gen"), 0)
	if err != nil {
		return fmt.Errorf("allocating a checkpoint generation: %w", err)
	}
	newest := g == s.lastGen+1
	s.lastGen = g
	if !newest {
		return nil
	}
	ok, err := s.c.kv.SetNX(ctx, s.c.baseKey(s.partition, g), []byte(strconv.FormatInt(v, 10)), cpTTL)
	if err != nil || !ok {
		return err
	}
	s.wgen, s.based, s.broken, s.committed, s.counts = g, true, false, v, [cpBits]int64{}
	return nil
}

// rebase starts a new generation whose base is v.
func (s *cpSession) rebase(ctx context.Context, v int64) error {
	for range 8 {
		g, err := s.c.kv.Incr(ctx, s.c.key(s.partition, "gen"), 0)
		if err != nil {
			return fmt.Errorf("allocating a checkpoint generation: %w", err)
		}
		s.lastGen = g
		ok, err := s.c.kv.SetNX(ctx, s.c.baseKey(s.partition, g), []byte(strconv.FormatInt(v, 10)), cpTTL)
		if err != nil {
			s.broken = true
			return fmt.Errorf("writing the checkpoint base: %w", err)
		}
		if !ok {
			continue // the store lost the allocator: try the next one
		}
		s.wgen, s.based, s.broken, s.committed, s.counts = g, true, false, v, [cpBits]int64{}
		s.rebaseAt = s.c.now().Add(cpRebase)
		return nil
	}
	s.broken = true
	return errNoGeneration
}

// watermark tracks a partition's events in the order they were received,
// and the sequence number up to which every one was settled final.
type watermark struct {
	mu      sync.Mutex
	done    int64      // every event up to here is final: the checkpoint
	pending []*wmEvent // received after done, in order
	held    bool       // an event failed: done stays below it
}

type wmEvent struct {
	seq     int64
	settled bool
	final   bool
}

func newWatermark(done int64) *watermark { return &watermark{done: done} }

// add tracks a received event. It returns nil when the event cannot move
// the watermark: it is held by a failed event, or the event is not after it.
func (w *watermark) add(seq int64) *wmEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.held || seq <= w.done {
		return nil
	}
	ev := &wmEvent{seq: seq}
	w.pending = append(w.pending, ev)
	return ev
}

// settle records an event's outcome and moves the watermark past the final
// events at the front. A failed event holds the watermark below it for good
// (this owner never reads it again); it reports whether ev did that.
func (w *watermark) settle(ev *wmEvent, final bool) (held bool) {
	if ev == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if ev.settled {
		return false
	}
	ev.settled, ev.final = true, final
	if !final {
		for i, p := range w.pending {
			if p == ev {
				// Nothing from here on can move the watermark.
				clear(w.pending[i:])
				w.pending = w.pending[:i]
				w.held = true
				held = true
				break
			}
		}
	}
	for len(w.pending) > 0 && w.pending[0].settled && w.pending[0].final {
		w.done = w.pending[0].seq
		w.pending[0] = nil
		w.pending = w.pending[1:]
	}
	return held
}

// value is the checkpoint: every event up to it is final.
func (w *watermark) value() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

// idle reports whether no tracked event is still in flight.
func (w *watermark) idle() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending) == 0
}
