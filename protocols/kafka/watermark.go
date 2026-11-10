package kafka

import (
	"cmp"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

// compactAfter is how many committed entries a partition keeps at the front
// of its slice before moving the live ones down.
const compactAfter = 1024

// partState is one assigned partition's watermark: it tracks records from
// poll to commit. Records are added in offset order as they are polled and
// settle in any order; the commit offset only moves past the contiguous
// prefix of records that settled final. A record that settles not final
// holds the partition: nothing at or after it is committed until the
// partition is assigned again (a restart or a rebalance), which resumes
// from it. The records after a held one are no longer tracked, so a hold
// costs no memory however long it lasts.
//
// Offsets need not be contiguous (compaction, transaction markers): the
// prefix follows the records polled, not offset arithmetic.
//
// A partState is guarded by its source's mutex.
type partState struct {
	tp tp
	// entries are the tracked records in offset order; entries[:head] are
	// behind the commit offset already.
	entries []entry
	head    int
	// next is the offset to commit, one past the final prefix, and epoch
	// the leader epoch of the record before it. next is -1 until a record
	// is final.
	next  int64
	epoch int32
	// committed is the offset last committed, -1 for none.
	committed int64
	// last is the highest offset polled, -1 for none.
	last int64
	// held: a tracked record failed, and the commit cannot pass it.
	held bool
	// dead: the partition was revoked or lost, or replaced after the
	// consumer rewound; its settles are ignored.
	dead bool
}

type entryState uint8

const (
	pending entryState = iota
	done
	failed
)

type entry struct {
	offset int64
	epoch  int32
	state  entryState
}

func newPartState(t tp) *partState {
	return &partState{tp: t, next: -1, committed: -1, last: -1}
}

// addResult is what add did with a polled record.
type addResult int

const (
	// tracked: the record's outcome moves the commit.
	tracked addResult = iota
	// untracked: the partition is held, so the record's outcome cannot
	// move the commit.
	untracked
	// rewound: the offset is not after the last one polled; the consumer
	// went back (it reset after data loss). The partition needs a new
	// state.
	rewound
)

// add tracks a polled record.
func (p *partState) add(offset int64, epoch int32) addResult {
	if p.held {
		p.last = max(p.last, offset)
		return untracked
	}
	if offset <= p.last {
		return rewound
	}
	p.last = offset
	p.entries = append(p.entries, entry{offset: offset, epoch: epoch})
	return tracked
}

// settle records a record's outcome: final (the broker may forget it) or
// not. It reports whether the record now holds the partition.
func (p *partState) settle(offset int64, final bool) (held bool) {
	i, ok := p.find(offset)
	if !ok || p.entries[i].state != pending {
		return false
	}
	if !final {
		p.entries[i].state = failed
		// Nothing after it can be committed now: forget those.
		p.entries = p.entries[:i+1]
		p.held = true
		return true
	}
	p.entries[i].state = done
	p.advance()
	return false
}

// find returns the index of a tracked record not yet behind the commit.
func (p *partState) find(offset int64) (int, bool) {
	i, ok := slices.BinarySearchFunc(p.entries[p.head:], offset, func(e entry, o int64) int {
		return cmp.Compare(e.offset, o)
	})
	return p.head + i, ok
}

// advance moves the commit offset past the records at the front that are
// done.
func (p *partState) advance() {
	for p.head < len(p.entries) && p.entries[p.head].state == done {
		e := p.entries[p.head]
		p.next, p.epoch = e.offset+1, e.epoch
		p.head++
	}
	switch {
	case p.head == len(p.entries):
		p.entries, p.head = p.entries[:0], 0
	case p.head >= compactAfter && 2*p.head >= len(p.entries):
		n := copy(p.entries, p.entries[p.head:])
		p.entries, p.head = p.entries[:n], 0
	}
}

// live is how many tracked records are not behind the commit yet.
func (p *partState) live() int { return len(p.entries) - p.head }

// toCommit returns the offset to commit, when it moved since the last
// commit.
func (p *partState) toCommit() (kgo.EpochOffset, bool) {
	if p.next <= p.committed {
		return kgo.EpochOffset{}, false
	}
	return kgo.EpochOffset{Epoch: p.epoch, Offset: p.next}, true
}

// markCommitted records a commit the broker accepted.
func (p *partState) markCommitted(offset int64) { p.committed = max(p.committed, offset) }
