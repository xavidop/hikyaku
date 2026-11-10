package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func trackAll(t *testing.T, p *partState, offsets ...int64) {
	t.Helper()
	for _, o := range offsets {
		require.Equal(t, tracked, p.add(o, 7), "offset %d", o)
	}
}

func commitAt(t *testing.T, p *partState) int64 {
	t.Helper()
	eo, ok := p.toCommit()
	if !ok {
		return -1
	}
	assert.Equal(t, int32(7), eo.Epoch, "the epoch of the last record of the prefix")
	return eo.Offset
}

func TestWatermarkCommitsTheContiguousFinalPrefix(t *testing.T) {
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 10, 11, 12, 13)
	assert.Equal(t, int64(-1), commitAt(t, p), "nothing is final yet")

	assert.False(t, p.settle(12, true))
	assert.False(t, p.settle(11, true))
	assert.Equal(t, int64(-1), commitAt(t, p), "10 is still in flight: 11 and 12 wait behind it")

	p.settle(10, true)
	assert.Equal(t, int64(13), commitAt(t, p), "10, 11 and 12 are final")
	assert.Equal(t, 1, p.live())

	p.markCommitted(13)
	_, ok := p.toCommit()
	assert.False(t, ok, "nothing new since the last commit")

	p.settle(13, true)
	assert.Equal(t, int64(14), commitAt(t, p))
	assert.Equal(t, 0, p.live(), "a fully committed partition keeps no entries")
}

func TestWatermarkFollowsRecordsNotOffsetArithmetic(t *testing.T) {
	// Compacted topics and transaction markers leave gaps.
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 3, 9, 20)
	p.settle(9, true)
	p.settle(3, true)
	assert.Equal(t, int64(10), commitAt(t, p))
	p.settle(20, true)
	assert.Equal(t, int64(21), commitAt(t, p))
}

func TestWatermarkFailedRecordHoldsThePartition(t *testing.T) {
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 0, 1, 2, 3, 4)
	p.settle(0, true)
	assert.True(t, p.settle(2, false), "a failed record holds the partition")
	assert.True(t, p.held)
	p.settle(1, true)
	p.settle(3, true)
	p.settle(4, true)
	assert.Equal(t, int64(2), commitAt(t, p), "the commit stops at the failed record: a restart resumes from it")
	assert.Equal(t, 1, p.live(), "the records after the hold are no longer tracked")

	assert.Equal(t, untracked, p.add(5, 7), "nothing polled after a hold can move the commit")
	assert.False(t, p.settle(5, true))
	assert.Equal(t, int64(2), commitAt(t, p))

	assert.False(t, p.settle(2, true), "a record settles once")
	assert.True(t, p.held, "the hold stays")
}

func TestWatermarkEarlierFailureMovesTheHold(t *testing.T) {
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 0, 1, 2, 3)
	assert.True(t, p.settle(3, false))
	assert.True(t, p.settle(1, false), "an earlier failure moves the hold down")
	p.settle(0, true)
	p.settle(2, true)
	assert.Equal(t, int64(1), commitAt(t, p))
}

func TestWatermarkDetectsARewind(t *testing.T) {
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 5, 6)
	assert.Equal(t, rewound, p.add(6, 7))
	assert.Equal(t, rewound, p.add(2, 7))
	assert.Equal(t, tracked, p.add(7, 7))

	held := newPartState(tp{"t", 0})
	trackAll(t, held, 5)
	held.settle(5, false)
	assert.Equal(t, untracked, held.add(1, 7), "a held partition stays held: its commit waits for a restart")
}

func TestWatermarkIgnoresUnknownOffsets(t *testing.T) {
	p := newPartState(tp{"t", 0})
	trackAll(t, p, 1, 2)
	assert.False(t, p.settle(9, true))
	assert.False(t, p.settle(0, false))
	assert.False(t, p.held)
	p.settle(1, true)
	assert.False(t, p.settle(1, true), "settling twice is a no-op")
	assert.Equal(t, int64(2), commitAt(t, p))
}

func TestWatermarkCompactsCommittedEntries(t *testing.T) {
	p := newPartState(tp{"t", 0})
	const n = 3 * compactAfter
	for o := int64(0); o < n; o++ {
		require.Equal(t, tracked, p.add(o, 7))
	}
	// The first record stays in flight while the rest settle.
	for o := int64(n - 1); o >= 1; o-- {
		p.settle(o, true)
	}
	assert.Equal(t, n, len(p.entries))
	p.settle(0, true)
	assert.Equal(t, int64(n), commitAt(t, p))
	assert.Equal(t, 0, p.live())

	// A long prefix ahead of one slow record is moved down.
	for o := int64(n); o < 2*n; o++ {
		p.add(o, 7)
	}
	for o := int64(n); o < 2*n-1; o++ {
		p.settle(o, true)
	}
	assert.Equal(t, 1, p.live())
	assert.Less(t, len(p.entries), compactAfter, "the committed entries were dropped")
	idx, ok := p.find(2*n - 1)
	require.True(t, ok)
	assert.Equal(t, int64(2*n-1), p.entries[idx].offset)
	p.settle(2*n-1, true)
	assert.Equal(t, int64(2*n), commitAt(t, p))
}

func TestWatermarkCommitOffsetCarriesTheEpoch(t *testing.T) {
	p := newPartState(tp{"t", 3})
	require.Equal(t, tracked, p.add(4, 2))
	require.Equal(t, tracked, p.add(5, 3))
	p.settle(4, true)
	p.settle(5, true)
	eo, ok := p.toCommit()
	require.True(t, ok)
	assert.Equal(t, kgo.EpochOffset{Epoch: 3, Offset: 6}, eo)
	p.markCommitted(6)
	p.markCommitted(2)
	assert.Equal(t, int64(6), p.committed, "a commit never moves back")
}
