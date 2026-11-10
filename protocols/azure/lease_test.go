package azure

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/store"
)

// fleet is replicas sharing a memory store on a fake clock.
type fleet struct {
	t     *testing.T
	clock time.Time
	mem   *store.Memory
}

func newFleet(t *testing.T) *fleet {
	f := &fleet{t: t, clock: time.Unix(1_700_000_000, 0), mem: store.NewMemory("m")} // an epoch boundary
	f.mem.SetClock(func() time.Time { return f.clock })
	return f
}

func (f *fleet) replica(id string, partitions ...string) *leases {
	l := newLeases(f.mem, "eventhubs:ns:hub:$Default", id, 10*time.Second, func() time.Time { return f.clock })
	l.partitions = partitions
	return l
}

func owned(t *testing.T, l *leases) []string {
	t.Helper()
	m, _, err := l.tick(context.Background())
	require.NoError(t, err)
	return slices.Sorted(maps.Keys(m))
}

func TestLeasesShareAndFailOver(t *testing.T) {
	f := newFleet(t)
	parts := []string{"0", "1", "2", "3"}
	a, b := f.replica("a", parts...), f.replica("b", parts...)

	assert.Equal(t, parts, owned(t, a), "a lone replica takes every free partition at once")
	assert.Empty(t, owned(t, b), "the partitions are taken for this epoch")

	var gotA, gotB []string
	for range 8 { // two epochs, a tick every quarter
		f.clock = f.clock.Add(2500 * time.Millisecond)
		gotA, gotB = owned(t, a), owned(t, b)
	}
	assert.Len(t, gotA, 2, "two replicas share the partitions")
	assert.Len(t, gotB, 2)
	assert.ElementsMatch(t, parts, append(slices.Clone(gotA), gotB...), "each partition has one owner")

	// The ownership is stable.
	for range 8 {
		f.clock = f.clock.Add(2500 * time.Millisecond)
		assert.Equal(t, gotA, owned(t, a))
		assert.Equal(t, gotB, owned(t, b))
	}

	// b stops: within two epochs a owns everything again.
	for range 8 {
		f.clock = f.clock.Add(2500 * time.Millisecond)
		gotA = owned(t, a)
	}
	assert.Equal(t, parts, gotA)
}

func TestLeasesAfterARestart(t *testing.T) {
	f := newFleet(t)
	parts := []string{"0", "1"}
	old := f.replica("old", parts...)
	assert.Equal(t, parts, owned(t, old))
	f.clock = f.clock.Add(5 * time.Second)
	assert.Equal(t, parts, owned(t, old), "the owner renews for the next epoch")

	// The process restarts with a new identity: it waits for the leases of
	// the old one to run out, then takes everything.
	fresh := f.replica("fresh", parts...)
	var got []string
	ticks := 0
	for ; len(got) < 2 && ticks < 12; ticks++ {
		f.clock = f.clock.Add(2500 * time.Millisecond)
		got = owned(t, fresh)
	}
	assert.Equal(t, parts, got)
	assert.LessOrEqual(t, ticks, 8, "within two epochs")
}

func TestLeasesHelpers(t *testing.T) {
	assert.Equal(t, []string{"0", "1", "2", "10", "11"}, sortPartitions([]string{"10", "2", "0", "11", "1"}))
	assert.Equal(t, []string{"a", "b", "c"}, sortPartitions([]string{"c", "a", "b"}))
	id := replicaID()
	assert.NotEmpty(t, id)
	assert.NotEqual(t, id, replicaID(), "every process is a new replica")
	assert.True(t, strings.Contains(id, "-"))

	l := newLeases(store.NewMemory("m"), "p", "x", 10*time.Second, nil)
	e, phase := l.at(time.Unix(1_700_000_007, 500_000_000))
	assert.Equal(t, int64(170_000_000), e)
	assert.InDelta(t, 0.75, phase, 1e-9)
}

// failingKV fails every call.
type failingKV struct{}

func (failingKV) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("down")
}
func (failingKV) SetNX(context.Context, string, []byte, time.Duration) (bool, error) {
	return false, errors.New("down")
}
func (failingKV) Incr(context.Context, string, time.Duration) (int64, error) {
	return 0, errors.New("down")
}

func TestLeasesStoreDown(t *testing.T) {
	l := newLeases(failingKV{}, "p", "x", 10*time.Second, time.Now)
	l.partitions = []string{"0"}
	_, _, err := l.tick(context.Background())
	assert.ErrorContains(t, err, "down")
}
