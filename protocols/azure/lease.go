package azure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/xavidop/hikyaku/store"
)

// Partition ownership: leases in the same store as the checkpoints.
//
// Time is cut into epochs of leaseEpoch. A replica owns a partition for an
// epoch when it wrote that epoch's lease first (SetNX):
//
//	eventhubs:<ns>:<hub>:<group>:<partition>:lease:<epoch>   the owner's id
//	eventhubs:<ns>:<hub>:<group>:members:<epoch>:<slot>      a replica alive in the epoch
//
// The owner of epoch e claims e+1 once a third of e has passed; the other
// replicas take a lease of e+1 still free after two thirds, so the
// partitions of a replica that stopped move within two epochs. A free lease
// of the current epoch is taken at once. A replica takes at most its share
// of the partitions, ceil(partitions / replicas), where replicas are those
// registered in the epoch, and leaves the partitions above its share for the
// others to take. The lease decides who reads a partition; Event Hubs
// enforces it, because every new reader opens the partition with a higher
// owner level than the readers before it, which disconnects them.

const (
	// defaultLeaseEpoch is the length of a lease.
	defaultLeaseEpoch = 10 * time.Second
	// maxMembers bounds the replicas counted in an epoch.
	maxMembers = 64
)

// leases is one replica's view of the partition leases of a consumer group.
type leases struct {
	kv     store.KV
	prefix string // eventhubs:<namespace>:<hub>:<group>
	id     string
	epoch  time.Duration
	now    func() time.Time

	partitions []string
	registered int64           // the last epoch this replica registered in
	counted    map[int64]int64 // member counts of finished epochs
}

func newLeases(kv store.KV, prefix, id string, epoch time.Duration, now func() time.Time) *leases {
	return &leases{kv: kv, prefix: prefix, id: id, epoch: epoch, now: now, registered: -1, counted: map[int64]int64{}}
}

// replicaID names this process: the host and a random suffix.
func replicaID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "hikyaku"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

// sortPartitions orders partition IDs numerically when they are numbers.
func sortPartitions(ids []string) []string {
	out := slices.Clone(ids)
	slices.SortFunc(out, func(a, b string) int {
		x, errA := strconv.Atoi(a)
		y, errB := strconv.Atoi(b)
		if errA == nil && errB == nil {
			return x - y
		}
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		}
		return 0
	})
	return out
}

func (l *leases) leaseKey(partition string, e int64) string {
	return l.prefix + ":" + partition + ":lease:" + strconv.FormatInt(e, 10)
}

func (l *leases) memberKey(e int64, slot int) string {
	return l.prefix + ":members:" + strconv.FormatInt(e, 10) + ":" + strconv.Itoa(slot)
}

// at is the epoch of t and how far into it t is, in [0, 1).
func (l *leases) at(t time.Time) (int64, float64) {
	ns, ep := t.UnixNano(), int64(l.epoch)
	return ns / ep, float64(ns%ep) / float64(ep)
}

// tick takes and renews leases, and returns the partitions this replica
// owns in the current epoch. On a store error it returns the error and the
// caller keeps what it reads.
func (l *leases) tick(ctx context.Context) (map[string]bool, int64, error) {
	e, phase := l.at(l.now())
	if err := l.register(ctx, e); err != nil {
		return nil, e, err
	}
	n, err := l.replicas(ctx, e, phase)
	if err != nil {
		return nil, e, err
	}
	share := (len(l.partitions) + n - 1) / max(n, 1)

	mine := map[string]bool{}
	var free []string
	for _, p := range l.partitions {
		v, _, err := l.kv.Get(ctx, l.leaseKey(p, e))
		if err != nil {
			return nil, e, err
		}
		switch string(v) {
		case l.id:
			mine[p] = true
		case "":
			free = append(free, p)
		}
	}
	for _, p := range free {
		if len(mine) >= share {
			break
		}
		ok, err := l.kv.SetNX(ctx, l.leaseKey(p, e), []byte(l.id), 3*l.epoch)
		if err != nil {
			return nil, e, err
		}
		if ok {
			mine[p] = true
		}
	}
	if phase >= 1.0/3 {
		if err := l.claimNext(ctx, e, phase, mine, share); err != nil {
			return mine, e, err
		}
	}
	return mine, e, nil
}

// claimNext renews this replica's partitions for the next epoch, up to its
// share, and after two thirds of the epoch takes the next epoch's free
// leases up to the share.
func (l *leases) claimNext(ctx context.Context, e int64, phase float64, mine map[string]bool, share int) error {
	next := make(map[string]string, len(l.partitions))
	held := 0
	for _, p := range l.partitions {
		v, _, err := l.kv.Get(ctx, l.leaseKey(p, e+1))
		if err != nil {
			return err
		}
		next[p] = string(v)
		if next[p] == l.id {
			held++
		}
	}
	take := func(p string) error {
		ok, err := l.kv.SetNX(ctx, l.leaseKey(p, e+1), []byte(l.id), 3*l.epoch)
		if ok {
			held++
			next[p] = l.id
		}
		return err
	}
	for _, p := range l.partitions {
		if held >= share {
			break
		}
		if mine[p] && next[p] == "" {
			if err := take(p); err != nil {
				return err
			}
		}
	}
	if phase < 2.0/3 {
		return nil
	}
	for _, p := range l.partitions {
		if held >= share {
			break
		}
		if next[p] == "" {
			if err := take(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// register records this replica as alive in epoch e, in the lowest free
// slot, so an epoch's slots are taken from 0 up without gaps.
func (l *leases) register(ctx context.Context, e int64) error {
	if l.registered == e {
		return nil
	}
	for slot := range maxMembers {
		ok, err := l.kv.SetNX(ctx, l.memberKey(e, slot), []byte(l.id), 3*l.epoch)
		if err != nil {
			return err
		}
		if ok {
			break
		}
	}
	l.registered = e
	return nil
}

// replicas is how many replicas share the partitions. Early in an epoch
// some have not registered in it yet, so the previous epoch counts too;
// after two thirds of it, every live replica has registered.
func (l *leases) replicas(ctx context.Context, e int64, phase float64) (int, error) {
	cur, err := l.count(ctx, e)
	if err != nil {
		return 0, err
	}
	if phase >= 2.0/3 {
		return max(int(cur), 1), nil
	}
	prev, ok := l.counted[e-1]
	if !ok {
		if prev, err = l.count(ctx, e-1); err != nil {
			return 0, err
		}
		clear(l.counted)
		l.counted[e-1] = prev
	}
	return max(int(prev), int(cur), 1), nil
}

// count is the number of replicas registered in epoch e.
func (l *leases) count(ctx context.Context, e int64) (int64, error) {
	for slot := range maxMembers {
		_, ok, err := l.kv.Get(ctx, l.memberKey(e, slot))
		if err != nil {
			return 0, err
		}
		if !ok {
			return int64(slot), nil
		}
	}
	return maxMembers, nil
}
