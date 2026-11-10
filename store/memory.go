package store

import (
	"context"
	"encoding/binary"
	"slices"
	"sort"
	"sync"
	"time"
)

// Memory is the in-process Store: history and KV in maps, with lazy expiry.
// It serves `hikyaku run` without stores: and every test. It is not shared
// between replicas.
type Memory struct {
	name string
	now  func() time.Time

	mu      sync.Mutex
	hist    map[histKey]*histList
	msgs    map[string]msgRef
	kv      map[string]kvEntry
	writes  int
	failErr error
}

type histKey struct{ route, key string }

type histList struct {
	recs    []CallRecord // oldest first
	expires time.Time
}

type msgRef struct {
	k       histKey
	expires time.Time
}

type kvEntry struct {
	val     []byte
	expires time.Time // zero: never
}

// NewMemory returns an empty memory store.
func NewMemory(name string) *Memory {
	return &Memory{
		name: name,
		now:  time.Now,
		hist: map[histKey]*histList{},
		msgs: map[string]msgRef{},
		kv:   map[string]kvEntry{},
	}
}

// SetClock replaces the clock (tests).
func (m *Memory) SetClock(now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = now
}

// SetError makes every operation fail with err (nil clears it): a dead
// backend, for tests of on_store_error.
func (m *Memory) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failErr = err
}

// Name is the store's name.
func (m *Memory) Name() string { return m.name }

// Health reports the injected error, if any.
func (m *Memory) Health(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failErr
}

// Close does nothing.
func (m *Memory) Close() error { return nil }

func (m *Memory) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.failErr
}

// Append adds a record as the newest of its key.
func (m *Memory) Append(ctx context.Context, route, key string, rec CallRecord, o AppendOptions) error {
	if o.TTL <= 0 {
		return ErrNoTTL
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return err
	}
	now := m.now()
	k := histKey{route, key}
	l := m.live(k, now)
	if l == nil {
		l = &histList{}
		m.hist[k] = l
	}
	l.recs = append(l.recs, rec)
	if keep := o.KeepOrDefault(); len(l.recs) > keep {
		for _, old := range l.recs[:len(l.recs)-keep] {
			delete(m.msgs, old.MsgID)
		}
		l.recs = slices.Clone(l.recs[len(l.recs)-keep:])
	}
	l.expires = now.Add(o.TTL)
	if rec.MsgID != "" {
		m.msgs[rec.MsgID] = msgRef{k: k, expires: l.expires}
	}
	m.sweep(now)
	return nil
}

// live returns a key's list, dropping it when expired.
func (m *Memory) live(k histKey, now time.Time) *histList {
	l, ok := m.hist[k]
	if !ok {
		return nil
	}
	if !l.expires.IsZero() && !now.Before(l.expires) {
		delete(m.hist, k)
		return nil
	}
	return l
}

// Recent returns the newest records within the window, oldest first.
func (m *Memory) Recent(ctx context.Context, route, key string, w Window) ([]CallRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return nil, err
	}
	now := m.now()
	l := m.live(histKey{route, key}, now)
	if l == nil {
		return nil, nil
	}
	var out []CallRecord
	for _, r := range l.recs {
		if w.MaxAge > 0 && now.Sub(r.At) > w.MaxAge {
			continue
		}
		out = append(out, r)
	}
	if n := w.Entries(); len(out) > n {
		out = out[len(out)-n:]
	}
	return slices.Clone(out), nil
}

// Feedback attaches a correction to a message's record.
func (m *Memory) Feedback(ctx context.Context, msgID string, fb Feedback) (CallRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return CallRecord{}, err
	}
	now := m.now()
	ref, ok := m.msgs[msgID]
	if !ok || !now.Before(ref.expires) {
		return CallRecord{}, ErrNotFound
	}
	l := m.live(ref.k, now)
	if l == nil {
		return CallRecord{}, ErrNotFound
	}
	for i := range l.recs {
		if l.recs[i].MsgID == msgID {
			if fb.At.IsZero() {
				fb.At = now
			}
			f := fb
			l.recs[i].Feedback = &f
			return l.recs[i], nil
		}
	}
	return CallRecord{}, ErrNotFound
}

// Forget deletes a key's history.
func (m *Memory) Forget(ctx context.Context, route, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return err
	}
	k := histKey{route, key}
	if l, ok := m.hist[k]; ok {
		for _, r := range l.recs {
			delete(m.msgs, r.MsgID)
		}
		delete(m.hist, k)
	}
	return nil
}

// Records walks every live record of a route (all routes when empty) since
// a time.
func (m *Memory) Records(ctx context.Context, route string, since time.Time, fn func(CallRecord) error) error {
	m.mu.Lock()
	if err := m.check(ctx); err != nil {
		m.mu.Unlock()
		return err
	}
	now := m.now()
	keys := make([]histKey, 0, len(m.hist))
	for k := range m.hist {
		if route == "" || k.route == route {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].route != keys[j].route {
			return keys[i].route < keys[j].route
		}
		return keys[i].key < keys[j].key
	})
	var recs []CallRecord
	for _, k := range keys {
		l := m.live(k, now)
		if l == nil {
			continue
		}
		for _, r := range l.recs {
			if !r.At.Before(since) {
				recs = append(recs, r)
			}
		}
	}
	m.mu.Unlock()
	for _, r := range recs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// Get returns a value.
func (m *Memory) Get(ctx context.Context, k string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return nil, false, err
	}
	e, ok := m.kvLive(k, m.now())
	if !ok {
		return nil, false, nil
	}
	return slices.Clone(e.val), true, nil
}

func (m *Memory) kvLive(k string, now time.Time) (kvEntry, bool) {
	e, ok := m.kv[k]
	if !ok {
		return kvEntry{}, false
	}
	if !e.expires.IsZero() && !now.Before(e.expires) {
		delete(m.kv, k)
		return kvEntry{}, false
	}
	return e, true
}

// SetNX stores v under k unless k exists.
func (m *Memory) SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return false, err
	}
	now := m.now()
	if _, ok := m.kvLive(k, now); ok {
		return false, nil
	}
	m.kv[k] = kvEntry{val: slices.Clone(v), expires: expiry(now, ttl)}
	m.sweep(now)
	return true, nil
}

// Delete removes k.
func (m *Memory) Delete(ctx context.Context, k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return err
	}
	delete(m.kv, k)
	return nil
}

// Incr adds one to a counter.
func (m *Memory) Incr(ctx context.Context, k string, ttl time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.check(ctx); err != nil {
		return 0, err
	}
	now := m.now()
	e, ok := m.kvLive(k, now)
	var n int64
	if ok && len(e.val) == 8 {
		n = int64(binary.BigEndian.Uint64(e.val))
	} else {
		e = kvEntry{expires: expiry(now, ttl)}
	}
	n++
	e.val = binary.BigEndian.AppendUint64(nil, uint64(n))
	m.kv[k] = e
	m.sweep(now)
	return n, nil
}

func expiry(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

// sweep drops expired entries every 1024 writes, so dedupe keys and counters
// do not pile up.
func (m *Memory) sweep(now time.Time) {
	m.writes++
	if m.writes%1024 != 0 {
		return
	}
	for k, e := range m.kv {
		if !e.expires.IsZero() && !now.Before(e.expires) {
			delete(m.kv, k)
		}
	}
	for k := range m.hist {
		m.live(k, now)
	}
	for id, r := range m.msgs {
		if !now.Before(r.expires) {
			delete(m.msgs, id)
		}
	}
}
