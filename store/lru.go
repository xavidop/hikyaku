package store

import (
	"container/list"
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"time"
)

// LRU is a bounded in-process KV with per-entry TTL: the AI decision cache
// when no store is named.
type LRU struct {
	size int
	now  func() time.Time

	mu    sync.Mutex
	ll    *list.List
	items map[string]*list.Element
}

type lruEntry struct {
	key     string
	val     []byte
	expires time.Time
}

// NewLRU returns an LRU holding at most size entries (10000 when size <= 0).
func NewLRU(size int) *LRU {
	if size <= 0 {
		size = 10000
	}
	return &LRU{size: size, now: time.Now, ll: list.New(), items: map[string]*list.Element{}}
}

// Len is the number of entries, expired ones included until touched.
func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

func (c *LRU) get(k string) (*lruEntry, bool) {
	el, ok := c.items[k]
	if !ok {
		return nil, false
	}
	e := el.Value.(*lruEntry)
	if !e.expires.IsZero() && !c.now().Before(e.expires) {
		c.ll.Remove(el)
		delete(c.items, k)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e, true
}

func (c *LRU) put(k string, v []byte, ttl time.Duration) {
	if el, ok := c.items[k]; ok {
		c.ll.Remove(el)
	}
	c.items[k] = c.ll.PushFront(&lruEntry{key: k, val: v, expires: expiry(c.now(), ttl)})
	for c.ll.Len() > c.size {
		old := c.ll.Back()
		c.ll.Remove(old)
		delete(c.items, old.Value.(*lruEntry).key)
	}
}

// Get returns a value.
func (c *LRU) Get(_ context.Context, k string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.get(k)
	if !ok {
		return nil, false, nil
	}
	return slices.Clone(e.val), true, nil
}

// SetNX stores v unless k is present.
func (c *LRU) SetNX(_ context.Context, k string, v []byte, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.get(k); ok {
		return false, nil
	}
	c.put(k, slices.Clone(v), ttl)
	return true, nil
}

// Incr adds one to a counter.
func (c *LRU) Incr(_ context.Context, k string, ttl time.Duration) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int64
	e, ok := c.get(k)
	if ok && len(e.val) == 8 {
		n = int64(binary.BigEndian.Uint64(e.val))
		n++
		e.val = binary.BigEndian.AppendUint64(nil, uint64(n))
		return n, nil
	}
	c.put(k, binary.BigEndian.AppendUint64(nil, 1), ttl)
	return 1, nil
}
