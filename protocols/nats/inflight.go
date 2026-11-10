package nats

import (
	"context"
	"sync"
)

// inflight holds the messages a source handed to the relay and has not
// settled yet, so Close can wait for them (their acks need the connection)
// and settle what is left.
type inflight[T comparable] struct {
	mu    sync.Mutex
	items map[T]struct{}
	idle  chan struct{} // closed when the set empties, while someone waits
}

func (f *inflight[T]) add(x T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.items == nil {
		f.items = map[T]struct{}{}
	}
	f.items[x] = struct{}{}
}

func (f *inflight[T]) remove(x T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.items, x)
	if len(f.items) == 0 && f.idle != nil {
		close(f.idle)
		f.idle = nil
	}
}

func (f *inflight[T]) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}

// wait returns once every message is settled, or ctx's error first.
func (f *inflight[T]) wait(ctx context.Context) error {
	f.mu.Lock()
	if len(f.items) == 0 {
		f.mu.Unlock()
		return nil
	}
	if f.idle == nil {
		f.idle = make(chan struct{})
	}
	idle := f.idle
	f.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// snapshot lists the messages not settled yet.
func (f *inflight[T]) snapshot() []T {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]T, 0, len(f.items))
	for x := range f.items {
		out = append(out, x)
	}
	return out
}
