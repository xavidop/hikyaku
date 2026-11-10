package azure

import (
	"context"
	"sync"
)

// counter counts work in flight and lets a caller wait until there is none.
// Unlike a sync.WaitGroup, adding while another goroutine waits is fine.
type counter struct {
	mu   sync.Mutex
	n    int
	zero chan struct{} // closed while n == 0
}

func newCounter() *counter {
	c := &counter{zero: make(chan struct{})}
	close(c.zero)
	return c
}

func (c *counter) add() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == 0 {
		c.zero = make(chan struct{})
	}
	c.n++
}

func (c *counter) done() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n--
	if c.n == 0 {
		close(c.zero)
	}
}

// count is the work in flight.
func (c *counter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// idle is closed once nothing is in flight.
func (c *counter) idle() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.zero
}

// wait waits until nothing is in flight; false when ctx ended first.
func (c *counter) wait(ctx context.Context) bool {
	select {
	case <-c.idle():
		return true
	case <-ctx.Done():
		return false
	}
}
