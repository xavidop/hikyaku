package mqtt

import (
	"context"
	"sync"
)

// ackQueue holds one connection's QoS 1 and 2 messages in arrival order.
//
// MQTT requires a client to send its PUBACKs and PUBRECs in the order the
// PUBLISHes arrived (MQTT 3.1.1 §4.6, MQTT 5 §4.6), but the relay settles
// messages in any order. So a message is acknowledged once it and every
// message received before it are settled final. A message settled not final
// (failed) is never acknowledged, and neither is anything after it: MQTT has
// no negative acknowledgement, so the source drops the connection and the
// broker redelivers them from the session. onFail reports the first failure.
type ackQueue struct {
	onFail func()

	mu       sync.Mutex
	entries  []*ackEntry // arrival order; the head is the oldest not acknowledged
	failed   bool
	closed   bool
	inflight int           // entries handed to the relay and not settled yet
	idle     chan struct{} // closed while inflight is 0
	acked    int

	// sendMu serializes the acknowledgements, so they leave in queue order
	// without holding mu while a client library writes them.
	sendMu sync.Mutex
}

type ackState int

const (
	ackPending ackState = iota
	ackFinal
	ackFailed
)

// ackEntry is one message in the queue.
type ackEntry struct {
	msg      *message
	state    ackState
	inflight bool
}

func newAckQueue(onFail func()) *ackQueue {
	idle := make(chan struct{})
	close(idle)
	return &ackQueue{onFail: onFail, idle: idle}
}

// add queues a message that is about to be handed to the relay, counting it
// in flight.
func (q *ackQueue) add(m *message) *ackEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	e := &ackEntry{msg: m, inflight: true}
	q.entries = append(q.entries, e)
	if q.inflight == 0 {
		q.idle = make(chan struct{})
	}
	q.inflight++
	return e
}

// drop takes back a message that never reached the relay. It stays in the
// queue unacknowledged, holding back every later message of the connection,
// so the broker redelivers it.
func (q *ackQueue) drop(e *ackEntry) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.landed(e)
}

// settle records the relay's outcome for e, once, and sends the
// acknowledgements it unblocks. e leaves the in-flight count only after
// that, so waitIdle returns once the acknowledgements are out.
func (q *ackQueue) settle(e *ackEntry, final bool) {
	q.mu.Lock()
	if e.state != ackPending {
		q.mu.Unlock()
		return
	}
	notify := false
	if final {
		e.state = ackFinal
	} else {
		e.state = ackFailed
		notify = !q.failed && !q.closed
		q.failed = true
	}
	q.mu.Unlock()
	if notify && q.onFail != nil {
		q.onFail()
	}
	if final {
		q.flush()
	}
	q.mu.Lock()
	q.landed(e)
	q.mu.Unlock()
}

// landed takes e out of the in-flight count. q.mu is held.
func (q *ackQueue) landed(e *ackEntry) {
	if !e.inflight {
		return
	}
	e.inflight = false
	q.inflight--
	if q.inflight == 0 {
		close(q.idle)
	}
}

// flush acknowledges the settled messages at the head of the queue, in
// order, stopping at the first one not settled final.
func (q *ackQueue) flush() {
	q.sendMu.Lock()
	defer q.sendMu.Unlock()
	for {
		q.mu.Lock()
		if q.closed || len(q.entries) == 0 || q.entries[0].state != ackFinal {
			q.mu.Unlock()
			return
		}
		e := q.entries[0]
		q.entries[0] = nil
		q.entries = q.entries[1:]
		q.acked++
		q.mu.Unlock()
		if e.msg.ack != nil {
			// An error means the connection is gone: the broker redelivers.
			_ = e.msg.ack()
		}
	}
}

// failedAlready reports whether a message of the queue failed.
func (q *ackQueue) failedAlready() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.failed
}

// ackedCount is how many messages the queue acknowledged.
func (q *ackQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked
}

// close stops the acknowledgements: the connection is gone or being
// replaced, and the broker redelivers what is left. It does not wait for an
// acknowledgement already on its way out, which a stalled connection could
// hold up; Close waits for those through waitIdle first.
func (q *ackQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
}

// waitIdle waits until every message handed to the relay is settled, or ctx
// ends.
func (q *ackQueue) waitIdle(ctx context.Context) error {
	q.mu.Lock()
	idle := q.idle
	q.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
