package mqtt

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ackLog records the order acknowledgements are sent in.
type ackLog struct {
	mu  sync.Mutex
	ids []uint16
}

func (l *ackLog) message(id uint16) *message {
	return &message{qos: 1, packetID: id, ack: func() error {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.ids = append(l.ids, id)
		return nil
	}}
}

func (l *ackLog) sent() []uint16 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint16(nil), l.ids...)
}

func TestAckQueueAcknowledgesInArrivalOrder(t *testing.T) {
	var log ackLog
	q := newAckQueue(nil)
	e1, e2, e3, e4 := q.add(log.message(1)), q.add(log.message(2)), q.add(log.message(3)), q.add(log.message(4))

	q.settle(e3, true)
	q.settle(e2, true)
	assert.Empty(t, log.sent(), "nothing goes out before the first message is settled")
	q.settle(e1, true)
	assert.Equal(t, []uint16{1, 2, 3}, log.sent(), "the head unblocks what settled behind it")
	q.settle(e4, true)
	assert.Equal(t, []uint16{1, 2, 3, 4}, log.sent())
	assert.Equal(t, 4, q.ackedCount())

	q.settle(e4, true) // a second settle is ignored
	assert.Equal(t, []uint16{1, 2, 3, 4}, log.sent())
}

func TestAckQueueStopsAtAFailure(t *testing.T) {
	var log ackLog
	failures := 0
	q := newAckQueue(func() { failures++ })
	e1, e2, e3 := q.add(log.message(1)), q.add(log.message(2)), q.add(log.message(3))

	q.settle(e3, true)
	q.settle(e2, false)
	assert.Equal(t, 1, failures)
	assert.True(t, q.failedAlready())
	q.settle(e1, true)
	assert.Equal(t, []uint16{1}, log.sent(), "what came before the failure is still acknowledged")

	e4 := q.add(log.message(4))
	q.settle(e4, true)
	q.settle(e1, false) // already settled: no second failure
	assert.Equal(t, []uint16{1}, log.sent(), "nothing after a failure is acknowledged")
	assert.Equal(t, 1, failures, "onFail runs once")
}

func TestAckQueueDroppedMessagesHoldBackLaterOnes(t *testing.T) {
	var log ackLog
	q := newAckQueue(nil)
	e1, e2 := q.add(log.message(1)), q.add(log.message(2))
	q.drop(e1) // never reached the relay: the broker redelivers it
	q.settle(e2, true)
	assert.Empty(t, log.sent())
	require.NoError(t, q.waitIdle(context.Background()), "a dropped message is not in flight")
}

func TestAckQueueClosedSendsNothing(t *testing.T) {
	var log ackLog
	q := newAckQueue(func() { t.Error("a closed queue reports no failure") })
	e1, e2 := q.add(log.message(1)), q.add(log.message(2))
	q.close()
	q.settle(e1, true)
	q.settle(e2, false)
	assert.Empty(t, log.sent())
}

func TestAckQueueWaitIdle(t *testing.T) {
	var log ackLog
	q := newAckQueue(nil)
	require.NoError(t, q.waitIdle(context.Background()), "an empty queue is idle")
	e1 := q.add(log.message(1))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, q.waitIdle(ctx), context.DeadlineExceeded)

	go func() {
		time.Sleep(20 * time.Millisecond)
		q.settle(e1, false) // a failure lands too
	}()
	require.NoError(t, q.waitIdle(context.Background()))
}

func TestAckQueueIdleOnlyOnceAcknowledged(t *testing.T) {
	release, acked := make(chan struct{}), make(chan struct{})
	q := newAckQueue(nil)
	e := q.add(&message{qos: 1, ack: func() error {
		<-release
		close(acked)
		return nil
	}})
	go q.settle(e, true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, q.waitIdle(ctx), context.DeadlineExceeded, "idle before its acknowledgement went out")
	close(release)
	require.NoError(t, q.waitIdle(context.Background()))
	select {
	case <-acked:
	default:
		t.Fatal("idle, but not acknowledged")
	}
	q.close() // Close closes the queue once idle: nothing is lost
}

func TestAckQueueCloseDoesNotWaitForAStalledAck(t *testing.T) {
	var log ackLog
	stalled, release := make(chan struct{}), make(chan struct{})
	q := newAckQueue(nil)
	e1 := q.add(&message{qos: 1, ack: func() error {
		close(stalled)
		<-release // a write to a broker that stopped reading
		return nil
	}})
	e2 := q.add(log.message(2))
	q.settle(e2, true)
	go q.settle(e1, true)
	<-stalled
	closedQueue := make(chan struct{})
	go func() {
		q.close()
		close(closedQueue)
	}()
	select {
	case <-closedQueue:
	case <-time.After(time.Second):
		t.Fatal("close waited for the stalled acknowledgement")
	}
	close(release)
	require.NoError(t, q.waitIdle(context.Background()))
	assert.Empty(t, log.sent(), "nothing more goes out once closed")
}

func TestAckQueueConcurrentSettlesKeepOrder(t *testing.T) {
	var log ackLog
	q := newAckQueue(nil)
	const n = 500
	entries := make([]*ackEntry, n)
	for i := range entries {
		entries[i] = q.add(log.message(uint16(i + 1)))
	}
	order := rand.Perm(n)
	var wg sync.WaitGroup
	for _, i := range order {
		wg.Go(func() { q.settle(entries[i], true) })
	}
	wg.Wait()
	sent := log.sent()
	require.Len(t, sent, n)
	for i, id := range sent {
		require.Equal(t, uint16(i+1), id, "acknowledgement %d out of order", i)
	}
}
