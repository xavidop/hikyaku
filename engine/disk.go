package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/wal"
)

// The disk buffer (buffer.type: disk). Sources hand messages to the intake
// channel. The writer appends them to the write-ahead log in groups (one
// fsync per group) and only then reports them queued, so a source with
// ack: accepted acknowledges a message once it is on disk. The dispatcher
// feeds the log's messages to the workers in order. A message leaves the
// log once its outcome is final. One that failed (its destination and the
// DLQ were down) is retried with backoff while the relay runs, and whatever
// is left at a crash or a shutdown is delivered on the next start.

const (
	defaultBufferPath = "hikyaku-buffer.db"
	diskIntake        = 1024
	diskGroup         = 1024
)

// The backoff of a failed message's retries from the buffer (vars: tests
// shorten them).
var (
	diskRetryBase = 5 * time.Second
	diskRetryMax  = time.Minute
)

type diskBuffer struct {
	e     *Engine
	log   *wal.Log
	limit int

	mu      sync.Mutex
	cond    *sync.Cond
	ready   []queuedMsg
	stopped bool          // the dispatcher stops taking messages
	quit    chan struct{} // closed with stopped: ends an ordered retry's wait
	retry   bool          // failed messages are retried in this run
	tries   map[uint64]int

	// deletes are the final messages to remove from the log: one goroutine
	// removes whatever is pending in one transaction, so a busy relay pays
	// one fsync per group of deletes, as it does for appends.
	deletes chan uint64
	deleted chan struct{}
}

type queuedMsg struct {
	seq uint64
	env *envelope.Envelope
}

func (e *Engine) openDisk() (*diskBuffer, error) {
	path := e.spec.Buffer.Path
	if path == "" {
		path = defaultBufferPath
	}
	fsync := e.spec.Buffer.Sync == nil || *e.spec.Buffer.Sync
	log, err := wal.Open(path, fsync)
	if err != nil {
		return nil, err
	}
	d := &diskBuffer{e: e, log: log, limit: max(e.spec.Buffer.Size, 1), retry: true, tries: map[uint64]int{}, quit: make(chan struct{}),
		deletes: make(chan uint64, diskGroup*4), deleted: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	err = log.Replay(func(seq uint64, env *envelope.Envelope) {
		d.ready = append(d.ready, queuedMsg{seq, env})
	}, func(seq uint64, err error) {
		e.log.Warn("buffer: dropping an entry that no longer decodes", "seq", seq, "err", err)
	})
	if err != nil {
		_ = log.Close()
		return nil, err
	}
	if n := len(d.ready); n > 0 {
		e.log.Info("buffer: delivering the messages left by the last run", "count", n, "path", path)
	}
	go d.deleter()
	return d, nil
}

// deleter removes final messages from the log in groups until deletes is
// closed (closeDeletes), then removes what is left.
func (d *diskBuffer) deleter() {
	defer close(d.deleted)
	group := make([]uint64, 0, diskGroup*4)
	for seq := range d.deletes {
		group = append(group[:0], seq)
	drain:
		for len(group) < cap(group) {
			select {
			case s, ok := <-d.deletes:
				if !ok {
					break drain
				}
				group = append(group, s)
			default:
				break drain
			}
		}
		if err := d.log.DeleteMany(group); err != nil {
			d.e.log.Error("buffer: delete failed; these messages will be delivered again after a restart", "count", len(group), "err", err)
		}
		d.mu.Lock()
		for _, s := range group {
			delete(d.tries, s)
		}
		d.cond.Broadcast()
		d.mu.Unlock()
		d.e.queueDepth()
	}
}

// closeDeletes stops the deleter once every queued delete is written. The
// engine calls it after the messages in flight finished, before closing
// the log.
func (d *diskBuffer) closeDeletes() {
	close(d.deletes)
	<-d.deleted
}

// write moves messages from the intake to the log until stop closes, then
// drains what the sources already handed over.
func (d *diskBuffer) write(ctx context.Context, intake <-chan *envelope.Envelope, stop <-chan struct{}) {
	for {
		var first *envelope.Envelope
		select {
		case first = <-intake:
		case <-stop:
			// Drain what the sources already handed over, in groups too.
			for {
				var group []*envelope.Envelope
			rest:
				for len(group) < diskGroup {
					select {
					case env := <-intake:
						group = append(group, env)
					default:
						break rest
					}
				}
				if len(group) == 0 {
					return
				}
				d.append(ctx, group, true)
			}
		}
		group := []*envelope.Envelope{first}
	fill:
		for len(group) < diskGroup {
			select {
			case env := <-intake:
				group = append(group, env)
			default:
				break fill
			}
		}
		d.append(ctx, group, false)
	}
}

// append writes a group to the log, waiting for room unless draining, and
// queues it for dispatch.
func (d *diskBuffer) append(ctx context.Context, group []*envelope.Envelope, draining bool) {
	d.mu.Lock()
	for !draining && !d.stopped && d.log.Len()+len(group) > d.limit && d.log.Len() > 0 {
		d.cond.Wait()
	}
	d.mu.Unlock()
	seqs, err := d.log.Append(group)
	if err != nil {
		d.e.log.Error("buffer: write failed; nacking the messages", "err", err, "count", len(group))
		for _, env := range group {
			env.Ack(ctx, envelope.Outcome{Status: envelope.Failed, Err: err})
		}
		return
	}
	d.mu.Lock()
	for i, env := range group {
		d.ready = append(d.ready, queuedMsg{seqs[i], env})
	}
	d.cond.Broadcast()
	d.mu.Unlock()
	d.e.queueDepth()
	for _, env := range group {
		env.Queued()
	}
}

// dispatch feeds the log's messages to the workers, in order, until stop.
func (d *diskBuffer) dispatch(ctx context.Context) {
	for {
		d.mu.Lock()
		for len(d.ready) == 0 && !d.stopped {
			d.cond.Wait()
		}
		if d.stopped {
			d.mu.Unlock()
			return
		}
		m := d.ready[0]
		d.ready[0] = queuedMsg{}
		d.ready = d.ready[1:]
		d.mu.Unlock()
		d.e.dispatchDone(ctx, m.env, d.doneFor(ctx, m))
	}
}

// doneFor deletes a finished message from the log, or retries a failed
// one: an ordered message where it stands, after its backoff (its key's
// next messages wait), any other one by queueing it again after its
// backoff.
func (d *diskBuffer) doneFor(ctx context.Context, m queuedMsg) doneFunc {
	return func(rep *Report, ordered bool) verdict {
		if rep.Status != envelope.Failed.String() {
			d.deletes <- m.seq
			return final
		}
		d.mu.Lock()
		if !d.retry || d.stopped {
			d.mu.Unlock()
			return left // stays in the log for the next start
		}
		d.tries[m.seq]++
		wait := backoff.Delay(diskRetryBase, diskRetryMax, "equal", d.tries[m.seq], d.e.rand)
		d.e.log.Warn("buffer: message failed; retrying it from the buffer", "id", m.env.ID, "in", wait.String())
		if ordered {
			d.mu.Unlock()
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-t.C:
				return again
			case <-d.quit:
				return left
			}
		}
		defer d.mu.Unlock()
		time.AfterFunc(wait, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.stopped {
				return
			}
			d.ready = append(d.ready, m)
			d.cond.Broadcast()
		})
		return final
	}
}

// waitIdle waits until nothing waits for dispatch, or ctx ends (finite
// sources: the run ends once every queued message was handed to a worker).
func (d *diskBuffer) waitIdle(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		d.mu.Lock()
		for len(d.ready) > 0 && !d.stopped {
			d.cond.Wait()
		}
		d.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		d.stop()
		<-done
	}
}

// stop ends dispatching; what is still queued stays in the log.
func (d *diskBuffer) stop() {
	d.mu.Lock()
	if !d.stopped {
		d.stopped = true
		close(d.quit)
	}
	d.cond.Broadcast()
	d.mu.Unlock()
}

// nackPending tells the sources of the messages still queued that this run
// will not deliver them (they stay in the log for the next start).
func (d *diskBuffer) nackPending(ctx context.Context) int {
	d.mu.Lock()
	left := d.ready
	d.ready = nil
	d.mu.Unlock()
	for _, m := range left {
		m.env.Ack(ctx, envelope.Outcome{Status: envelope.Failed, Err: errShutdown})
	}
	return len(left)
}

var errShutdown = errors.New("the relay shut down before delivering the message; it stays in the disk buffer for the next start")
