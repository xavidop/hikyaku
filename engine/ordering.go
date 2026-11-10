package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/transform"
)

// Ordering (source ordering:). Messages with the same ordering key are
// processed one at a time, in the order the source produced them; messages
// with other keys, or none, proceed in parallel. A key's worker holds one
// worker slot and drains its queue before letting the key go, so a slow or
// retrying message holds back the messages behind it (head-of-line, which
// is the point), never the other keys. With a disk buffer, a message that
// failed is retried where it stands; when it is left in the buffer for the
// next start, so is the rest of its key (blocked), to keep their order.

type orderer struct {
	e     *Engine
	keyOf map[string]*transform.Template // source name → key template (nil: natural key)

	mu      sync.Mutex
	cond    *sync.Cond
	queues  map[string]*keyQueue
	blocked map[string]bool // keys left in the disk buffer for the next start
	waiting int             // messages queued behind a running key
	limit   int
}

type keyQueue struct{ jobs []job }

type job struct {
	ctx  context.Context
	env  *envelope.Envelope
	done doneFunc
}

// errKeyLeft nacks the messages of a key whose earlier message stays in the
// disk buffer: they stay too, and the next start delivers them in order.
var errKeyLeft = errors.New("an earlier message with the same ordering key stays in the disk buffer; this one waits for it (the next start delivers both, in order)")

func newOrderer(e *Engine) (*orderer, error) {
	o := &orderer{e: e, keyOf: map[string]*transform.Template{}, queues: map[string]*keyQueue{}, blocked: map[string]bool{}, limit: max(e.spec.Buffer.Size, 1)}
	o.cond = sync.NewCond(&o.mu)
	for _, s := range e.spec.Sources {
		if s.Ordering == nil {
			continue
		}
		var tpl *transform.Template
		if strings.TrimSpace(s.Ordering.Key) != "" {
			t, err := transform.CompileTemplate(fmt.Sprintf("source %q.ordering.key", s.Name), s.Ordering.Key)
			if err != nil {
				return nil, err
			}
			tpl = t
		}
		o.keyOf[s.Name] = tpl
	}
	if len(o.keyOf) == 0 {
		return nil, nil
	}
	return o, nil
}

// key is env's ordering key, "" for an unordered message.
func (o *orderer) key(env *envelope.Envelope) string {
	tpl, ok := o.keyOf[env.Source]
	if !ok {
		return ""
	}
	if tpl == nil {
		if v, ok := env.Attribute("ordering_key"); ok {
			return fmt.Sprint(v)
		}
		return ""
	}
	k, err := tpl.Render(env)
	if err != nil {
		o.e.log.Warn("ordering key failed; delivering unordered", "source", env.Source, "id", env.ID, "err", err)
		return ""
	}
	return k
}

// submit runs env after the messages queued before it under the same key.
// It reports false for an unordered message, which the caller dispatches.
// It blocks while buffer.size messages already wait behind running keys.
func (o *orderer) submit(ctx context.Context, env *envelope.Envelope, done doneFunc) bool {
	k := o.key(env)
	if k == "" {
		return false
	}
	k = env.Source + "\x00" + k
	o.mu.Lock()
	if o.blocked[k] {
		o.mu.Unlock()
		env.Ack(ctx, envelope.Outcome{Status: envelope.Failed, Err: errKeyLeft})
		return true
	}
	if _, ok := o.queues[k]; ok {
		for o.waiting >= o.limit {
			o.cond.Wait()
		}
		if q, ok := o.queues[k]; ok { // the key may have finished meanwhile
			q.jobs = append(q.jobs, job{ctx, env, done})
			o.waiting++
			o.mu.Unlock()
			return true
		}
	}
	q := &keyQueue{}
	o.queues[k] = q
	o.mu.Unlock()
	o.e.sem <- struct{}{}
	o.e.inflight.Add(1)
	go func() {
		defer func() { <-o.e.sem; o.e.inflight.Done() }()
		j := job{ctx, env, done}
		for {
			if o.e.run(withSlot(j.ctx), j.env, j.done, true) == left {
				o.leave(k, q)
				return
			}
			o.mu.Lock()
			if len(q.jobs) == 0 {
				delete(o.queues, k)
				o.mu.Unlock()
				return
			}
			j = q.jobs[0]
			q.jobs[0] = job{}
			q.jobs = q.jobs[1:]
			o.waiting--
			o.cond.Broadcast()
			o.mu.Unlock()
		}
	}()
	return true
}

// leave blocks key k for the rest of the run once one of its messages
// stays in the disk buffer: the messages queued behind it are nacked and
// stay there too, so the next start delivers them after it, in order.
func (o *orderer) leave(k string, q *keyQueue) {
	o.mu.Lock()
	jobs := q.jobs
	q.jobs = nil
	o.waiting -= len(jobs)
	o.blocked[k] = true
	delete(o.queues, k)
	o.cond.Broadcast()
	o.mu.Unlock()
	for _, j := range jobs {
		j.env.Ack(j.ctx, envelope.Outcome{Status: envelope.Failed, Err: errKeyLeft})
	}
}
