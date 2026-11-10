package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// Dedupe (a source's dedupe:). Before routing, the engine drops a message
// whose key it saw within the ttl, for every source that does not dedupe
// itself (plugin.Deduper: the http source answers a duplicate on its own).
// A duplicate is acked as filtered, so a broker forgets it, and counted in
// hikyaku_deduped_total{source}. Dedupe never drops by accident: a key
// that renders empty, or a store that is down, lets the message through,
// the same message coming back (a retry from the disk buffer, a replay)
// is not a duplicate of itself, and the key of a message that failed is
// forgotten, so its redelivery is not taken for one.

type deduper struct {
	src   string
	key   *transform.Template
	ttl   time.Duration
	store func() (store.Store, error)
	log   func(msg string, args ...any)
}

func (e *Engine) buildDedupe() error {
	e.dedupers = map[string]*deduper{}
	self := map[string]bool{}
	for i, s := range e.sources {
		if d, ok := s.(plugin.Deduper); ok && d.Dedupes() {
			self[e.spec.Sources[i].Name] = true
		}
	}
	for _, src := range e.spec.Sources {
		if src.Dedupe == nil || self[src.Name] || len(e.sources) == 0 {
			continue
		}
		key, err := transform.CompileTemplate(fmt.Sprintf("source %q.dedupe.key", src.Name), src.Dedupe.Key)
		if err != nil {
			return err
		}
		ttl := src.Dedupe.TTL.Std()
		if ttl <= 0 {
			ttl = config.DefaultDedupeTTL
		}
		name := src.Dedupe.Store
		e.dedupers[src.Name] = &deduper{src: src.Name, key: key, ttl: ttl,
			store: func() (store.Store, error) { return e.Store(name) },
			log:   e.log.Warn}
	}
	return nil
}

// check reports whether env is a duplicate; key is the stored key ("" when
// env has none).
func (d *deduper) check(ctx context.Context, env *envelope.Envelope) (key string, dup bool) {
	k, err := d.key.Render(env)
	if err != nil || k == "" {
		return "", false
	}
	st, err := d.store()
	if err != nil || st == nil {
		return "", false
	}
	key = "dedupe:" + d.src + ":" + k
	fresh, err := st.SetNX(ctx, key, []byte(env.ID), d.ttl)
	if err != nil {
		d.log("dedupe store failed; letting the message through", "source", d.src, "id", env.ID, "err", err)
		return "", false
	}
	if fresh {
		return key, false
	}
	// The same message again (a retry, a replay) is not a duplicate.
	if id, ok, err := st.Get(ctx, key); err == nil && ok && string(id) == env.ID {
		return key, false
	}
	return "", true
}

// forget removes the key of a message that failed, when the store can.
func (d *deduper) forget(ctx context.Context, key string) {
	if key == "" {
		return
	}
	st, err := d.store()
	if err != nil || st == nil {
		return
	}
	if del, ok := st.(store.Deleter); ok {
		if err := del.Delete(context.WithoutCancel(ctx), key); err != nil {
			d.log("dedupe: could not forget the key of a failed message; its redelivery may be dropped as a duplicate", "source", d.src, "err", err)
		}
	}
}

// countRefused counts a message a source refused on a full buffer.
func (e *Engine) countRefused(source string) {
	if e.m != nil {
		e.m.Rejected.WithLabelValues(source).Inc()
	}
}

// countDeduped counts a dropped duplicate.
func (e *Engine) countDeduped(source string) {
	if e.m != nil {
		e.m.Deduped.WithLabelValues(source).Inc()
	}
}
