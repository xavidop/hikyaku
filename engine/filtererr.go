package engine

import (
	"sync/atomic"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/transform"
)

// Where a filter that could not be evaluated sits
// (hikyaku_filter_errors_total{scope}).
const (
	scopeRoute       = "route"
	scopeDestination = "destination"
)

// filterWarnEvery is how often one route or destination warns about a
// filter it cannot evaluate. Every message is counted.
const filterWarnEvery = time.Minute

// applyChain runs a chain of transforms on env. filterErr is the error of a
// filter step of this chain that could not be evaluated: the step kept the
// message. An error an earlier chain left on the message is not reported
// again.
func applyChain(c transform.Chain, env *envelope.Envelope) (keep bool, filterErr string, err error) {
	prev, had := env.Attributes[transform.FilterErrorAttr]
	delete(env.Attributes, transform.FilterErrorAttr)
	keep, err = c.Apply(env)
	if v, ok := env.Attributes[transform.FilterErrorAttr].(string); ok {
		return keep, v, err
	}
	if had {
		env.SetAttribute(transform.FilterErrorAttr, prev)
	}
	return keep, "", err
}

// filterFailed counts a filter that could not be evaluated for a message,
// and warns about it: the message went on as if the filter were not there.
func (e *Engine) filterFailed(scope, name string, env *envelope.Envelope, msg string) {
	if e.m != nil {
		e.m.FilterErrors.WithLabelValues(scope, name).Inc()
	}
	last, _ := e.filterWarned.LoadOrStore(scope+"/"+name, new(atomic.Int64))
	at, now := last.(*atomic.Int64), e.now().UnixNano()
	if prev := at.Load(); (prev == 0 || now-prev >= int64(filterWarnEvery)) && at.CompareAndSwap(prev, now) {
		e.log.Warn("a filter could not be evaluated; the message was kept", scope, name, "id", env.ID, "err", msg,
			"note", "counted in hikyaku_filter_errors_total; this warning comes at most once a minute")
	}
}
