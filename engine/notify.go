package engine

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// NotifySource is the source name and protocol of an alert's envelope, and
// NotifyEventAttribute the attribute that holds its event.
const (
	NotifySource         = "notify"
	NotifyEventAttribute = "notify.event"
)

// Alert is the JSON body of a notify rule's message.
type Alert struct {
	Event       string    `json:"event"`
	Relay       string    `json:"relay"`
	At          time.Time `json:"at"`
	Destination string    `json:"destination,omitempty"`
	// Endpoint is the endpoint whose circuit changed: the destination, or
	// one member of a group.
	Endpoint  string `json:"endpoint,omitempty"`
	Route     string `json:"route,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Error     string `json:"error,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	// Count is how many events the alert stands for: 1, or everything
	// that happened since Since while the rule's every held alerts back.
	// The other fields are then those of the last event.
	Count int        `json:"count"`
	Since *time.Time `json:"since,omitempty"`
}

// subject is what an alert is about: alerts are held back per subject.
func (a Alert) subject() string {
	switch {
	case a.Endpoint != "":
		return "endpoint/" + a.Endpoint
	case a.Destination != "":
		return "destination/" + a.Destination
	default:
		return "route/" + a.Route
	}
}

type notifyRule struct {
	events []string
	to     []string
	every  time.Duration
}

// alertWindow is the time after an alert in which the same event on the
// same subject is counted, not sent.
type alertWindow struct {
	timer   *time.Timer
	pending *Alert
}

type alertJob struct {
	to    []string
	alert Alert
}

// notifier sends the alerts of notify rules. It only works while the relay
// runs (Run starts it), so one-shot commands such as hikyaku send or dlq
// replay alert nobody. emit never blocks: it is called from the circuit
// breaker, under its lock. An alert that cannot be delivered is logged and
// counted, never dead-lettered, so an alert cannot cause another one.
type notifier struct {
	e     *Engine
	rules []notifyRule
	queue chan alertJob
	done  chan struct{}

	mu      sync.Mutex
	running bool
	windows map[windowKey]*alertWindow
}

// windowKey names one window: a rule, an event and what it is about.
type windowKey struct {
	rule    int
	event   string
	subject string
}

const notifyQueueSize = 256

func newNotifier(e *Engine, specs []config.Notify) *notifier {
	if len(specs) == 0 {
		return nil
	}
	n := &notifier{e: e, queue: make(chan alertJob, notifyQueueSize), done: make(chan struct{}), windows: map[windowKey]*alertWindow{}}
	for _, s := range specs {
		r := notifyRule{events: s.On, to: s.To, every: s.Every.Std()}
		if len(r.events) == 0 {
			r.events = []string{config.NotifyDeadLetter, config.NotifyCircuitOpen}
		}
		if r.every <= 0 {
			r.every = config.DefaultNotifyEvery
		}
		n.rules = append(n.rules, r)
	}
	return n
}

func (n *notifier) start() {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.running = true
	n.mu.Unlock()
	go n.work()
}

// stop sends what the windows still hold, then waits for the queue to
// empty (up to 10s). It is safe to call twice, or without start.
func (n *notifier) stop() {
	if n == nil {
		return
	}
	n.mu.Lock()
	if !n.running {
		n.mu.Unlock()
		return
	}
	n.running = false
	for key, w := range n.windows {
		w.timer.Stop()
		if w.pending != nil {
			n.enqueue(n.rules[key.rule], *w.pending)
		}
	}
	n.windows = map[windowKey]*alertWindow{}
	close(n.queue)
	n.mu.Unlock()
	select {
	case <-n.done:
	case <-time.After(10 * time.Second):
		n.e.log.Warn("alerts still being sent at shutdown were abandoned")
	}
}

// emit hands an event to every rule that listens for it.
func (n *notifier) emit(a Alert) {
	if n == nil {
		return
	}
	a.Relay, a.At, a.Count = n.e.spec.Name, n.e.now().UTC(), 1
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return
	}
	for i, r := range n.rules {
		if !slices.Contains(r.events, a.Event) {
			continue
		}
		key := windowKey{rule: i, event: a.Event, subject: a.subject()}
		w := n.windows[key]
		if w == nil {
			w = &alertWindow{}
			w.timer = time.AfterFunc(r.every, func() { n.windowOver(key) })
			n.windows[key] = w
			n.enqueue(r, a)
			continue
		}
		if w.pending == nil {
			first := a.At
			held := a
			held.Since = &first
			w.pending = &held
			continue
		}
		held := a
		held.Count, held.Since = w.pending.Count+1, w.pending.Since
		w.pending = &held
	}
}

// windowOver runs when a rule's every has passed since an alert. A quiet
// window ends there. One that held events back sends one alert for them
// and starts the next window, so a failure that lasts alerts once per
// every.
func (n *notifier) windowOver(key windowKey) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.windows[key]
	if w == nil || !n.running {
		return
	}
	if w.pending == nil {
		delete(n.windows, key)
		return
	}
	a := *w.pending
	w.pending = nil
	r := n.rules[key.rule]
	w.timer = time.AfterFunc(r.every, func() { n.windowOver(key) })
	n.enqueue(r, a)
}

// enqueue queues an alert for a rule's destinations, without the one the
// alert is about: an alert about a destination's failures would fail there
// too. The caller holds n.mu.
func (n *notifier) enqueue(r notifyRule, a Alert) {
	to := slices.DeleteFunc(slices.Clone(r.to), func(d string) bool { return d == a.Destination })
	if len(to) == 0 {
		return
	}
	select {
	case n.queue <- alertJob{to: to, alert: a}:
	default:
		n.count(a.Event, "dropped")
		n.e.log.Warn("alert dropped: too many alerts are waiting", "event", a.Event, "destination", a.Destination, "route", a.Route)
	}
}

func (n *notifier) count(event, outcome string) {
	if n.e.m != nil {
		n.e.m.Notifications.WithLabelValues(event, outcome).Inc()
	}
}

func (n *notifier) work() {
	defer close(n.done)
	for job := range n.queue {
		body, err := json.Marshal(job.alert)
		if err != nil {
			continue
		}
		for _, dest := range job.to {
			env := envelope.New(NotifySource, NotifySource, body)
			env.ContentType = "application/json"
			env.Subject = job.alert.Event
			env.SetAttribute(NotifyEventAttribute, job.alert.Event)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			var rep DeliveryReport
			if b := n.e.batchers[dest]; b != nil {
				rep = b.now(ctx, env)
			} else {
				rep = n.e.deliver(ctx, dest, env)
			}
			cancel()
			if rep.OK() {
				n.count(job.alert.Event, "sent")
				continue
			}
			n.count(job.alert.Event, "failed")
			n.e.log.Warn("alert not delivered", "event", job.alert.Event, "to", dest, "err", rep.Error)
		}
	}
}
