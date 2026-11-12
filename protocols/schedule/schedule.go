// Package schedule is hikyaku's schedule source: it emits a message when a
// cron expression matches, or every interval. With a route, a transform and
// a destination, that is a scheduled job: "every weekday at eight, fetch
// this and post it there".
package schedule

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/cron"
	"github.com/xavidop/hikyaku/plugin"
)

// Kind is the source type.
const Kind = "schedule"

// The attributes of a scheduled message.
const (
	// TimeAttribute is the time the message was scheduled for (RFC 3339, in
	// the schedule's time zone), which is not always when it was sent.
	TimeAttribute = "schedule.time"
	// CronAttribute and IntervalAttribute say which schedule made it.
	CronAttribute     = "schedule.cron"
	IntervalAttribute = "schedule.interval"
)

// DefaultBody is the body of a scheduled message when the source sets none.
const DefaultBody = "{}"

// maxSleep bounds one sleep. A timer counts on the monotonic clock, which
// stops while a machine is suspended and ignores a corrected wall clock, so
// a long sleep could wake late. The source looks at the clock again at
// least this often.
const maxSleep = time.Minute

// Register makes the schedule source available.
func Register() {
	plugin.RegisterSource(Kind, func(_ context.Context, s plugin.Spec) (plugin.Source, error) { return newSource(s) })
}

// Source emits one message per tick.
type Source struct {
	name     string
	sched    *cron.Schedule // nil with an interval
	expr     string
	every    time.Duration
	loc      *time.Location
	body     []byte
	ctype    string
	headers  map[string]string
	now      func() time.Time
	sleep    func(context.Context, time.Duration) bool // false when ctx ended
	mu       sync.Mutex
	nextTick time.Time
}

func newSource(s plugin.Spec) (*Source, error) {
	cfg, ok := s.Config.(*config.Source)
	if !ok || cfg == nil {
		return nil, fmt.Errorf("source %s: missing config", s.Name)
	}
	src := &Source{name: s.Name, loc: time.UTC, headers: cfg.Headers, now: time.Now, sleep: sleep}
	switch {
	case cfg.Cron != "" && cfg.Interval != 0:
		return nil, fmt.Errorf("source %s: set cron or interval, not both", s.Name)
	case cfg.Cron != "":
		sched, err := cron.Parse(cfg.Cron)
		if err != nil {
			return nil, fmt.Errorf("source %s: cron: %w", s.Name, err)
		}
		src.sched, src.expr = sched, cfg.Cron
	case cfg.Interval.Std() >= config.MinScheduleInterval:
		src.every = cfg.Interval.Std()
	case cfg.Interval != 0:
		return nil, fmt.Errorf("source %s: interval must be at least %s", s.Name, config.MinScheduleInterval)
	default:
		return nil, fmt.Errorf("source %s: a schedule source needs cron or interval", s.Name)
	}
	if cfg.Timezone != "" {
		loc, err := time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, fmt.Errorf("source %s: timezone %q is not a time zone name", s.Name, cfg.Timezone)
		}
		src.loc = loc
	}
	src.body = []byte(cfg.Body)
	if cfg.Body == "" {
		src.body = []byte(DefaultBody)
	}
	src.ctype = "text/plain; charset=utf-8"
	if json.Valid(src.body) {
		src.ctype = "application/json"
	}
	return src, nil
}

// Name is the source name.
func (s *Source) Name() string { return s.name }

// Health is always nil: a schedule depends on nothing.
func (s *Source) Health(context.Context) error { return nil }

// Next is the next time a message is due (zero before Start).
func (s *Source) Next() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextTick
}

// after is the first tick after t.
func (s *Source) after(t time.Time) time.Time {
	if s.sched != nil {
		return s.sched.Next(t.In(s.loc))
	}
	return t.Add(s.every)
}

// Start emits a message at each tick until ctx ends. A tick that passes
// while the relay is down, or while it cannot take the message, is not made
// up for: the next message is the next tick from now.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	next := s.after(s.now())
	for {
		if next.IsZero() {
			return fmt.Errorf("source %s: the schedule has no next time", s.name)
		}
		s.mu.Lock()
		s.nextTick = next
		s.mu.Unlock()
		for {
			d := next.Sub(s.now())
			if d <= 0 {
				break
			}
			if !s.sleep(ctx, min(d, maxSleep)) {
				return nil
			}
		}
		select {
		case out <- s.message(next):
		case <-ctx.Done():
			return nil
		}
		// From now when the relay held the message past the next tick.
		from := next
		if now := s.now(); s.after(from).Before(now) {
			from = now
		}
		next = s.after(from)
	}
}

// message is the message of the tick at t. Its ID is the source name and
// the tick: two replicas with the same cron schedule emit the same ID, so
// an idempotent destination takes the message once.
func (s *Source) message(t time.Time) *envelope.Envelope {
	env := envelope.New(s.name, Kind, append([]byte(nil), s.body...))
	env.ID = s.name + "@" + t.UTC().Format(time.RFC3339)
	env.ContentType = s.ctype
	for k, v := range s.headers {
		env.SetHeader(k, v)
	}
	env.Attributes[TimeAttribute] = t.In(s.loc).Format(time.RFC3339)
	if s.sched != nil {
		env.Attributes[CronAttribute] = s.expr
	} else {
		env.Attributes[IntervalAttribute] = s.every.String()
	}
	return env
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
