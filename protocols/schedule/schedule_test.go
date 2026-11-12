package schedule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func init() { Register() }

// clock is a clock a test moves: sleeping advances it at once.
type clock struct {
	mu     sync.Mutex
	t      time.Time
	sleeps []time.Duration
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) sleep(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	c.sleeps = append(c.sleeps, d)
	return true
}

func (c *clock) longest() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var m time.Duration
	for _, d := range c.sleeps {
		m = max(m, d)
	}
	return m
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func source(t *testing.T, cfg config.Source, start string) (*Source, *clock) {
	t.Helper()
	cfg.Name, cfg.Type = "tick", Kind
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "tick", Kind: Kind, Config: &cfg})
	require.NoError(t, err)
	at, err := time.Parse(time.RFC3339, start)
	require.NoError(t, err)
	c := &clock{t: at}
	src := s.(*Source)
	src.now, src.sleep = c.now, c.sleep
	return src, c
}

// run starts the source and returns its messages.
func run(t *testing.T, s *Source) <-chan *envelope.Envelope {
	t.Helper()
	out := make(chan *envelope.Envelope)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("the source did not stop")
		}
	})
	return out
}

func next(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func TestCron(t *testing.T) {
	s, c := source(t, config.Source{Cron: "0 8 * * 1-5", Timezone: "Europe/Madrid", Body: `{"report":"daily"}`, Headers: map[string]string{"X-Job": "report"}}, "2026-11-13T10:00:30Z") // a Friday
	out := run(t, s)

	env := next(t, out)
	assert.Equal(t, "tick@2026-11-16T07:00:00Z", env.ID) // Monday 08:00 in Madrid
	assert.Equal(t, "tick", env.Source)
	assert.Equal(t, Kind, env.Protocol)
	assert.JSONEq(t, `{"report":"daily"}`, string(env.Body))
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, "report", env.Header("X-Job"))
	assert.Equal(t, "2026-11-16T08:00:00+01:00", env.Attributes[TimeAttribute])
	assert.Equal(t, "0 8 * * 1-5", env.Attributes[CronAttribute])
	assert.LessOrEqual(t, c.longest(), maxSleep, "it looks at the clock at least every minute")

	assert.Equal(t, "tick@2026-11-17T07:00:00Z", next(t, out).ID)
	assert.Equal(t, "tick@2026-11-18T07:00:00Z", next(t, out).ID)
}

func TestInterval(t *testing.T) {
	s, _ := source(t, config.Source{Interval: config.Duration(90 * time.Second)}, "2026-11-12T10:00:00Z")
	out := run(t, s)
	env := next(t, out)
	assert.Equal(t, "tick@2026-11-12T10:01:30Z", env.ID)
	assert.Equal(t, DefaultBody, string(env.Body))
	assert.Equal(t, "1m30s", env.Attributes[IntervalAttribute])
	assert.Equal(t, "2026-11-12T10:01:30Z", env.Attributes[TimeAttribute])
	assert.Equal(t, "tick@2026-11-12T10:03:00Z", next(t, out).ID)
}

func TestMissedTicksAreNotMadeUpFor(t *testing.T) {
	s, c := source(t, config.Source{Cron: "* * * * *"}, "2026-11-12T10:00:00Z")
	out := run(t, s)
	assert.Equal(t, "tick@2026-11-12T10:01:00Z", next(t, out).ID)
	// The relay takes no message for an hour: the source waits to hand over
	// the 10:02 one.
	require.Eventually(t, func() bool { return s.Next().Equal(time.Date(2026, 11, 12, 10, 2, 0, 0, time.UTC)) }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !c.now().Before(time.Date(2026, 11, 12, 10, 2, 0, 0, time.UTC)) }, 5*time.Second, time.Millisecond)
	c.add(time.Hour)
	assert.Equal(t, "tick@2026-11-12T10:02:00Z", next(t, out).ID)
	// Not 10:03, 10:04, …: the next tick from now.
	assert.Equal(t, "tick@2026-11-12T11:03:00Z", next(t, out).ID)
}

func TestTextBody(t *testing.T) {
	s, _ := source(t, config.Source{Interval: config.Duration(time.Second), Body: "wake up"}, "2026-11-12T10:00:00Z")
	env := next(t, run(t, s))
	assert.Equal(t, "wake up", string(env.Body))
	assert.Equal(t, "text/plain; charset=utf-8", env.ContentType)
}

func TestRealClock(t *testing.T) {
	cfg := config.Source{Name: "tick", Type: Kind, Interval: config.Duration(time.Second)}
	s, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "tick", Kind: Kind, Config: &cfg})
	require.NoError(t, err)
	src := s.(*Source)
	src.every = 20 * time.Millisecond
	require.NoError(t, src.Health(context.Background()))
	out := run(t, src)
	start := time.Now()
	next(t, out)
	next(t, out)
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
}

func TestSettings(t *testing.T) {
	for want, cfg := range map[string]config.Source{
		"needs cron or interval":           {},
		"set cron or interval, not both":   {Cron: "* * * * *", Interval: config.Duration(time.Minute)},
		"cron: a cron expression has 5":    {Cron: "* * *"},
		"interval must be at least 1s":     {Interval: config.Duration(time.Millisecond)},
		`timezone "Mars/Olympus" is not a`: {Cron: "* * * * *", Timezone: "Mars/Olympus"},
	} {
		_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "tick", Kind: Kind, Config: &cfg})
		assert.ErrorContains(t, err, want)
	}
	_, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "tick", Kind: Kind})
	assert.ErrorContains(t, err, "missing config")
}
