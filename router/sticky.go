package router

import (
	"context"
	"fmt"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// StickyTarget returns where the newest call of a history (oldest first)
// went, when it was delivered: the correction recorded on it, else its
// chosen candidates, else the destinations that took it.
func StickyTarget(history []store.CallRecord) ([]string, bool) {
	if len(history) == 0 {
		return nil, false
	}
	last := history[len(history)-1]
	if last.Feedback != nil && len(last.Feedback.Correct) > 0 {
		return append([]string(nil), last.Feedback.Correct...), true
	}
	if !last.Delivered() {
		return nil, false
	}
	if c := last.Chosen(); len(c) > 0 {
		return c, true
	}
	if len(last.DeliveredTo) > 0 {
		return append([]string(nil), last.DeliveredTo...), true
	}
	return nil, false
}

// WindowOf converts a spec window.
func WindowOf(w config.Window) store.Window {
	return store.Window{MaxEntries: w.MaxEntries, MaxAge: w.MaxAge.Std()}
}

type sticky struct {
	route  string
	hist   store.HistoryStore
	key    *transform.Template
	window store.Window
}

// NewSticky routes to the last destinations of the message's history key,
// with no model call: sticky.mode always_last, or a router fallback that
// works when every model is down.
func NewSticky(route string, hist store.HistoryStore, h config.History) (plugin.Router, error) {
	key, err := transform.CompileTemplate("history.key", h.Key)
	if err != nil {
		return nil, err
	}
	return &sticky{route: route, hist: hist, key: key, window: WindowOf(h.Window)}, nil
}

func (s *sticky) Route(ctx context.Context, env *envelope.Envelope) (plugin.Decision, error) {
	key, err := s.key.Render(env)
	if err != nil {
		return plugin.Decision{Router: "sticky"}, err
	}
	if key == "" {
		return plugin.Decision{Router: "sticky"}, fmt.Errorf("%w (the history key rendered empty)", ErrNoHistory)
	}
	recs, err := s.hist.Recent(ctx, s.route, key, s.window)
	if err != nil {
		return plugin.Decision{Router: "sticky"}, fmt.Errorf("history: %w", err)
	}
	to, ok := StickyTarget(recs)
	if !ok {
		return plugin.Decision{Router: "sticky"}, ErrNoHistory
	}
	return plugin.Decision{Destinations: to, Router: "sticky", Reason: "the last call of " + key + " went there"}, nil
}
