// Package storetest is the conformance suite every store.Store runs: the
// memory store in the root module, the Redis store in stores/redis.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/store"
)

// Harness builds a fresh store for one test and moves its clock.
type Harness struct {
	New func(t *testing.T) store.Store
	// Advance moves the store's clock (and any server-side TTLs) forward.
	Advance func(d time.Duration)
	// Now is the store's current time.
	Now func() time.Time
}

func rec(id, route, key string, at time.Time, chosen ...string) store.CallRecord {
	r := store.CallRecord{MsgID: id, At: at, Route: route, Key: key, State: "state of " + id, Model: "m", Outcome: store.OutcomeDelivered}
	for _, c := range chosen {
		r.Decision = append(r.Decision, store.Answer{Candidate: c, Probability: 0.9, Chosen: true})
		r.DeliveredTo = append(r.DeliveredTo, c)
	}
	return r
}

// Run runs the suite.
func Run(t *testing.T, h Harness) {
	ctx := context.Background()
	opts := store.AppendOptions{TTL: time.Hour, Keep: 20}

	t.Run("AppendRecentOrderAndWindow", func(t *testing.T) {
		s := h.New(t)
		now := h.Now()
		for i := 0; i < 5; i++ {
			require.NoError(t, s.Append(ctx, "triage", "cust-1", rec(fmt.Sprintf("m%d", i), "triage", "cust-1", now.Add(time.Duration(i)*time.Second), "billing"), opts))
		}
		got, err := s.Recent(ctx, "triage", "cust-1", store.Window{MaxEntries: 3})
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, []string{"m2", "m3", "m4"}, ids(got), "newest last")
		assert.Equal(t, []string{"billing"}, got[0].Chosen())

		other, err := s.Recent(ctx, "triage", "cust-2", store.Window{})
		require.NoError(t, err)
		assert.Empty(t, other)
		otherRoute, err := s.Recent(ctx, "other", "cust-1", store.Window{})
		require.NoError(t, err)
		assert.Empty(t, otherRoute, "history is per route")
	})

	t.Run("MaxAge", func(t *testing.T) {
		s := h.New(t)
		now := h.Now()
		require.NoError(t, s.Append(ctx, "r", "k", rec("old", "r", "k", now.Add(-2*time.Hour)), opts))
		require.NoError(t, s.Append(ctx, "r", "k", rec("new", "r", "k", now), opts))
		got, err := s.Recent(ctx, "r", "k", store.Window{MaxAge: time.Hour})
		require.NoError(t, err)
		assert.Equal(t, []string{"new"}, ids(got))
	})

	t.Run("KeepTrims", func(t *testing.T) {
		s := h.New(t)
		now := h.Now()
		for i := 0; i < 6; i++ {
			require.NoError(t, s.Append(ctx, "r", "k", rec(fmt.Sprintf("m%d", i), "r", "k", now), store.AppendOptions{TTL: time.Hour, Keep: 4}))
		}
		got, err := s.Recent(ctx, "r", "k", store.Window{MaxEntries: 100})
		require.NoError(t, err)
		assert.Equal(t, []string{"m2", "m3", "m4", "m5"}, ids(got))
		_, err = s.Feedback(ctx, "m0", store.Feedback{Correct: []string{"x"}})
		assert.ErrorIs(t, err, store.ErrNotFound, "a trimmed record is gone")
	})

	t.Run("TTLRequiredAndExpires", func(t *testing.T) {
		s := h.New(t)
		assert.ErrorIs(t, s.Append(ctx, "r", "k", rec("m", "r", "k", h.Now()), store.AppendOptions{}), store.ErrNoTTL)
		require.NoError(t, s.Append(ctx, "r", "k", rec("m", "r", "k", h.Now()), store.AppendOptions{TTL: time.Minute}))
		h.Advance(2 * time.Minute)
		got, err := s.Recent(ctx, "r", "k", store.Window{})
		require.NoError(t, err)
		assert.Empty(t, got)
		_, err = s.Feedback(ctx, "m", store.Feedback{Correct: []string{"x"}})
		assert.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("FeedbackByMsgID", func(t *testing.T) {
		s := h.New(t)
		now := h.Now()
		require.NoError(t, s.Append(ctx, "triage", "cust-1", rec("a", "triage", "cust-1", now, "support"), opts))
		require.NoError(t, s.Append(ctx, "triage", "cust-1", rec("b", "triage", "cust-1", now, "support"), opts))
		updated, err := s.Feedback(ctx, "a", store.Feedback{Correct: []string{"billing"}, Note: "it was an invoice", Source: "api"})
		require.NoError(t, err)
		assert.Equal(t, "triage", updated.Route)
		require.NotNil(t, updated.Feedback)
		assert.Equal(t, []string{"billing"}, updated.Feedback.Correct)

		got, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.NotNil(t, got[0].Feedback)
		assert.Equal(t, "it was an invoice", got[0].Feedback.Note)
		assert.False(t, got[0].Feedback.At.IsZero())
		assert.Nil(t, got[1].Feedback)

		_, err = s.Feedback(ctx, "nope", store.Feedback{Correct: []string{"x"}})
		assert.ErrorIs(t, err, store.ErrNotFound)
	})

	t.Run("Forget", func(t *testing.T) {
		s := h.New(t)
		require.NoError(t, s.Append(ctx, "r", "k", rec("m", "r", "k", h.Now()), opts))
		require.NoError(t, s.Append(ctx, "r", "keep", rec("m2", "r", "keep", h.Now()), opts))
		require.NoError(t, s.Forget(ctx, "r", "k"))
		got, err := s.Recent(ctx, "r", "k", store.Window{})
		require.NoError(t, err)
		assert.Empty(t, got)
		_, err = s.Feedback(ctx, "m", store.Feedback{Correct: []string{"x"}})
		assert.ErrorIs(t, err, store.ErrNotFound)
		kept, err := s.Recent(ctx, "r", "keep", store.Window{})
		require.NoError(t, err)
		assert.Len(t, kept, 1)
		assert.NoError(t, s.Forget(ctx, "r", "never-existed"))
	})

	t.Run("Records", func(t *testing.T) {
		s := h.New(t)
		now := h.Now()
		require.NoError(t, s.Append(ctx, "a", "k1", rec("old", "a", "k1", now.Add(-48*time.Hour)), opts))
		require.NoError(t, s.Append(ctx, "a", "k1", rec("a1", "a", "k1", now), opts))
		require.NoError(t, s.Append(ctx, "a", "k2", rec("a2", "a", "k2", now), opts))
		require.NoError(t, s.Append(ctx, "b", "k1", rec("b1", "b", "k1", now), opts))
		var got []string
		require.NoError(t, s.Records(ctx, "a", now.Add(-time.Hour), func(r store.CallRecord) error {
			got = append(got, r.MsgID)
			return nil
		}))
		assert.ElementsMatch(t, []string{"a1", "a2"}, got)
		got = nil
		require.NoError(t, s.Records(ctx, "", time.Time{}, func(r store.CallRecord) error {
			got = append(got, r.MsgID)
			return nil
		}))
		assert.ElementsMatch(t, []string{"old", "a1", "a2", "b1"}, got)
		stop := errors.New("stop")
		assert.ErrorIs(t, s.Records(ctx, "", time.Time{}, func(store.CallRecord) error { return stop }), stop)
	})

	t.Run("KV", func(t *testing.T) {
		s := h.New(t)
		_, ok, err := s.Get(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok)
		set, err := s.SetNX(ctx, "k", []byte("v1"), time.Minute)
		require.NoError(t, err)
		assert.True(t, set)
		set, err = s.SetNX(ctx, "k", []byte("v2"), time.Minute)
		require.NoError(t, err)
		assert.False(t, set, "SetNX does not overwrite")
		v, ok, err := s.Get(ctx, "k")
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "v1", string(v))
		if del, isDeleter := s.(store.Deleter); isDeleter {
			require.NoError(t, del.Delete(ctx, "k"))
			_, ok, err = s.Get(ctx, "k")
			require.NoError(t, err)
			assert.False(t, ok, "Delete removes the key")
			set, err = s.SetNX(ctx, "k", []byte("v3"), time.Minute)
			require.NoError(t, err)
			assert.True(t, set, "a deleted key can be set again")
			require.NoError(t, del.Delete(ctx, "missing"), "deleting a missing key is fine")
		}

		for want := int64(1); want <= 3; want++ {
			n, err := s.Incr(ctx, "c", time.Minute)
			require.NoError(t, err)
			assert.Equal(t, want, n)
		}
		h.Advance(2 * time.Minute)
		_, ok, err = s.Get(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok, "values expire")
		n, err := s.Incr(ctx, "c", time.Minute)
		require.NoError(t, err)
		assert.Equal(t, int64(1), n, "counters expire")
	})

	t.Run("Health", func(t *testing.T) {
		s := h.New(t)
		assert.NoError(t, s.Health(ctx))
		assert.NotEmpty(t, s.Name())
	})
}

func ids(rs []store.CallRecord) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.MsgID
	}
	return out
}
