package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/store/storetest"
)

var hour = store.AppendOptions{TTL: time.Hour}

// newStore builds a store on spec for relay "relay", closed at cleanup. Name
// and Type default to a redis store named redis-main.
func newStore(t *testing.T, spec config.Store) *Store {
	t.Helper()
	return newRelayStore(t, spec, "relay")
}

func newRelayStore(t *testing.T, spec config.Store, relay string) *Store {
	t.Helper()
	if spec.Name == "" {
		spec.Name = "redis-main"
	}
	if spec.Type == "" {
		spec.Type = "redis"
	}
	s, err := New(context.Background(), spec, relay, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func record(id, route, key string, at time.Time, chosen ...string) store.CallRecord {
	r := store.CallRecord{MsgID: id, At: at, Route: route, Key: key, State: "state of " + id, Model: "m", Outcome: store.OutcomeDelivered}
	for _, c := range chosen {
		r.Decision = append(r.Decision, store.Answer{Candidate: c, Probability: 0.9, Chosen: true})
		r.DeliveredTo = append(r.DeliveredTo, c)
	}
	return r
}

// fakeClock is a settable clock for the breaker.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_800_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// useClock points the store's breaker at clk.
func useClock(t *testing.T, s *Store, clk *fakeClock) {
	t.Helper()
	require.NotNil(t, s.breaker, "the breaker is enabled")
	s.breaker.mu.Lock()
	defer s.breaker.mu.Unlock()
	s.breaker.now = clk.Now
	s.breaker.epoch = clk.Now()
}

// countingHook counts what reaches the network: dials, commands, pipelines.
type countingHook struct{ calls atomic.Int64 }

func (h *countingHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		h.calls.Add(1)
		return next(ctx, network, addr)
	}
}

func (h *countingHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmds)
	}
}

// freeAddr returns a local address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func TestConformance(t *testing.T) {
	var mr *miniredis.Miniredis
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			mr = miniredis.RunT(t)
			return newStore(t, config.Store{Addr: mr.Addr()})
		},
		Advance: func(d time.Duration) { mr.FastForward(d) },
		Now:     time.Now,
	})
}

func TestKeyLayout(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})

	require.NoError(t, s.Append(ctx, "triage", "cust-1", record("01JM", "triage", "cust-1", time.Now(), "billing"), hour))
	hist, idx := "hikyaku:relay:triage:hist:{cust-1}", "hikyaku:relay:msg:{01JM}"
	assert.ElementsMatch(t, []string{hist, idx}, mr.Keys())
	assert.Equal(t, time.Hour, mr.TTL(hist))
	assert.Equal(t, time.Hour, mr.TTL(idx))
	assert.Equal(t, "triage", mr.HGet(idx, "route"))
	assert.Equal(t, "cust-1", mr.HGet(idx, "key"))

	_, err := s.SetNX(ctx, "dedupe:abc", []byte("1"), time.Minute)
	require.NoError(t, err)
	_, err = s.Incr(ctx, "budget:x", time.Minute)
	require.NoError(t, err)
	assert.True(t, mr.Exists("hikyaku:relay:kv:dedupe:abc"))
	assert.True(t, mr.Exists("hikyaku:relay:kv:budget:x"))
	assert.Equal(t, time.Minute, mr.TTL("hikyaku:relay:kv:budget:x"))

	other := newRelayStore(t, config.Store{Addr: mr.Addr(), KeyPrefix: "acme"}, "edge")
	require.NoError(t, other.Append(ctx, "triage", "cust-1", record("01JN", "triage", "cust-1", time.Now()), hour))
	assert.True(t, mr.Exists("acme:edge:triage:hist:{cust-1}"))
	assert.True(t, mr.Exists("acme:edge:msg:{01JN}"))
}

func TestTrimAndTTLApplied(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})
	opts := store.AppendOptions{TTL: 90 * time.Minute, Keep: 5}
	for i := range 12 {
		id := fmt.Sprintf("m%02d", i)
		require.NoError(t, s.Append(ctx, "r", "k", record(id, "r", "k", time.Now()), opts))
	}

	hist := "hikyaku:relay:r:hist:{k}"
	list, err := mr.List(hist)
	require.NoError(t, err)
	require.Len(t, list, opts.Keep)
	assert.Equal(t, opts.TTL, mr.TTL(hist))
	newest, err := decodeRecord(list[0])
	require.NoError(t, err)
	assert.Equal(t, "m11", newest.MsgID, "the list is newest first")

	for i := range 12 {
		idx := fmt.Sprintf("hikyaku:relay:msg:{m%02d}", i)
		if i < 12-opts.Keep {
			assert.False(t, mr.Exists(idx), "%s: a trimmed record loses its index", idx)
			continue
		}
		assert.True(t, mr.Exists(idx), idx)
		assert.Equal(t, opts.TTL, mr.TTL(idx), idx)
	}

	for i := range 25 {
		require.NoError(t, s.Append(ctx, "r", "k2", record(fmt.Sprintf("d%02d", i), "r", "k2", time.Now()), hour))
	}
	list, err = mr.List("hikyaku:relay:r:hist:{k2}")
	require.NoError(t, err)
	assert.Len(t, list, hour.KeepOrDefault(), "keep defaults to 2 × max_entries")
}

func TestRedisDownFastFailsAndRecovers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.NewMiniRedis()
	require.NoError(t, mr.Start())
	t.Cleanup(mr.Close)
	addr := mr.Addr()

	const openFor = 30 * time.Second
	s := newStore(t, config.Store{
		Addr:    addr,
		Timeout: config.Duration(50 * time.Millisecond),
		CircuitBreaker: &config.CircuitBreaker{
			FailureRatio: 0.5,
			MinRequests:  4,
			OpenFor:      config.Duration(openFor),
			HalfOpenMax:  2,
			Window:       config.Duration(time.Minute),
		},
	})
	clk := newFakeClock()
	useClock(t, s, clk)
	hook := &countingHook{}
	s.client.AddHook(hook)

	require.NoError(t, s.Append(ctx, "triage", "cust-1", record("m1", "triage", "cust-1", time.Now(), "billing"), hour))

	mr.Close()
	failures := 0
	for {
		start := time.Now()
		_, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
		require.Error(t, err)
		if errors.Is(err, store.ErrUnavailable) {
			break
		}
		failures++
		assert.Less(t, time.Since(start), 500*time.Millisecond, "a dead Redis fails within about the timeout")
		require.Less(t, failures, 10, "the breaker never opened")
	}
	// One success and three failures: 4 requests (min_requests), 75% failed.
	assert.Equal(t, 3, failures)
	assert.Equal(t, stateOpen, s.breaker.current())

	before := hook.calls.Load()
	start := time.Now()
	_, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
	assert.ErrorIs(t, err, store.ErrUnavailable)
	assert.ErrorIs(t, s.Append(ctx, "triage", "cust-1", record("m2", "triage", "cust-1", time.Now()), hour), store.ErrUnavailable)
	_, _, err = s.Get(ctx, "k")
	assert.ErrorIs(t, err, store.ErrUnavailable)
	_, err = s.Incr(ctx, "c", time.Minute)
	assert.ErrorIs(t, err, store.ErrUnavailable)
	assert.ErrorIs(t, s.Health(ctx), store.ErrUnavailable)
	assert.ErrorIs(t, s.Records(ctx, "", time.Time{}, func(store.CallRecord) error { return nil }), store.ErrUnavailable)
	assert.Less(t, time.Since(start), 20*time.Millisecond, "an open breaker answers at once")
	assert.Equal(t, before, hook.calls.Load(), "an open breaker makes no network call")

	// Redis comes back on the same address (empty: a new instance).
	mr2 := miniredis.NewMiniRedis()
	require.NoError(t, mr2.StartAddr(addr))
	t.Cleanup(mr2.Close)
	_, err = s.Recent(ctx, "triage", "cust-1", store.Window{})
	assert.ErrorIs(t, err, store.ErrUnavailable, "still open until open_for has passed")

	clk.Advance(openFor)
	require.Eventually(t, func() bool {
		_, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
		if err != nil {
			clk.Advance(openFor) // a failed trial opens the breaker again
		}
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	assert.Equal(t, stateHalfOpen, s.breaker.current(), "one of two trials passed")
	require.NoError(t, s.Append(ctx, "triage", "cust-1", record("m3", "triage", "cust-1", time.Now()), hour))
	assert.Equal(t, stateClosed, s.breaker.current())
	got, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "m3", got[0].MsgID)
}

// TestHungRedisTimesOut points the store at a server that accepts connections
// and never answers: plain TCP stalls in the RESP handshake, TLS in the TLS
// handshake. Both must fail within about the timeout, not after go-redis's
// default 5s dial and read timeouts.
func TestHungRedisTimesOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})

	for _, tc := range []struct {
		name string
		tls  *config.TLS
	}{
		{"plain", nil},
		{"tls", &config.TLS{Enabled: config.Ptr(true), InsecureSkipVerify: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			s := newStore(t, config.Store{
				Addr:           ln.Addr().String(),
				Timeout:        config.Duration(50 * time.Millisecond),
				TLS:            tc.tls,
				CircuitBreaker: &config.CircuitBreaker{Enabled: config.Ptr(false)},
			})
			assert.Less(t, time.Since(start), time.Second, "New's ping is bounded too")

			start = time.Now()
			_, err := s.Recent(context.Background(), "r", "k", store.Window{})
			require.Error(t, err)
			assert.Less(t, time.Since(start), time.Second)
			assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond, "it waited for the timeout")
		})
	}
}

func TestNewWithRedisDownDoesNotFail(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	start := time.Now()
	s, err := New(context.Background(), config.Store{Name: "redis-main", Type: "redis", Addr: freeAddr(t)}, "relay", logger)
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Close() })
	assert.Less(t, time.Since(start), time.Second)
	assert.Contains(t, logs.String(), "Redis does not answer")
	assert.Contains(t, logs.String(), "store=redis-main")
	assert.Error(t, s.Health(context.Background()), "Health reports the dead Redis")
	assert.Equal(t, "redis-main", s.Name())
}

func TestNewRejectsUnusableSpecs(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, config.Store{Name: "x", Type: "redis"}, "relay", nil)
	assert.ErrorContains(t, err, "needs one of addr, sentinel or cluster")
	_, err = New(ctx, config.Store{Name: "x", Type: "redis", Addr: "localhost:6379"}, "", nil)
	assert.ErrorContains(t, err, "relay name is required")
	_, err = New(ctx, config.Store{Name: "x", Type: "redis", Addr: "localhost:6379", TLS: &config.TLS{CAFile: "/does/not/exist.pem"}}, "relay", nil)
	assert.ErrorContains(t, err, "tls ca_file")
}

func TestSentinelAndClusterClientSelection(t *testing.T) {
	const timeout = 75 * time.Millisecond
	sentinel := &config.Sentinel{MasterName: "mymaster", Addrs: []string{"s1:26379", "s2:26379"}, Password: "sentinel-pw"}
	cluster := &config.Cluster{Addrs: []string{"c1:6379", "c2:6379", "c3:6379"}}

	for _, tc := range []struct {
		name string
		spec config.Store
		want mode
	}{
		{"standalone", config.Store{Addr: "redis:6379", Username: "u", Password: "p", DB: 2}, modeStandalone},
		{"sentinel", config.Store{Sentinel: sentinel, Username: "u", Password: "p", DB: 3}, modeSentinel},
		{"cluster", config.Store{Cluster: cluster, Username: "u", Password: "p"}, modeCluster},
		{"cluster wins", config.Store{Cluster: cluster, Sentinel: sentinel, Addr: "redis:6379"}, modeCluster},
		{"sentinel wins over addr", config.Store{Sentinel: sentinel, Addr: "redis:6379"}, modeSentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, err := newClientConfig(tc.spec, timeout)
			require.NoError(t, err)
			require.Equal(t, tc.want, cc.mode)

			switch cc.mode {
			case modeStandalone:
				require.NotNil(t, cc.single)
				assert.Nil(t, cc.failover)
				assert.Nil(t, cc.cluster)
				o := cc.single
				assert.Equal(t, "redis:6379", o.Addr)
				assert.Equal(t, 2, o.DB)
				assert.Equal(t, []any{"u", "p"}, []any{o.Username, o.Password})
				assert.Equal(t, []any{timeout, timeout, true}, []any{o.ReadTimeout, o.WriteTimeout, o.ContextTimeoutEnabled})
				assert.Equal(t, []any{1, 10 * time.Millisecond, 1}, []any{o.MaxRetries, o.MinRetryBackoff, o.DialerRetries})
			case modeSentinel:
				require.NotNil(t, cc.failover)
				assert.Nil(t, cc.single)
				assert.Nil(t, cc.cluster)
				o := cc.failover
				assert.Equal(t, "mymaster", o.MasterName)
				assert.Equal(t, sentinel.Addrs, o.SentinelAddrs)
				assert.Equal(t, "sentinel-pw", o.SentinelPassword)
				assert.Equal(t, tc.spec.DB, o.DB)
				assert.Equal(t, []any{tc.spec.Username, tc.spec.Password}, []any{o.Username, o.Password})
				assert.Equal(t, []any{timeout, timeout, true}, []any{o.ReadTimeout, o.WriteTimeout, o.ContextTimeoutEnabled})
				assert.Equal(t, []any{1, 10 * time.Millisecond, 1}, []any{o.MaxRetries, o.MinRetryBackoff, o.DialerRetries})
			case modeCluster:
				require.NotNil(t, cc.cluster)
				assert.Nil(t, cc.single)
				assert.Nil(t, cc.failover)
				o := cc.cluster
				assert.Equal(t, cluster.Addrs, o.Addrs)
				assert.Equal(t, []any{tc.spec.Username, tc.spec.Password}, []any{o.Username, o.Password})
				assert.Equal(t, []any{timeout, timeout, true}, []any{o.ReadTimeout, o.WriteTimeout, o.ContextTimeoutEnabled})
				assert.Equal(t, []any{1, 10 * time.Millisecond, 1}, []any{o.MaxRetries, o.MinRetryBackoff, o.DialerRetries})
			}

			c := cc.newClient() // go-redis connects lazily: nothing is dialed here
			t.Cleanup(func() { _ = c.Close() })
			if cc.mode == modeCluster {
				assert.IsType(t, &goredis.ClusterClient{}, c)
			} else {
				assert.IsType(t, &goredis.Client{}, c)
			}
		})
	}

	t.Run("retry policy", func(t *testing.T) {
		cc, err := newClientConfig(config.Store{Addr: "redis:6379", Retry: &config.Retry{Attempts: 1}}, timeout)
		require.NoError(t, err)
		assert.Equal(t, -1, cc.single.MaxRetries, "one attempt: go-redis needs -1, as it reads 0 as 3")
		cc, err = newClientConfig(config.Store{Addr: "redis:6379", Retry: &config.Retry{
			Attempts: 4, Backoff: config.Duration(25 * time.Millisecond), MaxBackoff: config.Duration(time.Second),
		}}, timeout)
		require.NoError(t, err)
		assert.Equal(t, 3, cc.single.MaxRetries)
		assert.Equal(t, 25*time.Millisecond, cc.single.MinRetryBackoff)
		assert.Equal(t, time.Second, cc.single.MaxRetryBackoff)
	})

	t.Run("tls", func(t *testing.T) {
		cc, err := newClientConfig(config.Store{Cluster: cluster, TLS: &config.TLS{Enabled: config.Ptr(true), ServerName: "redis.internal"}}, timeout)
		require.NoError(t, err)
		require.NotNil(t, cc.cluster.TLSConfig)
		assert.Equal(t, "redis.internal", cc.cluster.TLSConfig.ServerName)

		cc, err = newClientConfig(config.Store{Sentinel: sentinel, TLS: &config.TLS{CAFile: ""}}, timeout)
		require.NoError(t, err)
		assert.Nil(t, cc.failover.TLSConfig, "an empty tls: block is plain TCP")
	})

	t.Run("no address", func(t *testing.T) {
		_, err := newClientConfig(config.Store{}, timeout)
		assert.Error(t, err)
	})
}

// TestFeedbackKeepsTheRestOfTheRecord checks the feedback script's splice:
// floats, empty arrays and a state that looks like a feedback member survive
// byte for byte, and a second correction replaces the first.
func TestFeedbackKeepsTheRestOfTheRecord(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})

	rec := store.CallRecord{
		MsgID:       "m1",
		At:          time.Date(2026, 11, 9, 10, 0, 0, 123456789, time.UTC),
		Route:       "triage",
		Key:         "cust-1",
		State:       `subject: x,"feedback":{"correct":["evil"]}`,
		Decision:    []store.Answer{{Candidate: "support", Probability: 0.12345678901234567, Chosen: true, Margin: 0.010000000000000009}},
		Model:       "m",
		DeliveredTo: []string{},
		Outcome:     store.OutcomeDelivered,
	}
	require.NoError(t, s.Append(ctx, "triage", "cust-1", rec, hour))
	hist := "hikyaku:relay:triage:hist:{cust-1}"
	before, err := mr.List(hist)
	require.NoError(t, err)

	updated, err := s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"billing"}, Note: "first", Source: "api"})
	require.NoError(t, err)
	assert.Equal(t, rec.Decision, updated.Decision, "floats keep every digit")
	assert.Equal(t, []string{}, updated.DeliveredTo, "an empty array stays an array")
	assert.Equal(t, rec.State, updated.State)
	require.NotNil(t, updated.Feedback)
	assert.Equal(t, []string{"billing"}, updated.Feedback.Correct)
	assert.False(t, updated.Feedback.At.IsZero(), "at defaults to now")

	after, err := mr.List(hist)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(after[0], strings.TrimSuffix(before[0], "}")), "the record is kept byte for byte")

	at := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	updated, err = s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"support"}, Note: "second", Source: "cli", At: at})
	require.NoError(t, err)
	assert.Equal(t, "second", updated.Feedback.Note)
	assert.True(t, at.Equal(updated.Feedback.At))
	after, err = mr.List(hist)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(after[0], feedbackMember), "the second correction replaces the first")

	got, err := s.Recent(ctx, "triage", "cust-1", store.Window{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "second", got[0].Feedback.Note)
	assert.Equal(t, rec.Decision, got[0].Decision)
	assert.Equal(t, rec.State, got[0].State)
}

func TestFeedbackOnARecordAppendedWithOne(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})
	rec := record("m1", "r", "k", time.Now(), "support")
	rec.Feedback = &store.Feedback{Correct: []string{"billing"}, Note: "from the destination", Source: "destination", At: time.Now()}
	require.NoError(t, s.Append(ctx, "r", "k", rec, hour))

	got, err := s.Recent(ctx, "r", "k", store.Window{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Feedback)
	assert.Equal(t, "from the destination", got[0].Feedback.Note)

	updated, err := s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"support"}, Note: "human"})
	require.NoError(t, err)
	assert.Equal(t, "human", updated.Feedback.Note)
	assert.Equal(t, []string{"support"}, updated.Chosen())
}

func TestFeedbackIndexWithoutRecord(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})
	require.NoError(t, s.Append(ctx, "r", "k", record("m1", "r", "k", time.Now()), hour))

	// The index points to a list that does not hold the message.
	mr.HSet("hikyaku:relay:msg:{ghost}", "route", "r", "key", "k")
	_, err := s.Feedback(ctx, "ghost", store.Feedback{Correct: []string{"x"}})
	assert.ErrorIs(t, err, store.ErrNotFound)

	// The index outlives its list.
	mr.Del("hikyaku:relay:r:hist:{k}")
	_, err = s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"x"}})
	assert.ErrorIs(t, err, store.ErrNotFound)
	assert.Equal(t, stateClosed, s.breaker.current(), "not found is not a failure")
}

func TestRecordsScope(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	a := newRelayStore(t, config.Store{Addr: mr.Addr()}, "a")
	b := newRelayStore(t, config.Store{Addr: mr.Addr()}, "b")
	now := time.Now()

	require.NoError(t, a.Append(ctx, "triage", "k2", record("a-k2-1", "triage", "k2", now), hour))
	require.NoError(t, a.Append(ctx, "triage", "k1", record("a-k1-1", "triage", "k1", now.Add(-time.Minute)), hour))
	require.NoError(t, a.Append(ctx, "triage", "k1", record("a-k1-2", "triage", "k1", now), hour))
	require.NoError(t, a.Append(ctx, "triage-eu", "k1", record("a-eu", "triage-eu", "k1", now), hour))
	require.NoError(t, b.Append(ctx, "triage", "k1", record("b-k1", "triage", "k1", now), hour))
	// KV keys whose names look like history lists are not history.
	_, err := a.SetNX(ctx, "x:hist:{y}", []byte("v"), time.Minute)
	require.NoError(t, err)
	_, err = a.Incr(ctx, "triage:hist:{z}", time.Minute)
	require.NoError(t, err)

	collect := func(s *Store, route string) []string {
		var ids []string
		require.NoError(t, s.Records(ctx, route, time.Time{}, func(r store.CallRecord) error {
			ids = append(ids, r.MsgID)
			return nil
		}))
		return ids
	}
	assert.Equal(t, []string{"a-k1-1", "a-k1-2", "a-k2-1"}, collect(a, "triage"), "key order, oldest first within a key")
	assert.ElementsMatch(t, []string{"a-k1-1", "a-k1-2", "a-k2-1", "a-eu"}, collect(a, ""))
	assert.Equal(t, []string{"b-k1"}, collect(b, ""))
}

func TestAuthAndDB(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	mr.RequireUserAuth("hikyaku", "s3cret")

	s := newStore(t, config.Store{Addr: mr.Addr(), Username: "hikyaku", Password: "s3cret", DB: 3})
	require.NoError(t, s.Health(ctx))
	require.NoError(t, s.Append(ctx, "r", "k", record("m1", "r", "k", time.Now()), hour))
	assert.Contains(t, mr.DB(3).Keys(), "hikyaku:relay:r:hist:{k}")
	assert.Empty(t, mr.DB(0).Keys())

	wrong := newStore(t, config.Store{Addr: mr.Addr(), Username: "hikyaku", Password: "nope"})
	assert.Error(t, wrong.Health(ctx))
}

func TestBreakerDisabled(t *testing.T) {
	addr := freeAddr(t)
	s := newStore(t, config.Store{Addr: addr, CircuitBreaker: &config.CircuitBreaker{Enabled: config.Ptr(false), MinRequests: 1}})
	assert.Nil(t, s.breaker)
	for range 5 {
		err := s.Health(context.Background())
		require.Error(t, err)
		assert.NotErrorIs(t, err, store.ErrUnavailable, "without a breaker every call reaches Redis")
	}
}

func TestCanceledCallIsNotAFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr(), CircuitBreaker: &config.CircuitBreaker{MinRequests: 1, FailureRatio: 0.1}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Recent(ctx, "r", "k", store.Window{})
	assert.ErrorIs(t, err, context.Canceled)
	_, _, err = s.Get(ctx, "k")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, stateClosed, s.breaker.current())
	_, _, err = s.Get(context.Background(), "k")
	assert.NoError(t, err)
}

func TestKVTTLs(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})

	set, err := s.SetNX(ctx, "forever", []byte("v"), 0)
	require.NoError(t, err)
	assert.True(t, set)
	assert.Zero(t, mr.TTL("hikyaku:relay:kv:forever"), "ttl 0: no expiry")

	set, err = s.SetNX(ctx, "short", []byte("v"), 1500*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, set)
	assert.Equal(t, 1500*time.Millisecond, mr.TTL("hikyaku:relay:kv:short"))

	n, err := s.Incr(ctx, "count", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Zero(t, mr.TTL("hikyaku:relay:kv:count"))

	_, err = s.Incr(ctx, "budget", time.Minute)
	require.NoError(t, err)
	mr.FastForward(30 * time.Second)
	n, err = s.Incr(ctx, "budget", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.Equal(t, 30*time.Second, mr.TTL("hikyaku:relay:kv:budget"), "later calls keep the first TTL")

	v, ok, err := s.Get(ctx, "forever")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, []byte("v"), v)
}

func TestErrorsNameTheStoreAndOperation(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newStore(t, config.Store{Addr: mr.Addr()})
	err := s.Append(context.Background(), "r", "k", record("m", "r", "k", time.Now()), store.AppendOptions{})
	assert.ErrorIs(t, err, store.ErrNoTTL)
	assert.EqualError(t, err, "redis store redis-main: append: "+store.ErrNoTTL.Error())

	_, err = s.Feedback(context.Background(), "nope", store.Feedback{})
	assert.ErrorIs(t, err, store.ErrNotFound)
	assert.ErrorContains(t, err, "redis store redis-main: feedback:")
}
