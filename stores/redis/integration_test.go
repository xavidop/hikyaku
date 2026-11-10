//go:build integration

package redis

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/store/storetest"
)

// images are the servers the integration suite runs against.
var images = []string{"redis:7-alpine", "valkey/valkey:8-alpine"}

// startRedis runs a server container for the test and returns its address
// and an admin client to it.
func startRedis(t *testing.T, image string) (string, *goredis.Client) {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcredis.Run(ctx, image)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	uri, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)
	opt, err := goredis.ParseURL(uri)
	require.NoError(t, err)
	admin := goredis.NewClient(&goredis.Options{Addr: opt.Addr})
	t.Cleanup(func() { _ = admin.Close() })
	return opt.Addr, admin
}

// integrationStore is a store on a container. Its timeout is generous: these
// tests check behaviour against a real server, not Docker's latency.
func integrationStore(t *testing.T, addr string) *Store {
	t.Helper()
	return newStore(t, config.Store{Addr: addr, Timeout: config.Duration(time.Second)})
}

// shiftTTLs moves every key's TTL d closer to expiry and deletes the keys it
// reaches: a real server's clock cannot be moved, but its TTLs can.
func shiftTTLs(ctx context.Context, c *goredis.Client, d time.Duration) error {
	iter := c.Scan(ctx, 0, "*", 1000).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		ttl, err := c.PTTL(ctx, k).Result()
		if err != nil {
			return err
		}
		switch {
		case ttl < 0: // -1: no expiry; -2: already gone
		case ttl <= d:
			err = c.Del(ctx, k).Err()
		default:
			err = c.PExpire(ctx, k, ttl-d).Err()
		}
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func TestIntegrationConformance(t *testing.T) {
	for _, image := range images {
		t.Run(image, func(t *testing.T) {
			addr, admin := startRedis(t, image)
			var cur *testing.T // the running subtest, for Advance
			storetest.Run(t, storetest.Harness{
				New: func(t *testing.T) store.Store {
					cur = t
					require.NoError(t, admin.FlushDB(context.Background()).Err())
					return integrationStore(t, addr)
				},
				Advance: func(d time.Duration) {
					require.NoError(cur, shiftTTLs(context.Background(), admin, d))
				},
				Now: time.Now,
			})
		})
	}
}

// TestIntegrationRealExpiry lets the server expire records, values and
// counters on its own clock.
func TestIntegrationRealExpiry(t *testing.T) {
	ctx := context.Background()
	addr, _ := startRedis(t, images[0])
	s := integrationStore(t, addr)
	const ttl = time.Second

	require.NoError(t, s.Append(ctx, "r", "k", record("m1", "r", "k", time.Now()), store.AppendOptions{TTL: ttl}))
	set, err := s.SetNX(ctx, "dedupe", []byte("v"), ttl)
	require.NoError(t, err)
	require.True(t, set)
	n, err := s.Incr(ctx, "budget", ttl)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	time.Sleep(ttl + 500*time.Millisecond)

	got, err := s.Recent(ctx, "r", "k", store.Window{})
	require.NoError(t, err)
	assert.Empty(t, got)
	_, err = s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"x"}})
	assert.ErrorIs(t, err, store.ErrNotFound)
	_, ok, err := s.Get(ctx, "dedupe")
	require.NoError(t, err)
	assert.False(t, ok)
	n, err = s.Incr(ctx, "budget", ttl)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

// TestIntegrationFeedbackScript runs the feedback script under the server's
// own cjson and RESP3 replies.
func TestIntegrationFeedbackScript(t *testing.T) {
	for _, image := range images {
		t.Run(image, func(t *testing.T) {
			ctx := context.Background()
			addr, admin := startRedis(t, image)
			s := integrationStore(t, addr)

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

			updated, err := s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"billing"}, Note: "first"})
			require.NoError(t, err)
			assert.Equal(t, rec.Decision, updated.Decision, "floats keep every digit")
			assert.Equal(t, []string{}, updated.DeliveredTo, "an empty array stays an array")
			assert.Equal(t, rec.State, updated.State)

			updated, err = s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"support"}, Note: "second"})
			require.NoError(t, err)
			assert.Equal(t, "second", updated.Feedback.Note)
			raw, err := admin.LIndex(ctx, hist, 0).Result()
			require.NoError(t, err)
			assert.Equal(t, 1, strings.Count(raw, feedbackMember), "the second correction replaces the first")

			// An index whose list does not hold the message: the script replies nil.
			require.NoError(t, admin.HSet(ctx, "hikyaku:relay:msg:{ghost}", "route", "triage", "key", "cust-1").Err())
			_, err = s.Feedback(ctx, "ghost", store.Feedback{Correct: []string{"x"}})
			assert.ErrorIs(t, err, store.ErrNotFound)

			// Trimmed records lose their message index.
			for _, id := range []string{"m2", "m3"} {
				require.NoError(t, s.Append(ctx, "triage", "cust-1", record(id, "triage", "cust-1", time.Now()), store.AppendOptions{TTL: time.Hour, Keep: 2}))
			}
			exists, err := admin.Exists(ctx, "hikyaku:relay:msg:{m1}").Result()
			require.NoError(t, err)
			assert.Zero(t, exists)
			_, err = s.Feedback(ctx, "m1", store.Feedback{Correct: []string{"x"}})
			assert.ErrorIs(t, err, store.ErrNotFound)
		})
	}
}

// startCluster runs a one-node Redis Cluster: every slot on one node,
// announcing its host-mapped port so a cluster client outside Docker can
// follow the slot map. A cluster still refuses multi-key commands whose
// keys hash to different slots (CROSSSLOT), so this proves the key layout
// keeps each history's keys in one slot.
func startCluster(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcredis.Run(ctx, "redis:7-alpine",
		testcontainers.WithCmd("redis-server", "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf", "--appendonly", "no"))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	uri, err := ctr.ConnectionString(ctx)
	require.NoError(t, err)
	opt, err := goredis.ParseURL(uri)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(opt.Addr)
	require.NoError(t, err)
	admin := goredis.NewClient(&goredis.Options{Addr: opt.Addr})
	t.Cleanup(func() { _ = admin.Close() })
	if host == "localhost" {
		host = "127.0.0.1"
	}
	require.NoError(t, admin.ConfigSet(ctx, "cluster-announce-ip", host).Err())
	require.NoError(t, admin.ConfigSet(ctx, "cluster-announce-port", port).Err())
	require.NoError(t, admin.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", "0", "16383").Err())
	require.Eventually(t, func() bool {
		info, err := admin.ClusterInfo(ctx).Result()
		return err == nil && strings.Contains(info, "cluster_state:ok")
	}, 20*time.Second, 100*time.Millisecond, "the cluster did not come up")
	return opt.Addr
}

// The conformance suite against Redis Cluster (design §13: standalone and
// cluster).
func TestIntegrationClusterConformance(t *testing.T) {
	addr := startCluster(t)
	cc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{addr}})
	t.Cleanup(func() { _ = cc.Close() })
	var cur *testing.T
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			cur = t
			require.NoError(t, cc.ForEachMaster(context.Background(), func(ctx context.Context, c *goredis.Client) error {
				return c.FlushDB(ctx).Err()
			}))
			return newStore(t, config.Store{Cluster: &config.Cluster{Addrs: []string{addr}}, Timeout: config.Duration(time.Second)})
		},
		Advance: func(d time.Duration) {
			require.NoError(cur, cc.ForEachMaster(context.Background(), func(ctx context.Context, c *goredis.Client) error {
				return shiftTTLs(ctx, c, d)
			}))
		},
		Now: time.Now,
	})
}

// Redis killed mid-run (design §13): with the server gone the store fails
// fast with ErrUnavailable, so routing does not wait on it, and it
// recovers once the server is back on the same address.
func TestIntegrationRedisKilledMidRun(t *testing.T) {
	ctx := context.Background()
	port := freeHostPort(t)
	ctr, err := tcredis.Run(ctx, "redis:7-alpine", testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		hc.PortBindings = network.PortMap{network.MustParsePort("6379/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
	}))
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	// open_for 1s: once Redis is back, the next half-open trial closes the
	// breaker within a second (the default waits 30s).
	s := newStore(t, config.Store{Addr: "127.0.0.1:" + port, Timeout: config.Duration(500 * time.Millisecond),
		CircuitBreaker: &config.CircuitBreaker{OpenFor: config.Duration(time.Second), MinRequests: 2}})
	rec := store.CallRecord{MsgID: "m1", At: time.Now(), Route: "triage", Key: "cus_1", Model: "llmfake/chat", DeliveredTo: []string{"billing"}}
	require.NoError(t, s.Append(ctx, "triage", "cus_1", rec, store.AppendOptions{TTL: time.Hour, Keep: 10}))

	stop := 2 * time.Second
	require.NoError(t, ctr.Stop(ctx, &stop))
	start := time.Now()
	err = s.Append(ctx, "triage", "cus_1", store.CallRecord{MsgID: "m2", At: time.Now(), Route: "triage", Key: "cus_1"}, store.AppendOptions{TTL: time.Hour, Keep: 10})
	require.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second, "a dead Redis fails fast")
	// After a few failures the store's breaker opens: reads fail at once,
	// without dialing, so routing never waits on it.
	require.Eventually(t, func() bool {
		_, err := s.Recent(ctx, "triage", "cus_1", store.Window{MaxEntries: 5})
		return errors.Is(err, store.ErrUnavailable)
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, ctr.Start(ctx))
	probe := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:" + port})
	t.Cleanup(func() { _ = probe.Close() })
	require.Eventually(t, func() bool { return probe.Ping(ctx).Err() == nil }, 30*time.Second, 200*time.Millisecond, "Redis is back on the same port")
	var last error
	defer func() {
		if t.Failed() {
			t.Logf("last append error: %v", last)
		}
	}()
	require.Eventually(t, func() bool {
		last = s.Append(ctx, "triage", "cus_2", store.CallRecord{MsgID: "m3", At: time.Now(), Route: "triage", Key: "cus_2"}, store.AppendOptions{TTL: time.Hour, Keep: 10})
		return last == nil
	}, 15*time.Second, 200*time.Millisecond, "the store recovers once Redis is back")
}

func freeHostPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, ln.Close())
	return port
}
