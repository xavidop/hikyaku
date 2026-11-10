// Package redis is the Redis implementation of store.Store: the routing
// history, decision cache, poll dedupe, idempotency keys and retry-budget
// counters that the replicas of a relay share. It talks to a standalone
// Redis, a Sentinel-managed one or a Redis Cluster through go-redis v9.
//
// Every operation is bounded by the store's timeout (default 50ms), because
// reads must never stall routing, and goes through a circuit breaker that
// fails fast with store.ErrUnavailable while Redis is down. New does not fail
// when Redis is unreachable: the relay keeps routing without it.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

// scanCount is the COUNT hint of each SCAN page in Records.
const scanCount = 1000

// Store is a store.Store on Redis. It is safe for concurrent use.
type Store struct {
	name    string
	timeout time.Duration
	keys    keys
	client  goredis.UniversalClient
	cluster *goredis.ClusterClient // set in cluster mode only
	breaker *breaker               // nil when circuit_breaker.enabled is false
	logger  *slog.Logger
	now     func() time.Time
}

var _ store.Store = (*Store)(nil)

// New returns the store a spec describes. relay is the relay's (spec) name:
// it is part of every key, so several relays can share one Redis.
//
// New pings Redis once, within the timeout, and only logs a warning when the
// ping fails, because a Redis that is down at startup must not stop the
// relay. It fails only on a spec it cannot use, such as a missing address or
// an unreadable TLS file.
func New(ctx context.Context, spec config.Store, relay string, logger *slog.Logger) (*Store, error) {
	name := spec.Name
	if name == "" {
		name = "redis"
	}
	if relay == "" {
		return nil, fmt.Errorf("redis store %s: the relay name is required", name)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With("store", name)
	timeout := spec.Timeout.Std()
	if timeout <= 0 {
		timeout = config.DefaultStoreTimeout
	}
	prefix := spec.KeyPrefix
	if prefix == "" {
		prefix = config.DefaultKeyPrefix
	}
	cc, err := newClientConfig(spec, timeout)
	if err != nil {
		return nil, fmt.Errorf("redis store %s: %w", name, err)
	}
	if cc.mode == modeCluster && spec.DB != 0 {
		logger.Warn("redis store: db is ignored in cluster mode", "db", spec.DB)
	}

	client := cc.newClient()
	s := &Store{
		name:    name,
		timeout: timeout,
		keys:    newKeys(prefix, relay),
		client:  client,
		logger:  logger,
		now:     time.Now,
	}
	s.cluster, _ = client.(*goredis.ClusterClient)
	s.breaker = newBreaker(config.ResolveBreaker(spec.CircuitBreaker), time.Now, s.breakerChanged)

	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		logger.Warn("redis store: Redis does not answer; routing goes on without it until it does",
			"mode", cc.mode.String(), "err", err)
	}
	return s, nil
}

// Name is the store's name in the spec.
func (s *Store) Name() string { return s.name }

// Health pings Redis within the timeout.
func (s *Store) Health(ctx context.Context) error {
	return s.run(ctx, "health", func(ctx context.Context) error {
		return s.client.Ping(ctx).Err()
	})
}

// Close closes the client and its connections.
func (s *Store) Close() error {
	if err := s.client.Close(); err != nil {
		return s.wrap("close", err)
	}
	return nil
}

// Append pushes rec onto the (route, key) list as its newest record, trims the
// list to o.Keep records and indexes rec by message ID, everything expiring
// after o.TTL, in one pipeline (MULTI/EXEC, except in cluster mode). It then
// deletes the message indexes of the records the trim dropped, best effort,
// as the memory store does. A zero TTL is store.ErrNoTTL.
func (s *Store) Append(ctx context.Context, route, key string, rec store.CallRecord, o store.AppendOptions) error {
	if o.TTL <= 0 {
		return s.wrap("append", store.ErrNoTTL)
	}
	data, err := encodeRecord(rec)
	if err != nil {
		return s.wrap("append", fmt.Errorf("encode record: %w", err))
	}
	keep := int64(o.KeepOrDefault())
	hist := s.keys.hist(route, key)
	return s.run(ctx, "append", func(ctx context.Context) error {
		pipe := s.pipeline()
		pipe.LPush(ctx, hist, data)
		dropped := pipe.LRange(ctx, hist, keep, -1) // what the LTRIM below removes
		pipe.LTrim(ctx, hist, 0, keep-1)
		pipe.PExpire(ctx, hist, o.TTL)
		if rec.MsgID != "" {
			idx := s.keys.msg(rec.MsgID)
			pipe.HSet(ctx, idx, "route", route, "key", key)
			pipe.PExpire(ctx, idx, o.TTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		s.unindex(ctx, msgIDs(dropped.Val()), rec.MsgID)
		return nil
	})
}

// unindex deletes the message indexes of ids, except keep's. It is best
// effort: an index left behind expires with its TTL, and Feedback through it
// finds no record.
func (s *Store) unindex(ctx context.Context, ids []string, keep string) {
	pipe := s.client.Pipeline()
	for _, id := range ids {
		if id != keep {
			pipe.Del(ctx, s.keys.msg(id))
		}
	}
	if pipe.Len() == 0 {
		return
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Debug("redis store: deleting the message indexes of trimmed records failed", "err", err)
	}
}

// Recent reads the newest w.Entries() records of (route, key), drops those
// older than w.MaxAge, and returns the rest oldest first (newest last).
func (s *Store) Recent(ctx context.Context, route, key string, w store.Window) ([]store.CallRecord, error) {
	var raw []string
	err := s.run(ctx, "recent", func(ctx context.Context) (err error) {
		raw, err = s.client.LRange(ctx, s.keys.hist(route, key), 0, int64(w.Entries()-1)).Result()
		return err
	})
	if err != nil {
		return nil, err
	}
	now := s.now()
	var out []store.CallRecord
	for i := len(raw) - 1; i >= 0; i-- { // the list is newest first
		rec, err := decodeRecord(raw[i])
		if err != nil {
			return nil, s.wrap("recent", fmt.Errorf("decode record: %w", err))
		}
		if w.MaxAge > 0 && now.Sub(rec.At) > w.MaxAge {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// Feedback finds the record of msgID through its message index and attaches
// fb to it (fb.At defaults to now) with one script on the history list. It
// returns the updated record; an unknown, expired or trimmed message is
// store.ErrNotFound.
func (s *Store) Feedback(ctx context.Context, msgID string, fb store.Feedback) (store.CallRecord, error) {
	if fb.At.IsZero() {
		fb.At = s.now()
	}
	fbJSON, err := json.Marshal(fb)
	if err != nil {
		return store.CallRecord{}, s.wrap("feedback", fmt.Errorf("encode feedback: %w", err))
	}
	var updated string
	err = s.run(ctx, "feedback", func(ctx context.Context) error {
		ref, err := s.client.HGetAll(ctx, s.keys.msg(msgID)).Result()
		if err != nil {
			return err
		}
		route, okRoute := ref["route"]
		key, okKey := ref["key"]
		if !okRoute || !okKey {
			return store.ErrNotFound
		}
		res, err := feedbackScript.Run(ctx, s.client, []string{s.keys.hist(route, key)}, msgID, fbJSON).Result()
		switch {
		case errors.Is(err, goredis.Nil):
			return store.ErrNotFound
		case err != nil:
			return err
		}
		str, ok := res.(string)
		if !ok {
			return fmt.Errorf("feedback script: unexpected %T reply", res)
		}
		updated = str
		return nil
	})
	if err != nil {
		return store.CallRecord{}, err
	}
	rec, err := decodeRecord(updated)
	if err != nil {
		return store.CallRecord{}, s.wrap("feedback", fmt.Errorf("decode record: %w", err))
	}
	return rec, nil
}

// Forget deletes the history of (route, key) and the message indexes of the
// records it holds.
func (s *Store) Forget(ctx context.Context, route, key string) error {
	hist := s.keys.hist(route, key)
	return s.run(ctx, "forget", func(ctx context.Context) error {
		raw, err := s.client.LRange(ctx, hist, 0, -1).Result()
		if err != nil {
			return err
		}
		pipe := s.pipeline()
		pipe.Del(ctx, hist)
		for _, id := range msgIDs(raw) {
			pipe.Del(ctx, s.keys.msg(id))
		}
		_, err = pipe.Exec(ctx)
		return err
	})
}

// Records walks the history lists of a route (of every route when route is
// empty) in key order, and calls fn for each record received at or after
// since, oldest first within a list. It returns fn's error as is. An export
// may read many lists, so each Redis call, not the whole walk, is bounded by
// the timeout; in cluster mode every master is scanned.
func (s *Store) Records(ctx context.Context, route string, since time.Time, fn func(store.CallRecord) error) error {
	const op = "records"
	if err := ctx.Err(); err != nil {
		return s.wrap(op, err)
	}
	t, err := s.breaker.allow()
	if err != nil {
		return s.wrap(op, err)
	}
	// Only Redis's own errors reach the breaker, not fn's or a decoding one.
	var (
		redisErr error
		lists    []string
	)
	defer func() { s.breaker.done(t, classify(ctx, redisErr)) }()

	lists, redisErr = s.histLists(ctx, s.keys.histPattern(route))
	if redisErr != nil {
		return s.wrap(op, redisErr)
	}
	for _, list := range lists {
		var raw []string
		redisErr = s.bounded(ctx, func(ctx context.Context) (err error) {
			raw, err = s.client.LRange(ctx, list, 0, -1).Result()
			return err
		})
		if redisErr != nil {
			return s.wrap(op, redisErr)
		}
		for i := len(raw) - 1; i >= 0; i-- {
			rec, err := decodeRecord(raw[i])
			if err != nil {
				return s.wrap(op, fmt.Errorf("decode record: %w", err))
			}
			if rec.At.Before(since) {
				continue
			}
			if err := fn(rec); err != nil {
				return err
			}
		}
	}
	return nil
}

// histLists returns the names of the history lists matching a SCAN pattern,
// sorted.
func (s *Store) histLists(ctx context.Context, match string) ([]string, error) {
	var (
		mu   sync.Mutex // ForEachMaster scans the masters concurrently
		seen = map[string]struct{}{}
	)
	add := func(names []string) {
		mu.Lock()
		defer mu.Unlock()
		for _, n := range names {
			seen[n] = struct{}{} // SCAN may return a key twice
		}
	}
	var err error
	if s.cluster != nil {
		err = s.cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			return s.scan(ctx, node, match, add)
		})
	} else {
		err = s.scan(ctx, s.client, match, add)
	}
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(seen)), nil
}

// scan runs SCAN MATCH match TYPE list over one node, a page per call, each
// page within the timeout.
func (s *Store) scan(ctx context.Context, c goredis.Cmdable, match string, add func([]string)) error {
	var cursor uint64
	for {
		var page []string
		err := s.bounded(ctx, func(ctx context.Context) (err error) {
			page, cursor, err = c.ScanType(ctx, cursor, match, scanCount, "list").Result()
			return err
		})
		if err != nil {
			return err
		}
		add(page)
		if cursor == 0 {
			return nil
		}
	}
}

// Get returns the value of a KV entry and whether it exists.
func (s *Store) Get(ctx context.Context, k string) ([]byte, bool, error) {
	var (
		v     []byte
		found bool
	)
	err := s.run(ctx, "get", func(ctx context.Context) error {
		b, err := s.client.Get(ctx, s.keys.kv(k)).Bytes()
		switch {
		case errors.Is(err, goredis.Nil):
			return nil
		case err != nil:
			return err
		}
		v, found = b, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return v, found, nil
}

// Delete removes k.
func (s *Store) Delete(ctx context.Context, k string) error {
	return s.run(ctx, "del", func(ctx context.Context) error {
		return s.client.Del(ctx, s.keys.kv(k)).Err()
	})
}

// SetNX stores v under k unless k exists, expiring after ttl (never when
// ttl ≤ 0), and reports whether it stored it.
func (s *Store) SetNX(ctx context.Context, k string, v []byte, ttl time.Duration) (bool, error) {
	var stored bool
	err := s.run(ctx, "setnx", func(ctx context.Context) (err error) {
		stored, err = s.client.SetNX(ctx, s.keys.kv(k), v, max(ttl, 0)).Result()
		return err
	})
	if err != nil {
		return false, err
	}
	return stored, nil
}

// Incr adds one to the counter k and returns the new value. A counter Incr
// creates expires after ttl (never when ttl ≤ 0); later calls keep its TTL.
func (s *Store) Incr(ctx context.Context, k string, ttl time.Duration) (int64, error) {
	var ms int64
	if ttl > 0 {
		ms = max(ttl.Milliseconds(), 1)
	}
	var n int64
	err := s.run(ctx, "incr", func(ctx context.Context) (err error) {
		n, err = incrScript.Run(ctx, s.client, []string{s.keys.kv(k)}, ms).Int64()
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// run is one store operation: it asks the breaker, bounds fn by the timeout,
// tells the breaker how it went, and wraps the error with the operation.
func (s *Store) run(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return s.wrap(op, err)
	}
	t, err := s.breaker.allow()
	if err != nil {
		return s.wrap(op, err)
	}
	err = s.bounded(ctx, fn)
	s.breaker.done(t, classify(ctx, err))
	if err != nil {
		return s.wrap(op, err)
	}
	return nil
}

// bounded runs fn under the store timeout.
func (s *Store) bounded(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return fn(ctx)
}

// classify says what an operation's error tells the breaker: a miss is an
// answer, and an error that comes after the caller gave up says nothing
// about Redis.
func classify(parent context.Context, err error) outcome {
	switch {
	case err == nil, errors.Is(err, goredis.Nil), errors.Is(err, store.ErrNotFound):
		return outcomeSuccess
	case parent.Err() != nil:
		return outcomeIgnored
	default:
		return outcomeFailure
	}
}

// pipeline is a MULTI/EXEC pipeline, or a plain one in cluster mode, where a
// history list and its message indexes live in different slots.
func (s *Store) pipeline() goredis.Pipeliner {
	if s.cluster != nil {
		return s.client.Pipeline()
	}
	return s.client.TxPipeline()
}

func (s *Store) wrap(op string, err error) error {
	return fmt.Errorf("redis store %s: %s: %w", s.name, op, err)
}

// breakerChanged logs the breaker's transitions.
func (s *Store) breakerChanged(from, to breakerState) {
	if to == stateOpen {
		s.logger.Warn("redis store: circuit open, failing fast", "from", from.String())
		return
	}
	s.logger.Info("redis store: circuit "+to.String(), "from", from.String())
}

// msgIDs returns the distinct message IDs of stored records, skipping the
// records it cannot read.
func msgIDs(raw []string) []string {
	var ids []string
	seen := make(map[string]bool, len(raw))
	for _, r := range raw {
		var rec struct {
			MsgID string `json:"msg_id"`
		}
		if json.Unmarshal([]byte(r), &rec) != nil || rec.MsgID == "" || seen[rec.MsgID] {
			continue
		}
		seen[rec.MsgID] = true
		ids = append(ids, rec.MsgID)
	}
	return ids
}
