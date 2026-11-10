package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// cache is the decision cache (design §6.4 step 3): a KV (a store, or an
// in-process LRU) keyed by the cache.key template, or by a hash of the
// state and history when no key is set.
type cache struct {
	kv    store.KV
	key   *transform.Template
	ttl   time.Duration
	route string
}

func newCache(route string, c *config.Cache, kv store.KV) (*cache, error) {
	if c == nil {
		return nil, nil
	}
	out := &cache{kv: kv, ttl: c.TTL.Std(), route: route}
	if out.ttl <= 0 {
		out.ttl = config.DefaultCacheTTL
	}
	if c.Key != "" {
		k, err := transform.CompileTemplate("cache.key", c.Key)
		if err != nil {
			return nil, err
		}
		out.key = k
	}
	return out, nil
}

func (c *cache) keyFor(env *envelope.Envelope, fingerprint string) string {
	if c.key != nil {
		k, err := c.key.Render(env)
		if err != nil || k == "" {
			return ""
		}
		return "aicache:" + c.route + ":" + k
	}
	sum := sha256.Sum256([]byte(fingerprint))
	return "aicache:" + c.route + ":" + hex.EncodeToString(sum[:16])
}

func (c *cache) get(ctx context.Context, key string) (FlowOutput, bool) {
	if c == nil || key == "" {
		return FlowOutput{}, false
	}
	b, ok, err := c.kv.Get(ctx, key)
	if err != nil || !ok {
		return FlowOutput{}, false
	}
	var out FlowOutput
	if json.Unmarshal(b, &out) != nil {
		return FlowOutput{}, false
	}
	out.Cached = true
	return out, true
}

func (c *cache) put(ctx context.Context, key string, out FlowOutput) {
	if c == nil || key == "" || out.LowConfidence || out.Sticky {
		return
	}
	out.Record, out.Cached = nil, false
	b, err := json.Marshal(out)
	if err != nil {
		return
	}
	_, _ = c.kv.SetNX(ctx, key, b, c.ttl)
}

// budget is router.budget (design §6.4 step 8): calls per minute (counted
// in the store, so shared across replicas when it is Redis) and tokens per
// hour (counted in this process).
type budget struct {
	route        string
	callsPerMin  int
	tokensPerHr  int
	kv           store.KV
	mu           sync.Mutex
	tokenHour    int64
	tokensInHour int
}

// ErrOverBudget sends the route to its router fallback.
var ErrOverBudget = errorString("the AI budget of the route is spent")

type errorString string

func (e errorString) Error() string { return string(e) }

func newBudget(route string, b *config.AIBudget, kv store.KV) *budget {
	if b == nil || (b.MaxCallsPerMinute <= 0 && b.MaxTokensPerHour <= 0) {
		return nil
	}
	return &budget{route: route, callsPerMin: b.MaxCallsPerMinute, tokensPerHr: b.MaxTokensPerHour, kv: kv}
}

// allow spends one call of the budget, or reports it spent.
func (b *budget) allow(ctx context.Context) bool {
	if b == nil {
		return true
	}
	now := time.Now()
	if b.tokensPerHr > 0 {
		b.mu.Lock()
		if h := now.Unix() / 3600; h != b.tokenHour {
			b.tokenHour, b.tokensInHour = h, 0
		}
		over := b.tokensInHour >= b.tokensPerHr
		b.mu.Unlock()
		if over {
			return false
		}
	}
	if b.callsPerMin > 0 {
		n, err := b.kv.Incr(ctx, "aibudget:"+b.route+":calls:"+strconv.FormatInt(now.Unix()/60, 10), 2*time.Minute)
		if err == nil && n > int64(b.callsPerMin) {
			return false
		}
	}
	return true
}

func (b *budget) spend(tokens int) {
	if b == nil || tokens <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokensInHour += tokens
}
