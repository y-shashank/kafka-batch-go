package fairness

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var tenantPartitionCheckoutScript = redis.NewScript(checkoutLua)

var tenantPartitionWarmScript = redis.NewScript(warmLua)

// warmLua reconciles a lane's free-partition set from the tenant→partition map
// ATOMICALLY. The old Go/Ruby version computed `free` from an HGETALL snapshot
// and then SADD'd the missing members in separate round trips: a checkout
// (SPOP + HSET) landing in that window put the just-taken partition back into
// the free set, so a second tenant could be assigned the same partition —
// breaking per-tenant ingest isolation. Doing the read and the writes inside
// one script closes the window.
//
// KEYS[1]=map hash KEYS[2]=free set KEYS[3]=meta (partition count)
// ARGV[1]=live partition count
// Returns the free-set size after reconciliation.
const warmLua = `
local count = tonumber(ARGV[1])
if not count or count < 1 then return -1 end

local taken = {}
local raw = redis.call('HGETALL', KEYS[1])
for i = 1, #raw, 2 do
  local tenant = raw[i]
  local p = tonumber(raw[i + 1])
  if p and p >= 0 and p < count then
    taken[p] = true
  else
    redis.call('HDEL', KEYS[1], tenant)
  end
end

local stored = tonumber(redis.call('GET', KEYS[3]) or '-1')
if stored ~= count then
  -- Partition count changed (or first warm): rebuild the free set wholesale.
  redis.call('DEL', KEYS[2])
  for p = 0, count - 1 do
    if not taken[p] then redis.call('SADD', KEYS[2], p) end
  end
  redis.call('SET', KEYS[3], count)
  return redis.call('SCARD', KEYS[2])
end

-- Steady state: drop out-of-range members, add genuinely-free ones.
local cur = {}
for _, s in ipairs(redis.call('SMEMBERS', KEYS[2])) do
  local p = tonumber(s)
  if not p or p < 0 or p >= count then
    redis.call('SREM', KEYS[2], s)
  else
    cur[p] = true
  end
end
for p = 0, count - 1 do
  if not taken[p] and not cur[p] then
    redis.call('SADD', KEYS[2], p)
  end
end
return redis.call('SCARD', KEYS[2])
`

const checkoutLua = `
local tenant = ARGV[1]
local count  = tonumber(ARGV[2])
if not tenant or not count or count < 1 then return -2 end

local existing = redis.call('HGET', KEYS[1], tenant)
if existing then
  local p = tonumber(existing)
  if p and p >= 0 and p < count then return p end
  redis.call('HDEL', KEYS[1], tenant)
end

local p = redis.call('SPOP', KEYS[2])
if not p then return -1 end

p = tonumber(p)
if not p or p < 0 or p >= count then
  redis.call('SADD', KEYS[2], p)
  return -2
end

redis.call('HSET', KEYS[1], tenant, p)
return p
`

// PartitionCounter returns live topic partition counts.
type PartitionCounter interface {
	TopicPartitionCount(ctx context.Context, topic string) (int, error)
}

// TenantPartitions resolves tenant_id → fairness ingest partition (Ruby parity).
type TenantPartitions struct {
	rdb         *redis.Client
	static      map[string]int32
	dynamic     bool
	cacheTTL    time.Duration
	counter     PartitionCounter
	ingestTopic func(lane string) string

	mu       sync.Mutex
	cache    map[cacheKey]cacheEntry
	lastWarm map[string]time.Time
}

type cacheKey struct {
	lane     string
	tenantID string
}

type cacheEntry struct {
	partition int32
	at        time.Time
}

// TenantPartitionsConfig configures dynamic checkout.
type TenantPartitionsConfig struct {
	Static      map[string]int32
	Dynamic     bool
	CacheTTL    time.Duration
	Counter     PartitionCounter
	IngestTopic func(lane string) string
}

// NewTenantPartitions builds a resolver.
func NewTenantPartitions(rdb *redis.Client, cfg TenantPartitionsConfig) *TenantPartitions {
	ttl := cfg.CacheTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	topicFn := cfg.IngestTopic
	if topicFn == nil {
		topicFn = func(lane string) string { return lane }
	}
	return &TenantPartitions{
		rdb:         rdb,
		static:      cfg.Static,
		dynamic:     cfg.Dynamic,
		cacheTTL:    ttl,
		counter:     cfg.Counter,
		ingestTopic: topicFn,
		cache:       map[cacheKey]cacheEntry{},
	}
}

// Resolve returns an explicit ingest partition or nil to fall back to key-hash routing.
func (tp *TenantPartitions) Resolve(ctx context.Context, tenantID, lane string) *int32 {
	if tenantID == "" {
		return nil
	}
	if hit := tp.readCache(lane, tenantID); hit != nil {
		return hit
	}
	if p, ok := tp.static[tenantID]; ok {
		tp.writeCache(lane, tenantID, p)
		return &p
	}
	if !tp.dynamic || tp.rdb == nil {
		return nil
	}
	tp.warmThrottled(ctx, lane)
	part, err := tp.checkout(ctx, lane, tenantID)
	if err != nil || part == nil {
		return nil
	}
	tp.writeCache(lane, tenantID, *part)
	return part
}

// Warm seeds/reconciles the free-partition pool for a lane in one atomic Lua
// call (see warmLua for the race it closes).
func (tp *TenantPartitions) Warm(ctx context.Context, lane string) error {
	if !tp.dynamic || tp.rdb == nil || tp.counter == nil {
		return nil
	}
	topic := tp.ingestTopic(lane)
	count, err := tp.counter.TopicPartitionCount(ctx, topic)
	if err != nil || count < 1 {
		return err
	}
	return tenantPartitionWarmScript.Run(ctx, tp.rdb,
		[]string{mapKey(lane), freeKey(lane), metaKey(lane)}, count).Err()
}

// warmThrottled runs Warm at most once per cacheTTL per lane. Resolve used to
// call Warm on EVERY cache miss — 4+ Redis round trips per cold tenant, on the
// enqueue path — even though the free pool only changes when the topic is
// repartitioned or a partition is released.
func (tp *TenantPartitions) warmThrottled(ctx context.Context, lane string) {
	tp.mu.Lock()
	last, ok := tp.lastWarm[lane]
	if ok && time.Since(last) < tp.cacheTTL {
		tp.mu.Unlock()
		return
	}
	if tp.lastWarm == nil {
		tp.lastWarm = map[string]time.Time{}
	}
	tp.lastWarm[lane] = time.Now()
	tp.mu.Unlock()
	_ = tp.Warm(ctx, lane)
}

func (tp *TenantPartitions) checkout(ctx context.Context, lane, tenantID string) (*int32, error) {
	topic := tp.ingestTopic(lane)
	count, err := tp.counter.TopicPartitionCount(ctx, topic)
	if err != nil || count < 1 {
		return nil, err
	}
	res, err := tenantPartitionCheckoutScript.Run(ctx, tp.rdb, []string{mapKey(lane), freeKey(lane)}, tenantID, count).Int()
	if err != nil {
		return nil, err
	}
	switch res {
	case -1:
		log.Printf("[kbatch-fairness] no free ingest partitions left on %s lane", lane)
		return nil, nil
	case -2:
		return nil, nil
	default:
		p := int32(res)
		return &p, nil
	}
}

func (tp *TenantPartitions) readCache(lane, tenantID string) *int32 {
	if tp.cacheTTL <= 0 {
		return nil
	}
	tp.mu.Lock()
	defer tp.mu.Unlock()
	entry, ok := tp.cache[cacheKey{lane: lane, tenantID: tenantID}]
	if !ok || time.Since(entry.at) >= tp.cacheTTL {
		delete(tp.cache, cacheKey{lane: lane, tenantID: tenantID})
		return nil
	}
	p := entry.partition
	return &p
}

func (tp *TenantPartitions) writeCache(lane, tenantID string, partition int32) {
	if tp.cacheTTL <= 0 {
		return
	}
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.cache[cacheKey{lane: lane, tenantID: tenantID}] = cacheEntry{partition: partition, at: time.Now()}
}

func mapKey(lane string) string  { return fmt.Sprintf("kafka_batch:tenant_partitions:%s", lane) }
func freeKey(lane string) string { return mapKey(lane) + ":free" }
func metaKey(lane string) string { return mapKey(lane) + ":partition_count" }

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func missingPartitions(count int, taken []int) []int {
	takenSet := map[int]struct{}{}
	for _, p := range taken {
		takenSet[p] = struct{}{}
	}
	out := make([]int, 0, count)
	for p := 0; p < count; p++ {
		if _, ok := takenSet[p]; !ok {
			out = append(out, p)
		}
	}
	return out
}
