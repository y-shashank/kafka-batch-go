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
	rdb     *redis.Client
	static  map[string]int32
	dynamic bool
	cacheTTL time.Duration
	counter PartitionCounter
	ingestTopic func(lane string) string

	mu    sync.Mutex
	cache map[cacheKey]cacheEntry
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
	Static                   map[string]int32
	Dynamic                  bool
	CacheTTL                 time.Duration
	Counter                  PartitionCounter
	IngestTopic              func(lane string) string
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
	_ = tp.Warm(ctx, lane)
	part, err := tp.checkout(ctx, lane, tenantID)
	if err != nil || part == nil {
		return nil
	}
	tp.writeCache(lane, tenantID, *part)
	return part
}

// Warm seeds the free-partition pool for a lane.
func (tp *TenantPartitions) Warm(ctx context.Context, lane string) error {
	if !tp.dynamic || tp.rdb == nil || tp.counter == nil {
		return nil
	}
	topic := tp.ingestTopic(lane)
	count, err := tp.counter.TopicPartitionCount(ctx, topic)
	if err != nil || count < 1 {
		return err
	}

	mapKey := mapKey(lane)
	freeKey := freeKey(lane)
	metaKey := metaKey(lane)

	raw, err := tp.rdb.HGetAll(ctx, mapKey).Result()
	if err != nil {
		return err
	}
	valid := map[int]struct{}{}
	for tenant, partStr := range raw {
		p := atoi(partStr)
		if p >= 0 && p < count {
			valid[p] = struct{}{}
			_ = tenant
		} else {
			_ = tp.rdb.HDel(ctx, mapKey, tenant).Err()
		}
	}
	taken := make([]int, 0, len(valid))
	for p := range valid {
		taken = append(taken, p)
	}
	free := missingPartitions(count, taken)

	stored, _ := tp.rdb.Get(ctx, metaKey).Int()
	if stored != count {
		_ = tp.rdb.Del(ctx, freeKey).Err()
		if len(free) > 0 {
			members := make([]interface{}, len(free))
			for i, p := range free {
				members[i] = p
			}
			_ = tp.rdb.SAdd(ctx, freeKey, members...).Err()
		}
		_ = tp.rdb.Set(ctx, metaKey, count, 0).Err()
		return nil
	}

	current, _ := tp.rdb.SMembers(ctx, freeKey).Result()
	curSet := map[int]struct{}{}
	for _, s := range current {
		p := atoi(s)
		curSet[p] = struct{}{}
		if p < 0 || p >= count {
			_ = tp.rdb.SRem(ctx, freeKey, s).Err()
		}
	}
	for _, p := range free {
		if _, ok := curSet[p]; !ok {
			_ = tp.rdb.SAdd(ctx, freeKey, p).Err()
		}
	}
	return nil
}

func (tp *TenantPartitions) checkout(ctx context.Context, lane, tenantID string) (*int32, error) {
	topic := tp.ingestTopic(lane)
	count, err := tp.counter.TopicPartitionCount(ctx, topic)
	if err != nil || count < 1 {
		return nil, err
	}
	res, err := tp.rdb.Eval(ctx, checkoutLua, []string{mapKey(lane), freeKey(lane)}, tenantID, count).Int()
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
