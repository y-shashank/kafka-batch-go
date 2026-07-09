package consumption

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// HybridControl reads pause state from Redis, falling back to MySQL when Redis
// is unavailable (mirrors Ruby ConsumptionControl backend selection).
type HybridControl struct {
	Redis    *redis.Client
	MySQL    *MySQLPauseStore
	Interval time.Duration

	mu       sync.Mutex
	lastLoad time.Time
	snap     Snapshot
}

func NewHybridControl(rdb *redis.Client, mysql *MySQLPauseStore, interval time.Duration) *HybridControl {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &HybridControl{Redis: rdb, MySQL: mysql, Interval: interval}
}

func (c *HybridControl) snapshot(ctx context.Context) Snapshot {
	if c == nil {
		return Snapshot{Topics: map[string]struct{}{}, Partitions: map[string]struct{}{}}
	}
	now := time.Now()
	c.mu.Lock()
	if !c.lastLoad.IsZero() && now.Sub(c.lastLoad) < c.Interval {
		s := c.snap
		c.mu.Unlock()
		return s
	}
	c.mu.Unlock()

	snap := c.loadSnapshot(ctx)
	c.mu.Lock()
	c.snap = snap
	c.lastLoad = now
	c.mu.Unlock()
	return snap
}

func (c *HybridControl) loadSnapshot(ctx context.Context) Snapshot {
	if c.Redis != nil {
		if err := c.Redis.Ping(ctx).Err(); err == nil {
			topics, _ := c.Redis.SMembers(ctx, topicsKey).Result()
			parts, _ := c.Redis.SMembers(ctx, partitionsKey).Result()
			return Snapshot{Topics: toSet(topics), Partitions: toSet(parts)}
		}
	}
	if c.MySQL != nil {
		if snap, err := c.MySQL.Snapshot(ctx); err == nil {
			return snap
		}
	}
	return Snapshot{Topics: map[string]struct{}{}, Partitions: map[string]struct{}{}}
}

func (c *HybridControl) TopicLevelPaused(ctx context.Context, group, topic string) bool {
	snap := c.snapshot(ctx)
	_, ok := snap.Topics[TopicKey(group, topic)]
	return ok
}

func (c *HybridControl) Paused(ctx context.Context, group, topic string, partition int32) bool {
	snap := c.snapshot(ctx)
	if _, ok := snap.Topics[TopicKey(group, topic)]; ok {
		return true
	}
	_, ok := snap.Partitions[PartitionKey(group, topic, partition)]
	return ok
}

func (c *HybridControl) ActiveHigherTopics(ctx context.Context, group string, higher []string) []string {
	out := make([]string, 0, len(higher))
	for _, topic := range higher {
		if topic == "" {
			continue
		}
		if c.TopicLevelPaused(ctx, group, topic) {
			continue
		}
		out = append(out, topic)
	}
	return out
}
