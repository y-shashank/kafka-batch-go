package consumption

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	topicsKey     = "kafka_batch:consumption:topics"
	partitionsKey = "kafka_batch:consumption:partitions"
	sep           = "\x1f"
)

// Snapshot holds topic- and partition-level pause state.
type Snapshot struct {
	Topics     map[string]struct{}
	Partitions map[string]struct{}
}

// Control reads cross-process consumer pause state (mirrors Ruby ConsumptionControl).
type Control struct {
	Client   *redis.Client
	Interval time.Duration

	mu       sync.Mutex
	lastLoad time.Time
	snap     Snapshot
}

func NewControl(client *redis.Client, interval time.Duration) *Control {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Control{Client: client, Interval: interval}
}

func TopicKey(group, topic string) string {
	return group + sep + topic
}

func PartitionKey(group, topic string, partition int32) string {
	return group + sep + topic + sep + strconv.FormatInt(int64(partition), 10)
}

func (c *Control) snapshot(ctx context.Context) Snapshot {
	if c == nil || c.Client == nil {
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

	topics, terr := c.Client.SMembers(ctx, topicsKey).Result()
	parts, perr := c.Client.SMembers(ctx, partitionsKey).Result()
	if terr != nil || perr != nil {
		// Redis read failed. NEVER cache an empty "nothing paused" snapshot —
		// that silently un-pauses a genuinely-paused topic for a full refresh
		// interval (the pause set is a killswitch). Retain the last good snapshot
		// and do NOT stamp lastLoad, so the next call retries immediately.
		// Mirrors Ruby ConsumptionControl's @last_good_snapshot.
		c.mu.Lock()
		s := c.snap
		loaded := !c.lastLoad.IsZero()
		c.mu.Unlock()
		if loaded {
			return s
		}
		// No last-good yet: return empty but do not stamp, so we retry next call
		// instead of latching unpaused for the whole interval.
		return Snapshot{Topics: map[string]struct{}{}, Partitions: map[string]struct{}{}}
	}
	snap := Snapshot{
		Topics:     toSet(topics),
		Partitions: toSet(parts),
	}
	c.mu.Lock()
	c.snap = snap
	c.lastLoad = now
	c.mu.Unlock()
	return snap
}

func toSet(items []string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

// TopicLevelPaused reports whether an entire topic is paused for a consumer group.
func (c *Control) TopicLevelPaused(ctx context.Context, group, topic string) bool {
	snap := c.snapshot(ctx)
	_, ok := snap.Topics[TopicKey(group, topic)]
	return ok
}

// Paused reports topic- or partition-level pause.
func (c *Control) Paused(ctx context.Context, group, topic string, partition int32) bool {
	snap := c.snapshot(ctx)
	if _, ok := snap.Topics[TopicKey(group, topic)]; ok {
		return true
	}
	_, ok := snap.Partitions[PartitionKey(group, topic, partition)]
	return ok
}

// ActiveHigherTopics excludes topic-level-paused higher topics (Ruby PriorityGate).
func (c *Control) ActiveHigherTopics(ctx context.Context, group string, higher []string) []string {
	if c == nil {
		return higher
	}
	out := make([]string, 0, len(higher))
	for _, topic := range higher {
		topic = strings.TrimSpace(topic)
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

// PauseTopic writes a topic-level pause (for integration tests / ops tooling).
func (c *Control) PauseTopic(ctx context.Context, group, topic string) error {
	return c.Client.SAdd(ctx, topicsKey, TopicKey(group, topic)).Err()
}

// ResumeTopic clears a topic-level pause.
func (c *Control) ResumeTopic(ctx context.Context, group, topic string) error {
	return c.Client.SRem(ctx, topicsKey, TopicKey(group, topic)).Err()
}
