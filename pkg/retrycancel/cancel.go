package retrycancel

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	cancelKey   = "kafka_batch:retry:cancel"
	skipKey     = "kafka_batch:retry:skip"
	defaultTTL  = 7 * 24 * time.Hour
)

// Store is the Redis control plane for operator-deleted retries (Ruby RetryCancel parity).
type Store struct {
	Client *redis.Client
	TTL    time.Duration
}

func (s *Store) ttl() time.Duration {
	if s == nil || s.TTL <= 0 {
		return defaultTTL
	}
	return s.TTL
}

func (s *Store) available() bool {
	return s != nil && s.Client != nil
}

// Cancel adds job IDs to the pending-skip set.
func (s *Store) Cancel(ctx context.Context, jobIDs []string) (int64, error) {
	if !s.available() || len(jobIDs) == 0 {
		return 0, nil
	}
	ids := make([]interface{}, 0, len(jobIDs))
	seen := map[string]struct{}{}
	for _, id := range jobIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	n, err := s.Client.SAdd(ctx, cancelKey, ids...).Result()
	if err != nil {
		return 0, err
	}
	_ = s.Client.Expire(ctx, cancelKey, s.ttl()).Err()
	return n, nil
}

func (s *Store) Cancelled(ctx context.Context, jobID string) bool {
	if !s.available() || jobID == "" {
		return false
	}
	ok, err := s.Client.SIsMember(ctx, cancelKey, jobID).Result()
	return err == nil && ok
}

func (s *Store) Acknowledge(ctx context.Context, jobID string) {
	if !s.available() || jobID == "" {
		return
	}
	_ = s.Client.SRem(ctx, cancelKey, jobID).Err()
}

func (s *Store) ClearCancelSet(ctx context.Context) error {
	if !s.available() {
		return nil
	}
	return s.Client.Del(ctx, cancelKey).Err()
}

// SetSkipWatermarks stores topic → partition → offset (inclusive skip).
func (s *Store) SetSkipWatermarks(ctx context.Context, marks map[string]map[int32]int64) error {
	if !s.available() || len(marks) == 0 {
		return nil
	}
	fields := map[string]interface{}{}
	for topic, parts := range marks {
		for p, off := range parts {
			fields[fmt.Sprintf("%s:%d", topic, p)] = off
		}
	}
	if len(fields) == 0 {
		return nil
	}
	if err := s.Client.HSet(ctx, skipKey, fields).Err(); err != nil {
		return err
	}
	return s.Client.Expire(ctx, skipKey, s.ttl()).Err()
}

func (s *Store) SkipUntil(ctx context.Context, topic string, partition int32) (int64, bool) {
	if !s.available() {
		return 0, false
	}
	v, err := s.Client.HGet(ctx, skipKey, fmt.Sprintf("%s:%d", topic, partition)).Result()
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (s *Store) ShouldSkip(ctx context.Context, topic string, partition int32, offset int64, jobID string) bool {
	if lim, ok := s.SkipUntil(ctx, topic, partition); ok && offset <= lim {
		return true
	}
	return s.Cancelled(ctx, jobID)
}
