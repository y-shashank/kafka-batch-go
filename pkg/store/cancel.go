package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// UpdateBatchStatus sets batch status (Ruby update_batch_status).
func (s *RedisStore) UpdateBatchStatus(ctx context.Context, id, status string) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis store not configured")
	}
	key := batchKey(id)
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("batch %s not found", id)
	}

	oldStatus, err := s.client.HGet(ctx, key, "status").Result()
	if err == redis.Nil {
		return fmt.Errorf("batch %s not found", id)
	}
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, key, "status", status)
	if status == "success" || status == "complete" || status == "cancelled" {
		pipe.ZRem(ctx, runningIndex, id)
	}
	if oldStatus != "" && oldStatus != status {
		pipe.HIncrBy(ctx, countsKey, oldStatus, -1)
		pipe.HIncrBy(ctx, countsKey, status, 1)
	}
	if status == "cancelled" {
		score := float64(time.Now().UnixNano()) / 1e9
		pipe.ZAdd(ctx, cancelledIndex, redis.Z{Score: score, Member: id})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return nil
}

// CancelBatch marks a batch cancelled (Ruby Batch.cancel).
func (s *RedisStore) CancelBatch(ctx context.Context, id string) error {
	return s.UpdateBatchStatus(ctx, id, "cancelled")
}

// CancelledBatchIDs returns batch IDs cancelled within the last 2× batch TTL
// window (Ruby RedisStore#cancelled_batch_ids). Prunes older ZSET members.
func (s *RedisStore) CancelledBatchIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("redis store not configured")
	}
	ttl := s.ttl
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	cutoff := float64(time.Now().Add(-2*ttl).UnixNano()) / 1e9
	pipe := s.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, cancelledIndex, "-inf", fmt.Sprintf("%f", cutoff))
	rangeCmd := pipe.ZRangeByScore(ctx, cancelledIndex, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", cutoff),
		Max: "+inf",
	})
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	return rangeCmd.Val(), nil
}
