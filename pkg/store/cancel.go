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
