package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// StaleBatches returns running-index batches older than threshold that are still running.
func (s *RedisStore) StaleBatches(ctx context.Context, olderThan time.Time) ([]*Batch, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("redis store not configured")
	}
	score := fmt.Sprintf("%f", float64(olderThan.UnixNano())/1e9)
	ids, err := s.client.ZRangeByScore(ctx, runningIndex, &redis.ZRangeBy{Min: "-inf", Max: score}).Result()
	if err != nil {
		return nil, err
	}
	return s.filterIndexedBatches(ctx, ids, runningIndex, func(b *Batch) bool {
		return b != nil && b.Status == "running"
	})
}

// DoneBatchesWithoutCallback returns done-index batches awaiting callback dispatch.
func (s *RedisStore) DoneBatchesWithoutCallback(ctx context.Context, olderThan time.Time) ([]*Batch, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("redis store not configured")
	}
	score := fmt.Sprintf("%f", float64(olderThan.UnixNano())/1e9)
	ids, err := s.client.ZRangeByScore(ctx, doneIndex, &redis.ZRangeBy{Min: "-inf", Max: score}).Result()
	if err != nil {
		return nil, err
	}
	return s.filterIndexedBatches(ctx, ids, doneIndex, func(b *Batch) bool {
		if b == nil || b.CallbackClaimed {
			return false
		}
		return b.Status == "success" || b.Status == "complete"
	})
}

func (s *RedisStore) filterIndexedBatches(ctx context.Context, ids []string, index string, keep func(*Batch) bool) ([]*Batch, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, batchKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	out := make([]*Batch, 0, len(ids))
	prune := make([]string, 0)
	for i, id := range ids {
		h, err := cmds[i].Result()
		if err != nil {
			return nil, err
		}
		b := hashToBatch(h)
		if b == nil || !keep(b) {
			prune = append(prune, id)
			continue
		}
		out = append(out, b)
	}
	if len(prune) > 0 {
		pipe = s.client.Pipeline()
		for _, id := range prune {
			pipe.ZRem(ctx, index, id)
		}
		_, _ = pipe.Exec(ctx)
	}
	return out, nil
}

// MarkFinishedIfRunning transitions a running batch to a terminal outcome.
func (s *RedisStore) MarkFinishedIfRunning(ctx context.Context, id, outcome string) (bool, error) {
	if s == nil || s.client == nil {
		return false, fmt.Errorf("redis store not configured")
	}
	now := time.Now().UTC()
	score := fmt.Sprintf("%f", float64(now.UnixNano())/1e9)
	res, err := s.client.Eval(ctx, markFinishedIfRunningLua,
		[]string{batchKey(id), countsKey, runningIndex, doneIndex},
		outcome, now.Format(time.RFC3339), score, id,
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// WithReconcilerLock runs fn when this process acquires the distributed lock.
func (s *RedisStore) WithReconcilerLock(ctx context.Context, ttl time.Duration, fn func() error) (bool, error) {
	if s == nil || s.client == nil {
		return false, fmt.Errorf("redis store not configured")
	}
	token, err := randomToken(16)
	if err != nil {
		return false, err
	}
	ttlSec := int(ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	acquired, err := s.client.Eval(ctx, acquireLockLua, []string{reconcilerLockKey}, token, ttlSec).Result()
	if err != nil {
		return false, err
	}
	if acquired != "OK" {
		return false, nil
	}
	defer func() {
		_, _ = s.client.Eval(context.Background(), releaseLockLua, []string{reconcilerLockKey}, token).Result()
	}()
	if err := fn(); err != nil {
		return true, err
	}
	return true, nil
}

// ReconcileBatchCounts rebuilds kafka_batch:counts from live batch hashes.
func (s *RedisStore) ReconcileBatchCounts(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis store not configured")
	}
	ids, err := s.client.ZRange(ctx, allIndex, 0, -1).Result()
	if err != nil {
		return err
	}
	counts := map[string]int64{}
	prune := make([]string, 0)
	if len(ids) > 0 {
		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(ids))
		for i, id := range ids {
			cmds[i] = pipe.HGet(ctx, batchKey(id), "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return err
		}
		for i, id := range ids {
			st, err := cmds[i].Result()
			if err == redis.Nil || st == "" {
				prune = append(prune, id)
				continue
			}
			counts[st]++
		}
	}
	if len(prune) > 0 {
		pipe := s.client.Pipeline()
		for _, id := range prune {
			pipe.ZRem(ctx, allIndex, id)
		}
		_, _ = pipe.Exec(ctx)
	}
	pipe := s.client.Pipeline()
	pipe.Del(ctx, countsKey)
	for k, v := range counts {
		if v > 0 {
			pipe.HSet(ctx, countsKey, k, v)
		}
	}
	_, err = pipe.Exec(ctx)
	return err
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
