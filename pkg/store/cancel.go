package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// updateStatusLua transitions a batch's status atomically. The old read-check-
// write ran in separate round trips: two concurrent cancels (or a cancel racing
// a terminal completion) both observed 'running' and double-decremented
// kafka_batch:counts — and a cancel could clobber a just-set terminal status.
// Transitions FROM a terminal status are rejected (code 3, idempotent no-op).
const updateStatusLua = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local old = redis.call('HGET', KEYS[1], 'status') or ''
local new = ARGV[1]
if old == new then return 2 end
if old == 'success' or old == 'complete' or old == 'cancelled' then return 3 end
redis.call('HSET', KEYS[1], 'status', new)
if new == 'success' or new == 'complete' or new == 'cancelled' then
  redis.call('ZREM', KEYS[3], ARGV[2])
end
if old ~= '' then redis.call('HINCRBY', KEYS[2], old, -1) end
redis.call('HINCRBY', KEYS[2], new, 1)
if new == 'cancelled' then
  redis.call('ZADD', KEYS[4], tonumber(ARGV[3]), ARGV[2])
end
return 1
`

var updateStatusScript = redis.NewScript(updateStatusLua)

// UpdateBatchStatus sets batch status (Ruby update_batch_status).
func (s *RedisStore) UpdateBatchStatus(ctx context.Context, id, status string) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis store not configured")
	}
	score := float64(time.Now().UnixNano()) / 1e9
	res, err := updateStatusScript.Run(ctx, s.client,
		[]string{batchKey(id), countsKey, runningIndex, cancelledIndex},
		status, id, fmt.Sprintf("%f", score),
	).Int()
	if err != nil {
		return err
	}
	if res == 0 {
		return fmt.Errorf("batch %s not found", id)
	}
	// 1 = updated; 2 = already that status; 3 = already terminal — both no-ops
	// are success for idempotent cancels.
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
