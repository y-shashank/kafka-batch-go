package cron

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

// leaderLockKey is the cluster-wide key gating recurring ticks. It mirrors the
// reconciler's lock pattern (SET NX EX + token-checked release).
const leaderLockKey = "kafka_batch:cron:leader_lock"

const releaseLockLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`

// Lock is a best-effort distributed lease. It is an optimization only: it keeps
// one node ticking so the DB isn't polled N times per window. Correctness comes
// from the (schedule_id, fire_at) unique key, not from this lock, so a brief
// split-brain window is harmless.
type Lock struct {
	rdb *redis.Client
	key string
	ttl time.Duration
}

// NewLock builds a leader lock over the given Redis client.
func NewLock(rdb *redis.Client, ttl time.Duration) *Lock {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &Lock{rdb: rdb, key: leaderLockKey, ttl: ttl}
}

// Acquire tries to take the lease. On success it returns a token to pass to
// Release. On failure (another node holds it) ok is false.
func (l *Lock) Acquire(ctx context.Context) (token string, ok bool, err error) {
	buf := make([]byte, 16)
	if _, err = rand.Read(buf); err != nil {
		return "", false, err
	}
	token = hex.EncodeToString(buf)
	got, err := l.rdb.SetNX(ctx, l.key, token, l.ttl).Result()
	if err != nil {
		return "", false, err
	}
	return token, got, nil
}

// Release drops the lease only if we still own it (token match).
func (l *Lock) Release(ctx context.Context, token string) {
	if token == "" {
		return
	}
	_, _ = l.rdb.Eval(ctx, releaseLockLua, []string{l.key}, token).Result()
}
