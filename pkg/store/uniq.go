package store

import (
	"context"

	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

// ReleaseUniqLock drops a per-worker uniqueness lock using _uniq_fp from the job wire
// format (mirrors KafkaBatch::Uniqueness.release_by_name fast path).
//
// This delegates to pkg/uniq.ReleaseLock, which is the single source of truth for the
// release Lua script and key derivation — previously this file kept its own byte-for-byte
// copy, which risked drifting from pkg/uniq's copy over time.
func (s *RedisStore) ReleaseUniqLock(ctx context.Context, fpHex, jobID string) error {
	if s == nil || s.client == nil {
		return nil
	}
	return uniq.ReleaseLock(ctx, s.client, fpHex, jobID)
}
