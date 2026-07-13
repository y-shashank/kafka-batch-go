package store

import "context"

// FailureRecorder persists per-batch failure rows for the Web UI.
type FailureRecorder interface {
	RecordFailure(ctx context.Context, e FailureEntry) error
	ClearFailure(ctx context.Context, batchID, jobID string) error
}

var (
	_ FailureRecorder = (*RedisStore)(nil)
	_ FailureRecorder = (*MySQLFailures)(nil)
	_ FailureRecorder = (*CompositeFailures)(nil)
)
