package store

import "context"

// FailureRecorder persists per-batch failure rows for the Web UI. Only
// MySQLFailures implements this durably (kafka_batch_failures table); the
// default Redis-backed store never persists per-job failure metadata, so a
// nil FailureRecorder means "don't record" (see BuildFailureRecorder).
type FailureRecorder interface {
	RecordFailure(ctx context.Context, e FailureEntry) error
	ClearFailure(ctx context.Context, batchID, jobID string) error
}

var _ FailureRecorder = (*MySQLFailures)(nil)
