package cron

import "context"

// EnqueueOpts carries per-fire routing hints.
type EnqueueOpts struct {
	// JobID is a deterministic id (sched-<id>-<fireUnix>) so a recovery re-enqueue
	// reuses the same key — combined with uniq handlers this yields end-to-end
	// exactly-once even though delivery is at-least-once.
	JobID    string
	TenantID string
}

// Enqueuer pushes a registered manifest job. The daemon wires this to
// client.Client.EnqueueJob, which owns all routing (plain/priority/fair),
// the message envelope, retries and uniq. The scheduler never runs code.
type Enqueuer interface {
	Enqueue(ctx context.Context, jobType string, payload map[string]interface{}, opts EnqueueOpts) (jobID string, err error)
}

// UniqChecker optionally reports whether a job type's handler deduplicates by
// uniq fingerprint. The ticker's exactly-once story depends on it: the
// deterministic sched-<id>-<fireUnix> job id only dedupes a recovery re-enqueue
// when the handler has uniq enabled. When the Enqueuer implements this, the
// ticker warns loudly for recurring schedules that target non-uniq handlers.
type UniqChecker interface {
	HandlerUniq(jobType string) (uniq bool, known bool)
}
