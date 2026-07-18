package instrument

import "time"

// JobPayload builds the common job-scoped instrumentation payload.
func JobPayload(jobID, batchID, workerClass string, extra map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func JobProcessed(jobID, batchID, workerClass string, durationMs float64) {
	Emit("job.processed", JobPayload(jobID, batchID, workerClass, nil), durationMs)
}

func JobCancelled(jobID, batchID, workerClass string) {
	Emit("job.cancelled", JobPayload(jobID, batchID, workerClass, nil), 0)
}

func JobExpired(jobID, batchID, workerClass, validTill string) {
	Emit("job.expired", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"valid_till": validTill,
	}), 0)
}

func JobRetried(jobID, batchID, workerClass string, attempt, nextAttempt int, retryTopic string) {
	Emit("job.retried", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"attempt":      attempt,
		"next_attempt": nextAttempt,
		"retry_topic":  retryTopic,
	}), 0)
}

func JobFailed(jobID, batchID, workerClass string, attempt int, errClass, errMsg string) {
	Emit("job.failed", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}), 0)
}

func JobEmitRetried(jobID, batchID string, attempt int, err error) {
	errClass, errMsg := "", ""
	if err != nil {
		errClass = err.Error()
		errMsg = err.Error()
	}
	Emit("job.emit_retried", map[string]interface{}{
		"job_id":        jobID,
		"batch_id":      batchID,
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}, 0)
}

func JobUniqSkipped(workerClass string, payload map[string]interface{}, jobID, batchID string) {
	Emit("job.uniq_skipped", map[string]interface{}{
		"worker_class": workerClass,
		"payload":      payload,
		"job_id":       jobID,
		"batch_id":     batchID,
	}, 0)
}

func DLTPublished(jobID, batchID, dltType, sourceTopic string) {
	Emit("dlt.published", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"dlt_type":     dltType,
		"source_topic": sourceTopic,
	}, 0)
}

func BatchCreated(batchID, description, tenantID, onSuccess, onComplete string) {
	Emit("batch.created", map[string]interface{}{
		"batch_id":    batchID,
		"description": description,
		"tenant_id":   tenantID,
		"on_success":  onSuccess,
		"on_complete": onComplete,
	}, 0)
}

func BatchSealed(batchID string, totalJobs int64) {
	Emit("batch.sealed", map[string]interface{}{
		"batch_id":   batchID,
		"total_jobs": totalJobs,
	}, 0)
}

func BatchCompleted(batchID, outcome string, totalJobs, completedCount, failedCount int64) {
	Emit("batch.completed", map[string]interface{}{
		"batch_id":        batchID,
		"outcome":         outcome,
		"total_jobs":      totalJobs,
		"completed_count": completedCount,
		"failed_count":    failedCount,
	}, 0)
}

func ScheduledEnqueued(jobID, batchID, workerClass string, runAt time.Time) {
	Emit("scheduled.enqueued", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"run_at":       runAt.UTC().Format(time.RFC3339),
	}, 0)
}

func ScheduledEnqueuedBulk(count int, batchID, workerClass string, runAt time.Time) {
	Emit("scheduled.enqueued_bulk", map[string]interface{}{
		"count":        count,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"run_at":       runAt.UTC().Format(time.RFC3339),
	}, 0)
}

func ScheduledIndexFailed(count int, batchID, jobID string, attempts int, err error) {
	errClass, errMsg := "", ""
	if err != nil {
		errClass = err.Error()
		errMsg = err.Error()
	}
	Emit("scheduled.index_failed", map[string]interface{}{
		"count":         count,
		"batch_id":      batchID,
		"job_id":        jobID,
		"attempts":      attempts,
		"error_class":   errClass,
		"error_message": errMsg,
	}, 0)
}

func ScheduledDispatched(jobID, batchID, workerClass, topic string) {
	Emit("scheduled.dispatched", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"topic":        topic,
	}, 0)
}

func CallbackInvoked(batchID, callbackClass, callbackMethod string) {
	Emit("callback.invoked", map[string]interface{}{
		"batch_id":        batchID,
		"callback_class":  callbackClass,
		"callback_method": callbackMethod,
	}, 0)
}

func CallbackFailed(batchID, callbackClass, callbackMethod, errClass, errMsg string) {
	Emit("callback.failed", map[string]interface{}{
		"batch_id":        batchID,
		"callback_class":  callbackClass,
		"callback_method": callbackMethod,
		"error_class":     errClass,
		"error_message":   errMsg,
	}, 0)
}

func ConsumerPriorityYielded(consumerClass, p0Topic, consumerGroup string, pauseMs int64, mode string, rank int, higherTopics []string) {
	Emit("consumer.priority_yielded", map[string]interface{}{
		"consumer_class": consumerClass,
		"p0_topic":       p0Topic,
		"consumer_group": consumerGroup,
		"pause_ms":       pauseMs,
		"mode":           mode,
		"rank":           rank,
		"higher_topics":  higherTopics,
	}, 0)
}

func ReconcilerRan(staleCount, lostCount int, duration time.Duration, triggeredBy string) {
	Emit("reconciler.ran", map[string]interface{}{
		"stale_count":    staleCount,
		"lost_count":     lostCount,
		"duration":       duration.Seconds(),
		"triggered_by":   triggeredBy,
	}, 0)
}

// WorksetReclaimed fires once per SuperFetch orphan-reclaim sweep (whether or
// not it found any orphans), mirroring ReconcilerRan so sweep frequency /
// duration is observable even at zero counts. Ruby parity: workset.reclaimed.
func WorksetReclaimed(checked, reclaimed, failed, skipped int, duration time.Duration) {
	Emit("workset.reclaimed", map[string]interface{}{
		"checked":   checked,
		"reclaimed": reclaimed,
		"failed":    failed,
		"skipped":   skipped,
		"duration":  duration.Seconds(),
	}, 0)
}

// SuperFetchDrained fires once per graceful-shutdown drain, whether it
// finished cleanly (remaining=0) or timed out with in-flight jobs left in
// the Redis workset for control-plane reclaim. Ruby parity: super_fetch.drained.
func SuperFetchDrained(remaining int, timeout time.Duration) {
	Emit("super_fetch.drained", map[string]interface{}{
		"remaining": remaining,
		"timeout":   timeout.Seconds(),
	}, 0)
}

// ── Recurring (cron) scheduler ──────────────────────────────────────────────
// Event names: cron.fired, cron.misfire_skipped, cron.enqueue_failed,
// cron.stale, cron.heartbeat. The metrics bridge maps each to
// kafka_batch.cron_*.count (+ .duration). Staleness values are carried in the
// duration slot so a Datadog monitor can alert on kafka_batch.cron_stale.duration
// or simply on kafka_batch.cron_stale.count > 0.

// CronFired fires once per successfully enqueued recurring occurrence.
func CronFired(schedule, jobType, jobID, tenantID string) {
	Emit("cron.fired", map[string]interface{}{
		"schedule":  schedule,
		"job_type":  jobType,
		"job_id":    jobID,
		"tenant_id": tenantID,
	}, 0)
}

// CronMisfireSkipped fires when a due schedule intentionally skipped missed
// instants (misfire_policy=skip).
func CronMisfireSkipped(schedule, jobType string) {
	Emit("cron.misfire_skipped", map[string]interface{}{
		"schedule": schedule,
		"job_type": jobType,
	}, 0)
}

// CronEnqueueFailed fires when a claimed occurrence could not be enqueued and
// was left pending for recovery.
func CronEnqueueFailed(schedule, jobType, errMsg string) {
	Emit("cron.enqueue_failed", map[string]interface{}{
		"schedule":      schedule,
		"job_type":      jobType,
		"error_message": errMsg,
	}, 0)
}

// CronStale fires (per schedule) when an enabled schedule has not fired within
// its staleness threshold (default 2× its interval) — the "the scheduler
// silently stopped" alert. staleSeconds is also carried as the event duration.
func CronStale(schedule, jobType string, staleSeconds, thresholdSeconds float64) {
	Emit("cron.stale", map[string]interface{}{
		"schedule":          schedule,
		"job_type":          jobType,
		"stale_seconds":     staleSeconds,
		"threshold_seconds": thresholdSeconds,
	}, staleSeconds*1000)
}

// CronHeartbeat fires once per heartbeat sweep as a liveness pulse for the
// recurring scheduler. maxStaleSeconds is carried as the event duration.
func CronHeartbeat(enabled, stale int, maxStaleSeconds float64) {
	Emit("cron.heartbeat", map[string]interface{}{
		"enabled_count": enabled,
		"stale_count":   stale,
	}, maxStaleSeconds*1000)
}

// CompletionDropped fires when a completion event could not be applied to a
// batch (e.g. the batch hash was absent → "not_found", or the event was
// malformed → "invalid"). This is a silent count loss made visible: the batch
// can no longer converge for that seq. Reason is the store's drop reason.
func CompletionDropped(batchID, jobID string, batchSeq int64, reason string) {
	Emit("completion.dropped", map[string]interface{}{
		"batch_id":  batchID,
		"job_id":    jobID,
		"batch_seq": batchSeq,
		"reason":    reason,
	}, 0)
}

// BatchPushRejected fires when a job could not be added to a batch because the
// batch is already sealed/terminal or cancelled (Ruby parity: a closed batch
// refuses new jobs). Surfaces the create-sealed-then-push race so an ignored
// error is still observable.
func BatchPushRejected(batchID, jobType, reason string) {
	Emit("batch.push_rejected", map[string]interface{}{
		"batch_id": batchID,
		"job_type": jobType,
		"reason":   reason,
	}, 0)
}

// CallbackProduceFailed fires when a completion callback could not be produced
// to the callbacks topic after retries and was parked on the dead-letter topic
// instead (so it is never silently lost).
func CallbackProduceFailed(batchID, outcome, errMsg string) {
	Emit("callback.produce_failed", map[string]interface{}{
		"batch_id":      batchID,
		"outcome":       outcome,
		"error_message": errMsg,
	}, 0)
}
