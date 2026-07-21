package instrument_test

import (
	"errors"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestAllEventHelpersEmit(t *testing.T) {
	seen := map[string]int{}
	instrument.SetHandler(func(event string, _ map[string]interface{}, _ float64) {
		seen[event]++
	})
	defer instrument.SetHandler(nil)

	instrument.JobProcessed("j", "b", "W", 1.5)
	instrument.JobCancelled("j", "b", "W")
	instrument.JobExpired("j", "b", "W", "2000-01-01T00:00:00Z")
	instrument.JobRetried("j", "b", "W", 1, 2, "retry.short")
	instrument.JobFailed("j", "b", "W", 1, "Err", "boom")
	instrument.JobEmitRetried("j", "b", 1, errors.New("down"))
	instrument.JobEmitRetried("j", "b", 1, nil)
	instrument.JobApplyAborted("j", "b", "g", errors.New("cancel"))
	instrument.JobApplyAborted("j", "b", "g", nil)
	instrument.JobUniqSkipped("W", map[string]interface{}{"k": 1}, "j", "b")
	instrument.DLTPublished("j", "b", "expired", "jobs")
	instrument.BatchCreated("b", "d", "t", "ok", "done")
	instrument.BatchSealed("b", 3)
	instrument.BatchCompleted("b", "success", 3, 3, 0)
	instrument.ScheduledEnqueued("j", "b", "W", time.Unix(1, 0).UTC())
	instrument.ScheduledEnqueuedBulk(2, "b", "W", time.Unix(1, 0).UTC())
	instrument.ScheduledIndexFailed(1, "b", "j", 2, errors.New("idx"))
	instrument.ScheduledIndexFailed(1, "b", "j", 2, nil)
	instrument.ScheduledDispatched("j", "b", "W", "jobs")
	instrument.CallbackInvoked("b", "C", "m")
	instrument.CallbackFailed("b", "C", "m", "E", "msg")
	instrument.ConsumerPriorityYielded("cls", "p0", "g", 10, "strict", 1, []string{"p0"})
	instrument.ReconcilerRan(1, 2, time.Second, "tick")
	instrument.CronFired("s", "jt", "j", "t")
	instrument.CronMisfireSkipped("s", "jt")
	instrument.CronEnqueueFailed("s", "jt", "err")
	instrument.CronStale("s", "jt", 90, 60)
	instrument.CronHeartbeat(3, 1, 12)
	instrument.CompletionDropped("b", "j", 1, "not_found")
	instrument.BatchPushRejected("b", "jt", "sealed")
	instrument.CallbackProduceFailed("b", "success", "down")
	instrument.WorksetPayloadMissing(4)
	instrument.WorksetUnreclaimable("decode")

	want := []string{
		"job.processed", "job.cancelled", "job.expired", "job.retried", "job.failed",
		"job.emit_retried", "job.apply_aborted", "job.uniq_skipped", "dlt.published",
		"batch.created", "batch.sealed", "batch.completed",
		"scheduled.enqueued", "scheduled.enqueued_bulk", "scheduled.index_failed", "scheduled.dispatched",
		"callback.invoked", "callback.failed", "consumer.priority_yielded", "reconciler.ran",
		"cron.fired", "cron.misfire_skipped", "cron.enqueue_failed", "cron.stale", "cron.heartbeat",
		"completion.dropped", "batch.push_rejected", "callback.produce_failed",
		"workset.payload_missing", "workset.unreclaimable",
	}
	for _, e := range want {
		if seen[e] == 0 {
			t.Fatalf("missing emit for %s", e)
		}
	}
	if seen["job.emit_retried"] < 2 || seen["job.apply_aborted"] < 2 || seen["scheduled.index_failed"] < 2 {
		t.Fatalf("nil-error branches not hit: %+v", seen)
	}
}

func TestJobPayloadMergesExtra(t *testing.T) {
	p := instrument.JobPayload("j", "b", "W", map[string]interface{}{"x": 1})
	if p["job_id"] != "j" || p["x"] != 1 {
		t.Fatalf("%v", p)
	}
}
