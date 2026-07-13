package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)
// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Processor runs plain-topic job messages (no fairness).
type Processor struct {
	Cfg            config.Daemon
	Manifest       config.Manifest
	Store          *store.RedisStore
	Failures       store.FailureRecorder
	Producer       Producer
	FairTime       *fairness.Scheduler
	FairThroughput *fairness.Scheduler
	Liveness       *liveness.Reporter
	Now            func() time.Time
}

// Outcome describes what happened to one job message.
type Outcome struct {
	CommitOffset bool
	Event        *protocol.EventMessage
	RetryTopic   string
	RetryKey     string
	RetryPayload []byte
	DLTPayload   []byte
	DLTKey       string
}

func (p *Processor) Process(ctx context.Context, raw []byte, src protocol.SourceCoords) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	started := p.now()
	var job protocol.JobMessage
	if err := json.Unmarshal(raw, &job); err != nil {
		dlt, key := p.dltPayload(map[string]interface{}{"raw_payload": string(raw)}, src.Topic, "json.ParseError", err.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		emitDLTPublished("", "", "job", src.Topic)
		return out, nil
	}

	if p.Cfg.SkipCancelledJobs && job.BatchID != nil {
		cancelled, err := p.Store.BatchCancelled(ctx, *job.BatchID)
		if err != nil {
			return out, err
		}
		if cancelled {
			p.releaseUniq(ctx, job)
			emitJobCancelled(job)
			return out, nil
		}
	}

	if jobexpiry.Expired(job.ValidTill, p.now()) {
		p.releaseFairSlotIfHeld(ctx, raw)
		drop := jobexpiry.BuildDrop(raw, src, p.now())
		out.Event = drop.Event
		out.DLTPayload = drop.DLTPayload
		out.DLTKey = drop.DLTKey
		if drop.Failure != nil {
			p.recordFailure(ctx, store.FailureEntry{
				BatchID: drop.Failure.BatchID, JobID: drop.Failure.JobID,
				WorkerClass: drop.Failure.WorkerClass, ErrorClass: drop.Failure.ErrorClass,
				ErrorMessage: drop.Failure.ErrorMessage, Status: drop.Failure.Status,
				Attempt: drop.Failure.Attempt,
			})
		}
		p.releaseUniq(ctx, job)
		emitJobExpired(job, job.ValidTill)
		if out.DLTPayload != nil {
			jid, bid, dt := dltMeta(out.DLTPayload)
			emitDLTPublished(jid, bid, dt, src.Topic)
		}
		return out, nil
	}

	entry, _ := p.Manifest.Handlers[job.JobType]
	handler, goHandler := kbatch.Lookup(job.JobType)
	if !goHandler {
		err := fmt.Errorf("unknown job_type %q", job.JobType)
		if entry.Runtime == "ruby" {
			err = fmt.Errorf("job_type %q is runtime ruby — consume via Karafka JobConsumer", job.JobType)
		}
		if job.BatchID != nil && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, "UnknownHandler", err.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		emitJobFailed(job, job.Attempt, "UnknownHandler", err.Error())
		emitDLTPublished(job.JobID, deref(job.BatchID), "job", src.Topic)
		return out, nil
	}

	jobLive := false
	defer func() {
		if jobLive && p.Liveness != nil {
			p.Liveness.JobFinished(ctx, job.JobID)
		}
	}()
	p.markJobStarted(ctx, job, src)
	jobLive = true

	run := func() error {
		hctx := &kbatch.Context{
			JobType: job.JobType,
			JobID:   job.JobID,
			Attempt: job.Attempt,
			Payload: job.Payload,
		}
		if job.BatchID != nil {
			hctx.BatchID = *job.BatchID
		}
		return handler(hctx)
	}

	if err := p.withFairSlot(ctx, raw, run); err != nil {
		if errors.Is(err, errFairSkipped) {
			return p.handleFairSkip(ctx, job, raw, src, run)
		}
		return p.handleFailure(ctx, job, raw, src, err)
	}

	if job.BatchID != nil && job.BatchSeq != nil && !job.BatchCounted {
		ev := p.buildEvent(job, "success", src)
		out.Event = &ev
	}
	if job.BatchID != nil && job.Attempt > 0 {
		p.clearFailure(ctx, *job.BatchID, job.JobID)
	}
	p.releaseUniq(ctx, job)
	emitJobProcessed(job, instrumentSince(started, p.now()))
	return out, nil
}

// fairSkipDeferGrace is added past a live slot lease's expiry before a deferred
// fair-skip redelivery is reconsidered, so the lease is unambiguously expired
// (and the holder confirmed dead) by the time it returns.
const fairSkipDeferGrace = 5 * time.Second

// handleFairSkip decides what to do when a fair slot's dedup key is already held
// (errFairSkipped). Three cases:
//
//  1. Completion already recorded → genuine duplicate delivery; drop it.
//  2. Not recorded, slot lease still live → the holder is alive and working.
//     Do NOT run (that would double-execute); defer the redelivery past the
//     lease expiry via the delayed retry topic and re-check then.
//  3. Not recorded, slot lease gone/expired → the holder died mid-perform before
//     counting. Re-run so the batch can finalize (jobs are at-least-once and
//     handlers must be idempotent, README pitfall #1) and emit the completion.
//
// The completion bitmap dedups counting, so even if a deferred copy and a slow
// holder both eventually emit, the batch is counted exactly once.
func (p *Processor) handleFairSkip(ctx context.Context, job protocol.JobMessage, raw []byte, src protocol.SourceCoords, run func() error) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	if p.Store == nil || job.BatchID == nil || job.BatchSeq == nil || job.BatchCounted {
		return out, nil
	}
	recorded, err := p.Store.CompletionRecorded(ctx, *job.BatchID, *job.BatchSeq)
	if err != nil {
		return out, err
	}
	if recorded {
		return out, nil // (1) genuine duplicate — already counted
	}

	// (2) Holder still alive? Defer past the lease expiry instead of double-running.
	sched := p.fairScheduler(raw)
	fm := parseFairMeta(raw)
	if sched != nil && fm.slotID != "" {
		active, expiry, lerr := sched.SlotLeaseActive(ctx, fm.slotID)
		if lerr != nil {
			return out, lerr
		}
		if active {
			retryAt := time.Unix(int64(expiry), 0).Add(fairSkipDeferGrace)
			if !retryAt.After(p.now()) {
				retryAt = p.now().Add(fairSkipDeferGrace)
			}
			payload, berr := buildFairDeferPayload(raw, retryAt, src.Topic)
			if berr != nil {
				return out, berr
			}
			out.RetryTopic = p.Cfg.RetryTopic(p.Cfg.RetryTierFor(job.Attempt+1, job.RetryTier))
			out.RetryKey = job.JobID
			out.RetryPayload = payload
			return out, nil
		}
	}

	// (3) Orphaned slot — holder is dead. Run to completion so the batch advances.
	if err := run(); err != nil {
		return p.handleFailure(ctx, job, raw, src, err)
	}
	ev := p.buildEvent(job, "success", src)
	out.Event = &ev
	if job.Attempt > 0 {
		p.clearFailure(ctx, *job.BatchID, job.JobID)
	}
	p.releaseUniq(ctx, job)
	return out, nil
}

// buildFairDeferPayload re-frames a fair job for delayed redelivery via the retry
// topic. Unlike buildRetryPayload it PRESERVES the _fair_slot* metadata (so the
// returning copy re-checks the same slot and its lease) and does NOT bump attempt
// — a defer is not a retry and must not consume the job's retry budget.
func buildFairDeferPayload(raw []byte, retryAt time.Time, retryTo string) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["retry_after"] = retryAt.UTC().Format(time.RFC3339)
	m["retry_to"] = retryTo
	return json.Marshal(m)
}

func (p *Processor) handleFailure(ctx context.Context, job protocol.JobMessage, raw []byte, src protocol.SourceCoords, execErr error) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	maxRetries := job.MaxRetries
	if maxRetries == 0 {
		maxRetries = p.Cfg.MaxRetries
	}
	completeAfter := job.CompleteAfterRetries
	if completeAfter == 0 {
		completeAfter = p.Cfg.CompleteAfter
	}

	if nonRetryable(execErr) {
		if job.BatchID != nil && !job.BatchCounted && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, className(execErr), execErr.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		emitJobFailed(job, job.Attempt, className(execErr), execErr.Error())
		emitDLTPublished(job.JobID, deref(job.BatchID), "job", src.Topic)
		return out, nil
	}

	if job.Attempt < maxRetries {
		next := job.Attempt + 1
		tier := p.Cfg.RetryTierFor(next, job.RetryTier)
		delay := p.retryDelay(tier)
		retryAt := p.now().Add(delay)

		p.recordExecutionFailure(ctx, job, execErr, "retrying", retryAt.UTC().Format(time.RFC3339))

		if job.BatchID != nil && !job.BatchCounted && job.Attempt >= completeAfter && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
			job.BatchCounted = true
		}

		retryPayload, err := p.buildRetryPayload(raw, job, retryAt, src.Topic)
		if err != nil {
			return out, err
		}
		out.RetryTopic = p.Cfg.RetryTopic(tier)
		out.RetryKey = job.JobID
		out.RetryPayload = retryPayload
		emitJobRetried(job, job.Attempt+1, out.RetryTopic)
		return out, nil
	}

	if job.BatchID != nil && !job.BatchCounted && job.BatchSeq != nil {
		ev := p.buildEvent(job, "failed", src)
		out.Event = &ev
	}
	p.recordExecutionFailure(ctx, job, execErr, "failed", "")
	if _, goHandler := kbatch.Lookup(job.JobType); goHandler {
		kbatch.RunRetriesExhausted(job, execErr, maxRetries)
	}
	dlt, key := p.dltPayload(jobMap(raw), src.Topic, className(execErr), execErr.Error())
	out.DLTPayload = dlt
	out.DLTKey = key
	p.releaseUniq(ctx, job)
	emitJobFailed(job, job.Attempt, className(execErr), execErr.Error())
	emitDLTPublished(job.JobID, deref(job.BatchID), "job", src.Topic)
	return out, nil
}

func (p *Processor) releaseUniq(ctx context.Context, job protocol.JobMessage) {
	if p.Store == nil {
		return
	}
	_ = p.Store.ReleaseUniqLock(ctx, job.UniqFP, job.JobID)
}

func instrumentSince(started, now time.Time) float64 {
	if started.IsZero() {
		return 0
	}
	return float64(now.Sub(started).Milliseconds())
}

func (p *Processor) buildEvent(job protocol.JobMessage, status string, src protocol.SourceCoords) protocol.EventMessage {
	ev := protocol.EventMessage{
		BatchID:      deref(job.BatchID),
		JobID:        job.JobID,
		Status:       status,
		WorkerClass:  job.WorkerClass,
		OccurredAt:   protocol.NowISO(),
		SrcTopic:     src.Topic,
		SrcPartition: src.Partition,
		SrcOffset:    src.Offset,
	}
	if job.BatchSeq != nil {
		ev.BatchSeq = *job.BatchSeq
	}
	return ev
}

func (p *Processor) buildRetryPayload(raw []byte, job protocol.JobMessage, retryAt time.Time, retryTo string) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["attempt"] = job.Attempt + 1
	m["retry_after"] = retryAt.UTC().Format(time.RFC3339)
	m["retry_to"] = retryTo
	if job.BatchCounted {
		m["batch_counted"] = true
	}
	delete(m, "_fair_slot")
	delete(m, "_fair_slot_id")
	delete(m, "_fair_type")
	return json.Marshal(m)
}

func (p *Processor) dltPayload(base map[string]interface{}, topic, errClass, errMsg string) ([]byte, string) {
	base["dlt_type"] = "job"
	base["dlt_source_topic"] = topic
	base["dlt_error_class"] = errClass
	base["dlt_error_message"] = errMsg
	base["dlt_at"] = protocol.NowISO()
	raw, _ := json.Marshal(base)
	key, _ := base["job_id"].(string)
	if key == "" {
		key = "dlt"
	}
	return raw, key
}

func (p *Processor) retryDelay(tier string) time.Duration {
	sec, ok := p.Cfg.RetryTiers[tier]
	if !ok || sec < 0 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func jobMap(raw []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if m == nil {
		m = map[string]interface{}{}
	}
	return m
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func className(err error) string {
	if he, ok := err.(*kbatch.HandlerError); ok && he.Class != "" {
		return he.Class
	}
	return "GoExecutionError"
}

func nonRetryable(err error) bool {
	switch className(err) {
	case "UnknownHandler", "Overloaded":
		return true
	default:
		return false
	}
}

func (p *Processor) recordFailure(ctx context.Context, e store.FailureEntry) {
	rec := p.Failures
	if rec == nil && p.Store != nil {
		rec = p.Store
	}
	if rec != nil {
		_ = rec.RecordFailure(ctx, e)
	}
}

func (p *Processor) clearFailure(ctx context.Context, batchID, jobID string) {
	rec := p.Failures
	if rec == nil && p.Store != nil {
		rec = p.Store
	}
	if rec != nil {
		_ = rec.ClearFailure(ctx, batchID, jobID)
	}
}

func (p *Processor) recordExecutionFailure(ctx context.Context, job protocol.JobMessage, execErr error, status, nextRetryAt string) {
	if job.BatchID == nil || *job.BatchID == "" {
		return
	}
	p.recordFailure(ctx, store.FailureEntry{
		BatchID:      *job.BatchID,
		JobID:        job.JobID,
		WorkerClass:  job.WorkerClass,
		ErrorClass:   className(execErr),
		ErrorMessage: execErr.Error(),
		Attempt:      job.Attempt,
		Status:       status,
		NextRetryAt:  nextRetryAt,
	})
}

func (p *Processor) markJobStarted(ctx context.Context, job protocol.JobMessage, src protocol.SourceCoords) {
	if p.Liveness == nil {
		return
	}
	meta := liveness.JobMeta{
		JobID:       job.JobID,
		WorkerClass: job.WorkerClass,
		Topic:       src.Topic,
		Partition:   src.Partition,
	}
	if job.BatchID != nil {
		meta.BatchID = *job.BatchID
	}
	p.Liveness.JobStarted(ctx, meta)
}
