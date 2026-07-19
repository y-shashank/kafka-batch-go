package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

type kafkaProducer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

func produceEventWithRetry(ctx context.Context, cfg config.Daemon, prod kafkaProducer, ev *protocol.EventMessage) error {
	if ev == nil {
		return nil
	}
	maxAttempts := cfg.EventEmitRetries
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoff := cfg.EventEmitBackoff
	raw, _ := json.Marshal(ev)
	key := fmt.Sprintf("%s/%d", ev.SrcTopic, ev.SrcPartition)

	attempts := 0
	for {
		err := prod.Produce(ctx, cfg.EventsTopic, key, raw)
		if err == nil {
			return nil
		}
		attempts++
		if attempts > maxAttempts {
			return err
		}
		instrument.JobEmitRetried(ev.JobID, ev.BatchID, attempts, err)
		if backoff > 0 {
			time.Sleep(time.Duration(attempts) * backoff)
		}
	}
}

// parkFailedEventEmit writes a recoverable DLT payload when the events topic
// reject completion emits after retries. SuperFetch has already acked the job
// offset, so without this park the completion is silently lost.
func parkFailedEventEmit(ctx context.Context, cfg config.Daemon, prod kafkaProducer, ev *protocol.EventMessage, emitErr error) error {
	if ev == nil {
		return nil
	}
	if cfg.DeadLetterTopic == "" {
		return fmt.Errorf("dead_letter_topic not configured")
	}
	errMsg := ""
	if emitErr != nil {
		errMsg = emitErr.Error()
	}
	dlt := map[string]interface{}{
		"dlt_type":          "event_emit_failed",
		"dlt_error_message": errMsg,
		"batch_id":          ev.BatchID,
		"job_id":            ev.JobID,
		"batch_seq":         ev.BatchSeq,
		"status":            ev.Status,
		"worker_class":      ev.WorkerClass,
		"src_topic":         ev.SrcTopic,
		"src_partition":     ev.SrcPartition,
		"src_offset":        ev.SrcOffset,
		"occurred_at":       ev.OccurredAt,
		"event":             ev,
	}
	raw, err := json.Marshal(dlt)
	if err != nil {
		return fmt.Errorf("marshal event_emit_failed DLT: %w", err)
	}
	key := ev.BatchID
	if key == "" {
		key = ev.JobID
	}
	if key == "" {
		key = "event_emit_failed"
	}
	if err := prod.Produce(ctx, cfg.DeadLetterTopic, key, raw); err != nil {
		return err
	}
	instrument.DLTPublished(ev.JobID, ev.BatchID, "event_emit_failed", ev.SrcTopic)
	log.Printf("[kbatch] event emit failed — parked on DLT batch_id=%s job_id=%s seq=%d err=%v",
		ev.BatchID, ev.JobID, ev.BatchSeq, emitErr)
	return nil
}

// emitEventOrPark tries events_topic, then parks on DLT. Returns nil only when
// the completion is durable on events OR DLT (never silently drop).
func emitEventOrPark(ctx context.Context, cfg config.Daemon, prod kafkaProducer, ev *protocol.EventMessage) error {
	if ev == nil {
		return nil
	}
	if err := produceEventWithRetry(ctx, cfg, prod, ev); err != nil {
		if dltErr := parkFailedEventEmit(ctx, cfg, prod, ev, err); dltErr != nil {
			return fmt.Errorf("event emit failed (%v) and DLT park failed: %w", err, dltErr)
		}
	}
	return nil
}

func applyJobOutcome(ctx context.Context, cfg config.Daemon, prod kafkaProducer, out job.Outcome) error {
	if err := applyJobSideEffects(ctx, cfg, prod, out); err != nil {
		return err
	}
	if !out.CommitOffset {
		return fmt.Errorf("job not committed")
	}
	return nil
}

// ApplyJobSideEffects publishes event/retry/DLT without gating on CommitOffset.
// Used by SuperFetch after the Kafka offset is already acked.
func ApplyJobSideEffects(ctx context.Context, cfg config.Daemon, prod kafkaProducer, out job.Outcome) error {
	return applyJobSideEffects(ctx, cfg, prod, out)
}

func applyJobSideEffects(ctx context.Context, cfg config.Daemon, prod kafkaProducer, out job.Outcome) error {
	// Bound emit so a wedged broker cannot hold a SuperFetch ClaimWindow forever.
	produceCtx, cancel := context.WithTimeout(ctx, jobProduceTimeout)
	defer cancel()

	if out.Event != nil {
		if err := emitEventOrPark(produceCtx, cfg, prod, out.Event); err != nil {
			return err
		}
	}
	if out.RetryPayload != nil {
		if err := prod.Produce(produceCtx, out.RetryTopic, out.RetryKey, out.RetryPayload); err != nil {
			return err
		}
	}
	if out.DLTPayload != nil {
		if err := prod.Produce(produceCtx, cfg.DeadLetterTopic, out.DLTKey, out.DLTPayload); err != nil {
			return err
		}
	}
	return nil
}

const (
	retryProduceTimeout = 30 * time.Second
	jobProduceTimeout   = 30 * time.Second
)

func applyRetryOutcome(ctx context.Context, cfg config.Daemon, prod kafkaProducer, out retry.Outcome, src protocol.SourceCoords) error {
	if out.Event != nil {
		if err := emitEventOrPark(ctx, cfg, prod, out.Event); err != nil {
			return err
		}
	}
	if out.ProduceBody != nil {
		produceCtx, cancel := context.WithTimeout(ctx, retryProduceTimeout)
		err := prod.Produce(produceCtx, out.ProduceTopic, out.ProduceKey, out.ProduceBody)
		cancel()
		if err != nil {
			return err
		}
		if out.ProduceTopic != src.Topic {
			log.Printf("[kbatch-retry] dispatched job_id=%s to %s (from %s partition=%d offset=%d)",
				out.ProduceKey, out.ProduceTopic, src.Topic, src.Partition, src.Offset)
		}
	}
	if out.DLTPayload != nil {
		if err := prod.Produce(ctx, cfg.DeadLetterTopic, out.DLTKey, out.DLTPayload); err != nil {
			return err
		}
		emitRetryDLT(out.DLTPayload, src.Topic)
	}
	if out.Pause {
		return &retryPausedError{duration: out.PauseFor}
	}
	return nil
}

// fairBackpressurePause matches Ruby Fairness::Dispatcher::BACKPRESSURE_PAUSE_MS.
const fairBackpressurePause = 250 * time.Millisecond

// fairBackpressureError signals a tenant's Redis ready window is full. The
// dispatch consumer pauses the ingest partition briefly without committing.
type fairBackpressureError struct {
	lane     string
	tenantID string
	duration time.Duration
}

func (e *fairBackpressureError) Error() string {
	return fmt.Sprintf("fair ingest backpressure lane=%s tenant=%s", e.lane, e.tenantID)
}

// retryPausedError signals that a retry message is not yet due. The retry
// consumer loop sleeps for duration outside the handler so other partitions
// are not blocked and poll cycles continue promptly after the wait.
type retryPausedError struct {
	duration time.Duration
}

func (e *retryPausedError) Error() string { return "retry paused" }

func pauseForRetry(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func emitRetryDLT(raw []byte, sourceTopic string) {
	jobID, batchID, dltType := dltMeta(raw)
	if dltType == "expired" {
		workerClass, validTill := "", ""
		var m map[string]interface{}
		if json.Unmarshal(raw, &m) == nil {
			if s, ok := m["worker_class"].(string); ok {
				workerClass = s
			}
			if s, ok := m["valid_till"].(string); ok {
				validTill = s
			}
		}
		instrument.JobExpired(jobID, batchID, workerClass, validTill)
	}
	instrument.DLTPublished(jobID, batchID, dltType, sourceTopic)
}

func dltMeta(raw []byte) (jobID, batchID, dltType string) {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if s, ok := m["job_id"].(string); ok {
		jobID = s
	}
	if s, ok := m["batch_id"].(string); ok {
		batchID = s
	}
	if s, ok := m["dlt_type"].(string); ok {
		dltType = s
	}
	return jobID, batchID, dltType
}
