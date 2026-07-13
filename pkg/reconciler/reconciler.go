package reconciler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Producer publishes callback messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Result is the outcome of one reconciler sweep.
type Result int

const (
	ResultCompleted Result = iota
	ResultLockSkipped
	ResultFailed
)

// Run sweeps stuck-running and lost-callback batches (Ruby KafkaBatch::Reconciler.run).
func Run(ctx context.Context, cfg config.Daemon, st *store.RedisStore, prod Producer, triggeredBy string) Result {
	start := time.Now()
	collector := NewCollector(triggeredBy)

	interval := cfg.ReconciliationInterval
	if interval <= 0 {
		interval = 300 * time.Second
	}
	lockTTL := cfg.ReconcilerLockTTL
	if lockTTL <= 0 {
		lockTTL = 600 * time.Second
	}
	max := cfg.MaxReconcilePerRun
	if max < 1 {
		max = 100
	}

	var ran bool
	lockOK, err := st.WithReconcilerLock(ctx, lockTTL, func() error {
		ran = true
		threshold := time.Now().Add(-interval)

		staleAll, err := st.StaleBatches(ctx, threshold)
		if err != nil {
			return err
		}
		stale := capSlice(staleAll, max)
		if len(staleAll) > max {
			log.Printf("[kbatch-reconciler] %d stuck-running batches; processing first %d", len(staleAll), max)
		}
		log.Printf("[kbatch-reconciler] found %d stuck-running, processing %d", len(staleAll), len(stale))

		lostAll, err := st.DoneBatchesWithoutCallback(ctx, threshold)
		if err != nil {
			return err
		}
		lost := capSlice(lostAll, max)
		if len(lostAll) > max {
			log.Printf("[kbatch-reconciler] %d lost-callback batches; processing first %d", len(lostAll), max)
		}
		log.Printf("[kbatch-reconciler] found %d lost-callback, processing %d", len(lostAll), len(lost))

		collector.Identify(len(staleAll), stale, len(lostAll), lost)

		for _, batch := range stale {
			outcome := reconcileRunning(ctx, st, prod, cfg, batch)
			collector.RecordStale(batch.ID, outcome, batch)
		}
		for _, batch := range lost {
			outcome := refireCallback(ctx, st, prod, cfg, batch)
			collector.RecordLost(batch.ID, outcome, batch)
		}

		_ = st.ReconcileBatchCounts(ctx)
		return nil
	})
	if err != nil {
		log.Printf("[kbatch-reconciler] sweep error: %v", err)
	}
	if !ran || !lockOK {
		SaveSkip(ctx, st)
		return ResultLockSkipped
	}
	if err != nil {
		return ResultFailed
	}

	duration := time.Since(start)
	summary := collector.Finish(duration)
	SaveLast(ctx, st, summary)
	instrument.ReconcilerRan(summary.RecoveredStale, summary.RefiredLost, duration, triggeredBy)
	log.Printf("[kbatch-reconciler] done in %.2fs", duration.Seconds())
	return ResultCompleted
}

type staleOutcome string

const (
	outcomeRecoveredRunning staleOutcome = "recovered_running"
	outcomeRecoveredEmpty   staleOutcome = "recovered_empty"
	outcomeSkippedGone      staleOutcome = "skipped_gone"
	outcomeSkippedNotRunning staleOutcome = "skipped_not_running"
	outcomeSkippedOpen      staleOutcome = "skipped_open"
	outcomeSkippedInProgress staleOutcome = "skipped_in_progress"
	outcomeProduceFailed    staleOutcome = "produce_failed"
)

type lostOutcome string

const (
	outcomeRefiredLost   lostOutcome = "refired_lost"
	outcomeSkippedNotDone lostOutcome = "skipped_not_done"
	outcomeLostProduceFailed lostOutcome = "produce_failed"
)

func reconcileRunning(ctx context.Context, st *store.RedisStore, prod Producer, cfg config.Daemon, batch *store.Batch) staleOutcome {
	id := batch.ID
	total := batch.TotalJobs
	done := batch.CompletedCount + batch.FailedCount

	fresh, err := st.FindBatch(ctx, id)
	if err != nil || fresh == nil {
		return outcomeSkippedGone
	}
	if fresh.Status != "running" {
		return outcomeSkippedNotRunning
	}
	batch = fresh

	if batch.LockedAt == "" {
		return outcomeSkippedOpen
	}

	if total == 0 {
		if ok, _ := st.MarkFinishedIfRunning(ctx, id, "success"); !ok {
			return outcomeSkippedNotRunning
		}
		batch.Status = "success"
		if !produceCallback(ctx, prod, cfg, batch, "success", false) {
			return outcomeProduceFailed
		}
		return outcomeRecoveredEmpty
	}

	if done < total {
		return outcomeSkippedInProgress
	}

	outcome := "success"
	if batch.FailedCount > 0 {
		outcome = "complete"
	}
	if ok, _ := st.MarkFinishedIfRunning(ctx, id, outcome); !ok {
		return outcomeSkippedNotRunning
	}
	batch.Status = outcome
	if !produceCallback(ctx, prod, cfg, batch, outcome, false) {
		return outcomeProduceFailed
	}
	return outcomeRecoveredRunning
}

func refireCallback(ctx context.Context, st *store.RedisStore, prod Producer, cfg config.Daemon, batch *store.Batch) lostOutcome {
	fresh, err := st.FindBatch(ctx, batch.ID)
	if err != nil || fresh == nil {
		return outcomeSkippedNotDone
	}
	if fresh.Status != "success" && fresh.Status != "complete" {
		return outcomeSkippedNotDone
	}
	if produceCallback(ctx, prod, cfg, fresh, fresh.Status, true) {
		return outcomeRefiredLost
	}
	return outcomeLostProduceFailed
}

func produceCallback(ctx context.Context, prod Producer, cfg config.Daemon, batch *store.Batch, outcome string, reconciled bool) bool {
	cb := protocol.CallbackMessage{
		BatchID:        batch.ID,
		Outcome:        outcome,
		TotalJobs:      batch.TotalJobs,
		CompletedCount: batch.CompletedCount,
		FailedCount:    batch.FailedCount,
		OnSuccess:      batch.OnSuccess,
		OnComplete:     batch.OnComplete,
		FinishedAt:     batch.FinishedAt,
		Reconciled:     reconciled,
		CallbackArgs:   protocol.DecodeJSONMap(batch.CallbackArgs),
	}
	if cb.FinishedAt == "" {
		cb.FinishedAt = protocol.NowISO()
	}
	raw, err := json.Marshal(cb)
	if err != nil {
		log.Printf("[kbatch-reconciler] marshal callback batch_id=%s: %v", batch.ID, err)
		return false
	}
	if err := prod.Produce(ctx, cfg.CallbacksTopic, batch.ID, raw); err != nil {
		log.Printf("[kbatch-reconciler] produce callback batch_id=%s: %v", batch.ID, err)
		return false
	}
	return true
}

func capSlice[T any](in []T, max int) []T {
	if len(in) <= max {
		return in
	}
	return in[:max]
}
