package event

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// BatchProducer optionally batches callback produces.
type BatchProducer interface {
	ProduceMany(ctx context.Context, reqs ...kafkaclient.ProduceRequest) error
}

// SyncBatchProducer batches callback produces with per-record outcomes.
// Preferred over BatchProducer: on partial failure only the records that
// actually failed are retried. Preclaimed callbacks skip the consumer's
// ClaimCallback dedup, so re-producing an already-delivered one invokes the
// callback twice — and its claim is already stamped in Redis, so it cannot be
// "demoted" to a claimed callback either.
type SyncBatchProducer interface {
	ProduceManySync(ctx context.Context, records []kafkaclient.ProduceRecord) ([]kafkaclient.ProduceOutcome, error)
}

// Processor applies completion events to the batch ledger.
type Processor struct {
	Cfg      config.Daemon
	Store    *store.RedisStore
	Producer Producer
	// NodeID is written to callback_dispatched_by when a callback is produced
	// (events Lua preclaims stamps without a runner). The callbacks consumer
	// overwrites this with its own node when it actually invokes.
	NodeID string
}

// Outcome per poll batch.
type Outcome struct {
	Callbacks []protocol.CallbackMessage
}

func (p *Processor) ProcessBatch(ctx context.Context, rawEvents [][]byte) (Outcome, error) {
	out := Outcome{}
	events := make([]store.CompletionEvent, 0, len(rawEvents))
	skipped := 0
	for _, raw := range rawEvents {
		var ev protocol.EventMessage
		if err := json.Unmarshal(raw, &ev); err != nil {
			skipped++
			log.Printf("[kbatch-event] skip malformed event: %v", err)
			continue
		}
		if ev.BatchID == "" || ev.BatchSeq <= 0 {
			skipped++
			log.Printf("[kbatch-event] skip invalid event batch_id=%q batch_seq=%d job_id=%q",
				ev.BatchID, ev.BatchSeq, ev.JobID)
			continue
		}
		events = append(events, store.CompletionEvent{
			BatchID: ev.BatchID, JobID: ev.JobID, Status: ev.Status, BatchSeq: ev.BatchSeq,
		})
	}
	if len(events) == 0 {
		if skipped > 0 {
			log.Printf("[kbatch-event] batch had %d invalid event(s) and no valid ones — acknowledging to avoid poison loop", skipped)
		}
		return out, nil
	}
	if skipped > 0 {
		log.Printf("[kbatch-event] skipped %d invalid event(s); processing %d valid", skipped, len(events))
	}

	result, err := p.Store.RecordCompletionsBatch(ctx, events)
	if err != nil {
		return out, err
	}

	// Dropped completions (batch hash absent / malformed) are silent count
	// losses that leave a batch stuck below total. Make them loud + monitorable
	// instead of swallowing them. Common root cause: the daemon reads a different
	// Redis than the producer that created the batch, or the batch TTL expired.
	for _, d := range result.Dropped {
		log.Printf("[kbatch-event] DROPPED completion reason=%s batch_id=%q batch_seq=%d job_id=%q — count lost; batch may never converge (check Redis/topic-prefix parity between producer and daemon)",
			d.Reason, d.BatchID, d.BatchSeq, d.JobID)
		instrument.CompletionDropped(d.BatchID, d.JobID, d.BatchSeq, d.Reason)
	}

	for _, fin := range result.Finished {
		if fin.Batch == nil {
			continue
		}
		instrument.BatchCompleted(
			fin.Batch.ID, fin.Outcome,
			fin.Batch.TotalJobs, fin.Batch.CompletedCount, fin.Batch.FailedCount,
		)
		finishedAt := fin.Batch.FinishedAt
		if finishedAt == "" {
			finishedAt = protocol.NowISO()
		}
		cb := protocol.CallbackMessage{
			BatchID:        fin.Batch.ID,
			Outcome:        fin.Outcome,
			TotalJobs:      fin.Batch.TotalJobs,
			CompletedCount: fin.Batch.CompletedCount,
			FailedCount:    fin.Batch.FailedCount,
			TouchedCount:   fin.Batch.TouchedCount,
			OnSuccess:      fin.Batch.OnSuccess,
			OnComplete:     fin.Batch.OnComplete,
			FinishedAt:     finishedAt,
			CallbackArgs:   protocol.DecodeJSONMap(fin.Batch.CallbackArgs),
			Preclaimed:     true,
		}
		out.Callbacks = append(out.Callbacks, cb)
	}

	replayBatches, err := p.Store.FindReplayCallbackBatches(ctx, result.Replays)
	if err != nil {
		return out, err
	}
	for _, batch := range replayBatches {
		out.Callbacks = append(out.Callbacks, protocol.CallbackMessage{
			BatchID:        batch.ID,
			Outcome:        batch.Status,
			TotalJobs:      batch.TotalJobs,
			CompletedCount: batch.CompletedCount,
			FailedCount:    batch.FailedCount,
			TouchedCount:   batch.TouchedCount,
			OnSuccess:      batch.OnSuccess,
			OnComplete:     batch.OnComplete,
			FinishedAt:     batch.FinishedAt,
			CallbackArgs:   protocol.DecodeJSONMap(batch.CallbackArgs),
		})
	}

	if err := p.produceCallbacks(ctx, out.Callbacks); err != nil {
		return out, err
	}
	return out, nil
}

// callbackProduceAttempts bounds the in-call retries before a callback is parked
// on the dead-letter topic. Kept small so a broker blip is absorbed without
// stalling the events consumer for long.
const callbackProduceAttempts = 3

func (p *Processor) produceCallbacks(ctx context.Context, callbacks []protocol.CallbackMessage) error {
	if len(callbacks) == 0 {
		return nil
	}
	// Preferred path: batch produce with per-record outcomes. Only the records
	// that actually failed are retried (then dead-lettered) — never the whole
	// batch: on a partial failure the successful records ARE durably in Kafka,
	// and re-producing a Preclaimed callback double-invokes it (the consumer
	// skips ClaimCallback dedup for preclaimed messages).
	if sbp, ok := p.Producer.(SyncBatchProducer); ok {
		records := make([]kafkaclient.ProduceRecord, 0, len(callbacks))
		marshalled := true
		for _, cb := range callbacks {
			raw, err := json.Marshal(cb)
			if err != nil {
				marshalled = false
				break
			}
			records = append(records, kafkaclient.ProduceRecord{Topic: p.Cfg.CallbacksTopic, Key: cb.BatchID, Payload: raw})
		}
		if marshalled {
			outs, _ := sbp.ProduceManySync(ctx, records)
			if len(outs) == len(callbacks) {
				for i, cb := range callbacks {
					if outs[i].Err == nil {
						p.recordCallbackRunners(ctx, []protocol.CallbackMessage{cb})
						continue
					}
					if err := p.produceOneCallback(ctx, cb); err != nil {
						p.deadLetterCallback(ctx, cb, err)
					} else {
						p.recordCallbackRunners(ctx, []protocol.CallbackMessage{cb})
					}
				}
				return nil
			}
			// Outcome shape mismatch: fall through to the per-item path, which
			// re-produces everything but is the only option left.
		}
	}
	// Legacy batch path for producers without per-record outcomes. All-or-nothing:
	// on ANY error we cannot know which records are durable, so do NOT fall back
	// to re-producing everything — go per-item only when the batch produce is
	// certain to have written nothing... which we cannot know either. Producers
	// should implement SyncBatchProducer; this path exists for custom test fakes.
	if bp, ok := p.Producer.(BatchProducer); ok {
		reqs := make([]kafkaclient.ProduceRequest, 0, len(callbacks))
		marshalled := true
		for _, cb := range callbacks {
			raw, err := json.Marshal(cb)
			if err != nil {
				marshalled = false
				break
			}
			reqs = append(reqs, kafkaclient.ProduceRequest{Topic: p.Cfg.CallbacksTopic, Key: cb.BatchID, Value: raw})
		}
		if marshalled {
			if err := bp.ProduceMany(ctx, reqs...); err == nil {
				p.recordCallbackRunners(ctx, callbacks)
				return nil
			}
			// fall through to per-item recovery
		}
	}
	// Per-item path: each callback is produced independently with bounded retry;
	// one that still fails is parked on the dead-letter topic so it is never
	// silently lost (the old behavior returned an error → redelivery → the
	// duplicate events were then excluded from replay → callback lost forever).
	for _, cb := range callbacks {
		if err := p.produceOneCallback(ctx, cb); err != nil {
			p.deadLetterCallback(ctx, cb, err)
		} else {
			p.recordCallbackRunners(ctx, []protocol.CallbackMessage{cb})
		}
	}
	return nil
}

func (p *Processor) recordCallbackRunners(ctx context.Context, callbacks []protocol.CallbackMessage) {
	if p == nil || p.Store == nil || p.NodeID == "" {
		return
	}
	for _, cb := range callbacks {
		if cb.BatchID == "" {
			continue
		}
		if err := p.Store.RecordCallbackRunner(ctx, cb.BatchID, p.NodeID); err != nil {
			log.Printf("[kbatch-event] record callback runner batch_id=%s: %v", cb.BatchID, err)
		}
	}
}

func (p *Processor) produceOneCallback(ctx context.Context, cb protocol.CallbackMessage) error {
	raw, err := json.Marshal(cb)
	if err != nil {
		return fmt.Errorf("callback marshal: %w", err)
	}
	for attempt := 1; ; attempt++ {
		err = p.Producer.Produce(ctx, p.Cfg.CallbacksTopic, cb.BatchID, raw)
		if err == nil {
			return nil
		}
		if attempt >= callbackProduceAttempts || ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
}

// deadLetterCallback parks an unproducible callback on the dead-letter topic so a
// completed batch's callback is preserved (operator-visible / replayable) rather
// than lost. Best-effort: if the DLT is unset or also fails, we log loudly.
func (p *Processor) deadLetterCallback(ctx context.Context, cb protocol.CallbackMessage, cause error) {
	log.Printf("[kbatch-event] callback produce failed batch_id=%s outcome=%s: %v — parking on dead-letter",
		cb.BatchID, cb.Outcome, cause)
	instrument.CallbackProduceFailed(cb.BatchID, cb.Outcome, cause.Error())
	if p.Cfg.DeadLetterTopic == "" {
		log.Printf("[kbatch-event] no dead_letter_topic configured — callback for batch_id=%s is LOST", cb.BatchID)
		return
	}
	dlt := map[string]interface{}{
		"batch_id":          cb.BatchID,
		"dlt_type":          "callback_produce_failed",
		"outcome":           cb.Outcome,
		"on_success":        cb.OnSuccess,
		"on_complete":       cb.OnComplete,
		"dlt_error_message": cause.Error(),
	}
	if raw, err := json.Marshal(cb); err == nil {
		dlt["dlt_raw_payload"] = string(raw)
	}
	rawDLT, err := json.Marshal(dlt)
	if err != nil {
		return
	}
	if err := p.Producer.Produce(ctx, p.Cfg.DeadLetterTopic, cb.BatchID, rawDLT); err != nil {
		log.Printf("[kbatch-event] dead-letter callback produce failed batch_id=%s: %v — callback LOST", cb.BatchID, err)
		return
	}
	instrument.DLTPublished("", cb.BatchID, "callback_produce_failed", p.Cfg.CallbacksTopic)
}
