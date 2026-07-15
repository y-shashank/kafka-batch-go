package event

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

// Processor applies completion events to the batch ledger.
type Processor struct {
	Cfg      config.Daemon
	Store    *store.RedisStore
	Producer Producer
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

func (p *Processor) produceCallbacks(ctx context.Context, callbacks []protocol.CallbackMessage) error {
	if len(callbacks) == 0 {
		return nil
	}
	reqs := make([]kafkaclient.ProduceRequest, 0, len(callbacks))
	for _, cb := range callbacks {
		raw, err := json.Marshal(cb)
		if err != nil {
			return fmt.Errorf("callback marshal: %w", err)
		}
		reqs = append(reqs, kafkaclient.ProduceRequest{
			Topic: p.Cfg.CallbacksTopic,
			Key:   cb.BatchID,
			Value: raw,
		})
	}
	if bp, ok := p.Producer.(BatchProducer); ok {
		if err := bp.ProduceMany(ctx, reqs...); err != nil {
			return fmt.Errorf("callback produce: %w", err)
		}
		return nil
	}
	for _, req := range reqs {
		if err := p.Producer.Produce(ctx, req.Topic, req.Key, req.Value); err != nil {
			return fmt.Errorf("callback produce: %w", err)
		}
	}
	return nil
}
