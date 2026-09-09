package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/schedule"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

type pushPlan struct {
	jobID   string
	payload map[string]interface{}
	fp      string
}

func (c *Client) chunkSize() int {
	if c.cfg.ProduceChunkSize < 1 {
		return 500
	}
	return c.cfg.ProduceChunkSize
}

// produceInChunks produces records and returns per-input produced flags.
// Deliveries complete out of input order, so on error the flags — not any
// prefix — say which records are durably in Kafka.
func (c *Client) produceInChunks(ctx context.Context, records []kafkaclient.ProduceRecord) ([]bool, error) {
	if len(records) == 0 {
		return nil, nil
	}
	size := c.chunkSize()
	produced := make([]bool, len(records))
	producedTotal := 0
	for i := 0; i < len(records); i += size {
		end := i + size
		if end > len(records) {
			end = len(records)
		}
		outs, err := c.prod.ProduceManySync(ctx, records[i:end])
		for j, o := range outs {
			if o.Err == nil {
				produced[i+j] = true
				producedTotal++
			}
		}
		if err != nil {
			return produced, &PartialProduceError{
				Message:       err.Error(),
				ProducedCount: producedTotal,
				Produced:      produced,
			}
		}
	}
	return produced, nil
}

// scheduleMessages produces to the scheduled topic and writes the schedule
// index. A message counts as produced only when BOTH succeed: the poller fires
// exclusively from index rows, so a Kafka-only record never dispatches and must
// be rolled back by the caller. Each index row carries its own record's
// broker-assigned partition/offset (correlated by identity, never position).
func (c *Client) scheduleMessages(ctx context.Context, messages []protocol.JobMessage, runAt time.Time, batchID string) error {
	if len(messages) == 0 {
		return nil
	}
	topic := c.cfg.resolveTopic(c.cfg.ScheduledTopic)
	size := c.chunkSize()
	produced := make([]bool, len(messages))
	producedTotal := 0

	fail := func(msg string) error {
		return &PartialProduceError{Message: msg, ProducedCount: producedTotal, Produced: produced}
	}

	for i := 0; i < len(messages); i += size {
		end := i + size
		if end > len(messages) {
			end = len(messages)
		}
		chunk := messages[i:end]
		records := make([]kafkaclient.ProduceRecord, len(chunk))
		for j, msg := range chunk {
			raw, err := json.Marshal(msg)
			if err != nil {
				return fail("marshal scheduled job: " + err.Error())
			}
			records[j] = kafkaclient.ProduceRecord{Topic: topic, Key: msg.JobID, Payload: raw}
		}

		outs, perr := c.prod.ProduceManySync(ctx, records)

		entries := make([]schedule.ScheduleEntry, 0, len(chunk))
		okIdx := make([]int, 0, len(chunk))
		for j, o := range outs {
			if o.Err != nil {
				continue
			}
			entries = append(entries, schedule.ScheduleEntry{
				JobID: chunk[j].JobID, RunAt: runAt, BatchID: batchID,
				Partition: o.Delivery.Partition, Offset: o.Delivery.Offset,
			})
			okIdx = append(okIdx, i+j)
		}
		if len(entries) > 0 {
			if werr := c.writeScheduleIndex(ctx, entries, batchID, chunk[0].JobID, len(entries)); werr != nil {
				// Produced to Kafka but not indexed → will never fire. Earlier
				// chunks keep their produced flags; only this chunk rolls back.
				return fail(werr.Error())
			}
			for _, gi := range okIdx {
				produced[gi] = true
			}
			producedTotal += len(okIdx)
		}
		if perr != nil {
			return fail(perr.Error())
		}
	}

	workerClass := messages[0].WorkerClass
	if producedTotal == 1 {
		instrument.ScheduledEnqueued(messages[0].JobID, batchID, workerClass, runAt)
	} else {
		instrument.ScheduledEnqueuedBulk(producedTotal, batchID, workerClass, runAt)
	}
	return nil
}

// producedFlags extracts per-input produced flags from a bulk produce error.
// nil means nothing was durably produced.
func producedFlags(err error) []bool {
	if pe, ok := err.(*PartialProduceError); ok {
		return pe.Produced
	}
	return nil
}

func (b *Batch) planPushes(ctx context.Context, jobType string, payloads []map[string]interface{}) (config.HandlerEntry, []pushPlan, []string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return entry, nil, nil, err
	}
	jobIDs := make([]string, len(payloads))
	for i := range payloads {
		jobIDs[i] = uuid.NewString()
	}
	workerName := workerClassName(entry, jobType)
	claimed, err := b.client.bulkUniqClaims(ctx, entry, workerName, payloads, jobIDs, b.id)
	if err != nil {
		return entry, nil, nil, err
	}
	plans := make([]pushPlan, 0, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			payload = map[string]interface{}{}
		}
		if !claimed[i] {
			jobIDs[i] = ""
			continue
		}
		fp := ""
		if entry.Uniq && b.client.cfg.UniqEnabled {
			fp = uniq.DigestHex(workerName, payload)
		}
		plans = append(plans, pushPlan{jobID: jobIDs[i], payload: payload, fp: fp})
	}
	return entry, plans, jobIDs, nil
}

// rollbackPlans releases uniq locks and returns reserved total_jobs for every
// plan NOT marked produced. produced is indexed 1:1 with plans; nil rolls back
// everything. Rolling back a produced plan would release a live job's uniq lock
// and shrink total_jobs below the completions that will arrive — never guess.
func (b *Batch) rollbackPlans(ctx context.Context, entry config.HandlerEntry, jobType string, plans []pushPlan, produced []bool) {
	unproduced := int64(0)
	for i, p := range plans {
		if i < len(produced) && produced[i] {
			continue
		}
		b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
		unproduced++
	}
	if unproduced > 0 {
		_, _ = b.client.store.AddJobs(ctx, b.id, -unproduced)
	}
}

// PushManyJobs enqueues many manifest jobs into this batch (Ruby push_many).
// Returns job IDs in payload order; empty string marks a uniq-skipped slot.
func (b *Batch) PushManyJobs(ctx context.Context, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := b.planPushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	win, err := b.reserve(ctx, int64(len(plans)))
	if err != nil {
		for _, p := range plans {
			b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
		}
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	records := make([]kafkaclient.ProduceRecord, 0, len(plans))
	for _, p := range plans {
		seq, err := win.take()
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, nil)
			return nil, err
		}
		msg, err := b.client.buildMessage(entry, jobType, p.payload, p.jobID, &b.id, opts, &seq)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, nil)
			return nil, err
		}
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, nil)
			return nil, err
		}
		route := b.client.routeFor(entry, p.jobID, tid, &b.id)
		rec := kafkaclient.ProduceRecord{Topic: route.Topic, Key: route.Key, Payload: raw, Partition: route.Partition}
		records = append(records, rec)
	}

	produced, err := b.client.produceInChunks(ctx, records)
	if err != nil {
		b.rollbackPlans(ctx, entry, jobType, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// PushManyJobsAt schedules many manifest jobs into this batch (Ruby push_many_at).
func (b *Batch) PushManyJobsAt(ctx context.Context, runAt interface{}, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := b.planPushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	win, err := b.reserve(ctx, int64(len(plans)))
	if err != nil {
		for _, p := range plans {
			b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
		}
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	at := clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon)
	messages := make([]protocol.JobMessage, 0, len(plans))
	for _, p := range plans {
		seq, err := win.take()
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, nil)
			return nil, err
		}
		msg, err := b.client.buildMessage(entry, jobType, p.payload, p.jobID, &b.id, opts, &seq)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, nil)
			return nil, err
		}
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		messages = append(messages, msg)
	}

	if err := b.client.scheduleMessages(ctx, messages, at, b.id); err != nil {
		b.rollbackPlans(ctx, entry, jobType, plans, producedFlags(err))
		return nil, err
	}
	return jobIDs, nil
}

// PushJobIn schedules one job after a duration (Ruby push_in).
func (b *Batch) PushJobIn(ctx context.Context, d time.Duration, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return b.PushJobAt(ctx, time.Now().Add(d), jobType, payload, opts)
}

// EnqueueJobIn enqueues a standalone job after a duration.
func (c *Client) EnqueueJobIn(ctx context.Context, d time.Duration, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return c.EnqueueJobAt(ctx, time.Now().Add(d), jobType, payload, opts)
}
