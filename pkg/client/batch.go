package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// noteBatchRejection surfaces a sealed/cancelled-batch push rejection via
// instrumentation so a caller that ignores the returned error still leaves a
// trace (the create-sealed-then-push race silently drops jobs otherwise).
func noteBatchRejection(err error, batchID, jobType string) {
	var closed BatchClosedError
	if errors.As(err, &closed) {
		instrument.BatchPushRejected(batchID, jobType, closed.Reason)
	}
}

// BatchOptions configures batch creation.
type BatchOptions struct {
	ID           string
	OnSuccess    string
	OnComplete   string
	Meta         map[string]interface{}
	CallbackArgs map[string]interface{}
	Description  string
	TenantID     string
}

// PushOptions configures a single enqueue/push.
type PushOptions struct {
	JobID     string
	TenantID  string
	ValidTill string
}

func (o PushOptions) jobID() string {
	if o.JobID != "" {
		return o.JobID
	}
	return uuid.NewString()
}

func (o PushOptions) tenantID(batchDefault string) string {
	if o.TenantID != "" {
		return o.TenantID
	}
	return batchDefault
}

// Batch is one open batch ledger (Ruby KafkaBatch::Batch).
type Batch struct {
	client      *Client
	id          string
	onSuccess   string
	onComplete  string
	meta        map[string]interface{}
	description string
	tenantID    string
}

// ID returns the batch uuid.
func (b *Batch) ID() string { return b.id }

// PushJob enqueues one manifest job into this batch.
func (b *Batch) PushJob(ctx context.Context, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniq(ctx, entry, jobType, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	win, err := b.reserve(ctx, 1)
	if err != nil {
		noteBatchRejection(err, b.id, jobType)
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	seq, err := win.take()
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	msg, err := b.client.buildMessage(entry, jobType, payload, jobID, &b.id, opts, &seq)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	route := b.client.routeFor(entry, jobID, opts.tenantID(b.tenantID), &b.id)
	if err := b.client.produceJob(ctx, route, msg); err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// PushJobAt schedules one manifest job into this batch.
func (b *Batch) PushJobAt(ctx context.Context, runAt interface{}, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniq(ctx, entry, jobType, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	win, err := b.reserve(ctx, 1)
	if err != nil {
		noteBatchRejection(err, b.id, jobType)
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	seq, err := win.take()
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	msg, err := b.client.buildMessage(entry, jobType, payload, jobID, &b.id, opts, &seq)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	if err := b.client.scheduleMessage(ctx, msg, clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon), b.id); err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// Seal opens the completion gate (Ruby seal!).
func (b *Batch) Seal(ctx context.Context) (*SealResult, error) {
	result, err := b.client.store.SealBatch(ctx, b.id)
	if err != nil {
		return nil, err
	}
	switch result.Status {
	case "not_found":
		return nil, BatchNotFoundError{BatchID: b.id}
	case "done":
		if result.Batch != nil {
			if err := b.client.produceCallback(ctx, result.Batch, result.Outcome); err != nil {
				return &SealResult{Status: result.Status, Outcome: result.Outcome}, fmt.Errorf("callback produce: %w", err)
			}
		}
	}
	row, _ := b.client.store.FindBatch(ctx, b.id)
	total := int64(0)
	if row != nil {
		total = row.TotalJobs
	}
	instrument.BatchSealed(b.id, total)
	return &SealResult{Status: result.Status, Outcome: result.Outcome}, nil
}

// SealResult summarizes a seal operation.
type SealResult struct {
	Status  string
	Outcome string
}

// seqWindow is a batch_seq range reserved by one AddJobs call. Each push call
// iterates its OWN window locally: the ranges come from an atomic Redis INCRBY,
// so concurrent pushes into the same *Batch get disjoint windows and never
// share cursor state (the old struct-level cursor was overwritten by every
// reserve — concurrent pushes silently reused or skipped batch_seq values,
// corrupting the completion bitmap).
type seqWindow struct {
	next, end int64
}

// take returns the next batch_seq from the window.
func (w *seqWindow) take() (int64, error) {
	if w.next == 0 || w.end == 0 {
		return 0, fmt.Errorf("no reserved batch_seq slots")
	}
	if w.next > w.end {
		return 0, fmt.Errorf("reserved too few batch_seq slots")
	}
	s := w.next
	w.next++
	return s, nil
}

func (b *Batch) reserve(ctx context.Context, count int64) (seqWindow, error) {
	res, err := b.client.store.AddJobs(ctx, b.id, count)
	if err != nil {
		return seqWindow{}, err
	}
	switch res.Status {
	case "closed":
		return seqWindow{}, BatchClosedError{BatchID: b.id, Reason: "closed"}
	case "cancelled":
		return seqWindow{}, BatchClosedError{BatchID: b.id, Reason: "cancelled"}
	case "not_found":
		return seqWindow{}, BatchNotFoundError{BatchID: b.id}
	}
	return seqWindow{next: res.SeqStart, end: res.SeqEnd}, nil
}
