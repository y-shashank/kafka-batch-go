package client

import (
	"context"

	"github.com/y-shashank/kafka-batch-go/pkg/cancellation"
)

// CancelBatch cancels a batch by id (Ruby KafkaBatch::Batch.cancel).
func (c *Client) CancelBatch(ctx context.Context, id string) error {
	row, err := c.store.FindBatch(ctx, id)
	if err != nil {
		return err
	}
	if row == nil {
		return BatchNotFoundError{BatchID: id}
	}
	if err := c.store.CancelBatch(ctx, id); err != nil {
		return err
	}
	// Same-process workers/schedule see the cancel immediately (Ruby CancellationCache#add).
	cancellation.AddToProcess(id)
	return nil
}

// Cancel cancels this open batch.
func (b *Batch) Cancel(ctx context.Context) error {
	return b.client.CancelBatch(ctx, b.id)
}
