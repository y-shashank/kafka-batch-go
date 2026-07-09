package client

import (
	"context"
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
	return c.store.CancelBatch(ctx, id)
}

// Cancel cancels this open batch.
func (b *Batch) Cancel(ctx context.Context) error {
	return b.client.CancelBatch(ctx, b.id)
}
