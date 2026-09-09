package kafkaclient

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Delivery holds broker-assigned coordinates after a sync produce.
type Delivery struct {
	Partition int32
	Offset    int64
}

// ProduceSync produces one record and returns delivery coordinates.
func (c *Client) ProduceSync(ctx context.Context, topic, key string, payload []byte, partition *int32) (Delivery, error) {
	r := kgo.Record{Topic: topic, Key: []byte(key), Value: payload, Partition: -1}
	if partition != nil && *partition >= 0 {
		r.Partition = *partition
	}
	results := c.inner.ProduceSync(ctx, &r)
	for _, res := range results {
		if res.Err != nil {
			return Delivery{}, res.Err
		}
		if res.Record != nil {
			return Delivery{Partition: res.Record.Partition, Offset: res.Record.Offset}, nil
		}
	}
	return Delivery{}, nil
}

// ProduceOutcome pairs one input record's broker-assigned coordinates with its
// per-record produce error. Err == nil means the record is durably produced and
// Delivery is valid.
type ProduceOutcome struct {
	Delivery Delivery
	Err      error
}

// ProduceManySync pipelines multiple records and returns one outcome per input,
// in input order. franz-go's ProduceSync completes promises in delivery order,
// not submission order (keyed records hash to different partitions/brokers), so
// outcomes are correlated back to inputs by record identity — never by result
// position. The returned error is the first per-record error as a convenience;
// callers that partially roll back MUST consult each outcome's Err instead of
// assuming a successful prefix.
func (c *Client) ProduceManySync(ctx context.Context, records []ProduceRecord) ([]ProduceOutcome, error) {
	if len(records) == 0 {
		return nil, nil
	}
	krecs := make([]*kgo.Record, len(records))
	idx := make(map[*kgo.Record]int, len(records))
	for i, pr := range records {
		r := &kgo.Record{Topic: pr.Topic, Key: []byte(pr.Key), Value: pr.Payload, Partition: -1}
		if pr.Partition != nil && *pr.Partition >= 0 {
			r.Partition = *pr.Partition
		}
		krecs[i] = r
		idx[r] = i
	}
	results := c.inner.ProduceSync(ctx, krecs...)
	return correlateOutcomes(len(records), idx, results)
}

// correlateOutcomes maps completion-ordered produce results back to input
// positions by record identity.
func correlateOutcomes(n int, idx map[*kgo.Record]int, results kgo.ProduceResults) ([]ProduceOutcome, error) {
	out := make([]ProduceOutcome, n)
	got := make([]bool, n)
	var firstErr error
	for _, res := range results {
		if res.Record == nil {
			if firstErr == nil && res.Err != nil {
				firstErr = res.Err
			}
			continue
		}
		i, ok := idx[res.Record]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("produce result for unknown record topic=%s", res.Record.Topic)
			}
			continue
		}
		got[i] = true
		if res.Err != nil {
			out[i].Err = res.Err
			if firstErr == nil {
				firstErr = res.Err
			}
			continue
		}
		out[i].Delivery = Delivery{Partition: res.Record.Partition, Offset: res.Record.Offset}
	}
	// A record with no result at all is not confirmed durable: fail it rather
	// than let a zero-value Delivery masquerade as partition 0 / offset 0.
	for i := range out {
		if !got[i] && out[i].Err == nil {
			out[i].Err = fmt.Errorf("produce result missing for record %d", i)
			if firstErr == nil {
				firstErr = out[i].Err
			}
		}
	}
	return out, firstErr
}

// ProduceRecord is one message for bulk produce.
type ProduceRecord struct {
	Topic     string
	Key       string
	Payload   []byte
	Partition *int32
}
