package client

import (
	"context"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func (c *Client) bulkUniqClaims(ctx context.Context, entry config.HandlerEntry, workerName string, payloads []map[string]interface{}, jobIDs []string, batchID string) ([]bool, error) {
	out := make([]bool, len(payloads))
	if len(payloads) == 0 {
		return out, nil
	}
	if !entry.Uniq || !c.cfg.UniqEnabled {
		for i := range out {
			out[i] = true
		}
		return out, nil
	}

	inputs := make([]uniq.ClaimInput, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			payload = map[string]interface{}{}
		}
		inputs[i] = uniq.ClaimInput{
			WorkerClassName: workerName,
			Payload:         payload,
			JobID:           jobIDs[i],
		}
	}
	claimed := c.uniq.ClaimMany(ctx, inputs)
	for i, ok := range claimed {
		if ok {
			out[i] = true
			continue
		}
		instrument.JobUniqSkipped(workerName, payloads[i], jobIDs[i], batchID)
		if c.cfg.UniqOnDuplicate == "raise" {
			return claimed, DuplicateJobError{WorkerClass: workerName}
		}
		out[i] = false
	}
	return out, nil
}
