package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func (c *Client) planStandalonePushes(ctx context.Context, jobType string, payloads []map[string]interface{}) (config.HandlerEntry, []pushPlan, []string, error) {
	entry, err := c.lookupHandler(jobType)
	if err != nil {
		return entry, nil, nil, err
	}
	jobIDs := make([]string, len(payloads))
	plans := make([]pushPlan, 0, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			payload = map[string]interface{}{}
		}
		jobID := uuid.NewString()
		skipped, err := c.claimUniq(ctx, entry, jobType, payload, jobID, "")
		if err != nil {
			return entry, nil, nil, err
		}
		if skipped {
			jobIDs[i] = ""
			continue
		}
		fp := ""
		if entry.Uniq && c.cfg.UniqEnabled {
			fp = uniq.DigestHex(workerClassName(entry, jobType), payload)
		}
		plans = append(plans, pushPlan{jobID: jobID, payload: payload, fp: fp})
		jobIDs[i] = jobID
	}
	return entry, plans, jobIDs, nil
}

func (c *Client) rollbackStandalonePlans(entry config.HandlerEntry, jobType string, plans []pushPlan, produced int) {
	for i := produced; i < len(plans); i++ {
		p := plans[i]
		c.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
	}
}

// EnqueueManyJobs enqueues many standalone manifest jobs immediately.
func (c *Client) EnqueueManyJobs(ctx context.Context, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := c.planStandalonePushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	tid := opts.tenantID("")
	records := make([]kafkaclient.ProduceRecord, 0, len(plans))
	for _, p := range plans {
		msg, err := c.buildMessage(entry, jobType, p.payload, p.jobID, nil, opts, nil)
		if err != nil {
			c.rollbackStandalonePlans(entry, jobType, plans, 0)
			return nil, err
		}
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			c.rollbackStandalonePlans(entry, jobType, plans, 0)
			return nil, err
		}
		route := c.routeFor(entry, p.jobID, tid, nil)
		records = append(records, kafkaclient.ProduceRecord{Topic: route.Topic, Key: route.Key, Payload: raw, Partition: route.Partition})
	}

	produced, err := c.produceInChunks(ctx, records)
	if err != nil {
		c.rollbackStandalonePlans(entry, jobType, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// EnqueueManyJobsAt schedules many standalone manifest jobs (Ruby enqueue_many_at).
func (c *Client) EnqueueManyJobsAt(ctx context.Context, runAt interface{}, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := c.planStandalonePushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	tid := opts.tenantID("")
	at := clampRunAt(runAt, c.cfg.MaxScheduleHorizon)
	messages := make([]protocol.JobMessage, 0, len(plans))
	for _, p := range plans {
		msg, err := c.buildMessage(entry, jobType, p.payload, p.jobID, nil, opts, nil)
		if err != nil {
			c.rollbackStandalonePlans(entry, jobType, plans, 0)
			return nil, err
		}
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		messages = append(messages, msg)
	}

	if err := c.scheduleMessages(ctx, messages, at, ""); err != nil {
		produced := 0
		if pe, ok := err.(*PartialProduceError); ok {
			produced = pe.ProducedCount
		}
		c.rollbackStandalonePlans(entry, jobType, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// EnqueueManyJobsIn schedules many standalone jobs after a duration.
func (c *Client) EnqueueManyJobsIn(ctx context.Context, d time.Duration, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	return c.EnqueueManyJobsAt(ctx, time.Now().Add(d), jobType, payloads, opts)
}
