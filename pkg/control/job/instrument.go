package job

import (
	"encoding/json"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func emitJobProcessed(job protocol.JobMessage, durationMs float64) {
	instrument.Emit("job.processed", jobInstrumentPayload(job, nil), durationMs)
}

func emitJobCancelled(job protocol.JobMessage) {
	instrument.Emit("job.cancelled", jobInstrumentPayload(job, nil), 0)
}

func emitJobExpired(job protocol.JobMessage, validTill string) {
	instrument.Emit("job.expired", jobInstrumentPayload(job, map[string]interface{}{
		"valid_till": validTill,
	}), 0)
}

func emitJobRetried(job protocol.JobMessage, nextAttempt int, retryTopic string) {
	instrument.Emit("job.retried", jobInstrumentPayload(job, map[string]interface{}{
		"attempt":      job.Attempt,
		"next_attempt": nextAttempt,
		"retry_topic":  retryTopic,
	}), 0)
}

func emitJobFailed(job protocol.JobMessage, attempt int, errClass, errMsg string) {
	instrument.Emit("job.failed", jobInstrumentPayload(job, map[string]interface{}{
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}), 0)
}

func jobInstrumentPayload(job protocol.JobMessage, extra map[string]interface{}) map[string]interface{} {
	payload := instrument.JobPayload(job.JobID, deref(job.BatchID), job.WorkerClass, extra)
	if job.JobType != "" {
		payload["job_type"] = job.JobType
	}
	return payload
}

func emitDLTPublished(jobID, batchID, dltType, sourceTopic string) {
	instrument.DLTPublished(jobID, batchID, dltType, sourceTopic)
}

func dltMeta(raw []byte) (jobID, batchID, dltType string) {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if s, ok := m["job_id"].(string); ok {
		jobID = s
	}
	if s, ok := m["batch_id"].(string); ok {
		batchID = s
	}
	if s, ok := m["dlt_type"].(string); ok {
		dltType = s
	}
	return jobID, batchID, dltType
}
