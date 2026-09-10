package retry

import (
	"context"
	"encoding/json"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/retrycancel"
)

// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Processor handles retry-tier messages.
type Processor struct {
	Producer Producer
	Cancel   *retrycancel.Store
	Now      func() time.Time
	MaxPause time.Duration
}

// Outcome for one retry message.
type Outcome struct {
	CommitOffset bool
	Pause        bool
	PauseFor     time.Duration
	ProduceTopic string
	ProduceKey   string
	ProduceBody  []byte
	DLTPayload   []byte
	DLTKey       string
	Event        *protocol.EventMessage
	// AckCancelJobID, when non-empty, is a cancelled job whose id must be removed
	// from the cancel set — but only AFTER the outcome is durably produced and the
	// record is committed. Acknowledging inside Process (before the failed event /
	// DLT is durable) let a produce failure redeliver the record with the id
	// already gone, so ShouldSkip would return false and the cancelled job would
	// run. The caller acks this only once applyRetryOutcome succeeds.
	AckCancelJobID string
	// Failure, when non-empty, is a per-job failure row to persist for the Web UI
	// (expired-in-retry job). Mirrors the job/expiry consumers; the default
	// Redis-backed store has no FailureRecorder so this is a no-op there.
	Failure *jobexpiry.FailureRecord
}

func (p *Processor) Process(ctx context.Context, raw []byte, src protocol.SourceCoords) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		// A literal JSON `null` (or a null Kafka value) unmarshals WITHOUT error
		// into a nil map. Treat it as malformed like an unmarshal error — routing
		// via dltRaw (which preserves the raw bytes) — so it can't reach dltMap's
		// nil-map write and panic → poison-pill the whole retry partition.
		dlt, key := dltRaw(raw, src.Topic)
		out.DLTPayload = dlt
		out.DLTKey = key
		return out, nil
	}

	jobID, _ := m["job_id"].(string)
	if p.Cancel != nil && p.Cancel.ShouldSkip(ctx, src.Topic, src.Partition, src.Offset, jobID) {
		if ev := failedEvent(m, src); ev != nil {
			out.Event = ev
		}
		// Defer the cancel-set Acknowledge until after the record is durably
		// handled/committed (see Outcome.AckCancelJobID). Acknowledging here would
		// let a produce failure redeliver a cancelled job that then runs.
		out.AckCancelJobID = jobID
		return out, nil
	}

	retryTo, _ := m["retry_to"].(string)
	if retryTo == "" {
		if ev := failedEvent(m, src); ev != nil {
			out.Event = ev
		}
		dlt, key := dltMap(m, raw, src.Topic)
		out.DLTPayload = dlt
		out.DLTKey = key
		return out, nil
	}

	if validTill, _ := m["valid_till"].(string); jobexpiry.Expired(validTill, p.now()) {
		drop := jobexpiry.BuildDrop(raw, src, p.now())
		out.Event = drop.Event
		out.DLTPayload = drop.DLTPayload
		out.DLTKey = drop.DLTKey
		// Persist the failure row (Web UI parity with the job/expiry consumers).
		// The other two expiry paths record this; retry previously dropped it.
		out.Failure = drop.Failure
		return out, nil
	}

	retryAfter := parseTime(m["retry_after"])
	wait := time.Duration(0)
	if retryAfter != nil {
		wait = retryAfter.Sub(p.now())
	}
	if wait > 0 {
		if wait > p.MaxPause {
			wait = p.MaxPause
		}
		out.CommitOffset = false
		out.Pause = true
		out.PauseFor = wait
		return out, nil
	}

	delete(m, "retry_after")
	delete(m, "retry_to")
	// Preserve attempt and mirror as retry_count for handlers (Ruby Worker#retry_count).
	if _, ok := m["attempt"]; ok {
		m["retry_count"] = m["attempt"]
	}
	body, _ := json.Marshal(m)
	key, _ := m["job_id"].(string)
	out.ProduceTopic = retryTo
	out.ProduceKey = key
	out.ProduceBody = body
	return out, nil
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func parseTime(v interface{}) *time.Time {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		t, err := time.Parse(layout, s)
		if err == nil {
			return &t
		}
	}
	return nil
}

func dltRaw(raw []byte, topic string) ([]byte, string) {
	m := map[string]interface{}{
		"dlt_type": "retry_routing", "dlt_source_topic": topic,
		"dlt_raw_payload": string(raw), "dlt_at": protocol.NowISO(),
	}
	b, _ := json.Marshal(m)
	return b, "retry"
}

func dltMap(m map[string]interface{}, raw []byte, topic string) ([]byte, string) {
	if m == nil {
		// Defensive: never write into a nil map (panics). Callers guard against
		// this, but a null payload reaching here must degrade, not crash.
		m = map[string]interface{}{}
	}
	m["dlt_type"] = "retry_routing"
	m["dlt_source_topic"] = topic
	m["dlt_raw_payload"] = string(raw)
	m["dlt_at"] = protocol.NowISO()
	b, _ := json.Marshal(m)
	key, _ := m["job_id"].(string)
	if key == "" {
		key = "retry"
	}
	return b, key
}

func failedEvent(m map[string]interface{}, src protocol.SourceCoords) *protocol.EventMessage {
	batchID, _ := m["batch_id"].(string)
	seqF, _ := m["batch_seq"].(float64)
	if batchID == "" || seqF <= 0 {
		return nil
	}
	return &protocol.EventMessage{
		BatchID: batchID, JobID: str(m["job_id"]), Status: "failed",
		WorkerClass: str(m["worker_class"]), OccurredAt: protocol.NowISO(),
		SrcTopic: src.Topic, SrcPartition: src.Partition, SrcOffset: src.Offset,
		BatchSeq: int64(seqF),
	}
}

func str(v interface{}) string {
	s, _ := v.(string)
	return s
}
