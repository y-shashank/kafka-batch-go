package jobexpiry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestBuildDropExpiredDLT(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "valid_till": "2000-01-01T00:00:00Z",
	})
	src := protocol.SourceCoords{Topic: "jobs", Partition: 1, Offset: 9}
	out := BuildDrop(raw, src, now)
	var m map[string]interface{}
	if err := json.Unmarshal(out.DLTPayload, &m); err != nil {
		t.Fatal(err)
	}
	if m["dlt_type"] != "expired" {
		t.Fatalf("dlt_type %v", m["dlt_type"])
	}
	if m["dlt_error_class"] != ExpiredErrorClass {
		t.Fatalf("class %v", m["dlt_error_class"])
	}
}

func TestBuildDropEmitsEventWhenNotCounted(t *testing.T) {
	now := time.Now().UTC()
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "batch_id": "b1", "batch_seq": float64(2),
		"worker_class": "W", "valid_till": "2000-01-01T00:00:00Z",
	})
	out := BuildDrop(raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1}, now)
	if out.Event == nil || out.Event.BatchSeq != 2 {
		t.Fatalf("event %+v", out.Event)
	}
	if out.Failure == nil || out.Failure.Status != "expired" {
		t.Fatalf("failure %+v", out.Failure)
	}
}

// Regression (#2): expiry is terminal. Even when the job already emitted an
// "executed" touch on a prior retry (batch_counted=true), the drop MUST still
// emit a terminal "failed" event so completed+failed can reach total. Otherwise
// a batch whose last job expires-after-retry is stuck forever (touched==total
// but completed+failed<total). The completion Lua dedups by seq-bit, so this
// bumps failed_count exactly once and never double-counts.
func TestBuildDropEmitsTerminalEvenWhenBatchCounted(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "batch_id": "b1", "batch_seq": float64(1),
		"batch_counted": true, "valid_till": "2000-01-01T00:00:00Z",
	})
	out := BuildDrop(raw, protocol.SourceCoords{Topic: "jobs"}, time.Now())
	if out.Event == nil {
		t.Fatal("expected terminal failed event, got nil")
	}
	if out.Event.Status != "failed" || out.Event.BatchSeq != 1 || out.Event.BatchID != "b1" {
		t.Fatalf("unexpected event: %+v", out.Event)
	}
}

func TestStampSource(t *testing.T) {
	m := map[string]interface{}{"job_id": "j1"}
	src := protocol.SourceCoords{Topic: "ingest", Partition: 2, Offset: 3}
	StampSource(m, src)
	if m["_src_topic"] != "ingest" {
		t.Fatalf("topic %v", m["_src_topic"])
	}
}

// Regression (#2): an expired job that already emitted an "executed" touch on a
// prior retry (batch_counted=true) must STILL emit a terminal "failed" event, or
// the batch never reaches completed+failed==total and gets stuck.
func TestBuildDropExpiredAfterRetryStillEmitsTerminal(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id":        "j1",
		"batch_id":      "b1",
		"batch_seq":     float64(3),
		"batch_counted": true,
		"valid_till":    "2000-01-01T00:00:00Z",
	})
	src := protocol.SourceCoords{Topic: "jobs", Partition: 1, Offset: 9}
	out := BuildDrop(raw, src, now)
	if out.Event == nil {
		t.Fatal("expected terminal failed event for expired batch_counted job, got nil")
	}
	if out.Event.Status != "failed" {
		t.Errorf("event status = %q, want failed", out.Event.Status)
	}
	if out.Event.BatchSeq != 3 || out.Event.BatchID != "b1" {
		t.Errorf("event batch attribution wrong: %+v", out.Event)
	}
}
