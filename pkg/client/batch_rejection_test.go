package client

import (
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// Regression (#3): a sealed/cancelled-batch push rejection must emit
// batch.push_rejected so an ignored BatchClosedError still leaves a trace.
func TestNoteBatchRejectionEmitsOnClosed(t *testing.T) {
	var events []map[string]interface{}
	remove := instrument.AddHandler(func(event string, payload map[string]interface{}, _ float64) {
		if event == "batch.push_rejected" {
			events = append(events, payload)
		}
	})
	defer remove()

	// Closed → emitted with reason + attribution.
	noteBatchRejection(BatchClosedError{BatchID: "b1", Reason: "closed"}, "b1", "hello.go")
	// Cancelled → emitted.
	noteBatchRejection(BatchClosedError{BatchID: "b2", Reason: "cancelled"}, "b2", "hello.go")
	// Unrelated error → NOT emitted.
	noteBatchRejection(BatchNotFoundError{BatchID: "b3"}, "b3", "hello.go")
	noteBatchRejection(nil, "b4", "hello.go")

	if len(events) != 2 {
		t.Fatalf("expected 2 push_rejected events, got %d", len(events))
	}
	if events[0]["reason"] != "closed" || events[0]["batch_id"] != "b1" || events[0]["job_type"] != "hello.go" {
		t.Fatalf("unexpected first event: %+v", events[0])
	}
	if events[1]["reason"] != "cancelled" {
		t.Fatalf("unexpected second event: %+v", events[1])
	}
}
