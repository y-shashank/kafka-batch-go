package instrument_test

import (
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestWorksetReclaimedEmitsCountsAndDuration(t *testing.T) {
	var gotEvent string
	var gotPayload map[string]interface{}
	instrument.SetHandler(func(event string, payload map[string]interface{}, _ float64) {
		gotEvent = event
		gotPayload = payload
	})
	defer instrument.SetHandler(nil)

	instrument.WorksetReclaimed(3, 2, 1, 0, 250*time.Millisecond)

	if gotEvent != "workset.reclaimed" {
		t.Fatalf("event = %q", gotEvent)
	}
	if gotPayload["checked"] != 3 || gotPayload["reclaimed"] != 2 || gotPayload["failed"] != 1 || gotPayload["skipped"] != 0 {
		t.Fatalf("payload = %v", gotPayload)
	}
	if d, ok := gotPayload["duration"].(float64); !ok || d != 0.25 {
		t.Fatalf("duration = %v", gotPayload["duration"])
	}
}

func TestSuperFetchDrainedEmitsRemaining(t *testing.T) {
	var gotEvent string
	var gotPayload map[string]interface{}
	instrument.SetHandler(func(event string, payload map[string]interface{}, _ float64) {
		gotEvent = event
		gotPayload = payload
	})
	defer instrument.SetHandler(nil)

	instrument.SuperFetchDrained(0, 30*time.Second)

	if gotEvent != "super_fetch.drained" {
		t.Fatalf("event = %q", gotEvent)
	}
	if gotPayload["remaining"] != 0 {
		t.Fatalf("remaining = %v", gotPayload["remaining"])
	}
	if d, ok := gotPayload["timeout"].(float64); !ok || d != 30 {
		t.Fatalf("timeout = %v", gotPayload["timeout"])
	}
}
