package instrument_test

import (
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestSince(t *testing.T) {
	if got := instrument.Since(time.Time{}); got != 0 {
		t.Fatalf("Since(zero) = %v, want 0", got)
	}
	if got := instrument.Since(time.Now()); got < 0 {
		t.Fatalf("Since(now) = %v, want >= 0", got)
	}
}

func TestJobPayload(t *testing.T) {
	got := instrument.JobPayload("j1", "b1", "HelloWorker", map[string]interface{}{
		"attempt": 2,
	})
	if got["job_id"] != "j1" || got["batch_id"] != "b1" || got["worker_class"] != "HelloWorker" {
		t.Fatalf("base fields = %v", got)
	}
	if got["attempt"] != 2 {
		t.Fatalf("extra attempt = %v", got["attempt"])
	}
}
