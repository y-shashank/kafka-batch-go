package instrument_test

import (
	"sync/atomic"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestEmitInvokesHandler(t *testing.T) {
	var count int32
	instrument.SetHandler(func(event string, payload map[string]interface{}, durationMs float64) {
		atomic.AddInt32(&count, 1)
		if event != "job.processed" {
			t.Fatalf("event = %q", event)
		}
	})
	defer instrument.SetHandler(nil)

	instrument.Emit("job.processed", map[string]interface{}{"job_id": "j1"}, 12.5)
	if count != 1 {
		t.Fatalf("handler calls = %d", count)
	}
}
