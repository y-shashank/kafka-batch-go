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

func TestAddHandlerCoexistsWithMultipleSubscribers(t *testing.T) {
	instrument.SetHandler(nil)
	defer instrument.SetHandler(nil)

	var a, b int32
	removeA := instrument.AddHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&a, 1) })
	removeB := instrument.AddHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&b, 1) })
	defer removeA()
	defer removeB()

	instrument.Emit("job.processed", map[string]interface{}{"job_id": "j1"}, 0)
	if a != 1 || b != 1 {
		t.Fatalf("a=%d b=%d, want both 1 (both handlers should coexist)", a, b)
	}

	removeA()
	instrument.Emit("job.processed", map[string]interface{}{"job_id": "j2"}, 0)
	if a != 1 || b != 2 {
		t.Fatalf("a=%d b=%d, want a=1 (removed) b=2 (still subscribed)", a, b)
	}
}

func TestAddHandlerRemoveIsIdempotent(t *testing.T) {
	instrument.SetHandler(nil)
	defer instrument.SetHandler(nil)

	var calls int32
	remove := instrument.AddHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&calls, 1) })
	remove()
	remove() // second call must not panic

	instrument.Emit("job.processed", nil, 0)
	if calls != 0 {
		t.Fatalf("calls = %d, want 0 after removal", calls)
	}
}

func TestSetHandlerReplacesHandlersAddedViaAddHandler(t *testing.T) {
	instrument.SetHandler(nil)
	defer instrument.SetHandler(nil)

	var addCalls, setCalls int32
	instrument.AddHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&addCalls, 1) })
	instrument.SetHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&setCalls, 1) })

	instrument.Emit("job.processed", nil, 0)
	if addCalls != 0 {
		t.Fatalf("addCalls = %d, want 0 (SetHandler replaces all handlers)", addCalls)
	}
	if setCalls != 1 {
		t.Fatalf("setCalls = %d, want 1", setCalls)
	}
}

func TestEmitRecoversPanickingHandlerAndStillInvokesOthers(t *testing.T) {
	instrument.SetHandler(nil)
	defer instrument.SetHandler(nil)

	var otherCalls int32
	removePanicker := instrument.AddHandler(func(string, map[string]interface{}, float64) { panic("boom") })
	defer removePanicker()
	removeOther := instrument.AddHandler(func(string, map[string]interface{}, float64) { atomic.AddInt32(&otherCalls, 1) })
	defer removeOther()

	instrument.Emit("job.processed", nil, 0) // must not panic
	if otherCalls != 1 {
		t.Fatalf("otherCalls = %d, want 1 (a panicking handler must not block others)", otherCalls)
	}
}
