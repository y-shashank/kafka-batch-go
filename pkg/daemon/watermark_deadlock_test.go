package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func newSmallWindowWM(t *testing.T, window int, process func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error)) *WatermarkExecutor {
	t.Helper()
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = window
	cfg.SuperFetchClaimWindow = window
	return NewWatermarkExecutor(cfg, "wm-dl", process,
		func(context.Context, job.Outcome) error { return nil })
}

// One poll batch larger than the Window must not wedge the member. Successful
// jobs release their slots only via FlushMarks, which runs on the dispatch
// goroutine — so a blocking Window acquire with no flush deadlocked forever
// (completions piled into pc.done freeing nothing, and the stall watchdog was
// kept alive by its heartbeat goroutine).
func TestWatermarkDispatchLargerThanWindowDoesNotDeadlock(t *testing.T) {
	e := newSmallWindowWM(t, 4, func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
		return job.Outcome{CommitOffset: true}, nil
	})
	mk := &wmMarker{}

	offsets := make([]int64, 12)
	for i := range offsets {
		offsets[i] = int64(100 + i)
	}
	done := make(chan struct{})
	go func() {
		e.DispatchAndCommit(context.Background(), mk, recs("t", 0, offsets...), "g")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("DispatchAndCommit deadlocked: poll batch (12) > window (4) with successful jobs")
	}

	waitInFlight(t, e, 0)
	e.FlushMarks(mk)
	if got := mk.got(); len(got) != 12 {
		t.Fatalf("committed %d offsets, want 12: %v", len(got), got)
	}
	if n := len(e.Window); n != 0 {
		t.Fatalf("window slots leaked: %d still held", n)
	}
}

// Redelivery of an offset whose first dispatch is still in flight must not take
// a second Window slot: FlushMarks releases one slot per committed offset, so
// every duplicate dispatch leaked a slot until the member wedged.
func TestWatermarkDuplicateRedeliveryDoesNotLeakSlots(t *testing.T) {
	release := map[int64]chan struct{}{200: make(chan struct{}), 201: make(chan struct{})}
	e := newSmallWindowWM(t, 4, gatedProcess(release, nil))
	mk := &wmMarker{}

	first := recs("t", 0, 200, 201)
	e.DispatchAndCommit(context.Background(), mk, first, "g")
	waitInFlight(t, e, 2)

	// Same offsets redelivered while the first runs are still in flight.
	e.DispatchAndCommit(context.Background(), mk, recs("t", 0, 200, 201), "g")
	if n := len(e.Window); n != 2 {
		t.Fatalf("window holds %d slots after duplicate dispatch, want 2 (duplicates must not hold slots)", n)
	}

	close(release[200])
	close(release[201])
	waitInFlight(t, e, 0)
	e.FlushMarks(mk)

	got := mk.got()
	if len(got) != 2 || got[0] != 200 || got[1] != 201 {
		t.Fatalf("marked %v, want exactly [200 201]", got)
	}
	if n := len(e.Window); n != 0 {
		t.Fatalf("window slots leaked: %d still held after commit", n)
	}

	// Post-commit redelivery is a legitimate re-run (at-least-once), not a
	// duplicate: it must dispatch again.
	release[200] = make(chan struct{})
	close(release[200])
	e.DispatchAndCommit(context.Background(), mk, recs("t", 0, 200), "g")
	waitInFlight(t, e, 0)
	e.FlushMarks(mk)
	if got := mk.got(); len(got) != 3 {
		t.Fatalf("post-commit redelivery must re-run and commit; marked %v", got)
	}
}
