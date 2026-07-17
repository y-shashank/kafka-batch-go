package daemon

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// wmMarker records committed offsets in the order MarkCommitRecords is called.
type wmMarker struct {
	mu      sync.Mutex
	offsets []int64
}

func (m *wmMarker) MarkCommitRecords(recs ...*kgo.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range recs {
		m.offsets = append(m.offsets, r.Offset)
	}
}

func (m *wmMarker) got() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.offsets...)
}

// gatedProcess returns a Process fn whose per-offset completion the test controls
// via release channels, and that fails the offsets listed in failOffsets.
func gatedProcess(release map[int64]chan struct{}, failOffsets map[int64]bool) func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
	return func(ctx context.Context, _ []byte, src protocol.SourceCoords) (job.Outcome, error) {
		if ch, ok := release[src.Offset]; ok {
			select {
			case <-ch:
			case <-ctx.Done():
				return job.Outcome{}, ctx.Err()
			}
		}
		if failOffsets[src.Offset] {
			return job.Outcome{}, fmt.Errorf("boom %d", src.Offset)
		}
		return job.Outcome{CommitOffset: true}, nil
	}
}

func recs(topic string, part int32, offsets ...int64) []*kgo.Record {
	out := make([]*kgo.Record, len(offsets))
	for i, o := range offsets {
		out[i] = &kgo.Record{Topic: topic, Partition: part, Offset: o, Value: []byte("{}")}
	}
	return out
}

func waitInFlight(t *testing.T, e *WatermarkExecutor, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.InFlightCount() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("in-flight = %d, want %d", e.InFlightCount(), want)
}

func newTestWM(t *testing.T, process func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error)) *WatermarkExecutor {
	t.Helper()
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 16
	cfg.SuperFetchClaimWindow = 32
	return NewWatermarkExecutor(cfg, "wm-1", process,
		func(context.Context, job.Outcome) error { return nil })
}

// Out-of-order completions must not advance the watermark past an incomplete
// head offset; once the head completes the whole contiguous prefix commits.
func TestWatermarkContiguousPrefixHoldback(t *testing.T) {
	release := map[int64]chan struct{}{100: make(chan struct{}), 101: make(chan struct{}), 102: make(chan struct{}), 103: make(chan struct{}), 104: make(chan struct{})}
	e := newTestWM(t, gatedProcess(release, nil))
	mk := &wmMarker{}

	e.DispatchAndCommit(context.Background(), mk, recs("t", 0, 100, 101, 102, 103, 104), "g")
	waitInFlight(t, e, 5)

	// Complete everything except the head (100).
	for _, o := range []int64{101, 102, 103, 104} {
		close(release[o])
	}
	waitInFlight(t, e, 1)
	e.FlushMarks(mk)
	if got := mk.got(); len(got) != 0 {
		t.Fatalf("nothing should commit while head 100 is pending, got %v", got)
	}

	// Head completes → the full prefix commits in order.
	close(release[100])
	waitInFlight(t, e, 0)
	e.FlushMarks(mk)
	want := []int64{100, 101, 102, 103, 104}
	got := mk.got()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("marked %v, want %v", got, want)
	}
	if len(e.Window) != 0 {
		t.Fatalf("window slots leaked: %d still held", len(e.Window))
	}
}

// A failed (infra) offset blocks its partition's watermark but frees its own
// window slot; completed offsets behind the gap stay uncommitted (holding their
// slots) so the member backpressures instead of committing past the failure.
func TestWatermarkFailureBlocksPrefix(t *testing.T) {
	e := newTestWM(t, gatedProcess(nil, map[int64]bool{101: true}))
	mk := &wmMarker{}

	e.DispatchAndCommit(context.Background(), mk, recs("t", 0, 100, 101, 102), "g")
	waitInFlight(t, e, 0) // all three finish (100 ok, 101 fail, 102 ok)
	e.FlushMarks(mk)

	got := mk.got()
	if fmt.Sprint(got) != fmt.Sprint([]int64{100}) {
		t.Fatalf("only offset 100 should commit (101 failed, blocks 102), got %v", got)
	}
	// 100 committed (slot released) + 101 failed (slot released) = 2 freed; 102
	// completed-but-uncommitted still holds its slot.
	if len(e.Window) != 1 {
		t.Fatalf("expected 1 held window slot (offset 102), got %d", len(e.Window))
	}
}

// Per-partition prefixes are independent: a stalled head on one partition does
// not block commits on another.
func TestWatermarkPerPartitionIndependent(t *testing.T) {
	release := map[int64]chan struct{}{10: make(chan struct{})} // partition 0 head stalls
	e := newTestWM(t, gatedProcess(release, nil))
	mk := &wmMarker{}

	batch := append(recs("t", 0, 10, 11), recs("t", 1, 20, 21)...)
	e.DispatchAndCommit(context.Background(), mk, batch, "g")
	waitInFlight(t, e, 1) // only partition-0 offset 10 still pending
	e.FlushMarks(mk)

	got := mk.got()
	// Partition 1 (20,21) commits; partition 0 blocked at 10.
	if fmt.Sprint(got) != fmt.Sprint([]int64{20, 21}) {
		t.Fatalf("partition 1 should commit independently, got %v", got)
	}
	close(release[10])
	waitInFlight(t, e, 0)
	e.FlushMarks(mk)
	if fmt.Sprint(mk.got()) != fmt.Sprint([]int64{20, 21, 10, 11}) {
		t.Fatalf("partition 0 should commit after head, got %v", mk.got())
	}
}

func TestExecutionModeConfig(t *testing.T) {
	def := config.DefaultDaemon()
	if def.WatermarkMode() {
		t.Fatal("default must be SuperFetch, not watermark")
	}
	if def.NormalizedExecutionMode() != config.ExecutionModeSuperFetch {
		t.Fatalf("default normalized = %q", def.NormalizedExecutionMode())
	}
	if err := def.ValidateExecutionMode(); err != nil {
		t.Fatalf("default should validate: %v", err)
	}

	wm := config.DefaultDaemon()
	wm.ExecutionMode = "WaterMark" // case-insensitive
	if !wm.WatermarkMode() {
		t.Fatal("expected watermark mode")
	}
	if err := wm.ValidateExecutionMode(); err != nil {
		t.Fatalf("watermark should validate: %v", err)
	}

	bad := config.DefaultDaemon()
	bad.ExecutionMode = "bogus"
	if err := bad.ValidateExecutionMode(); err == nil {
		t.Fatal("expected invalid execution_mode to error")
	}
}
