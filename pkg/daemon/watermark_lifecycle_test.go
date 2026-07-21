package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestWatermarkLifecycleHelpers(t *testing.T) {
	var nilE *WatermarkExecutor
	nilE.BindLife(context.Background())
	nilE.StopAccepting()
	if nilE.InFlightCount() != 0 || nilE.WaitInFlight(time.Millisecond) != 0 {
		t.Fatal("nil executor")
	}

	e := NewWatermarkExecutor(config.DefaultDaemon(), "c1",
		func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{CommitOffset: true}, nil
		},
		func(context.Context, job.Outcome) error { return nil },
	)
	life, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.BindLife(life)
	e.BindLife(context.Background()) // second bind ignored
	if e.life() != life {
		t.Fatal("life pinned")
	}
	if e.WaitInFlight(0) != 0 {
		t.Fatal("empty inflight")
	}
	e.StopAccepting()
	e.DispatchAndCommit(context.Background(), &wmMarker{}, recs("t", 0, 1), "g")
	if e.InFlightCount() != 0 {
		t.Fatal("stopped accepting")
	}
}

func TestSuperFetchStopAcceptingNilAndRewind(t *testing.T) {
	var nilSF *SuperFetchExecutor
	nilSF.StopAccepting()
	if nilSF.InFlightCount() != 0 {
		t.Fatal("nil")
	}
	rewindUndispatched(nil, []*kgo.Record{{Topic: "t", Partition: 0, Offset: 1}})
	if undispatchedRewindOffsets([]*kgo.Record{nil}) != nil {
		t.Fatal("all-nil recs")
	}
}

func TestProcessMissingJobID(t *testing.T) {
	cfg := config.DefaultDaemon()
	e := NewSuperFetchExecutor(cfg, nil, "c",
		func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{CommitOffset: true}, nil
		},
		func(context.Context, job.Outcome) error { return nil },
	)
	// Process/Apply error paths return before MarkCommitRecords, so nil client is safe.
	e.Process = func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
		return job.Outcome{}, context.Canceled
	}
	e.processMissingJobID(context.Background(), nil, &kgo.Record{Value: []byte(`{}`)}, "g")
	e.Process = func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
		return job.Outcome{CommitOffset: true}, nil
	}
	e.Apply = func(context.Context, job.Outcome) error { return context.Canceled }
	e.processMissingJobID(context.Background(), nil, &kgo.Record{Value: []byte(`{}`)}, "g")
}
