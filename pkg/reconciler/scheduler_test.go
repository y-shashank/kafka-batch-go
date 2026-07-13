package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type stubProducer struct{}

func (stubProducer) Produce(context.Context, string, string, []byte) error { return nil }

func waitSweepIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		busy := running
		mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for reconciler sweep")
}

func TestMaybeRunRespectsInterval(t *testing.T) {
	ResetScheduler()
	t.Cleanup(ResetScheduler)

	var runs atomic.Int32
	orig := runSweep
	runSweep = func(context.Context, config.Daemon, *store.RedisStore, Producer, string) Result {
		runs.Add(1)
		return ResultCompleted
	}
	t.Cleanup(func() { runSweep = orig })

	cfg := config.Daemon{ReconciliationInterval: 100 * time.Millisecond}
	ctx := context.Background()

	MaybeRun(ctx, cfg, nil, stubProducer{})
	waitSweepIdle(t)
	if got := runs.Load(); got != 1 {
		t.Fatalf("expected 1 sweep on first trigger, got %d", got)
	}

	MaybeRun(ctx, cfg, nil, stubProducer{})
	waitSweepIdle(t)
	if got := runs.Load(); got != 1 {
		t.Fatalf("expected still 1 sweep before interval elapsed, got %d", got)
	}

	time.Sleep(120 * time.Millisecond)
	MaybeRun(ctx, cfg, nil, stubProducer{})
	waitSweepIdle(t)
	if got := runs.Load(); got != 2 {
		t.Fatalf("expected 2 sweeps after interval elapsed, got %d", got)
	}
}
