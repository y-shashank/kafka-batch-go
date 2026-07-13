package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

func TestRunSchedulerTicksMaybeRun(t *testing.T) {
	ResetScheduler()
	t.Cleanup(ResetScheduler)

	var ticks atomic.Int32
	orig := runSweep
	runSweep = func(context.Context, config.Daemon, *store.RedisStore, Producer, string) Result {
		ticks.Add(1)
		return ResultCompleted
	}
	t.Cleanup(func() { runSweep = orig })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Daemon{ReconciliationInterval: 50 * time.Millisecond}
	RunScheduler(ctx, cfg, nil, stubProducer{}, nil)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		waitSweepIdle(t)
		if ticks.Load() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected scheduler to trigger at least one sweep, got %d", ticks.Load())
}

func TestMaybeRunRetriesAfterLockSkip(t *testing.T) {
	ResetScheduler()
	t.Cleanup(ResetScheduler)

	var runs atomic.Int32
	orig := runSweep
	runSweep = func(context.Context, config.Daemon, *store.RedisStore, Producer, string) Result {
		runs.Add(1)
		return ResultLockSkipped
	}
	t.Cleanup(func() { runSweep = orig })

	cfg := config.Daemon{ReconciliationInterval: time.Hour}
	ctx := context.Background()

	MaybeRun(ctx, cfg, nil, stubProducer{})
	waitSweepIdle(t)
	MaybeRun(ctx, cfg, nil, stubProducer{})
	waitSweepIdle(t)

	if got := runs.Load(); got != 2 {
		t.Fatalf("expected immediate retry after lock skip, got %d runs", got)
	}
}
