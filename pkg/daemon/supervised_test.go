package daemon

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunLoopSupervisedRecoversFromPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	go runLoopSupervised(ctx, "test-loop", nil, func(ctx context.Context) error {
		n := runs.Add(1)
		if n == 1 {
			panic("boom")
		}
		cancel()
		return nil
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runs.Load() >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected supervised loop to restart after panic, got %d runs", runs.Load())
}

func TestLoopHealthStale(t *testing.T) {
	h := &LoopHealth{
		registered: map[string]time.Time{"poller": time.Now().Add(-time.Minute)},
		lastTick:   map[string]time.Time{"poller": time.Now().Add(-2 * time.Minute)},
		maxStale:   30 * time.Second,
		bootGrace:  time.Second,
	}
	ok, detail := h.Healthy(context.Background())
	if ok {
		t.Fatalf("expected stale loop, got ok: %s", detail)
	}
}
