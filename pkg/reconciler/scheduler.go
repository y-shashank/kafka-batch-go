package reconciler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

var (
	mu              sync.Mutex
	lastReconcileAt time.Time
	running         bool
	// runSweep is the sweep implementation used by MaybeRun (overridable in tests).
	runSweep = Run
)

// RunScheduler starts an independent ticker that triggers MaybeRun until ctx is cancelled.
// This decouples reconciliation from events-topic traffic so sweeps continue when the
// events consumer is idle or wedged.
func RunScheduler(ctx context.Context, cfg config.Daemon, st *store.RedisStore, prod Producer, onTick func()) {
	interval := cfg.ReconciliationInterval
	if interval <= 0 {
		interval = 300 * time.Second
	}
	tick := interval / 10
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	} else if tick > 60*time.Second {
		tick = 60 * time.Second
	}
	go func() {
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		log.Printf("[kbatch-reconciler] scheduler started tick=%s interval=%s", tick, interval)
		MaybeRun(ctx, cfg, st, prod)
		for {
			select {
			case <-ctx.Done():
				log.Printf("[kbatch-reconciler] scheduler stopped")
				return
			case <-ticker.C:
				if onTick != nil {
					onTick()
				}
				MaybeRun(ctx, cfg, st, prod)
			}
		}
	}()
}

// MaybeRun triggers a background reconciler sweep when the interval has elapsed.
func MaybeRun(ctx context.Context, cfg config.Daemon, st *store.RedisStore, prod Producer) {
	interval := cfg.ReconciliationInterval
	if interval <= 0 {
		interval = 300 * time.Second
	}

	mu.Lock()
	if running {
		mu.Unlock()
		return
	}
	if !lastReconcileAt.IsZero() && time.Since(lastReconcileAt) < interval {
		mu.Unlock()
		return
	}
	lastReconcileAt = time.Now()
	running = true
	mu.Unlock()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[kbatch-reconciler] sweep panic: %v", r)
				clearLastReconcileAt()
			}
			mu.Lock()
			running = false
			mu.Unlock()
		}()
		log.Printf("[kbatch-reconciler] starting sweep triggered_by=consumer")
		switch runSweep(ctx, cfg, st, prod, "consumer") {
		case ResultLockSkipped:
			log.Printf("[kbatch-reconciler] sweep skipped — distributed lock held by another process")
			clearLastReconcileAt()
		case ResultFailed:
			log.Printf("[kbatch-reconciler] sweep failed — will retry on next tick")
			clearLastReconcileAt()
		case ResultCompleted:
			// Run logs completion details.
		}
	}()
}

func clearLastReconcileAt() {
	mu.Lock()
	lastReconcileAt = time.Time{}
	mu.Unlock()
}

// ResetScheduler clears in-process reconciler scheduling state (tests).
func ResetScheduler() {
	mu.Lock()
	defer mu.Unlock()
	lastReconcileAt = time.Time{}
	running = false
}
