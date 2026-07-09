package reconciler

import (
	"context"
	"sync"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

var (
	mu              sync.Mutex
	lastReconcileAt time.Time
	running         bool
)

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
			mu.Lock()
			running = false
			mu.Unlock()
		}()
		_ = Run(ctx, cfg, st, prod, "consumer")
	}()
}

// ResetScheduler clears in-process reconciler scheduling state (tests).
func ResetScheduler() {
	mu.Lock()
	defer mu.Unlock()
	lastReconcileAt = time.Time{}
	running = false
}
