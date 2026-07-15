package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/health"
)

// LoopHealth tracks background control-plane loops (schedule poller, reconciler, fair forwarder).
type LoopHealth struct {
	mu         sync.RWMutex
	registered map[string]time.Time
	lastTick   map[string]time.Time
	maxStale   time.Duration
	bootGrace  time.Duration
}

// NewLoopHealth builds tick tracking for non-Kafka background loops.
func NewLoopHealth(cfg config.Daemon) *LoopHealth {
	maxStale := cfg.LivenessHeartbeatIntervalDuration() * 3
	if maxStale < 60*time.Second {
		maxStale = 60 * time.Second
	}
	return &LoopHealth{
		registered: map[string]time.Time{},
		lastTick:   map[string]time.Time{},
		maxStale:   maxStale,
		bootGrace:  45 * time.Second,
	}
}

// Register marks a loop as expected to tick.
func (h *LoopHealth) Register(name string) {
	if h == nil || name == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.registered[name]; !ok {
		h.registered[name] = time.Now()
	}
}

// RecordTick records successful loop activity.
func (h *LoopHealth) RecordTick(name string) {
	if h == nil || name == "" {
		return
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastTick[name] = now
	if _, ok := h.registered[name]; !ok {
		h.registered[name] = now
	}
}

// Healthy implements health.Checker.
func (h *LoopHealth) Healthy(_ context.Context) (bool, string) {
	if h == nil {
		return true, "no loop health tracker"
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.registered) == 0 {
		return true, "no background loops registered yet"
	}
	now := time.Now()
	var stale []string
	for name, regAt := range h.registered {
		last, ticked := h.lastTick[name]
		if !ticked {
			if now.Sub(regAt) > h.bootGrace {
				stale = append(stale, fmt.Sprintf("%s: never ticked (registered %s ago)", name, now.Sub(regAt).Round(time.Second)))
			}
			continue
		}
		if now.Sub(last) > h.maxStale {
			stale = append(stale, fmt.Sprintf("%s: last tick %s ago", name, now.Sub(last).Round(time.Second)))
		}
	}
	if len(stale) > 0 {
		return false, fmt.Sprintf("stale loops: %v", stale)
	}
	return true, fmt.Sprintf("%d background loop(s) active", len(h.registered))
}

type compositeHealth struct {
	checkers []health.Checker
}

func (c compositeHealth) Healthy(ctx context.Context) (bool, string) {
	for _, ch := range c.checkers {
		ok, detail := ch.Healthy(ctx)
		if !ok {
			return false, detail
		}
	}
	return true, "ok"
}
