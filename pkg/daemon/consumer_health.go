package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ConsumerHealth tracks Kafka consumer poll activity for HTTP probes.
type ConsumerHealth struct {
	mu         sync.RWMutex
	registered map[string]time.Time // group -> first registered
	lastPoll   map[string]time.Time // group -> last successful poll
	maxStale   time.Duration
	bootGrace  time.Duration
	startedAt  time.Time
}

// NewConsumerHealthTracker builds a poll activity tracker (see consumer_health.go).
func NewConsumerHealthTracker(maxStale, bootGrace time.Duration) *ConsumerHealth {
	if maxStale <= 0 {
		maxStale = 90 * time.Second
	}
	if bootGrace <= 0 {
		bootGrace = 45 * time.Second
	}
	return &ConsumerHealth{
		registered: map[string]time.Time{},
		lastPoll:   map[string]time.Time{},
		maxStale:   maxStale,
		bootGrace:  bootGrace,
		startedAt:  time.Now(),
	}
}

// Register marks a consumer group as expected to poll (call before the consume loop starts).
func (h *ConsumerHealth) Register(group string) {
	if h == nil || group == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.registered[group]; !ok {
		h.registered[group] = time.Now()
	}
}

// RecordPoll records a successful Kafka poll for group.
func (h *ConsumerHealth) RecordPoll(group string) {
	if h == nil || group == "" {
		return
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastPoll[group] = now
	if _, ok := h.registered[group]; !ok {
		h.registered[group] = now
	}
}

// Healthy implements health.Checker — all registered groups must have polled within maxStale.
func (h *ConsumerHealth) Healthy(_ context.Context) (bool, string) {
	if h == nil {
		return true, "no consumer health tracker"
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.registered) == 0 {
		return true, "no consumers registered yet"
	}
	now := time.Now()
	var stale []string
	for group, regAt := range h.registered {
		last, polled := h.lastPoll[group]
		if !polled {
			if now.Sub(regAt) > h.bootGrace {
				stale = append(stale, fmt.Sprintf("%s: never polled (registered %s ago)", group, now.Sub(regAt).Round(time.Second)))
			}
			continue
		}
		if now.Sub(last) > h.maxStale {
			stale = append(stale, fmt.Sprintf("%s: last poll %s ago", group, now.Sub(last).Round(time.Second)))
		}
	}
	if len(stale) > 0 {
		return false, fmt.Sprintf("stale consumers: %v", stale)
	}
	return true, fmt.Sprintf("%d consumer group(s) active", len(h.registered))
}
