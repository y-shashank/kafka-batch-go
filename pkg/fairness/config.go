package fairness

import (
	"context"
	"time"
)

const LeaseTTLFloor = 60.0

// IngestLagCounter reports active ingest partitions for fairness cap denominators.
type IngestLagCounter interface {
	IngestActiveCount(ctx context.Context, group, topic string) (int, error)
}

// Settings holds per-lane fairness configuration (mirrors Ruby KafkaBatch.config).
type Settings struct {
	Lane                    Lane
	ReadyWindow             int
	GlobalConcurrency       int
	MaxInflightPerTenant    int
	LeaseTTL                float64
	DefaultWeight           float64
	WeightedConcurrency     bool
	ActiveCountTTL          time.Duration
	ActiveCountSource       string
	IngestLag               IngestLagCounter
	DispatchConsumerGroup   string
	IngestTopic             string
	ForwardingRecoveryGrace float64
	SlotDedupTTL            int
	WeightCacheTTL          time.Duration

	// ResetVtimeWhenIdle enables clearing the virtual-time ledger (weights kept)
	// once the lane goes fully quiescent — empty ring, no live leases, empty
	// forwarding buffer, and zero ingest lag — for a sustained debounce window.
	// This yields fresh per-active-period fairness and bounds vtime growth without
	// the mid-run disruption a fixed-interval reset would cause.
	ResetVtimeWhenIdle bool
	// VtimeIdleResetDebounce is how long the lane must stay quiescent before the
	// idle vtime reset fires, preventing resets during transient empty-ring lulls.
	VtimeIdleResetDebounce time.Duration
}

func DefaultSettings(lane Lane) Settings {
	return Settings{
		Lane:                    lane,
		ReadyWindow:             500,
		GlobalConcurrency:       50,
		MaxInflightPerTenant:    0,
		LeaseTTL:                1800,
		DefaultWeight:           1.0,
		WeightedConcurrency:     true,
		ActiveCountTTL:          5 * time.Second,
		ForwardingRecoveryGrace: 5.0,
		SlotDedupTTL:            0,
		WeightCacheTTL:          60 * time.Second,
		ResetVtimeWhenIdle:      true,
		VtimeIdleResetDebounce:  15 * time.Second,
	}
}

// EffectiveVtimeIdleResetDebounce returns the configured debounce or a safe floor.
func (s Settings) EffectiveVtimeIdleResetDebounce() time.Duration {
	if s.VtimeIdleResetDebounce >= time.Second {
		return s.VtimeIdleResetDebounce
	}
	return 15 * time.Second
}

func (s Settings) EffectiveLeaseTTL() float64 {
	if s.LeaseTTL >= LeaseTTLFloor {
		return s.LeaseTTL
	}
	return LeaseTTLFloor
}

func (s Settings) fetchN() int {
	n := s.GlobalConcurrency * 3
	if n < 60 {
		return 60
	}
	return n
}

func (s Settings) weightedFlag() int {
	if s.WeightedConcurrency {
		return 1
	}
	return 0
}

func (s Settings) slotDedupTTL() int {
	ttl := s.SlotDedupTTL
	if ttl <= 0 {
		ttl = int(s.EffectiveLeaseTTL())
	}
	if ttl < 60 {
		return 60
	}
	return ttl
}
