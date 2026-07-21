package topics

import (
	"testing"
	"time"
)

func TestScheduledConfigDefaultHorizon(t *testing.T) {
	cfg := scheduledConfig(0)
	// Default horizon is 30d; retention = (30d + 1d) in ms, floored at 7d.
	want := "2678400000" // 31 * 24 * 3600 * 1000
	if cfg["retention.ms"] != want {
		t.Fatalf("retention.ms = %q, want %q", cfg["retention.ms"], want)
	}
	if cfg["cleanup.policy"] != "delete" {
		t.Fatalf("cleanup.policy = %q", cfg["cleanup.policy"])
	}
}

func TestScheduledConfigShortHorizonUsesMinimum(t *testing.T) {
	cfg := scheduledConfig(time.Hour)
	// retention would be (3600+86400)*1000 = 90000000, below 7d min.
	want := "604800000" // 7 * 24 * 3600 * 1000
	if cfg["retention.ms"] != want {
		t.Fatalf("retention.ms = %q, want %q (7d floor)", cfg["retention.ms"], want)
	}
}
