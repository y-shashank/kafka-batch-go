package config

import "testing"

func TestValidateRetryConsumersRequiresTiers(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.RetryTiers = nil
	if err := cfg.ValidateRetryConsumers(); err == nil {
		t.Fatal("expected error when retry_tiers empty")
	}
}

func TestValidateRetryConsumersOK(t *testing.T) {
	cfg := DefaultDaemon()
	if err := cfg.ValidateRetryConsumers(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRetryConsumersAllowsZeroDelay(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.RetryTiers = map[string]int{"short": 0, "medium": 0, "large": 0}
	if err := cfg.ValidateRetryConsumers(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRetryConsumersRejectsNegativeDelay(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.RetryTiers = map[string]int{"short": -1}
	if err := cfg.ValidateRetryConsumers(); err == nil {
		t.Fatal("expected error for negative delay")
	}
}
