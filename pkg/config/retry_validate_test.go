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
