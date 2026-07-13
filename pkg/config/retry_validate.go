package config

import (
	"fmt"
	"strings"
)

// ValidateRetryConsumers ensures retry topic consumers can start. The daemon
// depends on retry consumers to drain retry tiers and dispatch back to job topics.
func (c Daemon) ValidateRetryConsumers() error {
	if strings.TrimSpace(c.RetryTopicBase) == "" {
		return fmt.Errorf("retry_topic is required for retry consumers")
	}
	if len(c.RetryTiers) == 0 {
		return fmt.Errorf("retry_tiers must define at least one tier (e.g. short/medium/large)")
	}
	for tier, delay := range c.RetryTiers {
		if delay <= 0 {
			return fmt.Errorf("retry_tiers.%s must be a positive delay in seconds", tier)
		}
	}
	return nil
}
