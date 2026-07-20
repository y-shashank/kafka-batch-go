package config

import "github.com/y-shashank/kafka-batch-go/pkg/fairness"

// FairnessThroughputSettings maps daemon YAML to the throughput-lane scheduler.
func (c Daemon) FairnessThroughputSettings() fairness.Settings {
	s := fairness.DefaultSettings(fairness.LaneThroughput)
	if c.FairnessReadyWindow > 0 {
		s.ReadyWindow = c.FairnessReadyWindow
	}
	if c.FairnessGlobalConcurrency > 0 {
		s.GlobalConcurrency = c.FairnessGlobalConcurrency
	}
	s.MaxInflightPerTenant = c.FairnessMaxInflightPerTenant
	if c.FairnessLeaseTTL > 0 {
		s.LeaseTTL = c.FairnessLeaseTTL
	}
	if c.FairnessDefaultWeight > 0 {
		s.DefaultWeight = c.FairnessDefaultWeight
	}
	s.WeightedConcurrency = c.FairnessWeightedConcurrency
	if c.FairnessActiveCountTTL > 0 {
		s.ActiveCountTTL = c.FairnessActiveCountTTL
	}
	s.ActiveCountSource = c.FairnessActiveCountSource
	s.ResetVtimeWhenIdle = c.FairnessResetVtimeWhenIdle
	if c.FairnessVtimeIdleResetDebounce > 0 {
		s.VtimeIdleResetDebounce = c.FairnessVtimeIdleResetDebounce
	}
	s.DispatchConsumerGroup = c.DispatchConsumerGroup("throughput")
	s.IngestTopic = c.FairnessThroughputIngest
	return s
}
