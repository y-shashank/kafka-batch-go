package client

import "github.com/y-shashank/kafka-batch-go/pkg/config"

// ConfigFromDaemon derives a producer Config from a loaded daemon config so the
// control plane can construct a Client (e.g. for the recurring scheduler) that
// routes identically to external producers — same manifest, topics, fairness
// ingest and tenant-partition settings.
func ConfigFromDaemon(d config.Daemon) Config {
	c := DefaultConfig()
	c.Brokers = d.Brokers
	c.TopicPrefix = d.TopicPrefix
	c.RedisURL = d.RedisURL
	c.ManifestPath = d.HandlerManifest
	if len(d.JobsTopics) > 0 {
		c.JobsTopic = d.JobsTopics[0]
	}
	if d.ScheduledTopic != "" {
		c.ScheduledTopic = d.ScheduledTopic
	}
	if d.CallbacksTopic != "" {
		c.CallbacksTopic = d.CallbacksTopic
	}
	if d.EventsTopic != "" {
		c.EventsTopic = d.EventsTopic
	}
	if d.DeadLetterTopic != "" {
		c.DeadLetterTopic = d.DeadLetterTopic
	}
	if d.ScheduleStore != "" {
		c.ScheduleStore = d.ScheduleStore
	}
	if d.ScheduleMySQLDSN != "" {
		c.ScheduleMySQLDSN = d.ScheduleMySQLDSN
	}
	if d.MaxRetries > 0 {
		c.MaxRetries = d.MaxRetries
	}
	if d.FairnessTimeIngest != "" {
		c.FairnessTimeIngest = d.FairnessTimeIngest
	}
	if d.FairnessThroughputIngest != "" {
		c.FairnessThroughputIngest = d.FairnessThroughputIngest
	}
	c.FairnessTenantPartitions = d.FairnessTenantPartitions
	c.FairnessDynamicTenantPartitions = d.FairnessDynamicTenantPartitions
	if d.FairnessTenantPartitionCacheTTL > 0 {
		c.FairnessTenantPartitionCacheTTL = d.FairnessTenantPartitionCacheTTL
	}
	return c
}
