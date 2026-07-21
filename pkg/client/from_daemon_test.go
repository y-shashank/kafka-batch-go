package client

import (
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestConfigFromDaemon(t *testing.T) {
	d := config.Daemon{
		Brokers:                         []string{"b1:9092", "b2:9092"},
		TopicPrefix:                     "ship",
		RedisURL:                        "redis://redis:6379/2",
		HandlerManifest:                 "/etc/handlers.yml",
		JobsTopics:                      []string{"kafka_batch.jobs", "kafka_batch.jobs.p1"},
		ScheduledTopic:                  "kafka_batch.scheduled",
		CallbacksTopic:                  "kafka_batch.callbacks",
		EventsTopic:                     "kafka_batch.events",
		DeadLetterTopic:                 "kafka_batch.dead_letter",
		ScheduleStore:                   "mysql",
		ScheduleMySQLDSN:                "user:pass@tcp(mysql)/sched",
		MaxRetries:                      9,
		FairnessTimeIngest:              "ship.fair_time_ingest",
		FairnessThroughputIngest:        "ship.fair_throughput_ingest",
		FairnessTenantPartitions:        map[string]int32{"acme": 3},
		FairnessDynamicTenantPartitions: false,
		FairnessTenantPartitionCacheTTL: 45 * time.Second,
	}

	c := ConfigFromDaemon(d)

	if len(c.Brokers) != 2 || c.Brokers[0] != "b1:9092" || c.Brokers[1] != "b2:9092" {
		t.Fatalf("brokers=%v", c.Brokers)
	}
	if c.TopicPrefix != "ship" {
		t.Fatalf("TopicPrefix=%q", c.TopicPrefix)
	}
	if c.RedisURL != "redis://redis:6379/2" {
		t.Fatalf("RedisURL=%q", c.RedisURL)
	}
	if c.ManifestPath != "/etc/handlers.yml" {
		t.Fatalf("ManifestPath=%q", c.ManifestPath)
	}
	if c.JobsTopic != "kafka_batch.jobs" {
		t.Fatalf("JobsTopic=%q (want first JobsTopics entry)", c.JobsTopic)
	}
	if c.ScheduledTopic != "kafka_batch.scheduled" {
		t.Fatalf("ScheduledTopic=%q", c.ScheduledTopic)
	}
	if c.CallbacksTopic != "kafka_batch.callbacks" {
		t.Fatalf("CallbacksTopic=%q", c.CallbacksTopic)
	}
	if c.EventsTopic != "kafka_batch.events" {
		t.Fatalf("EventsTopic=%q", c.EventsTopic)
	}
	if c.DeadLetterTopic != "kafka_batch.dead_letter" {
		t.Fatalf("DeadLetterTopic=%q", c.DeadLetterTopic)
	}
	if c.ScheduleStore != "mysql" {
		t.Fatalf("ScheduleStore=%q", c.ScheduleStore)
	}
	if c.ScheduleMySQLDSN != "user:pass@tcp(mysql)/sched" {
		t.Fatalf("ScheduleMySQLDSN=%q", c.ScheduleMySQLDSN)
	}
	if c.MaxRetries != 9 {
		t.Fatalf("MaxRetries=%d", c.MaxRetries)
	}
	if c.FairnessTimeIngest != "ship.fair_time_ingest" {
		t.Fatalf("FairnessTimeIngest=%q", c.FairnessTimeIngest)
	}
	if c.FairnessThroughputIngest != "ship.fair_throughput_ingest" {
		t.Fatalf("FairnessThroughputIngest=%q", c.FairnessThroughputIngest)
	}
	if c.FairnessTenantPartitions["acme"] != 3 {
		t.Fatalf("FairnessTenantPartitions=%v", c.FairnessTenantPartitions)
	}
	if c.FairnessDynamicTenantPartitions {
		t.Fatal("FairnessDynamicTenantPartitions: want false")
	}
	if c.FairnessTenantPartitionCacheTTL != 45*time.Second {
		t.Fatalf("FairnessTenantPartitionCacheTTL=%s", c.FairnessTenantPartitionCacheTTL)
	}
}

func TestConfigFromDaemonEmptyJobsTopicsKeepsDefault(t *testing.T) {
	def := DefaultConfig()
	c := ConfigFromDaemon(config.Daemon{})
	if c.JobsTopic != def.JobsTopic {
		t.Fatalf("JobsTopic=%q, want default %q", c.JobsTopic, def.JobsTopic)
	}
}
