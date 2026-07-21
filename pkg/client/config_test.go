package client

import (
	"testing"
	"time"
)

func TestDefaultConfigValues(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Fatalf("brokers=%v", cfg.Brokers)
	}
	if cfg.JobsTopic != "kafka_batch.jobs" || cfg.ScheduledTopic != "kafka_batch.scheduled" {
		t.Fatalf("topics jobs=%q scheduled=%q", cfg.JobsTopic, cfg.ScheduledTopic)
	}
	if cfg.MaxRetries != 7 || !cfg.UniqEnabled || cfg.UniqOnDuplicate != "skip" {
		t.Fatalf("retries/uniq %+v", cfg)
	}
	if cfg.ProduceChunkSize != 500 || cfg.MaxScheduleHorizon != 30*24*time.Hour {
		t.Fatalf("chunk/horizon %+v", cfg)
	}
	if cfg.FairnessTimeIngest == "" || cfg.FairnessThroughputIngest == "" {
		t.Fatalf("fairness topics missing")
	}
}

func TestResolveTopic(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		base   string
		want   string
	}{
		{name: "no prefix", prefix: "", base: "jobs.a", want: "jobs.a"},
		{name: "empty base", prefix: "ship", base: "", want: ""},
		{name: "applies prefix", prefix: "ship", base: "jobs.a", want: "ship.jobs.a"},
		{name: "already prefixed", prefix: "ship", base: "ship.jobs.a", want: "ship.jobs.a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{TopicPrefix: tt.prefix}
			if got := cfg.resolveTopic(tt.base); got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestDefaultJobsTopicUsesPrefix(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TopicPrefix = "env"
	if got := cfg.defaultJobsTopic(); got != "env.kafka_batch.jobs" {
		t.Fatalf("got=%q", got)
	}
}
