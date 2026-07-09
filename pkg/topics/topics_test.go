package topics

import (
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestSpecsFromClientManifest(t *testing.T) {
	specs := Specs(ClientTopics{
		JobsTopic:      "kafka_batch.jobs",
		ScheduledTopic: "kafka_batch.scheduled",
		CallbacksTopic: "kafka_batch.callbacks",
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"email.send": {Topic: "jobs.email", Runtime: "ruby"},
			"fair.job":   {FairnessType: "time", Runtime: "go"},
		}},
		FairnessTimeIngest: "kafka_batch.fair_time_ingest",
		MaxScheduleHorizon: 30 * 24 * time.Hour,
	})
	names := map[string]bool{}
	for _, s := range specs {
		names[s.Name] = true
	}
	for _, want := range []string{
		"kafka_batch.jobs",
		"jobs.email",
		"kafka_batch.scheduled",
		"kafka_batch.callbacks",
		"kafka_batch.fair_time_ingest",
	} {
		if !names[want] {
			t.Fatalf("missing %s in %v", want, names)
		}
	}
}

func TestScheduledRetentionConfig(t *testing.T) {
	cfg := scheduledConfig(48 * time.Hour)
	if cfg["retention.ms"] == "" {
		t.Fatal("expected retention")
	}
}
