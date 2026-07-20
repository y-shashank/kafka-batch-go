package worker

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
)

func configFixture(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Join(filepath.Dir(file), "..", "config", "testdata", name)
}

func TestDefaultJobsTopic(t *testing.T) {
	cfg := config.DefaultDaemon()
	if got := defaultJobsTopic(cfg); got != "kafka_batch.jobs" {
		t.Fatalf("got %q", got)
	}
	cfg.TopicPrefix = "acme"
	if got := defaultJobsTopic(cfg); got != "acme.kafka_batch.jobs" {
		t.Fatalf("got %q", got)
	}
}

func TestUniqueStrings(t *testing.T) {
	got := uniqueStrings([]string{"a", "", "b", "a", "c", "b"})
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("got %v", got)
	}
}

func TestFairReadyTopicNames(t *testing.T) {
	names := fairReadyTopicNames([]fairReadySpec{
		{lane: "time", topic: "ready.time"},
		{lane: "throughput", topic: "ready.tp"},
	})
	if len(names) != 2 || names[0] != "ready.time" {
		t.Fatalf("names=%v", names)
	}
}

func TestWorkerFairReadyTopicsSplitRuntime(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.FairnessEnabled = true
	cfg.FairnessTimeReadyGo = "acme.fair_time_ready.go"
	cfg.FairnessTimeReadyRuby = "acme.fair_time_ready.ruby"
	cfg.FairnessThroughputReadyGo = "acme.fair_tp_ready.go"

	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"go.fair":   {Runtime: config.RuntimeGo, FairnessType: "time"},
		"ruby.fair": {Runtime: config.RuntimeRuby, FairnessType: "time"},
		"go.tp":     {Runtime: config.RuntimeGo, FairnessType: "throughput"},
	}}

	specs := workerFairReadyTopics(cfg, manifest)
	if len(specs) != 2 {
		t.Fatalf("specs=%+v", specs)
	}
	if specs[0].topic != "acme.fair_time_ready.go" || specs[0].lane != "time" {
		t.Fatalf("time spec=%+v", specs[0])
	}
	if specs[1].topic != "acme.fair_tp_ready.go" {
		t.Fatalf("tp spec=%+v", specs[1])
	}
}

func TestWorkerFairReadyTopicsOmitsLaneWithoutGoReadyTopic(t *testing.T) {
	// Fair ready topics are always the runtime-split .go / .ruby names; there is
	// no legacy combined fallback. A lane whose go ready topic is unset yields no
	// spec even if it has go fair handlers.
	cfg := config.DefaultDaemon()
	cfg.FairnessEnabled = true
	cfg.FairnessTimeReadyGo = ""
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"go.fair": {Runtime: config.RuntimeGo, FairnessType: "time"},
	}}
	if specs := workerFairReadyTopics(cfg, manifest); len(specs) != 0 {
		t.Fatalf("expected no specs when go ready topic unset, got %+v", specs)
	}
}

func TestWorkerFairReadyTopicsDisabled(t *testing.T) {
	cfg := config.DefaultDaemon()
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"go.fair": {Runtime: config.RuntimeGo, FairnessType: "time"},
	}}
	if specs := workerFairReadyTopics(cfg, manifest); specs != nil {
		t.Fatalf("expected nil, got %+v", specs)
	}
}

func TestGoPriorityConfigsFiltersRubyTopics(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.ConsumerGroup = "kb"

	prioPath := configFixture(t, "priority.yaml")
	reg, err := priority.LoadRegistry([]string{prioPath}, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"go.p0":   {Runtime: config.RuntimeGo, Topic: "acme.jobs.p0"},
		"ruby.p1": {Runtime: config.RuntimeRuby, Topic: "acme.jobs.p1"},
	}}

	out := goPriorityConfigs(cfg, reg, manifest, "acme.kafka_batch.jobs")
	if len(out) != 1 {
		t.Fatalf("configs=%+v", out)
	}
	if out[0].ConsumerGroup != "kb-go-worker-jobs-fast" {
		t.Fatalf("group=%q", out[0].ConsumerGroup)
	}
	if len(out[0].Topics) != 1 || out[0].Topics[0] != "acme.jobs.p0" {
		t.Fatalf("topics=%v", out[0].Topics)
	}
}
