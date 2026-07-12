package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func testdata(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Join(filepath.Dir(file), "testdata", name)
}

func TestLoadDaemonFromYAML(t *testing.T) {
	path := testdata(t, "daemon.yaml")
	cfg, err := LoadDaemon(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsumerGroup != "acme.test-batch" {
		t.Fatalf("group=%q", cfg.ConsumerGroup)
	}
	if cfg.TopicPrefix != "acme" {
		t.Fatalf("prefix=%q", cfg.TopicPrefix)
	}
	if !cfg.SchedulePollerEnabled || !cfg.FairnessEnabled {
		t.Fatal("expected schedule + fairness enabled")
	}
	if cfg.EventsTopic != "acme.events" {
		t.Fatalf("events=%q", cfg.EventsTopic)
	}
	if !cfg.LivenessEnabled {
		t.Fatal("liveness should be enabled")
	}
}

func TestLoadDaemonEnvOverridesYAML(t *testing.T) {
	path := testdata(t, "daemon.yaml")
	t.Setenv("KAFKA_BROKERS", "env:9092")
	t.Setenv("REDIS_URL", "redis://env:6379/2")
	t.Setenv("KAFKA_PREFIX", "override")

	cfg, err := LoadDaemon(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "env:9092" {
		t.Fatalf("brokers=%v", cfg.Brokers)
	}
	if cfg.RedisURL != "redis://env:6379/2" {
		t.Fatalf("redis=%q", cfg.RedisURL)
	}
	if cfg.TopicPrefix != "override" {
		t.Fatalf("prefix=%q", cfg.TopicPrefix)
	}
	if cfg.EventsTopic != "override.acme.events" {
		t.Fatalf("prefixed events=%q", cfg.EventsTopic)
	}
}

func TestLoadDaemonEmptyPathUsesEnvOnly(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "solo:9092")
	t.Setenv("REDIS_URL", "redis://solo:6379/0")

	cfg, err := LoadDaemon("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Brokers[0] != "solo:9092" {
		t.Fatalf("brokers=%v", cfg.Brokers)
	}
}

func TestLoadManifestWithTopicPrefix(t *testing.T) {
	path := testdata(t, "handlers.yaml")
	m, err := LoadManifest(path, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if m.Handlers["segment.export"].Topic != "segment.exports" {
		t.Fatalf("topic=%q", m.Handlers["segment.export"].Topic)
	}
	topics := m.JobTopicsGo("acme.kafka_batch.jobs")
	if len(topics) != 1 || topics[0] != "segment.exports" {
		t.Fatalf("go topics=%v", topics)
	}
}

func TestRetryTopicsAndGroups(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.TopicPrefix = "p"
	cfg.RetryTopicBase = "p.kafka_batch.jobs.retry"
	topics := cfg.RetryTopics()
	if len(topics) != 3 {
		t.Fatalf("retry topics=%v", topics)
	}
	if cfg.GoWorkerJobsGroup() != cfg.ConsumerGroup+"-go-worker-jobs" {
		t.Fatalf("go jobs group=%q", cfg.GoWorkerJobsGroup())
	}
	if cfg.GoWorkerPriorityGroup("fast") != cfg.ConsumerGroup+"-go-worker-fast" {
		t.Fatalf("go priority group=%q", cfg.GoWorkerPriorityGroup("fast"))
	}
	if cfg.DispatchConsumerGroup("time") != cfg.ConsumerGroup+"-dispatch-time" {
		t.Fatalf("dispatch=%q", cfg.DispatchConsumerGroup("time"))
	}
}

func TestLoadDaemonMissingFile(t *testing.T) {
	_, err := LoadDaemon(filepath.Join(t.TempDir(), "missing.yml"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadManifestMissingFile(t *testing.T) {
	_, err := LoadManifest("/no/such/manifest.yml", "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHasFairHandlersForRuntimeAndReadySplit(t *testing.T) {
	m, err := LoadManifest(testdata(t, "handlers.yaml"), "")
	if err != nil {
		t.Fatal(err)
	}
	if !m.HasFairHandlersForRuntime(RuntimeGo, "time") {
		t.Fatal("expected go fair time handler")
	}
	if m.HasFairHandlersForRuntime(RuntimeGo, "throughput") {
		t.Fatal("unexpected throughput handler")
	}

	cfg := DefaultDaemon()
	cfg.FairnessEnabled = true
	cfg.FairnessTimeReadyGo = "ready.go"
	cfg.FairnessTimeReadyRuby = "ready.ruby"
	if err := cfg.ValidateFairReadySplit(m); err != nil {
		t.Fatalf("validate split: %v", err)
	}
}

func TestJobTopicsGoExcludesFairAndRuby(t *testing.T) {
	m, err := LoadManifest(testdata(t, "handlers.yaml"), "acme")
	if err != nil {
		t.Fatal(err)
	}
	got := m.JobTopicsGo("acme.kafka_batch.jobs")
	for _, want := range []string{"segment.exports"} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing %q in %v", want, got)
		}
	}
	for _, bad := range []string{"reports.build"} {
		for _, g := range got {
			if g == bad {
				t.Fatalf("ruby topic leaked: %v", got)
			}
		}
	}
}

func TestDefaultDaemonConsumerConcurrency(t *testing.T) {
	cfg := DefaultDaemon()
	if cfg.EventsConsumerConcurrency != 8 {
		t.Fatalf("events members=%d", cfg.EventsConsumerConcurrency)
	}
	if cfg.RetryConsumerConcurrency != 4 {
		t.Fatalf("retry members=%d", cfg.RetryConsumerConcurrency)
	}
	if cfg.RequiredAcks() != "all_isr" {
		t.Fatalf("acks=%q", cfg.RequiredAcks())
	}
	if cfg.EventsConsumerMembers() != 8 {
		t.Fatalf("events members helper=%d", cfg.EventsConsumerMembers())
	}
	if cfg.JobsConsumerMembers() != 8 {
		t.Fatalf("jobs members=%d", cfg.JobsConsumerMembers())
	}
	if cfg.FairReadyConsumerMembers() != 8 {
		t.Fatalf("fair ready members=%d", cfg.FairReadyConsumerMembers())
	}
	if cfg.PriorityConsumerMembers() != 4 {
		t.Fatalf("priority members=%d", cfg.PriorityConsumerMembers())
	}
	if cfg.JobProcessWorkers() != 1 {
		t.Fatalf("job process workers=%d", cfg.JobProcessWorkers())
	}
}

func TestWriteAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mini.yaml")
	content := "brokers:\n  - k:9092\nconsumer_group: roundtrip\nredis_url: redis://x\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadDaemon(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsumerGroup != "roundtrip" {
		t.Fatalf("group=%q", cfg.ConsumerGroup)
	}
}
