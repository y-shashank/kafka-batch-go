package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExecutionModeHelpers(t *testing.T) {
	cfg := DefaultDaemon()
	if cfg.NormalizedExecutionMode() != ExecutionModeSuperFetch {
		t.Fatalf("default mode=%q", cfg.NormalizedExecutionMode())
	}
	if cfg.WatermarkMode() {
		t.Fatal("default should not be watermark")
	}
	if err := cfg.ValidateExecutionMode(); err != nil {
		t.Fatal(err)
	}

	cfg.ExecutionMode = "  WATERMARK "
	if !cfg.WatermarkMode() || cfg.NormalizedExecutionMode() != ExecutionModeWatermark {
		t.Fatalf("watermark mode=%q", cfg.NormalizedExecutionMode())
	}
	if err := cfg.ValidateExecutionMode(); err != nil {
		t.Fatal(err)
	}

	cfg.ExecutionMode = "bogus"
	if err := cfg.ValidateExecutionMode(); err == nil {
		t.Fatal("expected invalid mode error")
	}
}

func TestDurationAndConcurrencyDefaults(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.JobsConsumerConcurrency = 0
	cfg.FairReadyConsumerConcurrency = 0
	cfg.PriorityConsumerConcurrency = 0
	cfg.SuperFetchConcurrency = 0
	cfg.SuperFetchDrainTimeout = 0
	cfg.LivenessTTL = 0
	cfg.LivenessHeartbeatInterval = 0
	cfg.ConsumerStallTimeout = 0
	cfg.ProducerRequiredAcks = ""

	if cfg.JobsConsumerMembers() != 1 {
		t.Fatalf("jobs members=%d", cfg.JobsConsumerMembers())
	}
	if cfg.FairReadyConsumerMembers() != 1 {
		t.Fatalf("fair members=%d", cfg.FairReadyConsumerMembers())
	}
	if cfg.PriorityConsumerMembers() != 1 {
		t.Fatalf("priority members=%d", cfg.PriorityConsumerMembers())
	}
	if cfg.SuperFetchWorkers() != 10 {
		t.Fatalf("workers=%d", cfg.SuperFetchWorkers())
	}
	if cfg.SuperFetchDrainTimeoutDuration() != 30*time.Second {
		t.Fatalf("drain=%s", cfg.SuperFetchDrainTimeoutDuration())
	}
	if cfg.ConsumerStallTimeoutDuration() != 90*time.Second {
		t.Fatalf("stall=%s", cfg.ConsumerStallTimeoutDuration())
	}
	if cfg.RequiredAcks() != "all_isr" {
		t.Fatalf("acks=%q", cfg.RequiredAcks())
	}

	cfg.SuperFetchDrainTimeout = 5 * time.Second
	cfg.ConsumerStallTimeout = 12 * time.Second
	cfg.ProducerRequiredAcks = "1"
	cfg.SuperFetchConcurrency = 4
	cfg.SuperFetchClaimWindow = 100
	if cfg.SuperFetchDrainTimeoutDuration() != 5*time.Second {
		t.Fatal("custom drain")
	}
	if cfg.ConsumerStallTimeoutDuration() != 12*time.Second {
		t.Fatal("custom stall")
	}
	if cfg.RequiredAcks() != "1" {
		t.Fatal("custom acks")
	}
	if cfg.SuperFetchClaimWindowSize() != 100 {
		t.Fatalf("claim window=%d", cfg.SuperFetchClaimWindowSize())
	}
}

func TestRetryTierFor(t *testing.T) {
	cfg := DefaultDaemon()
	if got := cfg.RetryTierFor(1, "medium"); got != "medium" {
		t.Fatalf("worker tier=%q", got)
	}
	if got := cfg.RetryTierFor(1, "unknown"); got != "short" {
		t.Fatalf("attempt1=%q", got)
	}
	if got := cfg.RetryTierFor(0, ""); got != "short" {
		t.Fatalf("attempt0=%q", got)
	}
	if got := cfg.RetryTierFor(99, ""); got != "large" {
		t.Fatalf("clamped=%q", got)
	}
}

func TestFairnessSettingsAndGroups(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.ConsumerGroup = "cg"
	cfg.FairnessReadyWindow = 2
	cfg.FairnessGlobalConcurrency = 7
	cfg.FairnessMaxInflightPerTenant = 3
	cfg.FairnessLeaseTTL = 90
	cfg.FairnessDefaultWeight = 2
	cfg.FairnessWeightedConcurrency = true
	cfg.FairnessActiveCountTTL = 11 * time.Second
	cfg.FairnessActiveCountSource = "redis"
	cfg.FairnessResetVtimeWhenIdle = true
	cfg.FairnessVtimeIdleResetDebounce = time.Second
	cfg.FairnessTimeIngest = "time.ingest"
	cfg.FairnessThroughputIngest = "tp.ingest"

	ts := cfg.FairnessTimeSettings()
	if ts.GlobalConcurrency != 7 || ts.ReadyWindow != 2 || ts.LeaseTTL != 90 || ts.IngestTopic != "time.ingest" {
		t.Fatalf("time settings=%+v", ts)
	}
	tps := cfg.FairnessThroughputSettings()
	if tps.IngestTopic != "tp.ingest" || tps.DispatchConsumerGroup != cfg.DispatchConsumerGroup("throughput") {
		t.Fatalf("throughput settings=%+v", tps)
	}
	if cfg.JobsFairConsumerGroup("time") != "cg-jobs-fair-time" {
		t.Fatalf("jobs fair=%q", cfg.JobsFairConsumerGroup("time"))
	}
	if cfg.GoWorkerFairReadyGroup("throughput") != "cg-go-worker-fair-ready-throughput" {
		t.Fatalf("go fair=%q", cfg.GoWorkerFairReadyGroup("throughput"))
	}
}

func TestRuntimeSplitFairReadyThroughput(t *testing.T) {
	cfg := DefaultDaemon()
	if !cfg.RuntimeSplitFairReady("throughput") {
		t.Fatal("default daemon should configure split throughput ready topics")
	}
	cfg.FairnessThroughputReadyGo = ""
	cfg.FairnessThroughputReadyRuby = ""
	if cfg.RuntimeSplitFairReady("throughput") {
		t.Fatal("empty ready topics")
	}
	cfg.FairnessThroughputReadyGo = "g"
	cfg.FairnessThroughputReadyRuby = "r"
	if !cfg.RuntimeSplitFairReady("throughput") {
		t.Fatal("expected split ready")
	}
	if cfg.RuntimeSplitFairReady("other") {
		t.Fatal("unknown lane")
	}
	topics := cfg.FairReadyTopics("throughput")
	if topics.Go != "g" || topics.Ruby != "r" {
		t.Fatalf("topics=%+v", topics)
	}
}

func TestManifestValidateAndHandlers(t *testing.T) {
	m, err := LoadManifest(testdata(t, "handlers.yaml"), "")
	if err != nil {
		t.Fatal(err)
	}
	if !m.HasRubyHandlers() || !m.HasGoHandlers() {
		t.Fatal("expected both runtimes")
	}
	types := m.JobTypes()
	if len(types) < 2 {
		t.Fatalf("job types=%v", types)
	}
	topics := m.JobTopics("default.jobs", true)
	foundRuby := false
	for _, tpc := range topics {
		if tpc == "reports.build" {
			foundRuby = true
		}
	}
	if !foundRuby {
		t.Fatalf("JobTopics includeRuby missing ruby topic: %v", topics)
	}
	goTopics := m.JobTopicsForRuntime(RuntimeGo, "default.jobs")
	if len(goTopics) != 1 || goTopics[0] != "segment.exports" {
		t.Fatalf("go topics=%v", goTopics)
	}
	if err := m.Validate("default.jobs"); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	empty, err := LoadManifest("", "")
	if err != nil || empty.Handlers != nil {
		t.Fatalf("empty path: %+v err=%v", empty, err)
	}
}

func TestManifestValidateRegistrationFailure(t *testing.T) {
	prev := lookupRegistered
	t.Cleanup(func() { lookupRegistered = prev })
	SetHandlerLookup(func(string) bool { return false })

	m := Manifest{Handlers: map[string]HandlerEntry{
		"missing.go": {Runtime: "go", Topic: "t"},
	}}
	if err := m.Validate("default"); err == nil {
		t.Fatal("expected registration error")
	}
}

func TestLoadDaemonInvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("brokers: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDaemon(path); err == nil {
		t.Fatal("expected yaml error")
	}
}

func TestLoadManifestInvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("handlers: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(path, ""); err == nil {
		t.Fatal("expected yaml error")
	}
}

func TestApplyEnvMoreBranches(t *testing.T) {
	t.Setenv("KAFKA_BATCH_HANDLER_MANIFEST", "/tmp/handlers.yaml")
	t.Setenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN", "mysql://sched")
	t.Setenv("KAFKA_BATCH_PRIORITY_CONFIG", "p1.yaml")
	t.Setenv("KAFKA_BATCH_PRIORITY_CONFIGS", "p2.yaml, p3.yaml")
	t.Setenv("KAFKA_BATCH_METRICS_ENABLED", "true")
	t.Setenv("KAFKA_BATCH_METRICS_PREFIX", "kb")
	t.Setenv("KAFKA_BATCH_STORE_MYSQL_DSN", "mysql://store")
	t.Setenv("KAFKA_BATCH_LIVENESS_HTTP_ADDR", ":9090")
	t.Setenv("KAFKA_BATCH_LIVENESS_ENABLED", "1")
	t.Setenv("KAFKA_BATCH_METRICS_STATSD_ADDR", "127.0.0.1:8125")
	t.Setenv("KAFKA_BATCH_ALERTS_ENABLED", "true")
	t.Setenv("KAFKA_BATCH_AI_ENCRYPTION_SALT", "salt")
	t.Setenv("KAFKA_BATCH_ALERTS_INTERVAL", "30")
	t.Setenv("KAFKA_BATCH_ALERTS_FOR_TICKS", "3")
	t.Setenv("KAFKA_BATCH_ALERTS_RESOLVE_TICKS", "2")
	t.Setenv("KAFKA_BATCH_ALERTS_COOLDOWN_SECONDS", "60")
	t.Setenv("KAFKA_BATCH_ALERTS_LAG_THRESHOLD", "1000")
	t.Setenv("KAFKA_BATCH_PERFORMANCE_METRICS_ENABLED", "1")
	t.Setenv("KAFKA_BATCH_PERFORMANCE_METRICS_RETENTION", "120")
	t.Setenv("KAFKA_BATCH_PERFORMANCE_METRICS_MAX_JOB_TYPES", "50")
	t.Setenv("KAFKA_BATCH_PERFORMANCE_METRICS_BUCKET_SECONDS", "10")
	t.Setenv("KAFKA_BATCH_PERFORMANCE_METRICS_SAMPLE_RATE", "0.5")
	t.Setenv("KAFKA_BATCH_REDIS_RTT_PROBE_INTERVAL", "5")
	t.Setenv("KAFKA_BATCH_REDIS_RTT_PROBE_TIMEOUT", "1")
	t.Setenv("KAFKA_BATCH_PRODUCER_REQUIRED_ACKS", "leader")
	t.Setenv("KAFKA_BATCH_JOBS_CONSUMER_CONCURRENCY", "3")
	t.Setenv("KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY", "4")
	t.Setenv("KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY", "2")
	t.Setenv("KAFKA_BATCH_SKIP_CANCELLED_JOBS", "true")
	t.Setenv("KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS", "false")
	t.Setenv("KAFKA_BATCH_CANCELLATION_CACHE_TTL", "8")
	t.Setenv("KAFKA_BATCH_SUPER_FETCH_CONCURRENCY", "6")
	t.Setenv("KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW", "12")
	t.Setenv("KAFKA_BATCH_EXECUTION_MODE", "watermark")
	t.Setenv("KAFKA_BATCH_CONSUMER_STALL_TIMEOUT", "45")
	t.Setenv("HOSTNAME", "pod-abc")

	cfg, err := LoadDaemon("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HandlerManifest != "/tmp/handlers.yaml" {
		t.Fatalf("manifest=%q", cfg.HandlerManifest)
	}
	if !cfg.MetricsEnabled || cfg.MetricsPrefix != "kb" {
		t.Fatal("metrics env")
	}
	if cfg.StoreMySQLDSN != "mysql://store" || cfg.ScheduleMySQLDSN != "mysql://sched" {
		t.Fatal("mysql dsn env")
	}
	if cfg.LivenessHTTPAddr != ":9090" || !cfg.LivenessEnabled {
		t.Fatal("liveness env")
	}
	if !cfg.AlertsEnabled || cfg.AlertsIntervalSec != 30 || cfg.AlertsLagThreshold != 1000 {
		t.Fatalf("alerts env %+v", cfg)
	}
	if !cfg.PerformanceMetricsEnabled || cfg.PerformanceMetricsSampleRate != 0.5 {
		t.Fatal("perf metrics env")
	}
	if cfg.JobsConsumerConcurrency != 3 || cfg.SuperFetchConcurrency != 6 {
		t.Fatal("concurrency env")
	}
	if !cfg.SkipCancelledJobs || cfg.FairnessDynamicTenantPartitions {
		t.Fatal("bool toggles")
	}
	if !cfg.WatermarkMode() {
		t.Fatal("execution mode env")
	}
	if cfg.ConsumerStallTimeoutDuration() != 45*time.Second {
		t.Fatalf("stall=%s", cfg.ConsumerStallTimeoutDuration())
	}
	if len(cfg.PriorityConfigPaths) < 3 {
		t.Fatalf("priority paths=%v", cfg.PriorityConfigPaths)
	}
	if h := hostname(); len(h) < 8 || h[:8] != "pod-abc#" {
		t.Fatalf("hostname=%q", h)
	}
}

func TestParsePositiveHelpers(t *testing.T) {
	if _, err := parsePositiveInt("0"); err == nil {
		t.Fatal("expected non-positive int error")
	}
	if _, err := parsePositiveInt("-1"); err == nil {
		t.Fatal("expected negative int error")
	}
	if _, err := parsePositiveFloat("0"); err == nil {
		t.Fatal("expected non-positive float error")
	}
	if _, err := parsePositiveFloat("x"); err == nil {
		t.Fatal("expected parse float error")
	}
	n, err := parsePositiveInt("9")
	if err != nil || n != 9 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
