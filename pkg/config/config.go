package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Daemon holds runtime configuration for kbatch daemon.
type Daemon struct {
	Brokers          []string
	TopicPrefix      string
	ConsumerGroup    string
	JobsTopics       []string
	EventsTopic      string
	CallbacksTopic   string
	DeadLetterTopic  string
	RetryTopicBase   string
	RetryTiers       map[string]int // seconds
	RetryProgression []string
	RetryJitter      float64
	// RetryMaxPause caps how long the retry consumer sleeps before re-checking a
	// not-yet-due message (mirrors Ruby retry_max_pause_seconds).
	RetryMaxPause     time.Duration
	MaxRetries        int
	EventEmitRetries  int
	EventEmitBackoff  time.Duration
	RedisURL          string
	BatchTTL          time.Duration
	HandlerManifest   string
	SkipCancelledJobs bool
	// CancellationCacheTTL is how long a process keeps the cancelled-batch
	// index locally before refreshing from Redis (Ruby cancellation_cache_ttl).
	// Default 120s. 0 forces a refresh on every check (tests).
	CancellationCacheTTL time.Duration
	NodeID               string
	// ProducerRequiredAcks is "all_isr" (default, safest) or "leader".
	ProducerRequiredAcks string
	// JobsConsumerConcurrency is in-process group members for plain go job topics.
	JobsConsumerConcurrency int
	// FairReadyConsumerConcurrency is in-process group members per fair-ready lane.
	FairReadyConsumerConcurrency int
	// PriorityConsumerConcurrency is in-process group members per priority group.
	PriorityConsumerConcurrency int
	// SuperFetchConcurrency is the per-member goroutine pool size for in-flight
	// #perform after Redis claim + Kafka ack (always-on SuperFetch). Default 10;
	// raise for IO-bound work (true Go parallelism). See README tuning profiles.
	SuperFetchConcurrency int
	// SuperFetchClaimWindow is max jobs per member in Claimed∨Queued∨Performing.
	// Claim+ack is gated on this window (not perform Sem) so rebalance is not
	// held for the full #perform. 0 → 2× SuperFetchConcurrency.
	SuperFetchClaimWindow int
	// SuperFetchLeaseTTL is the Redis working-set TTL renewed during #perform.
	SuperFetchLeaseTTL time.Duration
	// SuperFetchReclaimEvery is how often the daemon scans for orphaned working-set jobs.
	SuperFetchReclaimEvery time.Duration
	// SuperFetchReclaimLimit caps orphans processed per reclaim sweep.
	SuperFetchReclaimLimit int
	// SuperFetchOrphanGrace is how long after claim before a missing heartbeat
	// counts as death (default 40s ≈ 2× liveness heartbeat interval).
	SuperFetchOrphanGrace time.Duration
	// SuperFetchDrainTimeout is how long SIGTERM/SIGINT waits for in-flight
	// #perform to finish before cancelling lifeCtx (default 30s). Leftovers stay
	// in the Redis workset for control-plane reclaim.
	SuperFetchDrainTimeout time.Duration
	// ExecutionMode selects how tier-3 (worker) jobs are executed: "superfetch"
	// (default — Redis working-set ownership, offset acked ahead of #perform) or
	// "watermark" (Redis-free — commit the contiguous completed-offset prefix, re-run
	// everything after the watermark on crash). Watermark requires idempotent handlers
	// and similar per-topic job runtimes, and every worker on a topic must agree on
	// the mode. See WatermarkMode / the README "Execution mode" section.
	ExecutionMode string
	// ConsumerFetchMaxBytes caps total bytes per broker fetch (default 1 MiB).
	ConsumerFetchMaxBytes int32
	// ConsumerFetchMaxPartitionBytes caps bytes per partition in a fetch (default 128 KiB).
	ConsumerFetchMaxPartitionBytes int32
	// ConsumerFetchMaxWait is max broker wait before returning a partial fetch.
	ConsumerFetchMaxWait time.Duration
	// ConsumerStallTimeout is how long a consumer loop may go without progress
	// before the watchdog force-closes the client and reconnects.
	ConsumerStallTimeout              time.Duration
	SchedulePollerEnabled             bool
	ScheduledTopic                    string
	SchedulePollInterval              time.Duration
	ScheduleLeaseSeconds              int
	ScheduleBatchSize                 int
	ScheduleReclaimEvery              time.Duration
	SchedulePollMaxInterval           time.Duration
	SchedulePollJitter                float64
	ScheduleStore                     string
	ScheduleMySQLDSN                  string
	PriorityConfigPaths               []string
	PriorityLagCheckInterval          time.Duration
	PriorityWeightedInterleave        int
	ConsumptionControlRefreshInterval time.Duration
	FairnessEnabled                   bool
	FairnessTimeIngest                string
	FairnessTimeReady                 string
	FairnessTimeReadyGo               string
	FairnessTimeReadyRuby             string
	FairnessThroughputIngest          string
	FairnessThroughputReady           string
	FairnessThroughputReadyGo         string
	FairnessThroughputReadyRuby       string
	FairnessReadyWindow               int
	FairnessGlobalConcurrency         int
	FairnessMaxInflightPerTenant      int
	FairnessLeaseTTL                  float64
	FairnessDefaultWeight             float64
	FairnessWeightedConcurrency       bool
	FairnessActiveCountTTL            time.Duration
	FairnessActiveCountSource         string
	FairnessTenantPartitions          map[string]int32
	FairnessDynamicTenantPartitions   bool
	FairnessTenantPartitionCacheTTL   time.Duration
	Store                             string
	StoreMySQLDSN                     string
	LivenessEnabled                   bool
	// LivenessTTL is Redis EX on kafka_batch:live:consumer:* — pod considered
	// dead once the key expires without refresh. Default 180s (~9×20s heartbeats).
	LivenessTTL time.Duration
	// LivenessHeartbeatInterval is how often processes refresh the heartbeat key.
	// Default 20s. With TTL 180s, up to ~9 missed cycles before reclaim.
	LivenessHeartbeatInterval time.Duration
	LivenessHTTPAddr          string
	TrackRunningJobs          bool
	MetricsEnabled            bool
	MetricsPrefix             string
	MetricsStatsDAddr         string
	// PerformanceMetricsEnabled gates the opt-in Redis-backed throughput/error
	// history for the Web UI's Performance page (Ruby parity:
	// performance_metrics_enabled). Disabled by default.
	PerformanceMetricsEnabled bool
	// PerformanceMetricsRetention is the Redis EXPIRE on each per-bucket hash
	// and bounds the longest UI range (24h -> 86400s). Default 24h.
	PerformanceMetricsRetention time.Duration
	// PerformanceMetricsMaxJobTypes caps distinct job_type fields tracked per
	// bucket; overflow folds into "_other". Default 50.
	PerformanceMetricsMaxJobTypes int
	// PerformanceMetricsBucketSeconds is the bucket width (advanced). Default 60s.
	PerformanceMetricsBucketSeconds time.Duration
	// PerformanceMetricsSampleRate is the fraction (0, 1.0] of events actually
	// written to Redis. Default 1.0 (every event recorded).
	PerformanceMetricsSampleRate float64
	ReconciliationInterval       time.Duration
	ReconcilerLockTTL            time.Duration
	MaxReconcilePerRun           int
}

func DefaultDaemon() Daemon {
	return Daemon{
		Brokers:                           []string{"localhost:9092"},
		ConsumerGroup:                     "kafka-batch",
		EventsTopic:                       "kafka_batch.events",
		CallbacksTopic:                    "kafka_batch.callbacks",
		DeadLetterTopic:                   "kafka_batch.dead_letter",
		RetryTopicBase:                    "kafka_batch.jobs.retry",
		RetryTiers:                        map[string]int{"short": 30, "medium": 420, "large": 1200},
		RetryProgression:                  []string{"short", "medium", "large"},
		RetryJitter:                       0.1,
		RetryMaxPause:                     30 * time.Second,
		MaxRetries:                        7,
		EventEmitRetries:                  3,
		EventEmitBackoff:                  time.Second,
		RedisURL:                          "redis://localhost:6379/0",
		BatchTTL:                          7 * 24 * time.Hour,
		SkipCancelledJobs:                 true,
		CancellationCacheTTL:              120 * time.Second,
		NodeID:                            hostname(),
		ScheduledTopic:                    "kafka_batch.scheduled",
		SchedulePollInterval:              5 * time.Second,
		ScheduleLeaseSeconds:              60,
		ScheduleBatchSize:                 100,
		ScheduleReclaimEvery:              30 * time.Second,
		SchedulePollMaxInterval:           60 * time.Second,
		PriorityLagCheckInterval:          2 * time.Second,
		PriorityWeightedInterleave:        4,
		ConsumptionControlRefreshInterval: 30 * time.Second,
		FairnessTimeIngest:                "kafka_batch.fair_time_ingest",
		FairnessTimeReady:                 "kafka_batch.fair_time_ready",
		FairnessTimeReadyGo:               "kafka_batch.fair_time_ready.go",
		FairnessTimeReadyRuby:             "kafka_batch.fair_time_ready.ruby",
		FairnessThroughputIngest:          "kafka_batch.fair_throughput_ingest",
		FairnessThroughputReady:           "kafka_batch.fair_throughput_ready",
		FairnessThroughputReadyGo:         "kafka_batch.fair_throughput_ready.go",
		FairnessThroughputReadyRuby:       "kafka_batch.fair_throughput_ready.ruby",
		FairnessReadyWindow:               100,
		FairnessGlobalConcurrency:         50,
		FairnessLeaseTTL:                  1800,
		FairnessDefaultWeight:             1.0,
		FairnessWeightedConcurrency:       true,
		FairnessActiveCountTTL:            5 * time.Second,
		FairnessActiveCountSource:         "inflight_plus_ready",
		// Exclusive ingest partitions for hot tenants (static map still wins).
		// Default on — large tenant fleets rarely maintain a manual pin map.
		FairnessDynamicTenantPartitions: true,
		FairnessTenantPartitionCacheTTL: 30 * time.Second,
		LivenessTTL:                     180 * time.Second,
		LivenessHeartbeatInterval:       20 * time.Second,
		LivenessHTTPAddr:                ":8080",
		TrackRunningJobs:                true,
		MetricsPrefix:                   "kafka_batch",
		PerformanceMetricsRetention:     24 * time.Hour,
		PerformanceMetricsMaxJobTypes:   50,
		PerformanceMetricsBucketSeconds: 60 * time.Second,
		PerformanceMetricsSampleRate:    1.0,
		ReconciliationInterval:          300 * time.Second,
		ReconcilerLockTTL:               600 * time.Second,
		MaxReconcilePerRun:              100,
		ProducerRequiredAcks:            "all_isr",
		JobsConsumerConcurrency:         8,
		FairReadyConsumerConcurrency:    8,
		PriorityConsumerConcurrency:     4,
		SuperFetchConcurrency:           10,
		SuperFetchClaimWindow:           0, // → 2× concurrency via SuperFetchClaimWindowSize()
		SuperFetchLeaseTTL:              2 * time.Minute,
		SuperFetchReclaimEvery:          30 * time.Second,
		SuperFetchReclaimLimit:          100,
		SuperFetchOrphanGrace:           40 * time.Second,
		SuperFetchDrainTimeout:          30 * time.Second,
		ExecutionMode:                   ExecutionModeSuperFetch,
		ConsumerFetchMaxBytes:           DefaultConsumerFetchMaxBytes,
		ConsumerFetchMaxPartitionBytes:  DefaultConsumerFetchMaxPartitionBytes,
		ConsumerFetchMaxWait:            DefaultConsumerFetchMaxWait,
		ConsumerStallTimeout:            90 * time.Second,
	}
}

// RequiredAcks returns the franz-go ack level for the shared producer client.
func (c Daemon) RequiredAcks() string {
	if c.ProducerRequiredAcks == "" {
		return "all_isr"
	}
	return c.ProducerRequiredAcks
}

// JobsConsumerMembers returns in-process group members for plain go job topics.
func (c Daemon) JobsConsumerMembers() int {
	if c.JobsConsumerConcurrency < 1 {
		return 1
	}
	return c.JobsConsumerConcurrency
}

// FairReadyConsumerMembers returns in-process group members per fair-ready lane.
func (c Daemon) FairReadyConsumerMembers() int {
	if c.FairReadyConsumerConcurrency < 1 {
		return 1
	}
	return c.FairReadyConsumerConcurrency
}

// PriorityConsumerMembers returns in-process group members per priority group.
func (c Daemon) PriorityConsumerMembers() int {
	if c.PriorityConsumerConcurrency < 1 {
		return 1
	}
	return c.PriorityConsumerConcurrency
}

// Execution modes for tier-3 (worker) job execution. See Daemon.ExecutionMode.
const (
	ExecutionModeSuperFetch = "superfetch"
	ExecutionModeWatermark  = "watermark"
)

// NormalizedExecutionMode returns the lower-cased execution mode, defaulting to
// superfetch when unset. Callers should use WatermarkMode / SuperFetchExecution
// rather than comparing the raw string.
func (c Daemon) NormalizedExecutionMode() string {
	m := strings.ToLower(strings.TrimSpace(c.ExecutionMode))
	if m == "" {
		return ExecutionModeSuperFetch
	}
	return m
}

// WatermarkMode reports whether the worker should run the Redis-free watermark
// executor instead of SuperFetch.
func (c Daemon) WatermarkMode() bool {
	return c.NormalizedExecutionMode() == ExecutionModeWatermark
}

// ControlPlaneTopics returns the set of topics that must be handled by the control
// plane's own consumers (events ledger counting, retry, callbacks, dead-letter,
// scheduled, fair ingest) and must NEVER be executed as jobs by a worker's
// SuperFetch/watermark executor. Job execution re-runs on crash/rebalance, which
// would double-count batch completions or corrupt control state. Used by a worker
// boot guard (see worker.Run) to reject a manifest that routes a job topic here.
func (c Daemon) ControlPlaneTopics() map[string]struct{} {
	set := map[string]struct{}{}
	add := func(t string) {
		if t != "" {
			set[t] = struct{}{}
		}
	}
	add(c.EventsTopic)
	add(c.CallbacksTopic)
	add(c.DeadLetterTopic)
	add(c.ScheduledTopic)
	add(c.FairnessTimeIngest)
	add(c.FairnessThroughputIngest)
	for _, rt := range c.RetryTopics() {
		add(rt)
	}
	return set
}

// ValidateExecutionMode rejects an unknown execution_mode at boot.
func (c Daemon) ValidateExecutionMode() error {
	switch c.NormalizedExecutionMode() {
	case ExecutionModeSuperFetch, ExecutionModeWatermark:
		return nil
	default:
		return fmt.Errorf("invalid execution_mode %q (want %q or %q)",
			c.ExecutionMode, ExecutionModeSuperFetch, ExecutionModeWatermark)
	}
}

// SuperFetchWorkers returns the SuperFetch goroutine pool size (min 1).
func (c Daemon) SuperFetchWorkers() int {
	if c.SuperFetchConcurrency < 1 {
		return 10
	}
	return c.SuperFetchConcurrency
}

// SuperFetchDrainTimeoutDuration is the graceful-shutdown wait for in-flight
// SuperFetch jobs (default 30s).
func (c Daemon) SuperFetchDrainTimeoutDuration() time.Duration {
	if c.SuperFetchDrainTimeout <= 0 {
		return 30 * time.Second
	}
	return c.SuperFetchDrainTimeout
}

// SuperFetchClaimWindowSize returns the claim/ack outstanding window (min workers).
func (c Daemon) SuperFetchClaimWindowSize() int {
	workers := c.SuperFetchWorkers()
	if c.SuperFetchClaimWindow >= workers {
		return c.SuperFetchClaimWindow
	}
	// Default: 2× perform concurrency so ack can run ahead of long #perform.
	return workers * 2
}

// LivenessTTLDuration is the Redis EX TTL for kafka_batch:live:consumer:* heartbeats
// (pod-alive signal for /live and SuperFetch reclaim). Default 180s.
func (c Daemon) LivenessTTLDuration() time.Duration {
	if c.LivenessTTL <= 0 {
		return 180 * time.Second
	}
	return c.LivenessTTL
}

// LivenessHeartbeatIntervalDuration is how often to refresh the heartbeat key.
// Default 20s (≈9 misses fit inside the 180s TTL).
func (c Daemon) LivenessHeartbeatIntervalDuration() time.Duration {
	if c.LivenessHeartbeatInterval <= 0 {
		return 20 * time.Second
	}
	return c.LivenessHeartbeatInterval
}

// ConsumerStallTimeoutDuration returns the configured consumer stall watchdog duration.
func (c Daemon) ConsumerStallTimeoutDuration() time.Duration {
	if c.ConsumerStallTimeout <= 0 {
		return 90 * time.Second
	}
	return c.ConsumerStallTimeout
}

func LoadDaemon(path string) (Daemon, error) {
	cfg := DefaultDaemon()
	if path == "" {
		applyEnv(&cfg)
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	// Interpolate ${VAR} / ${VAR:-default} references so config can point at
	// deployment env vars (e.g. store_mysql_dsn: ${KB_MYSQL_URL}).
	raw = []byte(ExpandEnv(string(raw)))
	var doc struct {
		Brokers                              []string         `yaml:"brokers"`
		TopicPrefix                          string           `yaml:"topic_prefix"`
		ConsumerGroup                        string           `yaml:"consumer_group"`
		JobsTopics                           []string         `yaml:"jobs_topics"`
		EventsTopic                          string           `yaml:"events_topic"`
		CallbacksTopic                       string           `yaml:"callbacks_topic"`
		DeadLetterTopic                      string           `yaml:"dead_letter_topic"`
		RetryTopic                           string           `yaml:"retry_topic"`
		RetryTiers                           map[string]int   `yaml:"retry_tiers"`
		RedisURL                             string           `yaml:"redis_url"`
		HandlerManifest                      string           `yaml:"handler_manifest"`
		SkipCancelledJobs                    *bool            `yaml:"skip_cancelled_jobs"`
		CancellationCacheTTLSec              float64          `yaml:"cancellation_cache_ttl"`
		MaxRetries                           int              `yaml:"max_retries"`
		RetryMaxPauseSec                     float64          `yaml:"retry_max_pause"`
		ProducerRequiredAcks                 string           `yaml:"producer_required_acks"`
		JobsConsumerConcurrency              int              `yaml:"jobs_consumer_concurrency"`
		FairReadyConsumerConcurrency         int              `yaml:"fair_ready_consumer_concurrency"`
		PriorityConsumerConcurrency          int              `yaml:"priority_consumer_concurrency"`
		SuperFetchConcurrency                int              `yaml:"super_fetch_concurrency"`
		SuperFetchClaimWindow                int              `yaml:"super_fetch_claim_window"`
		SuperFetchLeaseTTLSec                float64          `yaml:"super_fetch_lease_ttl"`
		SuperFetchReclaimIntervalSec         float64          `yaml:"super_fetch_reclaim_interval"`
		SuperFetchReclaimLimit               int              `yaml:"super_fetch_reclaim_limit"`
		SuperFetchOrphanGraceSec             float64          `yaml:"super_fetch_orphan_grace"`
		SuperFetchDrainTimeoutSec            float64          `yaml:"super_fetch_drain_timeout"`
		ExecutionMode                        string           `yaml:"execution_mode"`
		ConsumerFetchMaxBytes                int32            `yaml:"consumer_fetch_max_bytes"`
		ConsumerFetchMaxPartitionBytes       int32            `yaml:"consumer_fetch_max_partition_bytes"`
		ConsumerFetchMaxWaitMs               float64          `yaml:"consumer_fetch_max_wait_ms"`
		ConsumerStallTimeoutSec              float64          `yaml:"consumer_stall_timeout"`
		SchedulePollerEnabled                bool             `yaml:"schedule_poller_enabled"`
		ScheduledTopic                       string           `yaml:"scheduled_topic"`
		ScheduleLeaseSeconds                 int              `yaml:"schedule_lease_seconds"`
		ScheduleBatchSize                    int              `yaml:"schedule_batch_size"`
		SchedulePollIntervalSec              float64          `yaml:"schedule_poll_interval"`
		ScheduleReclaimIntervalSec           float64          `yaml:"schedule_reclaim_interval"`
		SchedulePollMaxIntervalSec           float64          `yaml:"schedule_poll_max_interval"`
		SchedulePollJitter                   float64          `yaml:"schedule_poll_jitter"`
		ScheduleStore                        string           `yaml:"schedule_store"`
		ScheduleMySQLDSN                     string           `yaml:"schedule_mysql_dsn"`
		PriorityConfigPaths                  []string         `yaml:"priority_config_paths"`
		PriorityLagCheckIntervalSec          float64          `yaml:"priority_lag_check_interval"`
		PriorityWeightedInterleave           int              `yaml:"priority_weighted_interleave"`
		ConsumptionControlRefreshIntervalSec float64          `yaml:"consumption_control_refresh_interval"`
		FairnessEnabled                      bool             `yaml:"fairness_enabled"`
		FairnessTimeIngest                   string           `yaml:"fairness_time_ingest"`
		FairnessTimeReady                    string           `yaml:"fairness_time_ready"`
		FairnessTimeReadyGo                  string           `yaml:"fairness_time_ready_go"`
		FairnessTimeReadyRuby                string           `yaml:"fairness_time_ready_ruby"`
		FairnessThroughputIngest             string           `yaml:"fairness_throughput_ingest"`
		FairnessThroughputReady              string           `yaml:"fairness_throughput_ready"`
		FairnessThroughputReadyGo            string           `yaml:"fairness_throughput_ready_go"`
		FairnessThroughputReadyRuby          string           `yaml:"fairness_throughput_ready_ruby"`
		FairnessReadyWindow                  int              `yaml:"fairness_ready_window"`
		FairnessGlobalConcurrency            int              `yaml:"fairness_global_concurrency"`
		FairnessMaxInflightPerTenant         int              `yaml:"fairness_max_inflight_per_tenant"`
		FairnessLeaseTTL                     float64          `yaml:"fairness_lease_ttl"`
		FairnessDefaultWeight                float64          `yaml:"fairness_default_weight"`
		FairnessWeightedConcurrency          bool             `yaml:"fairness_weighted_concurrency"`
		FairnessActiveCountTTLSec            float64          `yaml:"fairness_active_count_ttl"`
		FairnessActiveCountSource            string           `yaml:"fairness_active_count_source"`
		FairnessTenantPartitions             map[string]int32 `yaml:"fairness_tenant_partitions"`
		FairnessDynamicTenantPartitions      *bool            `yaml:"fairness_dynamic_tenant_partitions"`
		FairnessTenantPartitionCacheTTLSec   float64          `yaml:"fairness_tenant_partition_cache_ttl"`
		Store                                string           `yaml:"store"`
		StoreMySQLDSN                        string           `yaml:"store_mysql_dsn"`
		LivenessEnabled                      bool             `yaml:"liveness_enabled"`
		LivenessTTLSec                       float64          `yaml:"liveness_ttl"`
		LivenessHeartbeatIntervalSec         float64          `yaml:"liveness_heartbeat_interval"`
		LivenessHTTPAddr                     string           `yaml:"liveness_http_addr"`
		TrackRunningJobs                     *bool            `yaml:"track_running_jobs"`
		MetricsEnabled                       bool             `yaml:"metrics_enabled"`
		MetricsPrefix                        string           `yaml:"metrics_prefix"`
		MetricsStatsDAddr                    string           `yaml:"metrics_statsd_addr"`
		PerformanceMetricsEnabled            bool             `yaml:"performance_metrics_enabled"`
		PerformanceMetricsRetentionSec       float64          `yaml:"performance_metrics_retention"`
		PerformanceMetricsMaxJobTypes        int              `yaml:"performance_metrics_max_job_types"`
		PerformanceMetricsBucketSecondsSec   float64          `yaml:"performance_metrics_bucket_seconds"`
		PerformanceMetricsSampleRate         float64          `yaml:"performance_metrics_sample_rate"`
		ReconciliationIntervalSec            float64          `yaml:"reconciliation_interval"`
		ReconcilerLockTTLSec                 float64          `yaml:"reconciler_lock_ttl"`
		MaxReconcilePerRun                   int              `yaml:"max_reconcile_per_run"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return cfg, err
	}
	if len(doc.Brokers) > 0 {
		cfg.Brokers = doc.Brokers
	}
	if doc.TopicPrefix != "" {
		cfg.TopicPrefix = doc.TopicPrefix
	}
	if doc.ConsumerGroup != "" {
		cfg.ConsumerGroup = doc.ConsumerGroup
	}
	if len(doc.JobsTopics) > 0 {
		cfg.JobsTopics = doc.JobsTopics
	}
	if doc.EventsTopic != "" {
		cfg.EventsTopic = doc.EventsTopic
	}
	if doc.CallbacksTopic != "" {
		cfg.CallbacksTopic = doc.CallbacksTopic
	}
	if doc.DeadLetterTopic != "" {
		cfg.DeadLetterTopic = doc.DeadLetterTopic
	}
	if doc.RetryTopic != "" {
		cfg.RetryTopicBase = doc.RetryTopic
	}
	if doc.RetryTiers != nil {
		cfg.RetryTiers = doc.RetryTiers
	}
	if doc.RedisURL != "" {
		cfg.RedisURL = doc.RedisURL
	}
	if doc.HandlerManifest != "" {
		cfg.HandlerManifest = doc.HandlerManifest
	}
	if doc.MaxRetries > 0 {
		cfg.MaxRetries = doc.MaxRetries
	}
	if doc.RetryMaxPauseSec > 0 {
		cfg.RetryMaxPause = time.Duration(doc.RetryMaxPauseSec * float64(time.Second))
	}
	if doc.ProducerRequiredAcks != "" {
		cfg.ProducerRequiredAcks = doc.ProducerRequiredAcks
	}
	if doc.JobsConsumerConcurrency > 0 {
		cfg.JobsConsumerConcurrency = doc.JobsConsumerConcurrency
	}
	if doc.FairReadyConsumerConcurrency > 0 {
		cfg.FairReadyConsumerConcurrency = doc.FairReadyConsumerConcurrency
	}
	if doc.PriorityConsumerConcurrency > 0 {
		cfg.PriorityConsumerConcurrency = doc.PriorityConsumerConcurrency
	}
	if doc.SkipCancelledJobs != nil {
		cfg.SkipCancelledJobs = *doc.SkipCancelledJobs
	}
	if doc.CancellationCacheTTLSec > 0 {
		cfg.CancellationCacheTTL = time.Duration(doc.CancellationCacheTTLSec * float64(time.Second))
	}
	if doc.SuperFetchConcurrency > 0 {
		cfg.SuperFetchConcurrency = doc.SuperFetchConcurrency
	}
	if doc.SuperFetchClaimWindow > 0 {
		cfg.SuperFetchClaimWindow = doc.SuperFetchClaimWindow
	}
	if doc.SuperFetchLeaseTTLSec > 0 {
		cfg.SuperFetchLeaseTTL = time.Duration(doc.SuperFetchLeaseTTLSec * float64(time.Second))
	}
	if doc.SuperFetchReclaimIntervalSec > 0 {
		cfg.SuperFetchReclaimEvery = time.Duration(doc.SuperFetchReclaimIntervalSec * float64(time.Second))
	}
	if doc.SuperFetchReclaimLimit > 0 {
		cfg.SuperFetchReclaimLimit = doc.SuperFetchReclaimLimit
	}
	if doc.SuperFetchOrphanGraceSec > 0 {
		cfg.SuperFetchOrphanGrace = time.Duration(doc.SuperFetchOrphanGraceSec * float64(time.Second))
	}
	if doc.SuperFetchDrainTimeoutSec > 0 {
		cfg.SuperFetchDrainTimeout = time.Duration(doc.SuperFetchDrainTimeoutSec * float64(time.Second))
	}
	if doc.ExecutionMode != "" {
		cfg.ExecutionMode = doc.ExecutionMode
	}
	if doc.ConsumerFetchMaxBytes > 0 {
		cfg.ConsumerFetchMaxBytes = doc.ConsumerFetchMaxBytes
	}
	if doc.ConsumerFetchMaxPartitionBytes > 0 {
		cfg.ConsumerFetchMaxPartitionBytes = doc.ConsumerFetchMaxPartitionBytes
	}
	if doc.ConsumerFetchMaxWaitMs > 0 {
		cfg.ConsumerFetchMaxWait = time.Duration(doc.ConsumerFetchMaxWaitMs * float64(time.Millisecond))
	}
	if doc.ConsumerStallTimeoutSec > 0 {
		cfg.ConsumerStallTimeout = time.Duration(doc.ConsumerStallTimeoutSec * float64(time.Second))
	}
	if doc.SchedulePollerEnabled {
		cfg.SchedulePollerEnabled = true
	}
	if doc.ScheduledTopic != "" {
		cfg.ScheduledTopic = doc.ScheduledTopic
	}
	if doc.ScheduleLeaseSeconds > 0 {
		cfg.ScheduleLeaseSeconds = doc.ScheduleLeaseSeconds
	}
	if doc.ScheduleBatchSize > 0 {
		cfg.ScheduleBatchSize = doc.ScheduleBatchSize
	}
	if doc.SchedulePollIntervalSec > 0 {
		cfg.SchedulePollInterval = time.Duration(doc.SchedulePollIntervalSec * float64(time.Second))
	}
	if doc.ScheduleReclaimIntervalSec > 0 {
		cfg.ScheduleReclaimEvery = time.Duration(doc.ScheduleReclaimIntervalSec * float64(time.Second))
	}
	if doc.SchedulePollMaxIntervalSec > 0 {
		cfg.SchedulePollMaxInterval = time.Duration(doc.SchedulePollMaxIntervalSec * float64(time.Second))
	}
	if doc.SchedulePollJitter > 0 {
		cfg.SchedulePollJitter = doc.SchedulePollJitter
	}
	if doc.ScheduleStore != "" {
		cfg.ScheduleStore = doc.ScheduleStore
	}
	if doc.ScheduleMySQLDSN != "" {
		cfg.ScheduleMySQLDSN = doc.ScheduleMySQLDSN
	}
	if len(doc.PriorityConfigPaths) > 0 {
		cfg.PriorityConfigPaths = doc.PriorityConfigPaths
	}
	if doc.PriorityLagCheckIntervalSec > 0 {
		cfg.PriorityLagCheckInterval = time.Duration(doc.PriorityLagCheckIntervalSec * float64(time.Second))
	}
	if doc.PriorityWeightedInterleave > 0 {
		cfg.PriorityWeightedInterleave = doc.PriorityWeightedInterleave
	}
	if doc.ConsumptionControlRefreshIntervalSec > 0 {
		cfg.ConsumptionControlRefreshInterval = time.Duration(doc.ConsumptionControlRefreshIntervalSec * float64(time.Second))
	}
	if doc.FairnessEnabled {
		cfg.FairnessEnabled = true
	}
	if doc.FairnessTimeIngest != "" {
		cfg.FairnessTimeIngest = doc.FairnessTimeIngest
	}
	if doc.FairnessTimeReady != "" {
		cfg.FairnessTimeReady = doc.FairnessTimeReady
	}
	if doc.FairnessTimeReadyGo != "" {
		cfg.FairnessTimeReadyGo = doc.FairnessTimeReadyGo
	}
	if doc.FairnessTimeReadyRuby != "" {
		cfg.FairnessTimeReadyRuby = doc.FairnessTimeReadyRuby
	}
	if doc.FairnessThroughputIngest != "" {
		cfg.FairnessThroughputIngest = doc.FairnessThroughputIngest
	}
	if doc.FairnessThroughputReady != "" {
		cfg.FairnessThroughputReady = doc.FairnessThroughputReady
	}
	if doc.FairnessThroughputReadyGo != "" {
		cfg.FairnessThroughputReadyGo = doc.FairnessThroughputReadyGo
	}
	if doc.FairnessThroughputReadyRuby != "" {
		cfg.FairnessThroughputReadyRuby = doc.FairnessThroughputReadyRuby
	}
	if doc.FairnessReadyWindow > 0 {
		cfg.FairnessReadyWindow = doc.FairnessReadyWindow
	}
	if doc.FairnessGlobalConcurrency > 0 {
		cfg.FairnessGlobalConcurrency = doc.FairnessGlobalConcurrency
	}
	if doc.FairnessMaxInflightPerTenant > 0 {
		cfg.FairnessMaxInflightPerTenant = doc.FairnessMaxInflightPerTenant
	}
	if doc.FairnessLeaseTTL > 0 {
		cfg.FairnessLeaseTTL = doc.FairnessLeaseTTL
	}
	if doc.FairnessDefaultWeight > 0 {
		cfg.FairnessDefaultWeight = doc.FairnessDefaultWeight
	}
	if doc.FairnessWeightedConcurrency {
		cfg.FairnessWeightedConcurrency = true
	}
	if doc.FairnessActiveCountTTLSec > 0 {
		cfg.FairnessActiveCountTTL = time.Duration(doc.FairnessActiveCountTTLSec * float64(time.Second))
	}
	if doc.FairnessActiveCountSource != "" {
		cfg.FairnessActiveCountSource = doc.FairnessActiveCountSource
	}
	if len(doc.FairnessTenantPartitions) > 0 {
		cfg.FairnessTenantPartitions = doc.FairnessTenantPartitions
	}
	if doc.FairnessDynamicTenantPartitions != nil {
		cfg.FairnessDynamicTenantPartitions = *doc.FairnessDynamicTenantPartitions
	}
	if doc.FairnessTenantPartitionCacheTTLSec > 0 {
		cfg.FairnessTenantPartitionCacheTTL = time.Duration(doc.FairnessTenantPartitionCacheTTLSec * float64(time.Second))
	}
	if doc.Store != "" {
		cfg.Store = doc.Store
	}
	if doc.StoreMySQLDSN != "" {
		cfg.StoreMySQLDSN = doc.StoreMySQLDSN
	}
	if doc.LivenessEnabled {
		cfg.LivenessEnabled = true
	}
	if doc.LivenessTTLSec > 0 {
		cfg.LivenessTTL = time.Duration(doc.LivenessTTLSec * float64(time.Second))
	}
	if doc.LivenessHeartbeatIntervalSec > 0 {
		cfg.LivenessHeartbeatInterval = time.Duration(doc.LivenessHeartbeatIntervalSec * float64(time.Second))
	}
	if doc.LivenessHTTPAddr != "" {
		cfg.LivenessHTTPAddr = doc.LivenessHTTPAddr
	}
	if doc.TrackRunningJobs != nil {
		cfg.TrackRunningJobs = *doc.TrackRunningJobs
	}
	if doc.MetricsEnabled {
		cfg.MetricsEnabled = true
	}
	if doc.MetricsPrefix != "" {
		cfg.MetricsPrefix = doc.MetricsPrefix
	}
	if doc.MetricsStatsDAddr != "" {
		cfg.MetricsStatsDAddr = doc.MetricsStatsDAddr
	}
	if doc.PerformanceMetricsEnabled {
		cfg.PerformanceMetricsEnabled = true
	}
	if doc.PerformanceMetricsRetentionSec > 0 {
		cfg.PerformanceMetricsRetention = time.Duration(doc.PerformanceMetricsRetentionSec * float64(time.Second))
	}
	if doc.PerformanceMetricsMaxJobTypes > 0 {
		cfg.PerformanceMetricsMaxJobTypes = doc.PerformanceMetricsMaxJobTypes
	}
	if doc.PerformanceMetricsBucketSecondsSec > 0 {
		cfg.PerformanceMetricsBucketSeconds = time.Duration(doc.PerformanceMetricsBucketSecondsSec * float64(time.Second))
	}
	if doc.PerformanceMetricsSampleRate > 0 {
		cfg.PerformanceMetricsSampleRate = doc.PerformanceMetricsSampleRate
	}
	if doc.ReconciliationIntervalSec > 0 {
		cfg.ReconciliationInterval = time.Duration(doc.ReconciliationIntervalSec * float64(time.Second))
	}
	if doc.ReconcilerLockTTLSec > 0 {
		cfg.ReconcilerLockTTL = time.Duration(doc.ReconcilerLockTTLSec * float64(time.Second))
	}
	if doc.MaxReconcilePerRun > 0 {
		cfg.MaxReconcilePerRun = doc.MaxReconcilePerRun
	}
	applyEnv(&cfg)
	cfg.prefixTopics()
	return cfg, nil
}

func applyEnv(cfg *Daemon) {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_PREFIX"); v != "" {
		cfg.TopicPrefix = strings.TrimSpace(v)
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("KAFKA_BATCH_HANDLER_MANIFEST"); v != "" {
		cfg.HandlerManifest = v
	}
	if v := os.Getenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN"); v != "" {
		cfg.ScheduleMySQLDSN = v
	}
	if v := os.Getenv("KAFKA_BATCH_PRIORITY_CONFIG"); v != "" {
		cfg.PriorityConfigPaths = append(cfg.PriorityConfigPaths, strings.TrimSpace(v))
	}
	if v := os.Getenv("KAFKA_BATCH_PRIORITY_CONFIGS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				cfg.PriorityConfigPaths = append(cfg.PriorityConfigPaths, p)
			}
		}
	}
	if v := os.Getenv("KAFKA_BATCH_METRICS_ENABLED"); v == "1" || strings.EqualFold(v, "true") {
		cfg.MetricsEnabled = true
	}
	if v := os.Getenv("KAFKA_BATCH_METRICS_PREFIX"); v != "" {
		cfg.MetricsPrefix = strings.TrimSpace(v)
	}
	if v := os.Getenv("KAFKA_BATCH_STORE_MYSQL_DSN"); v != "" {
		cfg.StoreMySQLDSN = v
	}
	if v := os.Getenv("KAFKA_BATCH_LIVENESS_HTTP_ADDR"); v != "" {
		cfg.LivenessHTTPAddr = strings.TrimSpace(v)
	}
	if v := os.Getenv("KAFKA_BATCH_LIVENESS_ENABLED"); v == "1" || strings.EqualFold(v, "true") {
		cfg.LivenessEnabled = true
	}
	if v := os.Getenv("KAFKA_BATCH_LIVENESS_TTL"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.LivenessTTL = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.LivenessHeartbeatInterval = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_METRICS_STATSD_ADDR"); v != "" {
		cfg.MetricsStatsDAddr = strings.TrimSpace(v)
	}
	if v := os.Getenv("KAFKA_BATCH_PERFORMANCE_METRICS_ENABLED"); v == "1" || strings.EqualFold(v, "true") {
		cfg.PerformanceMetricsEnabled = true
	}
	if v := os.Getenv("KAFKA_BATCH_PERFORMANCE_METRICS_RETENTION"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.PerformanceMetricsRetention = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_PERFORMANCE_METRICS_MAX_JOB_TYPES"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.PerformanceMetricsMaxJobTypes = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_PERFORMANCE_METRICS_BUCKET_SECONDS"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.PerformanceMetricsBucketSeconds = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_PERFORMANCE_METRICS_SAMPLE_RATE"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil && n <= 1.0 {
			cfg.PerformanceMetricsSampleRate = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_RETRY_MAX_PAUSE"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.RetryMaxPause = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_PRODUCER_REQUIRED_ACKS"); v != "" {
		cfg.ProducerRequiredAcks = strings.TrimSpace(v)
	}
	if v := os.Getenv("KAFKA_BATCH_JOBS_CONSUMER_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.JobsConsumerConcurrency = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.FairReadyConsumerConcurrency = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.PriorityConsumerConcurrency = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_SKIP_CANCELLED_JOBS"); v != "" {
		cfg.SkipCancelledJobs = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS"); v != "" {
		cfg.FairnessDynamicTenantPartitions = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("KAFKA_BATCH_CANCELLATION_CACHE_TTL"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.CancellationCacheTTL = time.Duration(n * float64(time.Second))
		}
	}
	if v := os.Getenv("KAFKA_BATCH_SUPER_FETCH_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.SuperFetchConcurrency = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.SuperFetchClaimWindow = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_EXECUTION_MODE"); v != "" {
		cfg.ExecutionMode = v
	}
	if v := os.Getenv("KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.ConsumerFetchMaxBytes = int32(n)
		}
	}
	if v := os.Getenv("KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.ConsumerFetchMaxPartitionBytes = int32(n)
		}
	}
	if v := os.Getenv("KAFKA_BATCH_CONSUMER_FETCH_MAX_WAIT_MS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.ConsumerFetchMaxWait = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv("KAFKA_BATCH_CONSUMER_STALL_TIMEOUT"); v != "" {
		if n, err := parsePositiveFloat(v); err == nil {
			cfg.ConsumerStallTimeout = time.Duration(n * float64(time.Second))
		}
	}
	cfg.prefixTopics()
}

func parsePositiveInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid positive int %q", s)
	}
	return n, nil
}

func parsePositiveFloat(s string) (float64, error) {
	var n float64
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%f", &n)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid positive float %q", s)
	}
	return n, nil
}

func (c *Daemon) prefixTopics() {
	if c.TopicPrefix == "" {
		return
	}
	p := c.TopicPrefix + "."
	c.EventsTopic = prefixName(p, c.EventsTopic)
	c.CallbacksTopic = prefixName(p, c.CallbacksTopic)
	c.DeadLetterTopic = prefixName(p, c.DeadLetterTopic)
	c.RetryTopicBase = prefixName(p, c.RetryTopicBase)
	c.ScheduledTopic = prefixName(p, c.ScheduledTopic)
	c.FairnessTimeIngest = prefixName(p, c.FairnessTimeIngest)
	c.FairnessTimeReady = prefixName(p, c.FairnessTimeReady)
	c.FairnessTimeReadyGo = prefixName(p, c.FairnessTimeReadyGo)
	c.FairnessTimeReadyRuby = prefixName(p, c.FairnessTimeReadyRuby)
	c.FairnessThroughputIngest = prefixName(p, c.FairnessThroughputIngest)
	c.FairnessThroughputReady = prefixName(p, c.FairnessThroughputReady)
	c.FairnessThroughputReadyGo = prefixName(p, c.FairnessThroughputReadyGo)
	c.FairnessThroughputReadyRuby = prefixName(p, c.FairnessThroughputReadyRuby)
	for i, t := range c.JobsTopics {
		c.JobsTopics[i] = prefixName(p, t)
	}
	if !strings.HasPrefix(c.ConsumerGroup, c.TopicPrefix) {
		c.ConsumerGroup = c.TopicPrefix + "." + c.ConsumerGroup
	}
}

func prefixName(prefix, name string) string {
	if strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

func (c Daemon) RetryTopic(tier string) string {
	return c.RetryTopicBase + "." + tier
}

func (c Daemon) RetryTopics() []string {
	out := make([]string, 0, len(c.RetryTiers))
	for tier := range c.RetryTiers {
		out = append(out, c.RetryTopic(tier))
	}
	sort.Strings(out)
	return out
}

func (c Daemon) RetryTierFor(nextAttempt int, workerTier string) string {
	if workerTier != "" {
		if _, ok := c.RetryTiers[workerTier]; ok {
			return workerTier
		}
	}
	idx := nextAttempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(c.RetryProgression) {
		idx = len(c.RetryProgression) - 1
	}
	return c.RetryProgression[idx]
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "kbatch-daemon"
	}
	// Match Ruby KafkaBatch.node_id: prefer K8s pod name via HOSTNAME, suffix PID.
	if env := os.Getenv("HOSTNAME"); env != "" {
		h = env
	}
	return fmt.Sprintf("%s#%d", h, os.Getpid())
}

// Manifest loads handler definitions (topic routing for Go handlers).
type Manifest struct {
	Handlers map[string]HandlerEntry `yaml:"handlers"`
}

type HandlerEntry struct {
	Runtime          string `yaml:"runtime"`
	WorkerClass      string `yaml:"worker_class"`
	Topic            string `yaml:"topic"`
	ApplyTopicPrefix bool   `yaml:"apply_topic_prefix"`
	MaxRetries       int    `yaml:"max_retries"`
	RetryTier        string `yaml:"retry_tier"`
	FairnessType     string `yaml:"fairness_type"`
	Uniq             bool   `yaml:"uniq"`
}

func LoadManifest(path, topicPrefix string) (Manifest, error) {
	var m Manifest
	if path == "" {
		return m, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	for name, h := range m.Handlers {
		if h.Topic != "" && h.ApplyTopicPrefix && topicPrefix != "" && !strings.HasPrefix(h.Topic, topicPrefix+".") {
			entry := m.Handlers[name]
			entry.Topic = topicPrefix + "." + h.Topic
			m.Handlers[name] = entry
		}
	}
	return m, nil
}

func (m Manifest) JobTopics(defaultTopic string, includeRuby bool) []string {
	return m.jobTopics(defaultTopic, includeRuby, false)
}

// JobTopicsGo returns plain topics for go handlers only.
func (m Manifest) JobTopicsGo(defaultTopic string) []string {
	return m.jobTopics(defaultTopic, false, true)
}

func (m Manifest) jobTopics(defaultTopic string, includeRuby, goOnly bool) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, h := range m.Handlers {
		if fairnessLane(h.FairnessType) != "" {
			continue
		}
		rt := strings.ToLower(strings.TrimSpace(h.Runtime))
		if goOnly {
			if rt != RuntimeGo {
				continue
			}
		} else if rt == RuntimeGo || (includeRuby && rt == RuntimeRuby) {
			// keep
		} else {
			continue
		}
		t := h.Topic
		if t == "" {
			t = defaultTopic
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func (m Manifest) HasRubyHandlers() bool {
	for _, h := range m.Handlers {
		if strings.EqualFold(h.Runtime, "ruby") {
			return true
		}
	}
	return false
}

func (m Manifest) HasGoHandlers() bool {
	for _, h := range m.Handlers {
		if strings.EqualFold(h.Runtime, "go") {
			return true
		}
	}
	return false
}

// ValidateRouting checks manifest shape and topic/runtime exclusivity.
// Safe for client and control tiers (no local handler registration required).
func (m Manifest) ValidateRouting(defaultTopic string) error {
	return m.ValidateTopicRuntimeExclusivity(defaultTopic)
}

// ValidateGoHandlersRegistered ensures runtime:go handlers are registered via kbatch.Register.
// Call from the execution tier (kbatch worker) only.
func (m Manifest) ValidateGoHandlersRegistered() error {
	for jobType, h := range m.Handlers {
		if strings.ToLower(strings.TrimSpace(h.Runtime)) != RuntimeGo {
			continue
		}
		if _, ok := lookupRegistered(jobType); !ok {
			return fmt.Errorf("handler %q not registered in Go (missing kbatch.Register)", jobType)
		}
	}
	return nil
}

// Validate runs routing checks plus Go handler registration (execution tier).
func (m Manifest) Validate(defaultTopic string) error {
	if err := m.ValidateRouting(defaultTopic); err != nil {
		return err
	}
	return m.ValidateGoHandlersRegistered()
}

// lookupRegistered is set by manifest package init from kbatch package.
var lookupRegistered = func(string) (struct{}, bool) { return struct{}{}, true }

func SetHandlerLookup(fn func(string) bool) {
	lookupRegistered = func(s string) (struct{}, bool) {
		if fn(s) {
			return struct{}{}, true
		}
		return struct{}{}, false
	}
}
