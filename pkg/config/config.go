package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Daemon holds runtime configuration for kbatch daemon.
type Daemon struct {
	Brokers                   []string
	TopicPrefix               string
	ConsumerGroup             string
	JobsTopics                []string
	EventsTopic               string
	CallbacksTopic            string
	DeadLetterTopic           string
	RetryTopicBase            string
	RetryTiers                map[string]int // seconds
	RetryProgression          []string
	RetryJitter               float64
	// RetryMaxPause caps how long the retry consumer sleeps before re-checking a
	// not-yet-due message (mirrors Ruby retry_max_pause_seconds).
	RetryMaxPause             time.Duration
	MaxRetries                int
	CompleteAfter             int
	EventEmitRetries          int
	EventEmitBackoff          time.Duration
	RedisURL                  string
	BatchTTL                  time.Duration
	HandlerManifest           string
	SkipCancelledJobs         bool
	NodeID                    string
	RetryTransactionalEnabled bool
	// EventsConsumerConcurrency is the number of in-process Kafka group members
	// for the events consumer (same group, partition assignment split by broker).
	EventsConsumerConcurrency int
	// RetryConsumerConcurrency is the number of in-process group members for the
	// non-transactional retry consumer.
	RetryConsumerConcurrency int
	// ProducerRequiredAcks is "all_isr" (default, safest) or "leader".
	ProducerRequiredAcks string
	// JobsConsumerConcurrency is in-process group members for plain go job topics.
	JobsConsumerConcurrency int
	// FairReadyConsumerConcurrency is in-process group members per fair-ready lane.
	FairReadyConsumerConcurrency int
	// PriorityConsumerConcurrency is in-process group members per priority group.
	PriorityConsumerConcurrency int
	// JobProcessConcurrency is parallel job executions per poll per consumer member
	// (Karafka concurrency equivalent). 1 = serial within each poll loop.
	JobProcessConcurrency int
	// ConsumerFetchMaxBytes caps total bytes per broker fetch (default 1 MiB).
	ConsumerFetchMaxBytes int32
	// ConsumerFetchMaxPartitionBytes caps bytes per partition in a fetch (default 128 KiB).
	ConsumerFetchMaxPartitionBytes int32
	// ConsumerFetchMaxWait is max broker wait before returning a partial fetch.
	ConsumerFetchMaxWait              time.Duration
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
	LivenessTTL                       time.Duration
	LivenessHTTPAddr                  string
	TrackRunningJobs                  bool
	MetricsEnabled                    bool
	MetricsPrefix                     string
	MetricsStatsDAddr                 string
	ReconciliationInterval            time.Duration
	ReconcilerLockTTL                 time.Duration
	MaxReconcilePerRun                int
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
		CompleteAfter:                     7,
		EventEmitRetries:                  3,
		EventEmitBackoff:                  time.Second,
		RedisURL:                          "redis://localhost:6379/0",
		BatchTTL:                          7 * 24 * time.Hour,
		SkipCancelledJobs:                 true,
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
		FairnessTenantPartitionCacheTTL:   30 * time.Second,
		LivenessTTL:                       30 * time.Second,
		LivenessHTTPAddr:                  ":8080",
		TrackRunningJobs:                  true,
		MetricsPrefix:                     "kafka_batch",
		ReconciliationInterval:            300 * time.Second,
		ReconcilerLockTTL:                 600 * time.Second,
		MaxReconcilePerRun:                100,
		EventsConsumerConcurrency:         8,
		RetryConsumerConcurrency:          4,
		ProducerRequiredAcks:              "all_isr",
		JobsConsumerConcurrency:           8,
		FairReadyConsumerConcurrency:      8,
		PriorityConsumerConcurrency:       4,
		JobProcessConcurrency:             1,
		ConsumerFetchMaxBytes:             DefaultConsumerFetchMaxBytes,
		ConsumerFetchMaxPartitionBytes:    DefaultConsumerFetchMaxPartitionBytes,
		ConsumerFetchMaxWait:              DefaultConsumerFetchMaxWait,
	}
}

// RequiredAcks returns the franz-go ack level for the shared producer client.
func (c Daemon) RequiredAcks() string {
	if c.ProducerRequiredAcks == "" {
		return "all_isr"
	}
	return c.ProducerRequiredAcks
}

// EventsConsumerMembers returns a positive member count for the events group.
func (c Daemon) EventsConsumerMembers() int {
	if c.EventsConsumerConcurrency < 1 {
		return 1
	}
	return c.EventsConsumerConcurrency
}

// RetryConsumerMembers returns a positive member count for the retry group.
func (c Daemon) RetryConsumerMembers() int {
	if c.RetryConsumerConcurrency < 1 {
		return 1
	}
	return c.RetryConsumerConcurrency
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

// JobProcessWorkers returns parallel job executions per poll (min 1).
func (c Daemon) JobProcessWorkers() int {
	if c.JobProcessConcurrency < 1 {
		return 1
	}
	return c.JobProcessConcurrency
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
		MaxRetries                           int              `yaml:"max_retries"`
		CompleteAfter                        int              `yaml:"complete_after_retries"`
		RetryTransactionalEnabled            bool             `yaml:"retry_transactional_enabled"`
		RetryMaxPauseSec                     float64          `yaml:"retry_max_pause"`
		EventsConsumerConcurrency            int              `yaml:"events_consumer_concurrency"`
		RetryConsumerConcurrency             int              `yaml:"retry_consumer_concurrency"`
		ProducerRequiredAcks                 string           `yaml:"producer_required_acks"`
		JobsConsumerConcurrency              int              `yaml:"jobs_consumer_concurrency"`
		FairReadyConsumerConcurrency         int              `yaml:"fair_ready_consumer_concurrency"`
		PriorityConsumerConcurrency          int              `yaml:"priority_consumer_concurrency"`
		JobProcessConcurrency                int              `yaml:"job_process_concurrency"`
		ConsumerFetchMaxBytes                int32            `yaml:"consumer_fetch_max_bytes"`
		ConsumerFetchMaxPartitionBytes       int32            `yaml:"consumer_fetch_max_partition_bytes"`
		ConsumerFetchMaxWaitMs               float64          `yaml:"consumer_fetch_max_wait_ms"`
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
		FairnessDynamicTenantPartitions      bool             `yaml:"fairness_dynamic_tenant_partitions"`
		FairnessTenantPartitionCacheTTLSec   float64          `yaml:"fairness_tenant_partition_cache_ttl"`
		Store                                string           `yaml:"store"`
		StoreMySQLDSN                        string           `yaml:"store_mysql_dsn"`
		LivenessEnabled                      bool             `yaml:"liveness_enabled"`
		LivenessTTLSec                       float64          `yaml:"liveness_ttl"`
		LivenessHTTPAddr                     string           `yaml:"liveness_http_addr"`
		TrackRunningJobs                     *bool            `yaml:"track_running_jobs"`
		MetricsEnabled                       bool             `yaml:"metrics_enabled"`
		MetricsPrefix                        string           `yaml:"metrics_prefix"`
		MetricsStatsDAddr                    string           `yaml:"metrics_statsd_addr"`
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
	if doc.CompleteAfter > 0 {
		cfg.CompleteAfter = doc.CompleteAfter
	}
	if doc.RetryTransactionalEnabled {
		cfg.RetryTransactionalEnabled = true
	}
	if doc.RetryMaxPauseSec > 0 {
		cfg.RetryMaxPause = time.Duration(doc.RetryMaxPauseSec * float64(time.Second))
	}
	if doc.EventsConsumerConcurrency > 0 {
		cfg.EventsConsumerConcurrency = doc.EventsConsumerConcurrency
	}
	if doc.RetryConsumerConcurrency > 0 {
		cfg.RetryConsumerConcurrency = doc.RetryConsumerConcurrency
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
	if doc.JobProcessConcurrency > 0 {
		cfg.JobProcessConcurrency = doc.JobProcessConcurrency
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
	if doc.FairnessDynamicTenantPartitions {
		cfg.FairnessDynamicTenantPartitions = true
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
	if v := os.Getenv("KAFKA_BATCH_METRICS_STATSD_ADDR"); v != "" {
		cfg.MetricsStatsDAddr = strings.TrimSpace(v)
	}
	if v := os.Getenv("KAFKA_BATCH_EVENTS_CONSUMER_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.EventsConsumerConcurrency = n
		}
	}
	if v := os.Getenv("KAFKA_BATCH_RETRY_CONSUMER_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.RetryConsumerConcurrency = n
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
	if v := os.Getenv("KAFKA_BATCH_JOB_PROCESS_CONCURRENCY"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.JobProcessConcurrency = n
		}
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
	if err != nil {
		return "kbatch-daemon"
	}
	return h
}

// Manifest loads handler definitions (topic routing for Go handlers).
type Manifest struct {
	Handlers map[string]HandlerEntry `yaml:"handlers"`
}

type HandlerEntry struct {
	Runtime              string `yaml:"runtime"`
	WorkerClass          string `yaml:"worker_class"`
	Topic                string `yaml:"topic"`
	ApplyTopicPrefix     bool   `yaml:"apply_topic_prefix"`
	MaxRetries           int    `yaml:"max_retries"`
	CompleteAfterRetries int    `yaml:"complete_after_retries"`
	RetryTier            string `yaml:"retry_tier"`
	FairnessType         string `yaml:"fairness_type"`
	Uniq                 bool   `yaml:"uniq"`
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
