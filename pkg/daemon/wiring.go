package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/consumption"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/health"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type pauseChecker interface {
	Paused(ctx context.Context, group, topic string, partition int32) bool
	ActiveHigherTopics(ctx context.Context, group string, higher []string) []string
}

func BuildPauseControl(cfg config.Daemon, rdb *redis.Client) (pauseChecker, *consumption.MySQLPauseStore, func(), error) {
	ctl := consumption.NewControl(rdb, cfg.ConsumptionControlRefreshInterval)
	if strings.ToLower(cfg.Store) != "mysql" {
		return ctl, nil, func() {}, nil
	}
	if strings.TrimSpace(cfg.StoreMySQLDSN) == "" {
		return nil, nil, nil, fmt.Errorf("store_mysql_dsn is required when store is mysql")
	}
	mysqlPause, err := consumption.NewMySQLPauseStore(cfg.StoreMySQLDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("store mysql pause: %w", err)
	}
	return consumption.NewHybridControl(rdb, mysqlPause, cfg.ConsumptionControlRefreshInterval), mysqlPause, func() { _ = mysqlPause.Close() }, nil
}

// BuildFailureRecorder returns the durable (MySQL-table) failure recorder
// when config.Store is "mysql", or nil otherwise. No per-job failure
// metadata is ever written to Redis: exhausted jobs land on the dead-letter
// topic and retrying jobs are listed live from the retry topics.
func BuildFailureRecorder(cfg config.Daemon, st *store.RedisStore) (store.FailureRecorder, func(), error) {
	if strings.ToLower(cfg.Store) != "mysql" {
		return nil, func() {}, nil
	}
	if strings.TrimSpace(cfg.StoreMySQLDSN) == "" {
		return nil, nil, fmt.Errorf("store_mysql_dsn is required when store is mysql")
	}
	mysqlFailures, err := store.NewMySQLFailures(cfg.StoreMySQLDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("store mysql failures: %w", err)
	}
	return mysqlFailures, func() { _ = mysqlFailures.Close() }, nil
}

func BuildTenantPartitions(cfg config.Daemon, rdb *redis.Client, prod *kafkaclient.Client) *fairness.TenantPartitions {
	if len(cfg.FairnessTenantPartitions) == 0 && !cfg.FairnessDynamicTenantPartitions {
		return nil
	}
	return fairness.NewTenantPartitions(rdb, fairness.TenantPartitionsConfig{
		Static:  cfg.FairnessTenantPartitions,
		Dynamic: cfg.FairnessDynamicTenantPartitions,
		CacheTTL: cfg.FairnessTenantPartitionCacheTTL,
		Counter: prod,
		IngestTopic: func(lane string) string {
			if lane == "throughput" {
				return cfg.FairnessThroughputIngest
			}
			return cfg.FairnessTimeIngest
		},
	})
}

func StartHealthServer(ctx context.Context, cfg config.Daemon, process string, checker health.Checker) {
	if !cfg.LivenessEnabled {
		return
	}
	go func() {
		_ = (&health.Server{Addr: cfg.LivenessHTTPAddr, Process: process, Checker: checker}).ListenAndServe(ctx)
	}()
}

// NewConsumerHealth builds poll tracking for HTTP probes.
// Uses heartbeat interval (not Redis TTL) so a 180s liveness_ttl does not make
// Kafka poll probes wait many minutes.
func NewConsumerHealth(cfg config.Daemon) *ConsumerHealth {
	maxStale := cfg.LivenessHeartbeatIntervalDuration() * 3
	if maxStale < 60*time.Second {
		maxStale = 60 * time.Second
	}
	return NewConsumerHealthTracker(maxStale, 45*time.Second)
}

func NewLivenessReporter(cfg config.Daemon, rdb *redis.Client) *liveness.Reporter {
	if !cfg.LivenessEnabled || rdb == nil {
		return nil
	}
	r := liveness.NewReporter(rdb, cfg.LivenessTTLDuration())
	r.HeartbeatEvery = cfg.LivenessHeartbeatIntervalDuration()
	r.TrackRunningJobs = cfg.TrackRunningJobs
	return r
}

// StartLivenessHeartbeatLoop starts the fixed-interval Redis heartbeat goroutine.
func StartLivenessHeartbeatLoop(ctx context.Context, live *liveness.Reporter) {
	if live == nil {
		return
	}
	live.StartHeartbeatLoop(ctx)
}

func attachIngestLag(settings fairness.Settings, lag fairness.IngestLagCounter) fairness.Settings {
	if settings.ActiveCountSource == "ingest_lag" && lag != nil {
		settings.IngestLag = lag
	}
	return settings
}
