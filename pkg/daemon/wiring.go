package daemon

import (
	"context"
	"strings"

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

func BuildPauseControl(cfg config.Daemon, rdb *redis.Client) (pauseChecker, *consumption.MySQLPauseStore, func()) {
	ctl := consumption.NewControl(rdb, cfg.ConsumptionControlRefreshInterval)
	if strings.ToLower(cfg.Store) != "mysql" || cfg.StoreMySQLDSN == "" {
		return ctl, nil, func() {}
	}
	mysqlPause, err := consumption.NewMySQLPauseStore(cfg.StoreMySQLDSN)
	if err != nil {
		return ctl, nil, func() {}
	}
	return consumption.NewHybridControl(rdb, mysqlPause, cfg.ConsumptionControlRefreshInterval), mysqlPause, func() { _ = mysqlPause.Close() }
}

func BuildFailureRecorder(cfg config.Daemon, st *store.RedisStore) (store.FailureRecorder, func()) {
	if strings.ToLower(cfg.Store) != "mysql" || cfg.StoreMySQLDSN == "" {
		return st, func() {}
	}
	mysqlFailures, err := store.NewMySQLFailures(cfg.StoreMySQLDSN)
	if err != nil {
		return st, func() {}
	}
	return &store.CompositeFailures{Redis: st, MySQL: mysqlFailures}, func() { _ = mysqlFailures.Close() }
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

func StartHealthServer(ctx context.Context, cfg config.Daemon, process string) {
	if !cfg.LivenessEnabled {
		return
	}
	go func() {
		_ = (&health.Server{Addr: cfg.LivenessHTTPAddr, Process: process}).ListenAndServe(ctx)
	}()
}

func NewLivenessReporter(cfg config.Daemon, rdb *redis.Client) *liveness.Reporter {
	if !cfg.LivenessEnabled || rdb == nil {
		return nil
	}
	r := liveness.NewReporter(rdb, cfg.LivenessTTL)
	r.TrackRunningJobs = cfg.TrackRunningJobs
	return r
}

func attachIngestLag(settings fairness.Settings, lag fairness.IngestLagCounter) fairness.Settings {
	if settings.ActiveCountSource == "ingest_lag" && lag != nil {
		settings.IngestLag = lag
	}
	return settings
}
