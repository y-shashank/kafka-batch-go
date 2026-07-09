package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/event"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/metrics"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
	"github.com/y-shashank/kafka-batch-go/pkg/reconciler"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/schedule"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Run starts the control plane (fair dispatch, events, retries, callbacks, schedule).
// Job execution is handled by kbatch worker (Go) or Karafka JobConsumer (Ruby).
func Run(ctx context.Context, cfgPath, manifestPath string) error {
	cfg, err := config.LoadDaemon(cfgPath)
	if err != nil {
		return err
	}
	if manifestPath != "" {
		cfg.HandlerManifest = manifestPath
	}
	manifest, err := config.LoadManifest(cfg.HandlerManifest, cfg.TopicPrefix)
	if err != nil {
		return err
	}
	defaultTopic := prefixOr(cfg.TopicPrefix, "") + "kafka_batch.jobs"
	if err := manifest.ValidateRouting(defaultTopic); err != nil {
		return err
	}
	if err := cfg.ValidateFairReadySplit(manifest); err != nil {
		return err
	}
	if manifest.HasGoHandlers() {
		log.Printf("kbatch daemon: runtime:go handlers run in kbatch worker (not in control plane)")
	}
	if manifest.HasRubyHandlers() {
		log.Printf("kbatch daemon: runtime:ruby handlers run in Karafka JobConsumer (not in control plane)")
	}
	if err := metrics.Install(metrics.FromDaemon(cfg)); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer metrics.Reset()

	var prioReg priority.Registry
	if len(cfg.PriorityConfigPaths) > 0 {
		prioReg, err = priority.LoadRegistry(cfg.PriorityConfigPaths, cfg, cfg.JobsTopics)
		if err != nil {
			return fmt.Errorf("priority config: %w", err)
		}
	}
	scheduleDefaultTopic := defaultTopic
	if len(cfg.JobsTopics) > 0 {
		scheduleDefaultTopic = cfg.JobsTopics[0]
	} else if topics := prioReg.AllTopics(); len(topics) > 0 {
		scheduleDefaultTopic = topics[0]
	}

	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(rOpts)
	defer rdb.Close()
	if err := PingRedis(ctx, rdb); err != nil {
		return err
	}

	st := store.NewRedisStore(rdb, cfg.BatchTTL)
	prod, err := kafkaclient.New(cfg.Brokers)
	if err != nil {
		return err
	}
	defer prod.Close()

	eventProc := &event.Processor{Cfg: cfg, Store: st, Producer: prod}
	retryProc := &retry.Processor{Producer: prod, MaxPause: cfg.RetryMaxPause}
	pauseCtl, _, closePauseCtl := BuildPauseControl(cfg, rdb)
	defer closePauseCtl()
	failures, closeFailures := BuildFailureRecorder(cfg, st)
	defer closeFailures()
	live := NewLivenessReporter(cfg, rdb)
	tenants := BuildTenantPartitions(cfg, rdb, prod)
	consumerHealth := NewConsumerHealth(cfg)
	StartHealthServer(ctx, cfg, "daemon", consumerHealth)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("lag client: %w", err)
	}
	defer lagClient.Close()
	ingestLag := priority.NewLagReader(lagClient)

	RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-events", []string{cfg.EventsTopic}, func(rec *kgo.Record) error {
		reconciler.MaybeRun(ctx, cfg, st, prod)
		_, err := eventProc.ProcessBatch(ctx, [][]byte{rec.Value})
		return err
	}, consumerHealth, nil, nil)

	retryTopics := cfg.RetryTopics()
	if len(retryTopics) > 0 {
		RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-retry", retryTopics, func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := retryProc.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			return applyRetryOutcome(ctx, cfg, prod, out, src)
		}, consumerHealth, pauseCtl, live)
	}

	if cfg.SchedulePollerEnabled {
		var schedStore schedule.IndexStore
		var mysqlSched *schedule.MysqlStore
		switch strings.ToLower(cfg.ScheduleStore) {
		case "mysql":
			ms, err := schedule.NewMysqlStore(cfg.ScheduleMySQLDSN, cfg.ScheduleBatchSize*5)
			if err != nil {
				return fmt.Errorf("schedule mysql store: %w", err)
			}
			mysqlSched = ms
			schedStore = ms
			defer mysqlSched.Close()
		default:
			schedStore = schedule.NewRedisStore(rdb, cfg.ScheduleBatchSize*5)
		}
		reader, err := schedule.NewReader(cfg.Brokers, cfg.ScheduledTopic)
		if err != nil {
			return fmt.Errorf("schedule reader: %w", err)
		}
		defer reader.Close()
		poller := &schedule.Poller{
			Cfg:      cfg,
			Store:    schedStore,
			Reader:   reader,
			Producer: prod,
			Router: schedule.DaemonRouter{
				Manifest: manifest,
				Cfg:      cfg,
				Default:  scheduleDefaultTopic,
				Tenants:  tenants,
			},
			Cancelled: st.BatchCancelled,
		}
		go poller.Run(ctx)
		log.Printf("kbatch schedule poller enabled topic=%s", cfg.ScheduledTopic)
	}

	if cfg.FairnessEnabled {
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, failures, consumerHealth, pauseCtl, live, ingestLag,
			fairness.LaneTime, cfg.FairnessTimeIngest, cfg.FairnessTimeSettings())
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, failures, consumerHealth, pauseCtl, live, ingestLag,
			fairness.LaneThroughput, cfg.FairnessThroughputIngest, cfg.FairnessThroughputSettings())
		log.Printf("kbatch fairness ingest dispatch enabled time=%s throughput=%s",
			cfg.FairnessTimeIngest, cfg.FairnessThroughputIngest)
	}

	log.Printf("kbatch daemon running group=%s (control plane — no job execution)", cfg.ConsumerGroup)
	if ready := os.Getenv("KBATCH_DAEMON_READY_FILE"); ready != "" {
		_ = os.WriteFile(ready, []byte("ok\n"), 0o644)
	}
	<-ctx.Done()
	return nil
}

func wireFairLane(
	ctx context.Context,
	cfg config.Daemon,
	manifest config.Manifest,
	rdb *redis.Client,
	prod *kafkaclient.Client,
	st *store.RedisStore,
	failures store.FailureRecorder,
	consumerHealth *ConsumerHealth,
	pauseCtl pauseChecker,
	live *liveness.Reporter,
	ingestLag fairness.IngestLagCounter,
	lane fairness.Lane,
	ingest string,
	settings fairness.Settings,
) {
	settings = attachIngestLag(settings, ingestLag)
	sched := fairness.NewScheduler(rdb, settings)
	laneName := string(lane)
	resolveReady := fairReadyResolver(manifest, cfg, laneName)
	expPub := newExpiredPublisher(cfg, prod, st, failures)
	coord := fairness.NewCoordinator(func(l fairness.Lane) {
		if l != lane {
			return
		}
		fwd := &fairness.Forwarder{
			Lane: l, Scheduler: sched, ResolveReadyTopic: resolveReady, Producer: prod,
			OnExpired: func(ctx context.Context, _ *fairness.CheckoutResult, raw []byte) error {
				var m map[string]interface{}
				_ = json.Unmarshal(raw, &m)
				src := jobexpiry.SourceCoords(m)
				return expPub.publish(ctx, raw, src)
			},
		}
		go fwd.Run(ctx)
	})
	disp := &fairness.Dispatcher{
		Lane: lane, Scheduler: sched, OnStartFwd: coord.OnStart(lane),
		OnExpired: func(ctx context.Context, raw []byte, src protocol.SourceCoords) error {
			return expPub.publish(ctx, raw, src)
		},
	}
	suffix := string(lane)
	dispatchGroup := cfg.DispatchConsumerGroup(suffix)
	RunConsumer(ctx, cfg.Brokers, dispatchGroup,
		[]string{ingest}, func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := disp.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			if !out.CommitOffset {
				return fmt.Errorf("fair ingest backpressure lane=%s tenant=%s", lane, out.TenantID)
			}
			return nil
		}, consumerHealth, pauseCtl, live)
}

func prefixOr(prefix, base string) string {
	if prefix == "" {
		return base
	}
	if strings.HasPrefix(base, prefix+".") {
		return base
	}
	return prefix + "." + base
}

// Main is a helper for cmd/kbatch daemon when no signal context provided.
func Main(cfgPath, manifestPath string) {
	if err := Run(context.Background(), cfgPath, manifestPath); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch daemon: %v\n", err)
		os.Exit(1)
	}
}
