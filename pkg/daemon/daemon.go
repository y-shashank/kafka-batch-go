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

	"github.com/y-shashank/kafka-batch-go/pkg/cancellation"
	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/event"
	"github.com/y-shashank/kafka-batch-go/pkg/control/callback"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/health"
	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/metrics"
	"github.com/y-shashank/kafka-batch-go/pkg/perfmetrics"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/reconciler"
	"github.com/y-shashank/kafka-batch-go/pkg/retrycancel"
	"github.com/y-shashank/kafka-batch-go/pkg/schedule"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

// Run starts the control plane (fair dispatch, events, retries, callbacks, schedule).
// Job execution is handled by kbatch worker (Go) or Karafka JobConsumer (Ruby).
func Run(ctx context.Context, cfgPath, manifestPath string) error {
	cfg, err := config.LoadDaemon(cfgPath)
	if err != nil {
		return err
	}
	SetConsumerStallTimeout(cfg.ConsumerStallTimeoutDuration())
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
	if err := cfg.ValidateExecutionMode(); err != nil {
		return err
	}
	if err := cfg.ValidateMySQLConfig(); err != nil {
		return err
	}
	if err := cfg.ValidateRetryConsumers(); err != nil {
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
	if err := perfmetrics.Install(perfmetrics.FromDaemon(cfg), rdb); err != nil {
		return fmt.Errorf("perfmetrics: %w", err)
	}
	defer perfmetrics.Reset()

	st := store.NewRedisStore(rdb, cfg.BatchTTL)
	acks, err := kafkaclient.RequiredAcksFromConfig(cfg.RequiredAcks())
	if err != nil {
		return fmt.Errorf("producer acks: %w", err)
	}
	prod, err := kafkaclient.New(cfg.Brokers, kafkaclient.WithRequiredAcks(acks))
	if err != nil {
		return err
	}
	defer prod.Close()

	eventProc := &event.Processor{Cfg: cfg, Store: st, Producer: prod, NodeID: cfg.NodeID}
	retryCancel := &retrycancel.Store{Client: rdb}
	retryProc := &retry.Processor{Producer: prod, Cancel: retryCancel, MaxPause: cfg.RetryMaxPause}
	pauseCtl, _, closePauseCtl, err := BuildPauseControl(cfg, rdb)
	if err != nil {
		return err
	}
	defer closePauseCtl()
	failures, closeFailures, err := BuildFailureRecorder(cfg, st)
	if err != nil {
		return err
	}
	defer closeFailures()
	live := NewLivenessReporter(cfg, rdb)
	tenants := BuildTenantPartitions(cfg, rdb, prod)
	if tenants != nil {
		_ = tenants.Warm(context.Background(), "time")
		_ = tenants.Warm(context.Background(), "throughput")
	}
	consumerHealth := NewConsumerHealth(cfg)
	loopHealth := NewLoopHealth(cfg)
	StartHealthServer(ctx, cfg, "daemon", compositeHealth{checkers: []health.Checker{consumerHealth, loopHealth}})

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	StartLivenessHeartbeatLoop(ctx, live)

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("lag client: %w", err)
	}
	defer lagClient.Close()
	ingestLag := priority.NewLagReader(lagClient)

	eventsGroup := cfg.ConsumerGroup + "-events"
	fetch := cfg.ConsumerFetchSettings()
	RunPartitionConsumer(ctx, partitionConsumerConfig{
		brokers: cfg.Brokers,
		group:   eventsGroup,
		topics:  []string{cfg.EventsTopic},
		fetch:   fetch,
		handle: func(ctx context.Context, recs []*kgo.Record) error {
			if len(recs) == 0 {
				return nil
			}
			raw := make([][]byte, len(recs))
			for i, rec := range recs {
				raw[i] = rec.Value
			}
			_, err := eventProc.ProcessBatch(ctx, raw)
			return err
		},
		health:     consumerHealth,
		loopHealth: loopHealth,
		loopName:   "events-" + eventsGroup,
		live:       live,
	})
	reconciler.RunScheduler(ctx, cfg, st, prod, func() {
		loopHealth.RecordTick("reconciler")
	})
	// Guardrail: the workset reclaim loop recovers orphaned SuperFetch in-flight
	// jobs from Redis. Watermark mode owns durability via Kafka offset commits and
	// writes nothing to the working set, so reclaim has nothing to do — skip it to
	// keep the "one execution mode per cluster" contract unambiguous.
	if cfg.WatermarkMode() {
		log.Printf("kbatch workset reclaim DISABLED (execution_mode=watermark; durability is via Kafka offset watermarks, not the Redis working set)")
	} else {
		workset.RunReclaimScheduler(ctx, workset.NewStore(rdb), prod, cfg.SuperFetchReclaimEvery, cfg.SuperFetchReclaimLimit, cfg.SuperFetchOrphanGrace, func() {
			loopHealth.RecordTick("workset-reclaim")
		})
		log.Printf("kbatch workset reclaim enabled every=%s limit=%d grace=%s", cfg.SuperFetchReclaimEvery, cfg.SuperFetchReclaimLimit, cfg.SuperFetchOrphanGrace)
	}
	cancelCache := cancellation.New(cfg.CancellationCacheTTL, st.CancelledBatchIDs)
	cancellation.SetProcessCache(cancelCache)
	defer cancellation.SetProcessCache(nil)
	log.Printf("kbatch events consumer group=%s topic=%s acks=%s fetch_max_bytes=%d fetch_max_partition_bytes=%d fetch_max_wait=%s (one client, goroutine-per-partition)",
		eventsGroup, cfg.EventsTopic, cfg.RequiredAcks(),
		fetch.MaxBytes, fetch.MaxPartitionBytes, fetch.MaxWait)

	retryTopics := cfg.RetryTopics()
	retryGroup := cfg.ConsumerGroup + "-retry"
	RunRetryConsumer(ctx, cfg.Brokers, retryGroup, retryTopics, fetch,
		func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := retryProc.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			return applyRetryOutcome(ctx, cfg, prod, out, src)
		}, consumerHealth, nil, live, loopHealth)
	log.Printf("kbatch retry consumer group=%s topics=%v (one client)",
		retryGroup, retryTopics)

	// Legacy batch callbacks (on_success / on_complete class names). Events produce
	// to callbacks_topic with preclaimed:true; this consumer invokes and writes
	// callback_dispatched_by for the UI "Callback ran on" field.
	callbacksGroup := cfg.ConsumerGroup + "-callbacks"
	cbProc := &callback.Processor{
		Store:   st,
		Invoker: callback.LogInvoker{},
		DLT:     &callbackDLT{prod: prod, topic: cfg.DeadLetterTopic},
		NodeID:  cfg.NodeID,
	}
	RunPartitionConsumer(ctx, partitionConsumerConfig{
		brokers: cfg.Brokers,
		group:   callbacksGroup,
		topics:  []string{cfg.CallbacksTopic},
		fetch:   fetch,
		handle: func(ctx context.Context, recs []*kgo.Record) error {
			for _, rec := range recs {
				out, err := cbProc.Process(ctx, rec.Value)
				if err != nil {
					return err
				}
				if !out.CommitOffset {
					return fmt.Errorf("callback process defer commit batch topic=%s partition=%d offset=%d",
						rec.Topic, rec.Partition, rec.Offset)
				}
			}
			return nil
		},
		health:     consumerHealth,
		loopHealth: loopHealth,
		loopName:   "callbacks-" + callbacksGroup,
		live:       live,
	})
	log.Printf("kbatch callbacks consumer group=%s topic=%s (records callback_dispatched_by)",
		callbacksGroup, cfg.CallbacksTopic)

	if cfg.SchedulePollerEnabled {
		var schedStore schedule.IndexStore
		var mysqlSched *schedule.MysqlStore
		switch strings.ToLower(cfg.ScheduleStore) {
		case "mysql":
			if strings.TrimSpace(cfg.ScheduleMySQLDSN) == "" {
				return fmt.Errorf("schedule_mysql_dsn is required when schedule_store is mysql")
			}
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
			Cancelled: cancelCache.Cancelled,
			RecordActivity: func() {
				loopHealth.RecordTick("schedule-poller")
			},
		}
		go runLoopSupervised(ctx, "schedule-poller", loopHealth, func(ctx context.Context) error {
			poller.Run(ctx)
			return nil
		})
		log.Printf("kbatch schedule poller enabled topic=%s store=%s", cfg.ScheduledTopic, cfg.ScheduleStore)
	}

	if cfg.RecurringSchedulerEnabled {
		closeCron, err := StartRecurringScheduler(ctx, cfg, rdb, loopHealth)
		if err != nil {
			return fmt.Errorf("recurring scheduler: %w", err)
		}
		defer closeCron()
	}

	if cfg.FairnessEnabled {
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, failures, consumerHealth, loopHealth, pauseCtl, live, ingestLag,
			fairness.LaneTime, cfg.FairnessTimeIngest, cfg.FairnessTimeSettings())
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, failures, consumerHealth, loopHealth, pauseCtl, live, ingestLag,
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
	loopHealth *LoopHealth,
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
	fwdName := "fair-forward-" + laneName
	fwd := &fairness.Forwarder{
		Lane: lane, Scheduler: sched, ResolveReadyTopic: resolveReady, Producer: prod,
		OnExpired: func(ctx context.Context, _ *fairness.CheckoutResult, raw []byte) error {
			var m map[string]interface{}
			_ = json.Unmarshal(raw, &m)
			src := jobexpiry.SourceCoords(m)
			return expPub.publish(ctx, raw, src)
		},
		RecordActivity: func() {
			loopHealth.RecordTick(fwdName)
		},
	}
	go runLoopSupervised(ctx, fwdName, loopHealth, func(ctx context.Context) error {
		fwd.Run(ctx)
		return nil
	})
	disp := &fairness.Dispatcher{
		Lane: lane, Scheduler: sched,
		OnExpired: func(ctx context.Context, raw []byte, src protocol.SourceCoords) error {
			return expPub.publish(ctx, raw, src)
		},
	}
	suffix := string(lane)
	dispatchGroup := cfg.DispatchConsumerGroup(suffix)
	RunConsumer(ctx, cfg.Brokers, dispatchGroup,
		[]string{ingest}, cfg.ConsumerFetchSettings(), func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := disp.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			if !out.CommitOffset {
				return &fairBackpressureError{
					lane:     laneName,
					tenantID: out.TenantID,
					duration: fairBackpressurePause,
				}
			}
			return nil
		}, consumerHealth, pauseCtl, live)
}

// callbackDLT adapts kafkaProducer to callback.DLTProducer.
type callbackDLT struct {
	prod  kafkaProducer
	topic string
}

func (d *callbackDLT) ProduceDLT(ctx context.Context, key string, payload []byte) error {
	if d == nil || d.prod == nil || d.topic == "" {
		return nil
	}
	return d.prod.Produce(ctx, d.topic, key, payload)
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
