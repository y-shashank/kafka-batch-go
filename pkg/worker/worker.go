package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/daemon"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/metrics"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Run starts the Go backend worker: plain go job topics + go fair ready topics.
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
	config.SetHandlerLookup(func(jt string) bool { _, ok := kbatch.Lookup(jt); return ok })
	defaultTopic := defaultJobsTopic(cfg)
	if err := manifest.Validate(defaultTopic); err != nil {
		return err
	}
	if err := cfg.ValidateFairReadySplit(manifest); err != nil {
		return err
	}
	if !manifest.HasGoHandlers() {
		return fmt.Errorf("no runtime:go handlers in manifest — kbatch worker has nothing to execute")
	}
	if err := metrics.Install(metrics.FromDaemon(cfg)); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer metrics.Reset()

	jobTopics := manifest.JobTopicsGo(defaultTopic)
	for _, t := range cfg.JobsTopics {
		if manifest.TopicRuntime(t, defaultTopic) == config.RuntimeGo {
			jobTopics = append(jobTopics, t)
		}
	}
	jobTopics = uniqueStrings(jobTopics)

	var prioReg priority.Registry
	if len(cfg.PriorityConfigPaths) > 0 {
		prioReg, err = priority.LoadRegistry(cfg.PriorityConfigPaths, cfg, cfg.JobsTopics)
		if err != nil {
			return fmt.Errorf("priority config: %w", err)
		}
		reserved := map[string]struct{}{}
		for _, t := range prioReg.AllTopics() {
			reserved[t] = struct{}{}
		}
		filtered := make([]string, 0, len(jobTopics))
		for _, t := range jobTopics {
			if _, skip := reserved[t]; skip {
				continue
			}
			filtered = append(filtered, t)
		}
		jobTopics = filtered
	}
	goPrio := goPriorityConfigs(cfg, prioReg, manifest, defaultTopic)
	fairReadyTopics := workerFairReadyTopics(cfg, manifest)

	if len(jobTopics) == 0 && len(fairReadyTopics) == 0 && len(goPrio) == 0 {
		return fmt.Errorf("no go worker topics (manifest has no runtime:go plain, priority, or fair-ready work)")
	}

	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(rOpts)
	defer rdb.Close()
	if err := daemon.PingRedis(ctx, rdb); err != nil {
		return err
	}

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

	jobProc := &job.Processor{
		Cfg:      cfg,
		Manifest: manifest,
		Store:    st,
		Producer: prod,
	}
	if cfg.FairnessEnabled {
		jobProc.FairTime = fairness.NewScheduler(rdb, cfg.FairnessTimeSettings())
		jobProc.FairThroughput = fairness.NewScheduler(rdb, cfg.FairnessThroughputSettings())
	}

	failures, closeFailures, err := daemon.BuildFailureRecorder(cfg, st)
	if err != nil {
		return err
	}
	defer closeFailures()
	jobProc.Failures = failures
	live := daemon.NewLivenessReporter(cfg, rdb)
	jobProc.Liveness = live
	handleJob := daemon.BuildJobHandler(cfg, prod, jobProc)
	pauseCtl, _, closePauseCtl, err := daemon.BuildPauseControl(cfg, rdb)
	if err != nil {
		return err
	}
	defer closePauseCtl()
	consumerHealth := daemon.NewConsumerHealth(cfg)
	daemon.StartHealthServer(ctx, cfg, "worker", consumerHealth)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	group := cfg.ConsumerGroup + "-go-worker"
	processWorkers := cfg.JobProcessWorkers()
	fetch := cfg.ConsumerFetchSettings()

	if len(jobTopics) > 0 {
		jobsGroup := group + "-jobs"
		daemon.RunConcurrentConsumerGroupMembers(ctx, cfg.JobsConsumerMembers(), processWorkers,
			cfg.Brokers, jobsGroup, jobTopics, fetch, handleJob, consumerHealth, pauseCtl, live)
		log.Printf("kbatch go-worker jobs group=%s members=%d process_workers=%d topics=%v",
			jobsGroup, cfg.JobsConsumerMembers(), processWorkers, jobTopics)
	}

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("lag client: %w", err)
	}
	defer lagClient.Close()
	priorityLag := priority.NewLagReader(lagClient)
	for _, pc := range goPrio {
		gate := priority.NewGate(priorityLag, cfg.PriorityLagCheckInterval)
		gate.Consumption = pauseCtl
		daemon.RunPriorityGroupMembers(ctx, cfg.PriorityConsumerMembers(), processWorkers,
			cfg, pc, gate, handleJob, consumerHealth, pauseCtl, live)
	}

	for _, spec := range fairReadyTopics {
		readyGroup := cfg.GoWorkerFairReadyGroup(spec.lane)
		daemon.RunConcurrentConsumerGroupMembers(ctx, cfg.FairReadyConsumerMembers(), processWorkers,
			cfg.Brokers, readyGroup, []string{spec.topic}, fetch, handleJob, consumerHealth, pauseCtl, live)
		log.Printf("kbatch go-worker fair-ready group=%s members=%d process_workers=%d topic=%s",
			readyGroup, cfg.FairReadyConsumerMembers(), processWorkers, spec.topic)
	}

	log.Printf("kbatch go-worker running group=%s plain=%v priority_groups=%d fair_ready=%v members(jobs=%d fair=%d prio=%d) process_workers=%d fetch_max_bytes=%d fetch_max_partition_bytes=%d fetch_max_wait=%s",
		group, jobTopics, len(goPrio), fairReadyTopicNames(fairReadyTopics),
		cfg.JobsConsumerMembers(), cfg.FairReadyConsumerMembers(), cfg.PriorityConsumerMembers(), processWorkers,
		fetch.MaxBytes, fetch.MaxPartitionBytes, fetch.MaxWait)
	if ready := os.Getenv("KBATCH_WORKER_READY_FILE"); ready != "" {
		_ = os.WriteFile(ready, []byte("ok\n"), 0o644)
	}

	<-ctx.Done()
	return nil
}

type fairReadySpec struct {
	lane  string
	topic string
}

func workerFairReadyTopics(cfg config.Daemon, manifest config.Manifest) []fairReadySpec {
	if !cfg.FairnessEnabled {
		return nil
	}
	var out []fairReadySpec
	for _, lane := range []string{"time", "throughput"} {
		if !manifest.HasFairHandlersForRuntime(config.RuntimeGo, lane) {
			continue
		}
		topics := cfg.FairReadyTopics(lane)
		if cfg.RuntimeSplitFairReady(lane) {
			if topics.Go != "" {
				out = append(out, fairReadySpec{lane: lane, topic: topics.Go})
			}
			continue
		}
		if topics.Legacy != "" {
			out = append(out, fairReadySpec{lane: lane, topic: topics.Legacy})
		}
	}
	return out
}

func fairReadyTopicNames(specs []fairReadySpec) []string {
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		out = append(out, s.topic)
	}
	return out
}

func goPriorityConfigs(cfg config.Daemon, reg priority.Registry, manifest config.Manifest, defaultTopic string) []priority.Config {
	var out []priority.Config
	for _, pc := range reg.Configs {
		topics := manifest.FilterTopicsForRuntime(config.RuntimeGo, pc.Topics, defaultTopic)
		if len(topics) == 0 {
			continue
		}
		out = append(out, pc.WithTopics(topics).WithConsumerGroup(cfg.GoWorkerPriorityGroup(pc.ConsumerGroupSuffix)))
	}
	return out
}

func defaultJobsTopic(cfg config.Daemon) string {
	base := "kafka_batch.jobs"
	if cfg.TopicPrefix != "" {
		return cfg.TopicPrefix + "." + base
	}
	return base
}

func uniqueStrings(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// Main is a helper for cmd/kbatch worker when no signal context provided.
func Main(cfgPath, manifestPath string) {
	if err := Run(context.Background(), cfgPath, manifestPath); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch worker: %v\n", err)
		os.Exit(1)
	}
}
