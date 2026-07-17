package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/cancellation"
	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/daemon"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/metrics"
	"github.com/y-shashank/kafka-batch-go/pkg/perfmetrics"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/retrycancel"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

// Run starts the Go backend worker: plain go job topics + go fair ready topics.
func Run(ctx context.Context, cfgPath, manifestPath string) error {
	cfg, err := config.LoadDaemon(cfgPath)
	if err != nil {
		return err
	}
	daemon.SetConsumerStallTimeout(cfg.ConsumerStallTimeoutDuration())
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
	if err := cfg.ValidateExecutionMode(); err != nil {
		return err
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

	// Guard: a worker executor (SuperFetch or watermark) re-runs jobs on
	// crash/rebalance, so it must never be pointed at a control-plane topic —
	// executing the events topic would double-count batch completions; retry/
	// callbacks/scheduled/fair-ingest would corrupt control state. These topics
	// are owned by the daemon's own consumers, which commit only after their
	// idempotent ledger update. Reject a manifest that routes execution here.
	control := cfg.ControlPlaneTopics()
	var execTopics []string
	execTopics = append(execTopics, jobTopics...)
	for _, s := range fairReadyTopics {
		execTopics = append(execTopics, s.topic)
	}
	for _, pc := range goPrio {
		execTopics = append(execTopics, pc.Topics...)
	}
	for _, t := range execTopics {
		if _, bad := control[t]; bad {
			return fmt.Errorf("execution topic %q is a control-plane topic (events/retry/callbacks/dead_letter/scheduled/fair-ingest) — a worker executor must never run jobs on it (would double-count completions or corrupt control state); fix the handler manifest", t)
		}
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

	cancelCache := cancellation.New(cfg.CancellationCacheTTL, st.CancelledBatchIDs)
	cancellation.SetProcessCache(cancelCache)
	defer cancellation.SetProcessCache(nil)

	jobProc := &job.Processor{
		Cfg:         cfg,
		Manifest:    manifest,
		Store:       st,
		Producer:    prod,
		CancelCache: cancelCache,
		RetryCancel: &retrycancel.Store{Client: rdb},
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

	// Watermark mode runs the execution path Redis-free: no working set, no
	// reclaim, and no live:consumer heartbeat (nothing depends on it). SuperFetch
	// keeps the reclaim heartbeat so orphaned in-flight jobs can be recovered.
	wmMode := cfg.WatermarkMode()
	var live *liveness.Reporter
	if !wmMode {
		liveCfg := cfg
		liveCfg.LivenessEnabled = true
		live = daemon.NewLivenessReporter(liveCfg, rdb)
		jobProc.Liveness = live
	}

	// runCtx: cancelled on SIGTERM/SIGINT → stop Kafka poll / new claims.
	// lifeCtx: stays alive through drain so renew/heartbeat/#perform finish
	// (or leave workset entries for control-plane reclaim).
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()

	if !wmMode {
		daemon.StartLivenessHeartbeatLoop(lifeCtx, live)
	}
	pauseCtl, _, closePauseCtl, err := daemon.BuildPauseControl(cfg, rdb)
	if err != nil {
		return err
	}
	defer closePauseCtl()
	consumerHealth := daemon.NewConsumerHealth(cfg)
	daemon.StartHealthServer(runCtx, cfg, "worker", consumerHealth)

	group := cfg.ConsumerGroup + "-go-worker"
	fetch := cfg.ConsumerFetchSettings()

	// Shared perform/apply closures for whichever executor mode is active.
	processFn := func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
		return jobProc.Process(ctx, raw, src)
	}
	applyFn := func(ctx context.Context, out job.Outcome) error {
		return daemon.ApplyJobSideEffects(ctx, cfg, prod, out)
	}

	var (
		work  *workset.Store // SuperFetch only; nil in watermark mode
		sfsMu sync.Mutex
		sfs   []*daemon.SuperFetchExecutor
		wmsMu sync.Mutex
		wms   []*daemon.WatermarkExecutor
	)
	newSF := func(consumerID string) *daemon.SuperFetchExecutor {
		sf := daemon.NewSuperFetchExecutor(cfg, work, consumerID, processFn, applyFn)
		sfsMu.Lock()
		sfs = append(sfs, sf)
		sfsMu.Unlock()
		return sf
	}
	newWM := func(consumerID string) *daemon.WatermarkExecutor {
		wm := daemon.NewWatermarkExecutor(cfg, consumerID, processFn, applyFn)
		wmsMu.Lock()
		wms = append(wms, wm)
		wmsMu.Unlock()
		return wm
	}

	if wmMode {
		// Boot guardrail: make the local config unambiguous. We cannot detect a
		// sibling SuperFetch worker on the same topics from here — the operator
		// must guarantee one execution mode per topic (see README).
		log.Printf("[kbatch-worker] ================ EXECUTION MODE = watermark ================")
		log.Printf("[kbatch-worker]  Redis-free execution: no working set, no reclaim, no live heartbeat.")
		log.Printf("[kbatch-worker]  REQUIRED: handlers MUST be idempotent (uncommitted work re-runs on crash/rebalance).")
		log.Printf("[kbatch-worker]  REQUIRED: keep per-topic job runtimes similar (a slow job holds the watermark).")
		log.Printf("[kbatch-worker]  REQUIRED: one mode per topic — never run a SuperFetch worker on these topics.")
		log.Printf("[kbatch-worker]  plain=%v priority_groups=%d fair_ready=%v concurrency=%d window=%d",
			jobTopics, len(goPrio), fairReadyTopicNames(fairReadyTopics),
			cfg.SuperFetchWorkers(), cfg.SuperFetchClaimWindowSize())
		log.Printf("[kbatch-worker] ============================================================")
	} else {
		work = workset.NewStore(rdb)
		log.Printf("kbatch go-worker superfetch concurrency=%d lease_ttl=%s drain_timeout=%s",
			cfg.SuperFetchWorkers(), cfg.SuperFetchLeaseTTL, cfg.SuperFetchDrainTimeoutDuration())
	}

	if len(jobTopics) > 0 {
		jobsGroup := group + "-jobs"
		if wmMode {
			daemon.RunWatermarkConsumerGroupMembers(runCtx, lifeCtx, cfg.JobsConsumerMembers(),
				cfg.Brokers, jobsGroup, jobTopics, fetch, newWM, consumerHealth, pauseCtl, live)
		} else {
			daemon.RunSuperFetchConsumerGroupMembers(runCtx, lifeCtx, cfg.JobsConsumerMembers(),
				cfg.Brokers, jobsGroup, jobTopics, fetch, newSF, consumerHealth, pauseCtl, live)
		}
		log.Printf("kbatch go-worker jobs group=%s members=%d mode=%s concurrency=%d topics=%v",
			jobsGroup, cfg.JobsConsumerMembers(), cfg.NormalizedExecutionMode(), cfg.SuperFetchWorkers(), jobTopics)
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
		if wmMode {
			daemon.RunPriorityGroupMembersWatermark(runCtx, lifeCtx, cfg.PriorityConsumerMembers(),
				cfg, pc, gate, newWM, consumerHealth, pauseCtl, live)
		} else {
			daemon.RunPriorityGroupMembersSuperFetch(runCtx, lifeCtx, cfg.PriorityConsumerMembers(),
				cfg, pc, gate, newSF, consumerHealth, pauseCtl, live)
		}
	}

	for _, spec := range fairReadyTopics {
		readyGroup := cfg.GoWorkerFairReadyGroup(spec.lane)
		if wmMode {
			daemon.RunWatermarkConsumerGroupMembers(runCtx, lifeCtx, cfg.FairReadyConsumerMembers(),
				cfg.Brokers, readyGroup, []string{spec.topic}, fetch, newWM, consumerHealth, pauseCtl, live)
		} else {
			daemon.RunSuperFetchConsumerGroupMembers(runCtx, lifeCtx, cfg.FairReadyConsumerMembers(),
				cfg.Brokers, readyGroup, []string{spec.topic}, fetch, newSF, consumerHealth, pauseCtl, live)
		}
		log.Printf("kbatch go-worker fair-ready group=%s members=%d mode=%s concurrency=%d topic=%s",
			readyGroup, cfg.FairReadyConsumerMembers(), cfg.NormalizedExecutionMode(), cfg.SuperFetchWorkers(), spec.topic)
	}

	log.Printf("kbatch go-worker running group=%s mode=%s plain=%v priority_groups=%d fair_ready=%v members(jobs=%d fair=%d prio=%d) concurrency=%d fetch_max_bytes=%d fetch_max_partition_bytes=%d fetch_max_wait=%s",
		group, cfg.NormalizedExecutionMode(), jobTopics, len(goPrio), fairReadyTopicNames(fairReadyTopics),
		cfg.JobsConsumerMembers(), cfg.FairReadyConsumerMembers(), cfg.PriorityConsumerMembers(),
		cfg.SuperFetchWorkers(), fetch.MaxBytes, fetch.MaxPartitionBytes, fetch.MaxWait)
	if ready := os.Getenv("KBATCH_WORKER_READY_FILE"); ready != "" {
		_ = os.WriteFile(ready, []byte("ok\n"), 0o644)
	}

	<-runCtx.Done()
	if wmMode {
		drainWorkerWatermark(&wmsMu, &wms, cfg.SuperFetchDrainTimeoutDuration())
	} else {
		drainWorkerSuperFetch(work, &sfsMu, &sfs, cfg.SuperFetchDrainTimeoutDuration())
	}
	lifeCancel()
	return nil
}

// drainWorkerWatermark stops new dispatches and waits for in-flight #perform to
// finish (best-effort — commits already marked flush on client close). Any work
// that finishes after the poll loop closes its client stays uncommitted and
// re-runs on restart, which is watermark's documented at-least-once contract.
func drainWorkerWatermark(mu *sync.Mutex, wms *[]*daemon.WatermarkExecutor, timeout time.Duration) {
	mu.Lock()
	list := append([]*daemon.WatermarkExecutor(nil), (*wms)...)
	mu.Unlock()
	if len(list) == 0 {
		return
	}
	for _, wm := range list {
		wm.StopAccepting()
	}
	log.Printf("kbatch go-worker draining watermark in-flight timeout=%s members=%d", timeout, len(list))
	remaining := 0
	for _, wm := range list {
		remaining += wm.WaitInFlight(timeout)
	}
	if remaining > 0 {
		log.Printf("kbatch go-worker watermark drain timed out with %d in-flight job(s) — they re-run on restart", remaining)
	} else {
		log.Printf("kbatch go-worker watermark drain complete")
	}
}

// drainWorkerSuperFetch stops new claims, waits for in-flight #perform, then
// deletes live:consumer keys for any leftovers so reclaim can start immediately.
func drainWorkerSuperFetch(work *workset.Store, mu *sync.Mutex, sfs *[]*daemon.SuperFetchExecutor, timeout time.Duration) {
	mu.Lock()
	list := append([]*daemon.SuperFetchExecutor(nil), (*sfs)...)
	mu.Unlock()
	if len(list) == 0 {
		return
	}
	for _, sf := range list {
		sf.StopAccepting()
	}
	log.Printf("kbatch go-worker draining superfetch in-flight timeout=%s members=%d", timeout, len(list))
	remaining := 0
	for _, sf := range list {
		remaining += sf.WaitInFlight(timeout)
	}
	instrument.SuperFetchDrained(remaining, timeout)
	if remaining > 0 {
		log.Printf("kbatch go-worker drain timed out with %d in-flight job(s) — leaving workset for reclaim", remaining)
		bg := context.Background()
		for _, sf := range list {
			if err := work.DeleteConsumer(bg, sf.ConsumerID); err != nil {
				log.Printf("kbatch go-worker delete live consumer=%s: %v", sf.ConsumerID, err)
			}
		}
	} else {
		log.Printf("kbatch go-worker drain complete")
	}
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
