package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
)

// defaultPartitionPollRecords bounds each poll so a rebalance is never blocked for
// an unbounded batch (franz-go BlockRebalanceOnPoll guidance). Records are still fanned
// out per partition, so raising this only makes each partition's batch larger.
const defaultPartitionPollRecords = 500

// defaultPartitionChanBuffer is the per-partition worker channel capacity. Small on
// purpose: when a worker falls behind, the poll loop pauses that one partition
// (PauseFetchPartitions) instead of buffering unbounded records in memory.
const defaultPartitionChanBuffer = 2

// PartitionBatchHandler processes one partition's records in offset order. Returning
// nil marks the whole slice committed; returning an error marks nothing, so the batch
// is redelivered (at-least-once). Handlers must route true poison to a dead-letter
// topic and return nil rather than erroring forever on the same record.
//
// It is an alias of BatchHandler so it reuses safeBatchHandle's panic recovery; the
// distinct name documents that each call receives a single partition's records.
type PartitionBatchHandler = BatchHandler

// partitionConsumerConfig configures a single-client, goroutine-per-partition consumer.
// One kgo.Client joins the group and is assigned partitions; each assigned partition
// gets its own worker goroutine, so processing runs in parallel across partitions while
// staying in order within a partition. This replaces the old "N in-process members = N
// clients" pattern: horizontal scale comes from adding pods, not in-process members.
type partitionConsumerConfig struct {
	brokers      []string
	group        string
	topics       []string
	fetch        config.ConsumerFetchSettings
	maxRecords   int
	chanBuffer   int
	handle       PartitionBatchHandler
	health       *ConsumerHealth
	loopHealth   *LoopHealth
	loopName     string
	stallTimeout time.Duration
	pauseCtl     pauseChecker
	live         *liveness.Reporter
}

type partitionKey struct {
	topic     string
	partition int32
}

type partitionWorker struct {
	topic     string
	partition int32
	recs      chan kgo.FetchTopicPartition
	quit      chan struct{}
	done      chan struct{}
}

// offsetCommitter is the subset of *kgo.Client the engine uses to commit on revoke.
type offsetCommitter interface {
	CommitMarkedOffsets(ctx context.Context) error
}

type partitionEngine struct {
	cfg partitionConsumerConfig
	cl  *consumerClient

	// Operation seams. Nil in production (derived from cl); overridden in tests so the
	// worker/route/revoke logic can be exercised without a live broker.
	markOps   recordCommitter
	pauseOps  fetchPauser
	commitOps offsetCommitter

	mu      sync.Mutex
	workers map[partitionKey]*partitionWorker
	wg      sync.WaitGroup

	// procCtx is the context handed to worker processing; it is cancelled when a
	// rebalance is blocked (OnPartitionsCallbackBlocked) so in-flight work aborts
	// promptly and does not wedge the group.
	procMu  sync.Mutex
	procCtx context.Context
}

// RunPartitionConsumer starts a supervised, single-client goroutine-per-partition
// consumer that restarts with backoff on broker blips. Use it for throughput-oriented
// consumers whose per-partition work is independent (e.g. the events ledger). One
// process holds exactly one group member; Kafka splits partitions across pods.
func RunPartitionConsumer(ctx context.Context, cfg partitionConsumerConfig) {
	go func() {
		if cfg.health != nil {
			cfg.health.Register(cfg.group)
		}
		log.Printf("[kbatch-daemon] partition consumer group=%s topics=%v poll=%d",
			cfg.group, cfg.topics, effectivePartitionPoll(cfg.maxRecords))
		backoff := consumerRestartInitial
		for {
			if ctx.Err() != nil {
				return
			}
			err := runPartitionConsumerOnce(ctx, cfg)
			if ctx.Err() != nil || err == nil {
				return
			}
			log.Printf("[kbatch] partition consumer group=%s error=%v — restarting in %s", cfg.group, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < consumerRestartMax {
				backoff *= 2
				if backoff > consumerRestartMax {
					backoff = consumerRestartMax
				}
			}
		}
	}()
}

func effectivePartitionPoll(n int) int {
	if n > 0 {
		return n
	}
	return defaultPartitionPollRecords
}

func runPartitionConsumerOnce(ctx context.Context, cfg partitionConsumerConfig) error {
	e := &partitionEngine{
		cfg:     cfg,
		workers: map[partitionKey]*partitionWorker{},
	}
	cl, err := newPartitionConsumerClient(cfg, e)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", cfg.group, err)
	}
	e.cl = cl
	defer closeGroupConsumer(cl)

	return e.poll(ctx)
}

// newPartitionConsumerClient builds the group client with the same guarantees as the
// serial consumers (BlockRebalanceOnPoll, AutoCommitMarks, rebalance-abort) plus the
// partition lifecycle callbacks that spawn/stop per-partition workers. Supplying our own
// OnPartitionsRevoked overrides franz-go's default auto-commit-on-revoke, so the engine
// commits marks explicitly in revoked()/lost() before releasing partitions.
func newPartitionConsumerClient(cfg partitionConsumerConfig, e *partitionEngine) (*consumerClient, error) {
	cc := &consumerClient{
		topics:      append([]string(nil), cfg.topics...),
		topicPaused: map[string]bool{},
		partPaused:  map[string][]int32{},
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumerGroup(cfg.group),
		kgo.ConsumeTopics(cfg.topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(e.assigned),
		kgo.OnPartitionsRevoked(e.revoked),
		kgo.OnPartitionsLost(e.lost),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
			log.Printf("[kbatch-daemon] group=%s rebalance waiting — aborting in-flight processing", cfg.group)
			cc.abort.trigger()
		}),
	}
	opts = append(opts, kafkaclient.FetchOpts(cfg.fetch)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	cc.Client = cl
	return cc, nil
}

// assigned spawns one worker goroutine per newly assigned partition. It runs on
// franz-go's group-management goroutine while the poll gate is released, so the workers
// map is guarded by mu against the poll loop's routing.
func (e *partitionEngine) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for topic, parts := range assigned {
		for _, p := range parts {
			key := partitionKey{topic, p}
			if _, ok := e.workers[key]; ok {
				continue
			}
			buf := e.cfg.chanBuffer
			if buf < 1 {
				buf = defaultPartitionChanBuffer
			}
			w := &partitionWorker{
				topic:     topic,
				partition: p,
				recs:      make(chan kgo.FetchTopicPartition, buf),
				quit:      make(chan struct{}),
				done:      make(chan struct{}),
			}
			e.workers[key] = w
			e.wg.Add(1)
			go e.runWorker(w)
		}
	}
}

func (e *partitionEngine) revoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	e.stopPartitions(revoked)
	// Commit what workers marked before the partitions move to another member,
	// otherwise the next owner reprocesses from the last periodic auto-commit.
	if err := e.committer().CommitMarkedOffsets(ctx); err != nil {
		log.Printf("[kbatch-daemon] group=%s commit-on-revoke error: %v", e.cfg.group, err)
	}
}

func (e *partitionEngine) lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	// Partitions are already gone (session/heartbeat loss); stop workers but do not
	// try to commit — the broker will reject offsets for partitions we no longer own.
	e.stopPartitions(lost)
}

func (e *partitionEngine) stopPartitions(parts map[string][]int32) {
	e.mu.Lock()
	stop := make([]*partitionWorker, 0)
	for topic, ps := range parts {
		for _, p := range ps {
			key := partitionKey{topic, p}
			if w, ok := e.workers[key]; ok {
				close(w.quit)
				stop = append(stop, w)
				delete(e.workers, key)
			}
		}
	}
	e.mu.Unlock()
	for _, w := range stop {
		<-w.done
	}
}

func (e *partitionEngine) marker() recordCommitter {
	if e.markOps != nil {
		return e.markOps
	}
	return e.cl.Client
}

func (e *partitionEngine) pauser() fetchPauser {
	if e.pauseOps != nil {
		return e.pauseOps
	}
	return e.cl.pauser()
}

func (e *partitionEngine) committer() offsetCommitter {
	if e.commitOps != nil {
		return e.commitOps
	}
	return e.cl.Client
}

func (e *partitionEngine) workerFor(topic string, partition int32) *partitionWorker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.workers[partitionKey{topic, partition}]
}

func (e *partitionEngine) currentProcCtx() context.Context {
	e.procMu.Lock()
	defer e.procMu.Unlock()
	if e.procCtx == nil {
		return context.Background()
	}
	return e.procCtx
}

func (e *partitionEngine) runWorker(w *partitionWorker) {
	defer e.wg.Done()
	defer close(w.done)
	for {
		select {
		case <-w.quit:
			return
		case ftp := <-w.recs:
			if len(ftp.Records) == 0 {
				continue
			}
			ctx := e.currentProcCtx()
			if e.cfg.live != nil {
				e.cfg.live.Heartbeat(ctx, ftp.Topic)
			}
			if err := safeBatchHandle(ctx, e.cfg.handle, ftp.Records); err != nil {
				if !isContextErr(err) {
					log.Printf("[kbatch-daemon] partition handler error group=%s topic=%s partition=%d records=%d: %v",
						e.cfg.group, ftp.Topic, ftp.Partition, len(ftp.Records), err)
				}
				// Do not mark: the whole partition batch redelivers (at-least-once).
				continue
			}
			e.marker().MarkCommitRecords(ftp.Records...)
		}
	}
}

// poll implements the franz-go BlockRebalanceOnPoll contract, routing each partition's
// records to its worker without blocking the poll goroutine. A full worker channel means
// that one partition is paused (PauseFetchPartitions) until the worker drains, giving
// per-partition backpressure with no head-of-line blocking across partitions.
func (e *partitionEngine) poll(ctx context.Context) error {
	loopCtx, touch, stopGuard := attachConsumerStallGuardFor(ctx, e.cl, "partition consumer group="+e.cfg.group, effectiveStallTimeout(e.cfg.stallTimeout))
	defer stopGuard()
	defer e.drainAllWorkers()

	maxRecords := effectivePartitionPoll(e.cfg.maxRecords)
	healthKey := e.cfg.group

	for {
		if err := consumerLoopDoneErr(loopCtx); err != nil {
			if errors.Is(err, errConsumerStalled) {
				return stalledRestartErr(e.cfg.group)
			}
			return err
		}

		e.cl.AllowRebalance()
		e.cl.syncConsumptionFetchPause(loopCtx, e.cfg.pauseCtl, e.cfg.group)

		touch()
		fetches := e.cl.PollRecords(loopCtx, maxRecords)
		if err := checkFetchErrs(loopCtx, e.cl, fetches, e.cfg.group); err != nil {
			return err
		}

		if e.cfg.health != nil {
			e.cfg.health.RecordPoll(healthKey)
		}
		if e.cfg.loopHealth != nil && e.cfg.loopName != "" {
			e.cfg.loopHealth.RecordTick(e.cfg.loopName)
		}
		touch()

		procCtx, endProc := e.cl.abort.begin(loopCtx)
		e.setProcCtx(procCtx)
		e.route(loopCtx, fetches)
		endProc()

		e.cl.AllowRebalance()
	}
}

func (e *partitionEngine) setProcCtx(ctx context.Context) {
	e.procMu.Lock()
	e.procCtx = ctx
	e.procMu.Unlock()
}

func (e *partitionEngine) route(ctx context.Context, fetches kgo.Fetches) {
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		if len(ftp.Records) == 0 {
			return
		}
		w := e.workerFor(ftp.Topic, ftp.Partition)
		if w == nil {
			return
		}
		e.deliver(ctx, w, ftp)
	})
}

// deliver hands a partition batch to its worker without blocking the poll goroutine.
// When the worker's channel is full, the partition is paused (so we stop fetching just
// that partition) and the batch is handed off out-of-band; the partition resumes once
// the worker accepts it. This gives per-partition backpressure with no head-of-line
// blocking across the other partitions.
func (e *partitionEngine) deliver(ctx context.Context, w *partitionWorker, ftp kgo.FetchTopicPartition) {
	select {
	case w.recs <- ftp:
		return
	default:
	}
	parts := map[string][]int32{ftp.Topic: {ftp.Partition}}
	e.pauser().PauseFetchPartitions(parts)
	go func() {
		select {
		case w.recs <- ftp:
			e.pauser().ResumeFetchPartitions(parts)
		case <-w.quit:
			// Partition revoked while we waited; leave it paused — a resume here
			// would target a partition we no longer own.
		case <-ctx.Done():
		}
	}()
}

func (e *partitionEngine) drainAllWorkers() {
	e.mu.Lock()
	stop := make([]*partitionWorker, 0, len(e.workers))
	for key, w := range e.workers {
		close(w.quit)
		stop = append(stop, w)
		delete(e.workers, key)
	}
	e.mu.Unlock()
	for _, w := range stop {
		<-w.done
	}
}
