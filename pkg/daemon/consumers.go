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
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
)

const (
	consumerRestartInitial = time.Second
	consumerRestartMax     = 30 * time.Second
)

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func safeHandle(handle func(*kgo.Record) error, rec *kgo.Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch] handler panic topic=%s partition=%d offset=%d: %v",
				rec.Topic, rec.Partition, rec.Offset, r)
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return handle(rec)
}

// RunConsumer starts a supervised Kafka consumer that restarts on broker blips.
func RunConsumer(ctx context.Context, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	go runConsumerSupervised(ctx, consumerSpec{
		brokers:  brokers,
		group:    group,
		topics:   topics,
		fetch:    fetch,
		handle:   handle,
		health:   health,
		pauseCtl: pauseCtl,
		live:     live,
	})
}

// RunConsumerGroupMembers starts N supervised consumers in the same process that
// join the same consumer group. Kafka assigns partitions across them as if they
// were separate pods.
func RunConsumerGroupMembers(ctx context.Context, members int, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	RunConcurrentConsumerGroupMembers(ctx, members, 1, brokers, group, topics, fetch, handle, health, pauseCtl, live)
}

// RunConcurrentConsumerGroupMembers starts N group members and runs up to
// processWorkers job handlers in parallel per poll (Karafka concurrency).
func RunConcurrentConsumerGroupMembers(ctx context.Context, members, processWorkers int, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	if members < 1 {
		members = 1
	}
	if processWorkers < 1 {
		processWorkers = 1
	}
	for range members {
		if processWorkers == 1 {
			RunConsumer(ctx, brokers, group, topics, fetch, handle, health, pauseCtl, live)
			continue
		}
		go runConcurrentConsumerSupervised(ctx, concurrentConsumerSpec{
			brokers:        brokers,
			group:          group,
			topics:         topics,
			fetch:          fetch,
			handle:         handle,
			processWorkers: processWorkers,
			health:         health,
			pauseCtl:       pauseCtl,
			live:           live,
		})
	}
}

// BatchHandler processes all records from one PollFetches call together.
type BatchHandler func(ctx context.Context, recs []*kgo.Record) error

// RunBatchedConsumerGroupMembers starts N supervised consumers that batch each
// poll into a single handler call and commit all records together on success.
// onPoll, when set, runs after every successful poll (even when the batch is empty).
func RunBatchedConsumerGroupMembers(ctx context.Context, members int, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle BatchHandler, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, onPoll func(context.Context)) {
	if members < 1 {
		members = 1
	}
	for range members {
		go runBatchedConsumerSupervised(ctx, batchedConsumerSpec{
			brokers:  brokers,
			group:    group,
			topics:   topics,
			fetch:    fetch,
			handle:   handle,
			health:   health,
			pauseCtl: pauseCtl,
			live:     live,
			onPoll:   onPoll,
		})
	}
}

func newGroupConsumerClient(brokers []string, fetch config.ConsumerFetchSettings, group string, topics []string) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	}
	opts = append(opts, kafkaclient.FetchOpts(fetch)...)
	return kgo.NewClient(opts...)
}

type consumerSpec struct {
	brokers  []string
	group    string
	topics   []string
	fetch    config.ConsumerFetchSettings
	handle   func(*kgo.Record) error
	health   *ConsumerHealth
	pauseCtl pauseChecker
	live     *liveness.Reporter
	loopHealth *LoopHealth
	loopName   string
}

type batchedConsumerSpec struct {
	brokers  []string
	group    string
	topics   []string
	fetch    config.ConsumerFetchSettings
	handle   BatchHandler
	health   *ConsumerHealth
	pauseCtl pauseChecker
	live     *liveness.Reporter
	onPoll   func(context.Context)
}

type concurrentConsumerSpec struct {
	brokers        []string
	group          string
	topics         []string
	fetch          config.ConsumerFetchSettings
	handle         func(*kgo.Record) error
	processWorkers int
	health         *ConsumerHealth
	pauseCtl       pauseChecker
	live           *liveness.Reporter
}

func runConcurrentConsumerSupervised(ctx context.Context, spec concurrentConsumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.group)
	}
	backoff := consumerRestartInitial
	for {
		if ctx.Err() != nil {
			return
		}
		err := runConcurrentConsumerLoop(ctx, spec)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] concurrent consumer group=%s error=%v — restarting in %s", spec.group, err, backoff)
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
}

func runConcurrentConsumerLoop(ctx context.Context, spec concurrentConsumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				if isContextErr(e.Err) {
					return nil
				}
				return fmt.Errorf("poll group=%s topic=%s: %w", spec.group, e.Topic, e.Err)
			}
		}
		if spec.health != nil {
			spec.health.RecordPoll(spec.group)
		}
		recs := collectPollRecords(ctx, spec.group, fetches, spec.pauseCtl, spec.live)
		if len(recs) > 0 {
			processRecordsConcurrent(ctx, cl, spec.handle, recs, spec.processWorkers, spec.group)
		}
		cl.AllowRebalance()
	}
}

func processRecordsConcurrent(ctx context.Context, cl *kgo.Client, handle func(*kgo.Record) error, recs []*kgo.Record, workers int, group string) {
	if workers < 1 {
		workers = 1
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, rec := range recs {
		wg.Add(1)
		go func(rec *kgo.Record) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := safeHandle(handle, rec); err != nil {
				log.Printf("[kbatch-worker] handler error group=%s topic=%s offset=%d: %v",
					group, rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		}(rec)
	}
	wg.Wait()
}

func runBatchedConsumerSupervised(ctx context.Context, spec batchedConsumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.group)
	}
	backoff := consumerRestartInitial
	for {
		if ctx.Err() != nil {
			return
		}
		err := runBatchedConsumerLoop(ctx, spec)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] batched consumer group=%s error=%v — restarting in %s", spec.group, err, backoff)
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
}

func runBatchedConsumerLoop(ctx context.Context, spec batchedConsumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				if isContextErr(e.Err) {
					return nil
				}
				return fmt.Errorf("poll group=%s topic=%s: %w", spec.group, e.Topic, e.Err)
			}
		}
		if spec.health != nil {
			spec.health.RecordPoll(spec.group)
		}
		if spec.onPoll != nil {
			spec.onPoll(ctx)
		}
		recs := collectPollRecords(ctx, spec.group, fetches, spec.pauseCtl, spec.live)
		if len(recs) > 0 {
			if err := safeBatchHandle(ctx, spec.handle, recs); err != nil {
				log.Printf("[kbatch-daemon] batched handler error group=%s records=%d: %v",
					spec.group, len(recs), err)
			} else {
				cl.MarkCommitRecords(recs...)
			}
		}
		cl.AllowRebalance()
	}
}

func collectPollRecords(ctx context.Context, group string, fetches kgo.Fetches, pauseCtl pauseChecker, live *liveness.Reporter) []*kgo.Record {
	recs := make([]*kgo.Record, 0)
	fetches.EachRecord(func(rec *kgo.Record) {
		if pauseCtl != nil && pauseCtl.Paused(ctx, group, rec.Topic, rec.Partition) {
			return
		}
		if live != nil {
			live.Heartbeat(ctx, rec.Topic)
		}
		recs = append(recs, rec)
	})
	return recs
}

func safeBatchHandle(ctx context.Context, handle BatchHandler, recs []*kgo.Record) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if len(recs) > 0 {
				first := recs[0]
				log.Printf("[kbatch] batch handler panic topic=%s partition=%d offset=%d records=%d: %v",
					first.Topic, first.Partition, first.Offset, len(recs), r)
			} else {
				log.Printf("[kbatch] batch handler panic records=0: %v", r)
			}
			err = fmt.Errorf("batch handler panic: %v", r)
		}
	}()
	return handle(ctx, recs)
}

func runConsumerSupervised(ctx context.Context, spec consumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.group)
	}
	backoff := consumerRestartInitial
	for {
		if ctx.Err() != nil {
			return
		}
		err := runConsumerLoop(ctx, spec)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] consumer group=%s error=%v — restarting in %s", spec.group, err, backoff)
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
}

func runConsumerLoop(ctx context.Context, spec consumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				if isContextErr(e.Err) {
					return nil
				}
				return fmt.Errorf("poll group=%s topic=%s: %w", spec.group, e.Topic, e.Err)
			}
		}
		if spec.health != nil {
			spec.health.RecordPoll(spec.group)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if pauseCtl := spec.pauseCtl; pauseCtl != nil && pauseCtl.Paused(ctx, spec.group, rec.Topic, rec.Partition) {
				time.Sleep(time.Second)
				return
			}
			if spec.live != nil {
				spec.live.Heartbeat(ctx, rec.Topic)
			}
			if err := safeHandle(spec.handle, rec); err != nil {
				log.Printf("[kbatch-daemon] handler error group=%s topic=%s offset=%d: %v",
					spec.group, rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
	}
}

// RunPriorityGroup starts a supervised priority consumer with lag gating.
func RunPriorityGroup(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	RunPriorityGroupMembers(ctx, 1, 1, cfg, pc, gate, handle, health, pauseCtl, live)
}

// RunPriorityGroupMembers starts N in-process priority consumers for the same group.
func RunPriorityGroupMembers(ctx context.Context, members, processWorkers int, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	if members < 1 {
		members = 1
	}
	if processWorkers < 1 {
		processWorkers = 1
	}
	for range members {
		go runPrioritySupervised(ctx, cfg, pc, gate, handle, health, pauseCtl, live, processWorkers)
	}
}

func runPrioritySupervised(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, processWorkers int) {
	group := pc.ConsumerGroup
	if health != nil {
		health.Register(group)
	}
	specByTopic := map[string]priority.TopicSpec{}
	for _, s := range pc.TopicSpecs() {
		specByTopic[s.Topic] = s
	}
	yieldSleep := cfg.PriorityLagCheckInterval
	if yieldSleep <= 0 {
		yieldSleep = 2 * time.Second
	}
	weightedTicks := map[string]int{}
	backoff := consumerRestartInitial

	for {
		if ctx.Err() != nil {
			return
		}
		err := runPriorityOnce(ctx, cfg, pc, gate, handle, health, pauseCtl, live, specByTopic, yieldSleep, weightedTicks, processWorkers)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] priority consumer group=%s error=%v — restarting in %s", group, err, backoff)
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
}

func runPriorityOnce(
	ctx context.Context,
	cfg config.Daemon,
	pc priority.Config,
	gate *priority.Gate,
	handle func(*kgo.Record) error,
	health *ConsumerHealth,
	pauseCtl pauseChecker,
	live *liveness.Reporter,
	specByTopic map[string]priority.TopicSpec,
	yieldSleep time.Duration,
	weightedTicks map[string]int,
	processWorkers int,
) error {
	cl, err := newGroupConsumerClient(cfg.Brokers, cfg.ConsumerFetchSettings(), pc.ConsumerGroup, pc.Topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", pc.ConsumerGroup, err)
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				if isContextErr(e.Err) {
					return nil
				}
				return fmt.Errorf("poll group=%s topic=%s: %w", pc.ConsumerGroup, e.Topic, e.Err)
			}
		}
		if health != nil {
			health.RecordPoll(pc.ConsumerGroup)
		}
		ready := make([]*kgo.Record, 0)
		fetches.EachRecord(func(rec *kgo.Record) {
			spec, ok := specByTopic[rec.Topic]
			if !ok {
				return
			}
			if pauseCtl != nil && pauseCtl.Paused(ctx, pc.ConsumerGroup, rec.Topic, rec.Partition) {
				time.Sleep(yieldSleep)
				return
			}
			if live != nil {
				live.Heartbeat(ctx, rec.Topic)
			}
			tick := weightedTicks[rec.Topic]
			if yield, _ := priority.ShouldYield(spec, gate, &tick, ctx); yield {
				weightedTicks[rec.Topic] = tick
				p0 := ""
				if len(spec.HigherTopics) > 0 {
					p0 = spec.HigherTopics[0]
				}
				instrument.ConsumerPriorityYielded(
					"kbatch.priority", p0, spec.ConsumerGroup,
					yieldSleep.Milliseconds(), string(spec.Mode), spec.Rank, spec.HigherTopics,
				)
				time.Sleep(yieldSleep)
				return
			}
			weightedTicks[rec.Topic] = tick
			ready = append(ready, rec)
		})
		if len(ready) > 0 {
			if processWorkers <= 1 {
				for _, rec := range ready {
					if err := safeHandle(handle, rec); err != nil {
						log.Printf("[kbatch-priority] handler error group=%s topic=%s offset=%d: %v",
							pc.ConsumerGroup, rec.Topic, rec.Offset, err)
						continue
					}
					cl.MarkCommitRecords(rec)
				}
			} else {
				processRecordsConcurrent(ctx, cl, handle, ready, processWorkers, pc.ConsumerGroup)
			}
		}
		cl.AllowRebalance()
	}
}
