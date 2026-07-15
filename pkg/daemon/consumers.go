package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
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
		brokers:     brokers,
		group:       group,
		topics:      topics,
		fetch:       fetch,
		handle:      handle,
		health:      health,
		pauseCtl:    pauseCtl,
		live:        live,
		maxRecords:  defaultDispatchPollRecords,
		memberLabel: memberLabel(1, 1),
		healthKey:   healthMemberKey(group, 1, 1),
	})
}

// RunSuperFetchConsumerGroupMembers starts N supervised job consumers. Each
// member claims Redis ownership, Kafka-acks immediately, then runs #perform on
// a bounded pool (SuperFetch — the only supported job execution mode).
func RunSuperFetchConsumerGroupMembers(
	ctx context.Context, members int,
	brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings,
	newSF func(consumerID string) *SuperFetchExecutor,
	health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter,
) {
	if members < 1 {
		members = 1
	}
	if newSF == nil {
		log.Printf("[kbatch-worker] RunSuperFetchConsumerGroupMembers requires SuperFetch factory group=%s", group)
		return
	}
	for member := 1; member <= members; member++ {
		label := memberLabel(member, members)
		hk := healthMemberKey(group, member, members)
		cid := group + ":" + label
		if live != nil && live.ConsumerID != "" {
			cid = live.ConsumerID + ":" + label
		}
		sf := newSF(cid)
		if sf == nil {
			log.Printf("[kbatch-worker] SuperFetch factory returned nil group=%s member=%s", group, label)
			continue
		}
		sf.BindLife(ctx)
		go runConcurrentConsumerSupervised(ctx, concurrentConsumerSpec{
			brokers:     brokers,
			group:       group,
			topics:      topics,
			fetch:       fetch,
			superFetch:  sf,
			health:      health,
			pauseCtl:    pauseCtl,
			live:        live,
			memberLabel: label,
			healthKey:   hk,
		})
	}
}

// BatchHandler processes all records from one PollFetches call together.
type BatchHandler func(ctx context.Context, recs []*kgo.Record) error

type consumerSpec struct {
	brokers     []string
	group       string
	topics      []string
	fetch       config.ConsumerFetchSettings
	handle      func(*kgo.Record) error
	maxRecords  int
	health      *ConsumerHealth
	pauseCtl    pauseChecker
	live        *liveness.Reporter
	loopHealth  *LoopHealth
	loopName    string
	memberLabel string
	healthKey   string
}

func collectPollRecords(ctx context.Context, cl *consumerClient, group string, fetches kgo.Fetches, pauseCtl pauseChecker, live *liveness.Reporter) []*kgo.Record {
	recs := make([]*kgo.Record, 0)
	fetches.EachRecord(func(rec *kgo.Record) {
		if pauseCtl != nil && pauseCtl.Paused(ctx, group, rec.Topic, rec.Partition) {
			if cl != nil && !pauseCtl.Paused(ctx, group, rec.Topic, 0) {
				cl.pauseConsumptionPartition(rec.Topic, rec.Partition)
			}
			return
		}
		if live != nil {
			live.Heartbeat(ctx, rec.Topic)
		}
		recs = append(recs, rec)
	})
	return recs
}

type concurrentConsumerSpec struct {
	brokers     []string
	group       string
	topics      []string
	fetch       config.ConsumerFetchSettings
	superFetch  *SuperFetchExecutor
	health      *ConsumerHealth
	pauseCtl    pauseChecker
	live        *liveness.Reporter
	memberLabel string
	healthKey   string
}

func runConcurrentConsumerSupervised(ctx context.Context, spec concurrentConsumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.healthKey)
	}
	log.Printf("[kbatch-daemon] concurrent consumer member=%s group=%s topics=%v",
		spec.memberLabel, spec.group, spec.topics)
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
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.memberLabel, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "concurrent consumer group=" + spec.group + " member=" + spec.memberLabel,
		group:       spec.group,
		memberLabel: spec.memberLabel,
		healthKey:   spec.healthKey,
		topics:      spec.topics,
		health:      spec.health,
		pauseCtl:    spec.pauseCtl,
		live:        spec.live,
	}, func(ctx context.Context, recs []*kgo.Record) error {
		spec.superFetch.DispatchClaimsAndAcks(ctx, cl.Client, recs, spec.group)
		return nil
	})
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
		spec.health.Register(spec.healthKey)
	}
	log.Printf("[kbatch-daemon] consumer member=%s group=%s topics=%v",
		spec.memberLabel, spec.group, spec.topics)
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
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.memberLabel, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "consumer group=" + spec.group + " member=" + spec.memberLabel,
		group:       spec.group,
		memberLabel: spec.memberLabel,
		healthKey:   spec.healthKey,
		topics:      spec.topics,
		maxRecords:  spec.maxRecords,
		health:      spec.health,
		pauseCtl:    spec.pauseCtl,
		live:        spec.live,
	}, func(ctx context.Context, recs []*kgo.Record) error {
		for _, rec := range recs {
			if err := safeHandle(spec.handle, rec); err != nil {
				var bp *fairBackpressureError
				if errors.As(err, &bp) {
					deferClientPartitionPause(cl, rec, bp.duration)
					continue
				}
				log.Printf("[kbatch-daemon] handler error group=%s topic=%s offset=%d: %v",
					spec.group, rec.Topic, rec.Offset, err)
				continue
			}
			cl.MarkCommitRecords(rec)
		}
		return nil
	})
}

// RunPriorityGroupMembersSuperFetch starts N priority consumers with SuperFetch
// (claim → ack → pool perform) and lag gating.
func RunPriorityGroupMembersSuperFetch(
	ctx context.Context, members int, cfg config.Daemon, pc priority.Config, gate *priority.Gate,
	newSF func(consumerID string) *SuperFetchExecutor,
	health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter,
) {
	if members < 1 {
		members = 1
	}
	if newSF == nil {
		log.Printf("[kbatch-worker] RunPriorityGroupMembersSuperFetch requires SuperFetch factory group=%s", pc.ConsumerGroup)
		return
	}
	group := pc.ConsumerGroup
	log.Printf("[kbatch-daemon] starting priority consumers group=%s members=%d topics=%v", group, members, pc.Topics)
	for member := 1; member <= members; member++ {
		label := memberLabel(member, members)
		hk := healthMemberKey(group, member, members)
		cid := group + ":" + label
		if live != nil && live.ConsumerID != "" {
			cid = live.ConsumerID + ":" + label
		}
		sf := newSF(cid)
		if sf == nil {
			log.Printf("[kbatch-worker] SuperFetch factory returned nil group=%s member=%s", group, label)
			continue
		}
		sf.BindLife(ctx)
		go runPrioritySupervised(ctx, cfg, pc, gate, health, pauseCtl, live, sf, label, hk)
	}
}

func runPrioritySupervised(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, sf *SuperFetchExecutor, memberLabel, healthKey string) {
	group := pc.ConsumerGroup
	if health != nil {
		health.Register(healthKey)
	}
	log.Printf("[kbatch-daemon] priority consumer member=%s group=%s topics=%v", memberLabel, group, pc.Topics)
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
		err := runPriorityOnce(ctx, cfg, pc, gate, health, pauseCtl, live, specByTopic, yieldSleep, weightedTicks, sf, memberLabel, healthKey)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] priority consumer group=%s member=%s error=%v — restarting in %s", group, memberLabel, err, backoff)
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
	health *ConsumerHealth,
	pauseCtl pauseChecker,
	live *liveness.Reporter,
	specByTopic map[string]priority.TopicSpec,
	yieldSleep time.Duration,
	weightedTicks map[string]int,
	sf *SuperFetchExecutor,
	memberLabel string,
	healthKey string,
) error {
	cl, err := newGroupConsumerClient(cfg.Brokers, cfg.ConsumerFetchSettings(), pc.ConsumerGroup, memberLabel, pc.Topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", pc.ConsumerGroup, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "priority consumer group=" + pc.ConsumerGroup + " member=" + memberLabel,
		group:       pc.ConsumerGroup,
		memberLabel: memberLabel,
		healthKey:   healthKey,
		topics:      pc.Topics,
		maxRecords:  defaultPriorityPollRecords,
		health:      health,
		pauseCtl:    pauseCtl,
		live:        live,
	}, func(ctx context.Context, recs []*kgo.Record) error {
		ready := filterPriorityRecords(ctx, cl, pc, gate, pauseCtl, live, specByTopic, yieldSleep, weightedTicks, recs)
		if len(ready) == 0 {
			return nil
		}
		sf.DispatchClaimsAndAcks(ctx, cl.Client, ready, pc.ConsumerGroup)
		return nil
	})
}

func filterPriorityRecords(
	ctx context.Context,
	cl *consumerClient,
	pc priority.Config,
	gate *priority.Gate,
	pauseCtl pauseChecker,
	live *liveness.Reporter,
	specByTopic map[string]priority.TopicSpec,
	yieldSleep time.Duration,
	weightedTicks map[string]int,
	recs []*kgo.Record,
) []*kgo.Record {
	ready := make([]*kgo.Record, 0, len(recs))
	for _, rec := range recs {
		spec, ok := specByTopic[rec.Topic]
		if !ok {
			continue
		}
		if pauseCtl != nil && pauseCtl.Paused(ctx, pc.ConsumerGroup, rec.Topic, rec.Partition) {
			if !pauseCtl.Paused(ctx, pc.ConsumerGroup, rec.Topic, 0) {
				cl.pauseConsumptionPartition(rec.Topic, rec.Partition)
			}
			continue
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
			deferClientPartitionPause(cl, rec, yieldSleep)
			continue
		}
		weightedTicks[rec.Topic] = tick
		ready = append(ready, rec)
	}
	return ready
}
