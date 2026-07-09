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
func RunConsumer(ctx context.Context, brokers []string, group string, topics []string, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
	go runConsumerSupervised(ctx, consumerSpec{
		brokers:  brokers,
		group:    group,
		topics:   topics,
		handle:   handle,
		health:   health,
		pauseCtl: pauseCtl,
		live:     live,
	})
}

type consumerSpec struct {
	brokers  []string
	group    string
	topics   []string
	handle   func(*kgo.Record) error
	health   *ConsumerHealth
	pauseCtl pauseChecker
	live     *liveness.Reporter
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
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(spec.brokers...),
		kgo.ConsumerGroup(spec.group),
		kgo.ConsumeTopics(spec.topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
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
	go runPrioritySupervised(ctx, cfg, pc, gate, handle, health, pauseCtl, live)
}

func runPrioritySupervised(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter) {
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
		err := runPriorityOnce(ctx, cfg, pc, gate, handle, health, pauseCtl, live, specByTopic, yieldSleep, weightedTicks)
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
) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(pc.ConsumerGroup),
		kgo.ConsumeTopics(pc.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
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
			if err := safeHandle(handle, rec); err != nil {
				log.Printf("[kbatch-priority] handler error group=%s topic=%s offset=%d: %v",
					pc.ConsumerGroup, rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
	}
}
