package daemon

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
)

func RunPriorityGroup(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, errCh chan<- error, pauseCtl pauseChecker, live *liveness.Reporter) {
	specByTopic := map[string]priority.TopicSpec{}
	for _, s := range pc.TopicSpecs() {
		specByTopic[s.Topic] = s
	}
	weightedTicks := map[string]int{}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(pc.ConsumerGroup),
		kgo.ConsumeTopics(pc.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		errCh <- err
		return
	}
	defer cl.Close()

	yieldSleep := cfg.PriorityLagCheckInterval
	if yieldSleep <= 0 {
		yieldSleep = 2 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != nil {
					errCh <- e.Err
					return
				}
			}
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
			if err := handle(rec); err != nil {
				log.Printf("[kbatch-priority] handler error topic=%s offset=%d: %v", rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
	}
}

func RunConsumer(ctx context.Context, brokers []string, group string, topics []string, handle func(*kgo.Record) error, errCh chan<- error, pauseCtl pauseChecker, live *liveness.Reporter) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		errCh <- err
		return
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != nil {
					errCh <- e.Err
					return
				}
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if pauseCtl != nil && pauseCtl.Paused(ctx, group, rec.Topic, rec.Partition) {
				time.Sleep(time.Second)
				return
			}
			if live != nil {
				live.Heartbeat(ctx, rec.Topic)
			}
			if err := handle(rec); err != nil {
				log.Printf("[kbatch-daemon] handler error topic=%s offset=%d: %v", rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
	}
}
