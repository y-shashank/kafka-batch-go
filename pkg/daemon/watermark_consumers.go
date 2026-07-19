package daemon

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
)

// RunWatermarkConsumerGroupMembers starts N supervised job consumers running the
// Redis-free watermark executor (config.ExecutionModeWatermark) — the sibling of
// RunSuperFetchConsumerGroupMembers. runCtx cancels the poll loop; lifeCtx should
// outlive it during drain so in-flight #perform can finish.
func RunWatermarkConsumerGroupMembers(
	runCtx, lifeCtx context.Context, members int,
	brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings,
	newWM func(consumerID string) *WatermarkExecutor,
	health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter,
) {
	if members < 1 {
		members = 1
	}
	if newWM == nil {
		log.Printf("[kbatch-worker] RunWatermarkConsumerGroupMembers requires a watermark factory group=%s", group)
		return
	}
	if lifeCtx == nil {
		lifeCtx = runCtx
	}
	for member := 1; member <= members; member++ {
		label := memberLabel(member, members)
		hk := healthMemberKey(group, member, members)
		cid := group + ":" + label
		if live != nil && live.ConsumerID != "" {
			cid = live.ConsumerID + ":" + label
		}
		wm := newWM(cid)
		if wm == nil {
			log.Printf("[kbatch-worker] watermark factory returned nil group=%s member=%s", group, label)
			continue
		}
		wm.BindLife(lifeCtx)
		go runWatermarkConsumerSupervised(runCtx, watermarkConsumerSpec{
			brokers:     brokers,
			group:       group,
			topics:      topics,
			fetch:       fetch,
			watermark:   wm,
			health:      health,
			pauseCtl:    pauseCtl,
			live:        live,
			memberLabel: label,
			healthKey:   hk,
		})
	}
}

type watermarkConsumerSpec struct {
	brokers     []string
	group       string
	topics      []string
	fetch       config.ConsumerFetchSettings
	watermark   *WatermarkExecutor
	health      *ConsumerHealth
	pauseCtl    pauseChecker
	live        *liveness.Reporter
	memberLabel string
	healthKey   string
}

func runWatermarkConsumerSupervised(ctx context.Context, spec watermarkConsumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.healthKey)
	}
	log.Printf("[kbatch-worker] watermark consumer member=%s group=%s topics=%v",
		spec.memberLabel, spec.group, spec.topics)
	backoff := consumerRestartInitial
	for {
		if ctx.Err() != nil {
			return
		}
		err := runWatermarkConsumerLoop(ctx, spec)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] watermark consumer group=%s error=%v — restarting in %s", spec.group, err, backoff)
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

func runWatermarkConsumerLoop(ctx context.Context, spec watermarkConsumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.memberLabel, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "watermark consumer group=" + spec.group + " member=" + spec.memberLabel,
		group:       spec.group,
		memberLabel: spec.memberLabel,
		healthKey:   spec.healthKey,
		topics:      spec.topics,
		health:      spec.health,
		pauseCtl:    spec.pauseCtl,
		live:        spec.live,
		// onPoll flushes any completions that became committable since the last
		// poll — including on idle polls that return no new records.
		onPoll: func(context.Context) { spec.watermark.FlushMarks(cl.Client) },
	}, func(ctx context.Context, recs []*kgo.Record) error {
		spec.watermark.DispatchAndCommit(ctx, cl.Client, recs, spec.group)
		return nil
	})
}

// RunPriorityGroupMembersWatermark starts N priority consumers with the watermark
// executor and lag gating — the sibling of RunPriorityGroupMembersSuperFetch.
func RunPriorityGroupMembersWatermark(
	runCtx, lifeCtx context.Context, members int, cfg config.Daemon, pc priority.Config, gate *priority.Gate,
	newWM func(consumerID string) *WatermarkExecutor,
	health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter,
) {
	if members < 1 {
		members = 1
	}
	if newWM == nil {
		log.Printf("[kbatch-worker] RunPriorityGroupMembersWatermark requires a watermark factory group=%s", pc.ConsumerGroup)
		return
	}
	if lifeCtx == nil {
		lifeCtx = runCtx
	}
	group := pc.ConsumerGroup
	log.Printf("[kbatch-daemon] starting watermark priority consumers group=%s members=%d topics=%v", group, members, pc.Topics)
	for member := 1; member <= members; member++ {
		label := memberLabel(member, members)
		hk := healthMemberKey(group, member, members)
		cid := group + ":" + label
		if live != nil && live.ConsumerID != "" {
			cid = live.ConsumerID + ":" + label
		}
		wm := newWM(cid)
		if wm == nil {
			log.Printf("[kbatch-worker] watermark factory returned nil group=%s member=%s", group, label)
			continue
		}
		wm.BindLife(lifeCtx)
		go runWatermarkPrioritySupervised(runCtx, cfg, pc, gate, health, pauseCtl, live, wm, label, hk)
	}
}

func runWatermarkPrioritySupervised(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, wm *WatermarkExecutor, memberLabel, healthKey string) {
	group := pc.ConsumerGroup
	if health != nil {
		health.Register(healthKey)
	}
	log.Printf("[kbatch-daemon] watermark priority consumer member=%s group=%s topics=%v", memberLabel, group, pc.Topics)
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
		err := runWatermarkPriorityOnce(ctx, cfg, pc, gate, health, pauseCtl, live, specByTopic, yieldSleep, weightedTicks, wm, memberLabel, healthKey)
		if ctx.Err() != nil || err == nil {
			return
		}
		log.Printf("[kbatch] watermark priority consumer group=%s member=%s error=%v — restarting in %s", group, memberLabel, err, backoff)
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

func runWatermarkPriorityOnce(
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
	wm *WatermarkExecutor,
	memberLabel string,
	healthKey string,
) error {
	cl, err := newGroupConsumerClient(cfg.Brokers, cfg.ConsumerFetchSettings(), pc.ConsumerGroup, memberLabel, pc.Topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", pc.ConsumerGroup, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "watermark priority consumer group=" + pc.ConsumerGroup + " member=" + memberLabel,
		group:       pc.ConsumerGroup,
		memberLabel: memberLabel,
		healthKey:   healthKey,
		topics:      pc.Topics,
		maxRecords:  defaultPriorityPollRecords,
		health:      health,
		pauseCtl:    pauseCtl,
		live:        live,
		onPoll:      func(context.Context) { wm.FlushMarks(cl.Client) },
		onPrePoll:   priorityPrePollHook(cl, gate, specByTopic, weightedTicks, yieldSleep),
	}, func(ctx context.Context, recs []*kgo.Record) error {
		ready := filterPriorityRecords(ctx, cl, pc, pauseCtl, live, specByTopic, recs)
		if len(ready) == 0 {
			return nil
		}
		wm.DispatchAndCommit(ctx, cl.Client, ready, pc.ConsumerGroup)
		return nil
	})
}
