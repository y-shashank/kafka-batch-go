package daemon

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
)

// defaultDispatchPollRecords caps fair-dispatch polls at one record so backpressure
// and AllowRebalance run per message (franz-go BlockRebalanceOnPoll guidance).
const defaultDispatchPollRecords = 1

// defaultPriorityPollRecords bounds priority worker polls per franz-go guidance.
const defaultPriorityPollRecords = 100

type pollLoopConfig struct {
	label        string
	group        string
	memberLabel  string
	healthKey    string
	topics       []string
	maxRecords   int // 0 = PollFetches (all buffered), >0 = PollRecords(n)
	stallTimeout time.Duration
	health       *ConsumerHealth
	loopHealth   *LoopHealth
	loopName     string
	pauseCtl     pauseChecker
	live         *liveness.Reporter
	onPoll       func(context.Context)
}

type consumerClient struct {
	*kgo.Client
	abort       pollAbortController
	topics      []string
	pauseMu     sync.Mutex
	topicPaused map[string]bool
	partPaused  map[string][]int32 // consumption killswitch partition pauses (synced via pauseCtl)
	// deferredPaused tracks retry/yield/backpressure pauses that must not be cleared by
	// syncConsumptionFetchPause. Value is the offset to SetOffsets on resume so the
	// not-yet-due (or deferred) record is redelivered — franz-go does not re-emit a
	// Polled-but-unmarked record after ResumeFetchPartitions alone.
	deferredPaused map[string]map[int32]int64
	// enginePaused tracks partition_engine deliver backpressure pauses so pollWaitCtx
	// bounds PollRecords while a single-partition assignment is fetch-paused.
	enginePaused map[string]map[int32]struct{}
	pauseOps     fetchPauser // tests only; nil uses embedded Client

	// deferGen / deferLife cancel outstanding deferred-pause timers on client close
	// so SetOffsets/Resume cannot run against a restarted or closed client.
	deferMu   sync.Mutex
	deferGen  uint64
	deferLife context.Context
	deferStop context.CancelFunc
}

func (c *consumerClient) pauser() fetchPauser {
	if c == nil {
		return nil
	}
	if c.pauseOps != nil {
		return c.pauseOps
	}
	return c.Client
}

func newGroupConsumerClient(brokers []string, fetch config.ConsumerFetchSettings, group, memberLabel string, topics []string) (*consumerClient, error) {
	cc := &consumerClient{
		topics:         append([]string(nil), topics...),
		topicPaused:    map[string]bool{},
		partPaused:     map[string][]int32{},
		deferredPaused: map[string]map[int32]int64{},
		enginePaused:   map[string]map[int32]struct{}{},
	}
	cc.initDeferLifecycle()
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsCallbackBlocked(func(context.Context, *kgo.Client) {
			log.Printf("[kbatch-daemon] group=%s member=%s rebalance waiting — aborting in-flight processing",
				group, memberLabel)
			cc.abort.trigger()
		}),
	}
	opts = append(opts, kafkaclient.FetchOpts(fetch)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	cc.Client = cl
	return cc, nil
}

// runGroupPollLoop implements the franz-go BlockRebalanceOnPoll contract:
// AllowRebalance → poll → process → AllowRebalance. See franz-go group_test.go
// and BlockRebalanceOnPoll docs — holding the poll gate across slow work or
// sleeping blocks group rebalances and can wedge all members.
func runGroupPollLoop(
	ctx context.Context,
	cl *consumerClient,
	cfg pollLoopConfig,
	process func(ctx context.Context, recs []*kgo.Record) error,
) error {
	loopCtx, touch, stopGuard := attachConsumerStallGuardFor(ctx, cl, cfg.label, effectiveStallTimeout(cfg.stallTimeout))
	defer stopGuard()

	healthKey := cfg.healthKey
	if healthKey == "" {
		healthKey = cfg.group
	}

	for {
		if err := consumerLoopDoneErr(loopCtx); err != nil {
			if errors.Is(err, errConsumerStalled) {
				return stalledRestartErr(cfg.group)
			}
			return err
		}

		cl.AllowRebalance()
		cl.syncConsumptionFetchPause(loopCtx, cfg.pauseCtl, cfg.group)

		touch()
		pollCtx, endPollWait := cl.pollWaitCtx(loopCtx)
		var fetches kgo.Fetches
		if cfg.maxRecords > 0 {
			fetches = cl.PollRecords(pollCtx, cfg.maxRecords)
		} else {
			fetches = cl.PollFetches(pollCtx)
		}
		endPollWait()
		if err := checkFetchErrs(loopCtx, cl, fetches, cfg.group); err != nil {
			return err
		}

		if cfg.health != nil {
			cfg.health.RecordPoll(healthKey)
		}
		if cfg.loopHealth != nil && cfg.loopName != "" {
			cfg.loopHealth.RecordTick(cfg.loopName)
		}
		if cfg.onPoll != nil {
			cfg.onPoll(loopCtx)
		}
		touch()

		procCtx, endProc := cl.abort.begin(loopCtx)
		recs := collectPollRecords(procCtx, cl, cfg.group, fetches, cfg.pauseCtl, cfg.live)
		var procErr error
		if len(recs) > 0 {
			procErr = runWithStallHeartbeat(touch, cfg.stallTimeout, func() error {
				return process(procCtx, recs)
			})
		}
		endProc()

		if procErr != nil && !isContextErr(procErr) {
			return procErr
		}

		cl.AllowRebalance()
	}
}
