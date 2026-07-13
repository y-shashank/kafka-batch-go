package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
)

// RunRetryConsumerMembers starts N supervised retry consumers in the same group.
// Each poll handles at most one record (PollRecords(1)) so a not-yet-due
// message can stay uncommitted without franz-go losing track of it across
// batched PollFetches — matching Ruby RetryConsumer pause/break semantics.
func RunRetryConsumerMembers(ctx context.Context, members int, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, loopHealth *LoopHealth) {
	if members < 1 {
		members = 1
	}
	log.Printf("[kbatch-daemon] starting retry consumers group=%s members=%d topics=%v", group, members, topics)
	spec := consumerSpec{
		brokers:    brokers,
		group:      group,
		topics:     topics,
		fetch:      fetch,
		handle:     handle,
		health:     health,
		pauseCtl:   pauseCtl,
		live:       live,
		loopHealth: loopHealth,
		loopName:   "retry-" + group,
	}
	for range members {
		go runRetryConsumerSupervised(ctx, spec)
	}
}

func runRetryConsumerSupervised(ctx context.Context, spec consumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.group)
	}
	runLoopSupervised(ctx, spec.loopName, spec.loopHealth, func(ctx context.Context) error {
		return runRetryConsumerLoop(ctx, spec)
	})
}

func runRetryConsumerLoop(ctx context.Context, spec consumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer closeGroupConsumer(cl)

	label := "retry consumer group=" + spec.group
	loopCtx, touch, stopGuard := attachConsumerStallGuard(ctx, cl, label)
	defer stopGuard()

	for {
		if err := consumerLoopDoneErr(loopCtx); err != nil {
			if errors.Is(err, errConsumerStalled) {
				return stalledRestartErr(spec.group)
			}
			return err
		}
		if touch != nil {
			touch()
		}
		fetches := cl.PollRecords(loopCtx, 1)
		if err := checkFetchErrs(loopCtx, cl, fetches, spec.group); err != nil {
			return err
		}
		if spec.health != nil {
			spec.health.RecordPoll(spec.group)
		}
		if spec.loopHealth != nil && spec.loopName != "" {
			spec.loopHealth.RecordTick(spec.loopName)
		}
		if touch != nil {
			touch()
		}
		recs := collectPollRecords(loopCtx, spec.group, fetches, spec.pauseCtl, spec.live)
		if len(recs) > 0 {
			processOneRetryRecord(loopCtx, cl, spec.handle, recs[0], spec.group)
		}
		cl.AllowRebalance()
	}
}

func processOneRetryRecord(ctx context.Context, cl retryCommitMarker, handle func(*kgo.Record) error, rec *kgo.Record, group string) {
	if err := safeHandle(handle, rec); err != nil {
		var pe *retryPausedError
		if errors.As(err, &pe) && pe.duration > 0 {
			pauseForRetry(ctx, pe.duration)
			return
		}
		log.Printf("[kbatch-daemon] retry handler error group=%s topic=%s partition=%d offset=%d: %v",
			group, rec.Topic, rec.Partition, rec.Offset, err)
		return
	}
	cl.MarkCommitRecords(rec)
}

type retryCommitMarker interface {
	MarkCommitRecords(...*kgo.Record)
}
