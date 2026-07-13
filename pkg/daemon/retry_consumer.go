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
	for member := 1; member <= members; member++ {
		spec := consumerSpec{
			brokers:     brokers,
			group:       group,
			topics:      topics,
			fetch:       fetch,
			handle:      handle,
			health:      health,
			pauseCtl:    pauseCtl,
			live:        live,
			loopHealth:  loopHealth,
			loopName:    "retry-" + group,
			memberLabel: memberLabel(member, members),
			healthKey:   healthMemberKey(group, member, members),
		}
		go runRetryConsumerSupervised(ctx, spec)
	}
}

func runRetryConsumerSupervised(ctx context.Context, spec consumerSpec) {
	if spec.health != nil {
		spec.health.Register(spec.healthKey)
	}
	log.Printf("[kbatch-daemon] retry consumer member=%s group=%s topics=%v",
		spec.memberLabel, spec.group, spec.topics)
	runLoopSupervised(ctx, spec.loopName, spec.loopHealth, func(ctx context.Context) error {
		return runRetryConsumerLoop(ctx, spec)
	})
}

func runRetryConsumerLoop(ctx context.Context, spec consumerSpec) error {
	cl, err := newGroupConsumerClient(spec.brokers, spec.fetch, spec.group, spec.memberLabel, spec.topics)
	if err != nil {
		return fmt.Errorf("kafka client group=%s: %w", spec.group, err)
	}
	defer closeGroupConsumer(cl)

	return runGroupPollLoop(ctx, cl, pollLoopConfig{
		label:       "retry consumer group=" + spec.group + " member=" + spec.memberLabel,
		group:       spec.group,
		memberLabel: spec.memberLabel,
		healthKey:   spec.healthKey,
		topics:      spec.topics,
		maxRecords:  1,
		health:      spec.health,
		loopHealth:  spec.loopHealth,
		loopName:    spec.loopName,
		pauseCtl:    spec.pauseCtl,
		live:        spec.live,
	}, func(ctx context.Context, recs []*kgo.Record) error {
		processOneRetryRecord(ctx, cl, cl, spec.handle, recs[0], spec.group)
		return nil
	})
}

func processOneRetryRecord(ctx context.Context, commit recordCommitter, pause fetchPauser, handle func(*kgo.Record) error, rec *kgo.Record, group string) {
	if err := safeHandle(handle, rec); err != nil {
		var pe *retryPausedError
		if errors.As(err, &pe) && pe.duration > 0 {
			deferPartitionPause(pause, rec, pe.duration)
			return
		}
		log.Printf("[kbatch-daemon] retry handler error group=%s topic=%s partition=%d offset=%d: %v",
			group, rec.Topic, rec.Partition, rec.Offset, err)
		return
	}
	commit.MarkCommitRecords(rec)
}

type recordCommitter interface {
	MarkCommitRecords(...*kgo.Record)
}

type fetchPauser interface {
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	ResumeFetchPartitions(map[string][]int32)
	PauseFetchTopics(...string) []string
	ResumeFetchTopics(...string)
}
