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

// RunRetryConsumer starts a single supervised retry consumer (one kgo.Client, one group
// member) for this process. Each poll handles at most one record (PollRecords(1)) so a
// not-yet-due message stays uncommitted without franz-go advancing the committed offset
// past it — matching Ruby RetryConsumer pause/break semantics. Retry topics are low
// volume; scale horizontally by adding pods, which Kafka assigns partitions across.
func RunRetryConsumer(ctx context.Context, brokers []string, group string, topics []string, fetch config.ConsumerFetchSettings, handle func(*kgo.Record) error, health *ConsumerHealth, pauseCtl pauseChecker, live *liveness.Reporter, loopHealth *LoopHealth) {
	log.Printf("[kbatch-daemon] starting retry consumer group=%s topics=%v", group, topics)
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
		memberLabel: memberLabel(1, 1),
		healthKey:   healthMemberKey(group, 1, 1),
	}
	go runRetryConsumerSupervised(ctx, spec)
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
