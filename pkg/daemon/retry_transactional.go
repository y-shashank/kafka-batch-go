package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// RunRetryConsumerTransactional runs the retry-topic pipeline as an exactly-once
// consume-transform-produce loop: a retry message's outcome (a retarget produce, a
// dead-letter produce, and/or a completion event produce — retry.Processor.Process can
// ask for any combination) is committed atomically together with that record's own
// consumer offset, via a franz-go GroupTransactSession.
//
// Previously (see applyRetryOutcome), these were independent operations: produce, then
// produce, then a separate offset commit back in the consumer loop. A crash between "the
// retarget message landed" and "the offset committed" meant the same retry record would
// be redelivered on restart and reprocessed from scratch — producing a second retarget
// message for a job that was not otherwise uniqueness-locked. A transaction makes that
// window disappear: either everything this record decided to do lands together with its
// offset, or none of it does and the record is cleanly redelivered.
//
// Records are processed one at a time (poll size 1) rather than batched into a single
// transaction. This is deliberate: retry.Processor's "pause" outcome (a retry_after that
// isn't due yet) must leave its own offset uncommitted without blocking any other
// partition, and Kafka only supports committing a single "next offset" per partition —
// there is no way to atomically commit a later record in a batch while leaving an earlier
// one in the same partition/batch intentionally uncommitted. One-record transactions
// sidestep that: each poll's single record gets committed or not, in isolation. The cost
// is a Begin/End round trip per retry message, which is expected to be negligible since
// retries are the exception path, not the primary job-execution volume.
func RunRetryConsumerTransactional(ctx context.Context, cfg config.Daemon, topics []string, retryProc *retry.Processor, health *ConsumerHealth) {
	if len(topics) == 0 {
		return
	}
	go runRetryTransactionalSupervised(ctx, cfg, topics, retryProc, health)
}

// retryTransactionalID must be stable across restarts of the same logical daemon
// replica (so Kafka's producer-fencing can correctly recognize and evict a genuine
// zombie predecessor) and distinct across concurrently-running replicas (so horizontal
// scaling — this daemon's normal deployment model — doesn't fence healthy siblings off
// of each other). cfg.NodeID defaults to hostname, which is unique-per-replica under
// typical container/pod deployments; operators on deployments where that does not hold
// (e.g. multiple bare processes on one host) should set the node id explicitly.
func retryTransactionalID(cfg config.Daemon) string {
	node := cfg.NodeID
	if node == "" {
		node = "unknown-node"
	}
	return cfg.ConsumerGroup + "-retry-txn-" + node
}

func runRetryTransactionalSupervised(ctx context.Context, cfg config.Daemon, topics []string, retryProc *retry.Processor, health *ConsumerHealth) {
	group := cfg.ConsumerGroup + "-retry"
	if health != nil {
		health.Register(group)
	}
	runLoopSupervised(ctx, "retry-txn-"+group, nil, func(ctx context.Context) error {
		return runRetryTransactionalLoop(ctx, cfg, topics, retryProc, health, group)
	})
}

func runRetryTransactionalLoop(ctx context.Context, cfg config.Daemon, topics []string, retryProc *retry.Processor, health *ConsumerHealth, group string) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.TransactionalID(retryTransactionalID(cfg)),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	opts = append(opts, kafkaclient.FetchOpts(cfg.ConsumerFetchSettings())...)
	sess, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("retry-txn client group=%s: %w", group, err)
	}
	defer sess.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fetches := sess.PollRecords(ctx, 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				if isContextErr(e.Err) {
					return nil
				}
				return fmt.Errorf("poll group=%s topic=%s: %w", group, e.Topic, e.Err)
			}
		}
		if health != nil {
			health.RecordPoll(group)
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		rec := recs[0]

		if err := sess.Begin(); err != nil {
			return fmt.Errorf("retry-txn begin group=%s: %w", group, err)
		}

		src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
		out, procErr := safeRetryProcess(retryProc, ctx, rec.Value, src)

		var produceErr error
		if procErr == nil {
			produceErr = produceRetryOutcomeTxn(ctx, cfg, sess, out, src)
		}

		// Commit iff processing succeeded, producing succeeded, and the outcome
		// isn't an intentional "not due yet" pause. Anything else aborts: nothing
		// produced in this transaction lands, and the offset stays uncommitted so
		// the exact same record is redelivered on the next PollRecords call.
		commit := shouldCommitRetryOutcome(procErr, produceErr, out)
		committed, endErr := sess.End(ctx, kgo.TransactionEndTry(commit))
		if endErr != nil {
			return fmt.Errorf("retry-txn end group=%s: %w", group, endErr)
		}
		if procErr != nil {
			log.Printf("[kbatch-retry-txn] handler error group=%s topic=%s offset=%d: %v",
				group, rec.Topic, rec.Offset, procErr)
		}
		if produceErr != nil {
			log.Printf("[kbatch-retry-txn] produce error group=%s topic=%s offset=%d: %v",
				group, rec.Topic, rec.Offset, produceErr)
		}
		if out.Pause && !committed {
			pauseForRetry(ctx, out.PauseFor)
		}
	}
}

// safeRetryProcess recovers a panic in retry.Processor.Process the same way safeHandle
// does for the plain consumer loop, so a bug in decision logic aborts this one
// transaction (record redelivered, nothing produced) instead of crashing the daemon.
func safeRetryProcess(retryProc *retry.Processor, ctx context.Context, raw []byte, src protocol.SourceCoords) (out retry.Outcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch-retry-txn] handler panic topic=%s partition=%d offset=%d: %v",
				src.Topic, src.Partition, src.Offset, r)
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return retryProc.Process(ctx, raw, src)
}

// produceRetryOutcomeTxn produces every message a retry.Outcome asks for
// (event/retarget/DLT) as part of the currently-open transaction. A single
// synchronous ProduceSync call is used instead of the ad hoc "retry the produce a few
// times with backoff" helper used by the non-transactional path (produceEventWithRetry):
// once a transaction wraps the produce, there is no partial-failure state left to guard
// against with application-level retries — any produce error just aborts the
// transaction and the record is cleanly redelivered, so retrying here would only be
// retrying something the caller's own poll loop already retries for free.
func produceRetryOutcomeTxn(ctx context.Context, cfg config.Daemon, sess *kgo.GroupTransactSession, out retry.Outcome, src protocol.SourceCoords) error {
	recs, err := buildRetryOutcomeRecords(cfg, out)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return nil
	}
	results := sess.ProduceSync(ctx, recs...)
	if err := results.FirstErr(); err != nil {
		return err
	}
	if out.DLTPayload != nil {
		emitRetryDLT(out.DLTPayload, src.Topic)
	}
	return nil
}

// buildRetryOutcomeRecords translates a retry.Outcome into the Kafka records it asks
// to have produced, with no side effects — kept separate from produceRetryOutcomeTxn
// so the translation itself is unit-testable without a live Kafka client.
func buildRetryOutcomeRecords(cfg config.Daemon, out retry.Outcome) ([]*kgo.Record, error) {
	var recs []*kgo.Record
	if out.Event != nil {
		raw, err := json.Marshal(out.Event)
		if err != nil {
			return nil, fmt.Errorf("marshal retry event: %w", err)
		}
		key := fmt.Sprintf("%s/%d", out.Event.SrcTopic, out.Event.SrcPartition)
		recs = append(recs, &kgo.Record{Topic: cfg.EventsTopic, Key: []byte(key), Value: raw})
	}
	if out.ProduceBody != nil {
		recs = append(recs, &kgo.Record{Topic: out.ProduceTopic, Key: []byte(out.ProduceKey), Value: out.ProduceBody})
	}
	if out.DLTPayload != nil {
		recs = append(recs, &kgo.Record{Topic: cfg.DeadLetterTopic, Key: []byte(out.DLTKey), Value: out.DLTPayload})
	}
	return recs, nil
}

// shouldCommitRetryOutcome decides whether the currently-open transaction should be
// committed or aborted. Kept as a pure function (no Kafka/session dependency) so the
// decision table is unit-testable in isolation from the transaction plumbing around it.
func shouldCommitRetryOutcome(procErr, produceErr error, out retry.Outcome) bool {
	return procErr == nil && produceErr == nil && !out.Pause
}
