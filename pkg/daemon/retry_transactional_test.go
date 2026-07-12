package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// Note: the broker-level transaction semantics (atomic produce+commit, abort-on-crash,
// producer fencing) are provided entirely by franz-go's GroupTransactSession and require
// a real Kafka cluster (or a compatible fake broker) to exercise end to end — that isn't
// available in this environment. What's covered here is everything on this side of that
// boundary: the pure decision of what to produce and whether to commit, which is exactly
// the logic that determines correctness given whatever the broker does.

func TestRetryTransactionalIDStableAndDistinct(t *testing.T) {
	cfgA := config.Daemon{ConsumerGroup: "kbatch", NodeID: "node-a"}
	cfgB := config.Daemon{ConsumerGroup: "kbatch", NodeID: "node-b"}

	idA1 := retryTransactionalID(cfgA)
	idA2 := retryTransactionalID(cfgA)
	idB := retryTransactionalID(cfgB)

	if idA1 != idA2 {
		t.Fatalf("expected stable ID across calls for the same node, got %q vs %q", idA1, idA2)
	}
	if idA1 == idB {
		t.Fatalf("expected distinct IDs for distinct nodes, both got %q", idA1)
	}
	if idA1 == "" {
		t.Fatal("expected non-empty transactional ID")
	}
}

func TestRetryTransactionalIDFallsBackWhenNodeIDEmpty(t *testing.T) {
	cfg := config.Daemon{ConsumerGroup: "kbatch"}
	id := retryTransactionalID(cfg)
	if id == "" {
		t.Fatal("expected a non-empty fallback transactional ID even with no NodeID configured")
	}
}

func TestBuildRetryOutcomeRecordsIncludesEverythingRequested(t *testing.T) {
	cfg := config.Daemon{EventsTopic: "events.topic", DeadLetterTopic: "dlt.topic"}
	out := retry.Outcome{
		Event:        &protocol.EventMessage{SrcTopic: "jobs", SrcPartition: 3, JobID: "j1", Status: "failed"},
		ProduceBody:  []byte(`{"job_id":"j1"}`),
		ProduceKey:   "j1",
		ProduceTopic: "jobs.retry.target",
		DLTPayload:   []byte(`{"dlt_type":"retry_routing"}`),
		DLTKey:       "j1",
	}

	recs, err := buildRetryOutcomeRecords(cfg, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records (event + retarget + dlt), got %d: %+v", len(recs), recs)
	}

	topics := map[string]bool{}
	for _, r := range recs {
		topics[r.Topic] = true
	}
	for _, want := range []string{"events.topic", "jobs.retry.target", "dlt.topic"} {
		if !topics[want] {
			t.Fatalf("expected a record produced to %q, got topics %v", want, topics)
		}
	}
}

func TestBuildRetryOutcomeRecordsEmptyOutcomeProducesNothing(t *testing.T) {
	cfg := config.Daemon{EventsTopic: "events.topic", DeadLetterTopic: "dlt.topic"}
	recs, err := buildRetryOutcomeRecords(cfg, retry.Outcome{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected no records for an empty outcome, got %+v", recs)
	}
}

func TestBuildRetryOutcomeRecordsOnlyRetarget(t *testing.T) {
	cfg := config.Daemon{EventsTopic: "events.topic", DeadLetterTopic: "dlt.topic"}
	out := retry.Outcome{
		ProduceBody:  []byte(`{"job_id":"j1"}`),
		ProduceKey:   "j1",
		ProduceTopic: "jobs.retry.target",
	}
	recs, err := buildRetryOutcomeRecords(cfg, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Topic != "jobs.retry.target" {
		t.Fatalf("expected exactly 1 retarget record, got %+v", recs)
	}
}

func TestShouldCommitRetryOutcome(t *testing.T) {
	someErr := errors.New("boom")
	cases := []struct {
		name       string
		procErr    error
		produceErr error
		out        retry.Outcome
		want       bool
	}{
		{"clean success commits", nil, nil, retry.Outcome{}, true},
		{"process error aborts", someErr, nil, retry.Outcome{}, false},
		{"produce error aborts", nil, someErr, retry.Outcome{}, false},
		{"pause aborts even with no errors", nil, nil, retry.Outcome{Pause: true}, false},
		{"pause plus error still aborts", someErr, nil, retry.Outcome{Pause: true}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := shouldCommitRetryOutcome(c.procErr, c.produceErr, c.out)
			if got != c.want {
				t.Fatalf("shouldCommitRetryOutcome() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestSafeRetryProcessRecoversPanic(t *testing.T) {
	// retry.Processor.Process reads p.Now / p.MaxPause off its receiver once it decides
	// a message needs a valid_till/pause check, so calling it on a nil *Processor with a
	// message that reaches that path panics with a nil pointer dereference. This
	// exercises the real safeRetryProcess wrapper (not a reimplementation of it) to
	// confirm the panic is actually recovered rather than crashing the daemon.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic escaped safeRetryProcess wrapper: %v", r)
		}
	}()

	var nilProc *retry.Processor
	raw := []byte(`{"retry_to":"jobs.retry.target","valid_till":"2020-01-01T00:00:00Z","job_id":"j1"}`)

	out, err := safeRetryProcess(nilProc, context.Background(), raw, protocol.SourceCoords{Topic: "retry.test"})
	if err == nil {
		t.Fatal("expected an error after recovering the panic, got nil")
	}
	if out.CommitOffset {
		t.Fatalf("expected zero-value outcome after panic, got %+v", out)
	}
}
