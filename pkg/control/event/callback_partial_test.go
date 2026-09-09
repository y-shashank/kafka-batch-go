package event

import (
	"context"
	"errors"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// outcomeProducer fails chosen keys in the batch produce (and optionally in
// per-item produces too) while recording every durable delivery, so tests can
// count exactly how many times each callback landed on the topic.
type outcomeProducer struct {
	failKeys     map[string]error
	failItemsToo bool
	produced     map[string]int // key → times durably produced on callbacks topic
	dltProduced  map[string]int // key → times parked on DLT
	dltTopic     string
}

func newOutcomeProducer(dltTopic string, failKeys map[string]error, failItemsToo bool) *outcomeProducer {
	return &outcomeProducer{
		failKeys: failKeys, failItemsToo: failItemsToo, dltTopic: dltTopic,
		produced: map[string]int{}, dltProduced: map[string]int{},
	}
}

func (o *outcomeProducer) Produce(_ context.Context, topic string, key string, _ []byte) error {
	if topic == o.dltTopic {
		o.dltProduced[key]++
		return nil
	}
	if err, ok := o.failKeys[key]; ok && o.failItemsToo {
		return err
	}
	o.produced[key]++
	return nil
}

func (o *outcomeProducer) ProduceManySync(_ context.Context, records []kafkaclient.ProduceRecord) ([]kafkaclient.ProduceOutcome, error) {
	outs := make([]kafkaclient.ProduceOutcome, len(records))
	var firstErr error
	for i, r := range records {
		if err, ok := o.failKeys[r.Key]; ok {
			outs[i].Err = err
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Durable even though a sibling record failed — the partial-failure
		// reality these tests exist to pin.
		o.produced[r.Key]++
	}
	return outs, firstErr
}

// A partial batch-produce failure must re-produce ONLY the failed callbacks.
// Preclaimed callbacks skip the consumer's ClaimCallback dedup, so a re-produce
// of an already-durable one invokes the callback (e.g. settle_billing) twice.
func TestProduceCallbacksPartialFailureRetriesOnlyFailed(t *testing.T) {
	boom := errors.New("transient partition blip")
	prod := newOutcomeProducer("dlt", map[string]error{"b-flaky": boom}, false)
	p := &Processor{Cfg: config.Daemon{CallbacksTopic: "cb", DeadLetterTopic: "dlt"}, Producer: prod}

	callbacks := []protocol.CallbackMessage{
		{BatchID: "b-ok-1", Outcome: "success", Preclaimed: true},
		{BatchID: "b-flaky", Outcome: "success", Preclaimed: true},
		{BatchID: "b-ok-2", Outcome: "complete", Preclaimed: true},
	}
	if err := p.produceCallbacks(context.Background(), callbacks); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"b-ok-1", "b-ok-2"} {
		if got := prod.produced[key]; got != 1 {
			t.Fatalf("%s produced %d times, want exactly 1 (a duplicate double-invokes a preclaimed callback)", key, got)
		}
	}
	// The batch attempt failed b-flaky; the per-item retry succeeded → exactly once.
	if got := prod.produced["b-flaky"]; got != 1 {
		t.Fatalf("b-flaky produced %d times, want 1 via per-item retry", got)
	}
	if got := prod.dltProduced["b-flaky"]; got != 0 {
		t.Fatalf("b-flaky dead-lettered %d times despite successful retry", got)
	}
}

// When the per-item retry also keeps failing, the callback is parked on the
// dead-letter topic — and the healthy callbacks are still produced exactly once.
func TestProduceCallbacksPersistentFailureDeadLetters(t *testing.T) {
	boom := errors.New("partition leader down")
	prod := newOutcomeProducer("dlt", map[string]error{"b-dead": boom}, true)
	p := &Processor{Cfg: config.Daemon{CallbacksTopic: "cb", DeadLetterTopic: "dlt"}, Producer: prod}

	callbacks := []protocol.CallbackMessage{
		{BatchID: "b-ok", Outcome: "success", Preclaimed: true},
		{BatchID: "b-dead", Outcome: "complete", Preclaimed: true},
	}
	if err := p.produceCallbacks(context.Background(), callbacks); err != nil {
		t.Fatal(err)
	}
	if got := prod.produced["b-ok"]; got != 1 {
		t.Fatalf("b-ok produced %d times, want exactly 1", got)
	}
	if got := prod.produced["b-dead"]; got != 0 {
		t.Fatalf("b-dead landed on the callbacks topic %d times, want 0", got)
	}
	if got := prod.dltProduced["b-dead"]; got != 1 {
		t.Fatalf("b-dead dead-lettered %d times, want 1 (never silently lost)", got)
	}
}
