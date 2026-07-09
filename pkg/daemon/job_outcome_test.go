package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestApplyJobOutcomeProducesEventRetryAndDLT(t *testing.T) {
	var topics []string
	prod := recordingProducer{fn: func(_ context.Context, topic, _ string, _ []byte) error {
		topics = append(topics, topic)
		return nil
	}}
	cfg := config.Daemon{
		EventsTopic:      "events",
		DeadLetterTopic:  "dlt",
		EventEmitRetries: 1,
	}
	out := job.Outcome{
		Event:        &protocol.EventMessage{JobID: "j1", BatchID: "b1"},
		RetryPayload: []byte(`{"retry":true}`),
		RetryTopic:   "retry.short",
		RetryKey:     "j1",
		DLTPayload:   []byte(`{"dlt":true}`),
		DLTKey:       "j1",
		CommitOffset: true,
	}
	if err := applyJobOutcome(context.Background(), cfg, prod, out); err != nil {
		t.Fatal(err)
	}
	if len(topics) != 3 {
		t.Fatalf("topics=%v", topics)
	}
}

func TestApplyJobOutcomeNotCommitted(t *testing.T) {
	cfg := config.Daemon{}
	out := job.Outcome{CommitOffset: false}
	if err := applyJobOutcome(context.Background(), cfg, recordingProducer{}, out); err == nil {
		t.Fatal("expected commit error")
	}
}

func TestApplyRetryOutcomePausedReturnsError(t *testing.T) {
	cfg := config.Daemon{EventsTopic: "events"}
	out := retry.Outcome{Pause: true, PauseFor: time.Millisecond}
	if err := applyRetryOutcome(context.Background(), cfg, recordingProducer{}, out, protocol.SourceCoords{}); err == nil {
		t.Fatal("expected paused error")
	}
}

type recordingProducer struct {
	fn func(context.Context, string, string, []byte) error
}

func (r recordingProducer) Produce(ctx context.Context, topic, key string, payload []byte) error {
	if r.fn != nil {
		return r.fn(ctx, topic, key, payload)
	}
	return nil
}
