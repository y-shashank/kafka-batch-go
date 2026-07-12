//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_DLTExhaustedRubyExec covers a Ruby job that exhausts its retries
// under a Go control plane and lands in the dead-letter topic. The retry cycle
// crosses runtimes: the Ruby JobConsumer fails and emits a retry message, the Go
// daemon's retry consumer re-enqueues it after the delay, the Ruby consumer
// fails again at the retry ceiling, and the job is dead-lettered. This is the
// Ruby-exec counterpart to the existing Go-exec DLT scenario, and exercises the
// shared retry-tier + DLT envelope across runtimes.
func TestMatrix_DLTExhaustedRubyExec(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-runtime DLT requires live Kafka + Ruby")
	}
	if !e2e.RubyItestAvailable() {
		t.Skip("Ruby exec unavailable (compat/ruby bundle install && kafka-batch gem)")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Ruby: true}})
	defer s.Stop()

	ctx := context.Background()
	c := s.NewClient()
	defer c.Close()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "ruby-dlt"}, func(b *client.Batch) error {
		_, err := b.PushJob(ctx, "integration.ruby_always_fail", map[string]interface{}{"x": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drain long enough for: fail → Go retry re-enqueue → fail at ceiling → DLT.
	s.DrainRubyExecution(120 * time.Second)

	row := s.WaitBatchTimeout(ctx, 120*time.Second, batch.ID(), "complete")
	if row.FailedCount != 1 {
		t.Fatalf("failed_count = %d want 1", row.FailedCount)
	}

	dlt := s.PollTopic(ctx, s.DLTTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID()
	}, 30*time.Second)
	if ec, _ := dlt["dlt_error_class"].(string); ec == "" {
		t.Fatalf("dead-lettered message missing dlt_error_class: %v", dlt)
	}
}
