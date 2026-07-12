//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_CallbackMessageCrossRuntime verifies the cross-runtime callback
// handoff: when a batch carrying a legacy class-string on_complete finalizes
// under the Go control plane, the Go daemon does NOT run the Ruby class itself
// (it has no Ruby VM) — instead it produces a CallbackMessage to the callbacks
// topic for a Ruby Karafka CallbackConsumer to pick up. This test asserts that
// message is emitted with the on_complete class name, outcome, and counts intact,
// which is exactly what the Ruby consumer needs. Without it, batches using legacy
// class callbacks under Go control would finalize with no callback handoff.
func TestMatrix_CallbackMessageCrossRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("callback handoff requires live Kafka")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Go: true}})
	defer s.Stop()

	ctx := context.Background()
	c := s.NewClient()
	defer c.Close()

	const onComplete = "Reporting::OnCompleteWorker"
	batch, err := c.CreateBatch(ctx, client.BatchOptions{
		Description:  "cross-callback",
		OnComplete:   onComplete,
		CallbackArgs: map[string]interface{}{"run_id": "42"},
	}, func(b *client.Batch) error {
		_, err := b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	s.WaitBatch(ctx, batch.ID(), "success")

	cb := s.PollTopic(ctx, s.CallbacksTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID()
	}, 45*time.Second)

	if cb["on_complete"] != onComplete {
		t.Fatalf("callback on_complete = %v want %q", cb["on_complete"], onComplete)
	}
	if out, _ := cb["outcome"].(string); out != "success" && out != "complete" {
		t.Fatalf("callback outcome = %v want success|complete", cb["outcome"])
	}
	if tj, _ := cb["total_jobs"].(float64); tj != 1 {
		t.Fatalf("callback total_jobs = %v want 1", cb["total_jobs"])
	}
	// callback_args must survive finalization so the Ruby CallbackConsumer sees them.
	args, _ := cb["callback_args"].(map[string]interface{})
	if args == nil || args["run_id"] != "42" {
		t.Fatalf("callback_args = %v want run_id=42", cb["callback_args"])
	}
}
