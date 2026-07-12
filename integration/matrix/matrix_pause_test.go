//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_ConsumptionPauseCrossRuntime verifies the killswitch contract: a
// pause written to the shared Redis consumption-control set (the exact key/format
// Ruby's KafkaBatch::ConsumptionControl.pause_topic uses) is honored by the Go
// worker, which stops processing the paused topic; after resume, the buffered job
// drains. This proves an operator can pause a topic from the Ruby control tooling
// and have Go execution obey it (and vice versa), rather than each runtime having
// its own private pause state.
func TestMatrix_ConsumptionPauseCrossRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("consumption pause requires live Kafka")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, e2e.ApplyFastConsumptionRefresh)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Go: true}})
	defer s.Stop()

	ctx := context.Background()
	c := s.NewClient()
	defer c.Close()

	group := s.GoWorkerJobsGroup()

	// Pause the Go worker's jobs topic via the shared Redis set, then enqueue.
	s.SetTopicPaused(group, s.WorkerTopic, true)
	// Give the worker's pause snapshot (1s refresh) time to observe the pause.
	time.Sleep(2 * time.Second)

	jobID, err := c.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// While paused, the job must NOT be processed.
	s.AssertNoMarkerAt(s.MarkerPath, 8*time.Second)

	// Resume — the job should now drain.
	s.SetTopicPaused(group, s.WorkerTopic, false)
	if got := s.WaitMarkerAt(s.MarkerPath, 30*time.Second); got != jobID {
		t.Fatalf("marker after resume = %q want %q", got, jobID)
	}
}
