//go:build integration

package matrix

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_ConsumptionPauseCrossRuntime verifies the killswitch contract: a
// pause written to the shared Redis consumption-control set (the exact key/format
// Ruby's KafkaBatch::ConsumptionControl.pause_topic uses) is honored by the Go
// worker — it stops processing the paused topic — and processing recovers once
// the pause is cleared. This proves an operator can flip the killswitch from
// either runtime's tooling and have Go execution obey it.
//
// NOTE ON SEMANTICS: Ruby's Karafka ConsumptionGate pauses the partition and
// seeks back to the first unprocessed offset, so messages produced during a
// pause are redelivered on resume. The Go worker currently *drops* records it
// polls while paused (pkg/daemon/consumers.go collectPollRecords skips them and
// the fetch cursor advances), so a mid-pause message is not redelivered on
// resume within the same session. This test therefore asserts block-then-recover
// (a job enqueued AFTER resume runs), not mid-pause redelivery. The redelivery
// divergence is a known cross-runtime gap, tracked separately.
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

	if _, err := c.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{}); err != nil {
		t.Fatal(err)
	}

	// While paused, nothing on the topic is processed.
	s.AssertNoMarkerAt(s.MarkerPath, 8*time.Second)

	// Clear the pause, then enqueue a fresh job — the worker must resume
	// processing the topic. Poll until the marker reaches the resume job (a
	// dropped mid-pause job never reappears today; if redelivery is added later,
	// it is processed at a lower offset first and the marker still converges).
	s.SetTopicPaused(group, s.WorkerTopic, false)
	time.Sleep(2 * time.Second)
	resumeID, err := c.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 2}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitMarkerEquals(t, s.MarkerPath, resumeID, 30*time.Second)
}

// waitMarkerEquals polls the marker file until it equals want, tolerating an
// earlier transient value that is later overwritten.
func waitMarkerEquals(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			last = strings.TrimSpace(string(b))
			if last == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("marker at %s = %q, want %q", path, last, want)
}
