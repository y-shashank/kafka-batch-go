//go:build integration

package e2e

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

func TestE2E_BatchCancellationScheduled(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyScheduleConfig)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "cancel-sched"}, func(b *client.Batch) error {
		_, err := b.PushJobIn(ctx, 8*time.Second, "integration.go_scheduled", map[string]interface{}{"n": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Cancel(ctx); err != nil {
		t.Fatal(err)
	}

	s.AssertNoMarker(12 * time.Second)

	row, err := s.Store().FindBatch(ctx, batch.ID())
	if err != nil {
		t.Fatal(err)
	}
	if row == nil || row.Status != "cancelled" {
		t.Fatalf("batch status = %v", row)
	}
}

func TestE2E_UniqDuplicateSkip(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClientOptions(true)
	defer c.Close()
	ctx := context.Background()

	payload := map[string]interface{}{"order_id": 42}
	id1, err := c.EnqueueJob(ctx, "integration.go_uniq", payload, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.EnqueueJob(ctx, "integration.go_uniq", payload, client.PushOptions{})
	if !errors.Is(err, client.ErrJobSkipped) {
		t.Fatalf("duplicate enqueue err = %v want ErrJobSkipped", err)
	}
	if id2 != "" {
		t.Fatalf("duplicate job id = %q want empty", id2)
	}

	if got := s.WaitMarker(45 * time.Second); got != id1 {
		t.Fatalf("marker = %q want %q", got, id1)
	}
}

func TestE2E_JobExpiry(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "expired"}, func(b *client.Batch) error {
		_, err := b.PushJob(ctx, "integration.go_expired", map[string]interface{}{"x": 1}, client.PushOptions{
			ValidTill: "2000-01-01T00:00:00Z",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	row := s.WaitBatch(ctx, batch.ID(), "complete")
	if row.FailedCount != 1 {
		t.Fatalf("failed_count = %d want 1", row.FailedCount)
	}
	if row.CompletedCount != 0 {
		t.Fatalf("completed_count = %d want 0", row.CompletedCount)
	}
	s.AssertNoMarker(5 * time.Second)

	ev := s.PollTopic(ctx, s.EventsTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID() && m["status"] == "failed"
	}, 30*time.Second)
	if ev["job_id"] == "" {
		t.Fatalf("missing job_id in event: %v", ev)
	}
}

func TestE2E_FairThroughputRouting(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyFairConfig)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	jobID, err := c.EnqueueJob(ctx, "integration.go_fair_throughput", map[string]interface{}{"tenant": "tp-a"}, client.PushOptions{TenantID: "tenant-tp"})
	if err != nil {
		t.Fatal(err)
	}

	msg := s.PollTopic(ctx, s.TpReadyGo, func(m map[string]interface{}) bool {
		return m["job_id"] == jobID
	}, 60*time.Second)
	if msg["job_type"] != "integration.go_fair_throughput" {
		t.Fatalf("job_type = %v", msg["job_type"])
	}
	s.WaitMarker(60 * time.Second)
}

func TestE2E_PriorityP1(t *testing.T) {
	s := NewStack(t, priorityHandlersForStack, func(stack *Stack, cfg *daemonYAML) {
		prioPath := writePriorityConfig(t, stack.TmpDir, stack.Suffix, stack.P0Topic, stack.P1Topic)
		cfg.PriorityConfigPaths = []string{prioPath}
		cfg.PriorityLagCheckInterval = 1
	})
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	jobID, err := c.EnqueueJob(ctx, "integration.go_p1", map[string]interface{}{"rank": 1}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.WaitMarker(45 * time.Second); got != jobID {
		t.Fatalf("marker = %q want %q", got, jobID)
	}
}

func TestE2E_StandaloneEnqueueJobIn(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyScheduleConfig)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	jobID, err := c.EnqueueJobIn(ctx, 2*time.Second, "integration.go_scheduled", map[string]interface{}{"standalone": true}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}

	s.PollTopic(ctx, s.ScheduledTopic, func(m map[string]interface{}) bool {
		return m["job_id"] == jobID
	}, 15*time.Second)

	if got := s.WaitMarker(60 * time.Second); got != jobID {
		t.Fatalf("marker = %q want %q", got, jobID)
	}
}

func TestE2E_EventsTopicJobSuccess(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	var jobID string
	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "events"}, func(b *client.Batch) error {
		var err error
		jobID, err = b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	ev := s.PollTopic(ctx, s.EventsTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID() && m["job_id"] == jobID && m["status"] == "success"
	}, 45*time.Second)
	if ev["worker_class"] == "" {
		t.Fatalf("missing worker_class: %v", ev)
	}
	s.WaitBatch(ctx, batch.ID(), "success")
}

func TestE2E_PartialBatchCompleteAfterRetries(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "partial"}, func(b *client.Batch) error {
		if _, err := b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ok": 1}, client.PushOptions{}); err != nil {
			return err
		}
		_, err := b.PushJob(ctx, "integration.go_always_fail", map[string]interface{}{"fail": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	row := s.WaitBatch(ctx, batch.ID(), "complete")
	if row.CompletedCount != 1 {
		t.Fatalf("completed_count = %d want 1", row.CompletedCount)
	}
	if row.FailedCount != 1 {
		t.Fatalf("failed_count = %d want 1", row.FailedCount)
	}
}

func TestE2E_HealthEndpoint(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyHealthConfig)
	s.Start()
	defer s.Stop()

	s.WaitHealthOK(30 * time.Second)

	resp, err := http.Get(s.HealthURL())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d want 200", resp.StatusCode)
	}
}
