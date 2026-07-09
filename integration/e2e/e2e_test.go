//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

func TestE2E_BatchCompletion(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	var jobID string
	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "e2e batch"}, func(b *client.Batch) error {
		var err error
		jobID, err = b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	row := s.WaitBatch(ctx, batch.ID(), "success")
	if row.CompletedCount != 1 {
		t.Fatalf("completed_count = %d", row.CompletedCount)
	}
	got := s.WaitMarker(45 * time.Second)
	if got != jobID {
		t.Fatalf("marker = %q want %q", got, jobID)
	}
}

func TestE2E_StandaloneEnqueue(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	jobID, err := c.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"k": "v"}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}

	msg := s.PollTopic(ctx, s.WorkerTopic, func(m map[string]interface{}) bool {
		return m["job_id"] == jobID
	}, 30*time.Second)
	if msg["job_type"] != "integration.go_daemon" {
		t.Fatalf("job_type = %v", msg["job_type"])
	}
}

func TestE2E_MultiJobCallback(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{
		Description: "multi",
		OnSuccess:   "TestCb",
		OnComplete:  "TestCb",
	}, func(b *client.Batch) error {
		for i := 1; i <= 3; i++ {
			if _, err := b.PushJob(ctx, "integration.go_multi", map[string]interface{}{"n": i}, client.PushOptions{}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	row := s.WaitBatch(ctx, batch.ID(), "success")
	if row.CompletedCount != 3 {
		t.Fatalf("completed_count = %d", row.CompletedCount)
	}

	cb := s.PollTopic(ctx, s.CallbacksTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID()
	}, 30*time.Second)
	if cb["outcome"] != "success" {
		t.Fatalf("outcome = %v", cb["outcome"])
	}
}

func TestE2E_RetryThenSuccess(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	var jobID string
	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "retry"}, func(b *client.Batch) error {
		var err error
		jobID, err = b.PushJob(ctx, "integration.go_retry_once", map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	s.WaitBatch(ctx, batch.ID(), "success")
	if got := s.WaitMarker(45 * time.Second); got != jobID {
		t.Fatalf("marker = %q", got)
	}
}

func TestE2E_RetriesExhaustedDLT(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "dlt"}, func(b *client.Batch) error {
		_, err := b.PushJob(ctx, "integration.go_always_fail", map[string]interface{}{"x": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	row := s.WaitBatch(ctx, batch.ID(), "complete")
	if row.FailedCount != 1 {
		t.Fatalf("failed_count = %d", row.FailedCount)
	}

	dlt := s.PollTopic(ctx, s.DLTTopic, func(m map[string]interface{}) bool {
		return m["batch_id"] == batch.ID()
	}, 30*time.Second)
	if dlt["dlt_error_class"] != "Permanent" {
		t.Fatalf("dlt_error_class = %v", dlt["dlt_error_class"])
	}
}

func TestE2E_ClientPushesRubyPlainJob(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	jobID, err := c.EnqueueJob(ctx, "integration.ruby_plain", map[string]interface{}{"order_id": 1}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}

	rt := rubyTopic(s)
	msg := s.PollTopic(ctx, rt, func(m map[string]interface{}) bool {
		return m["job_id"] == jobID
	}, 30*time.Second)
	if msg["job_type"] != "integration.ruby_plain" {
		t.Fatalf("job_type = %v", msg["job_type"])
	}
	if msg["worker_class"] != "RubyPlainWorker" {
		t.Fatalf("worker_class = %v", msg["worker_class"])
	}
}

func TestE2E_FairRoutingGoAndRuby(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyFairConfig)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	goID, err := c.EnqueueJob(ctx, "integration.go_fair", map[string]interface{}{"tenant": "a"}, client.PushOptions{TenantID: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	rubyID, err := c.EnqueueJob(ctx, "integration.ruby_fair", map[string]interface{}{"tenant": "b"}, client.PushOptions{TenantID: "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}

	goMsg := s.PollTopic(ctx, s.TimeReadyGo, func(m map[string]interface{}) bool {
		return m["job_id"] == goID
	}, 60*time.Second)
	if goMsg["job_type"] != "integration.go_fair" {
		t.Fatalf("go job_type = %v", goMsg["job_type"])
	}

	rubyMsg := s.PollTopic(ctx, s.TimeReadyRuby, func(m map[string]interface{}) bool {
		return m["job_id"] == rubyID
	}, 60*time.Second)
	if rubyMsg["job_type"] != "integration.ruby_fair" {
		t.Fatalf("ruby job_type = %v", rubyMsg["job_type"])
	}

	s.WaitMarker(60 * time.Second)
}

func TestE2E_ScheduledJob(t *testing.T) {
	s := NewStack(t, baseHandlersStack, applyScheduleConfig)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "sched"}, func(b *client.Batch) error {
		_, err := b.PushJobIn(ctx, 2*time.Second, "integration.go_scheduled", map[string]interface{}{"n": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	s.WaitBatch(ctx, batch.ID(), "success")
	s.WaitMarker(60 * time.Second)
}

func TestE2E_PriorityP0(t *testing.T) {
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

	jobID, err := c.EnqueueJob(ctx, "integration.go_p0", map[string]interface{}{"rank": 0}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if got := s.WaitMarkerAt(s.P0MarkerPath, 45*time.Second); got != jobID {
		t.Fatalf("marker = %q want %q", got, jobID)
	}
}

func TestE2E_ClientPushesGoAndRubyWithoutRegistration(t *testing.T) {
	s := NewStack(t, baseHandlersStack, nil)
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	if _, err := c.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"n": 1}, client.PushOptions{}); err != nil {
		t.Fatalf("go enqueue: %v", err)
	}
	if _, err := c.EnqueueJob(ctx, "integration.ruby_plain", map[string]interface{}{"n": 2}, client.PushOptions{}); err != nil {
		t.Fatalf("ruby enqueue: %v", err)
	}
}

func baseHandlersStack(s *Stack) map[string]handlerYAML {
	return baseHandlers(s.WorkerTopic)
}
