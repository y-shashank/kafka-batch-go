package client

import (
	"context"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestEnqueueManyJobsEmpty(t *testing.T) {
	c := &Client{
		cfg: DefaultConfig(),
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"orders.process": {Runtime: "go", Topic: "jobs.export"},
		}},
	}
	ids, err := c.EnqueueManyJobs(context.Background(), "orders.process", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("empty payloads ids=%v err=%v", ids, err)
	}
	ids, err = c.EnqueueManyJobs(context.Background(), "orders.process", []map[string]interface{}{}, PushOptions{TenantID: "acme"})
	if err != nil || ids != nil {
		t.Fatalf("empty slice ids=%v err=%v", ids, err)
	}
}

func TestEnqueueManyEmpty(t *testing.T) {
	c := &Client{
		cfg: DefaultConfig(),
		workerByClass: map[string]workerBinding{
			"Orders::ProcessWorker": {
				jobType: "orders.process",
				entry:   config.HandlerEntry{Runtime: "ruby", WorkerClass: "Orders::ProcessWorker", Topic: "jobs.ruby"},
			},
		},
	}
	ids, err := c.EnqueueMany(context.Background(), "Orders::ProcessWorker", nil, PushOptions{TenantID: "acme"})
	if err != nil || ids != nil {
		t.Fatalf("empty payloads ids=%v err=%v", ids, err)
	}
}

func TestPlanStandalonePushesFairTenant(t *testing.T) {
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.time.go": {Runtime: "go", FairnessType: "time"},
		}},
	}
	entry, plans, jobIDs, err := c.planStandalonePushes(context.Background(), "fair.time.go", []map[string]interface{}{
		{"n": 1}, {"n": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if entry.FairnessType != "time" {
		t.Fatalf("entry %+v", entry)
	}
	if len(plans) != 2 || len(jobIDs) != 2 {
		t.Fatalf("plans=%d ids=%d", len(plans), len(jobIDs))
	}
	route := c.routeFor(entry, plans[0].jobID, "acme", nil)
	want := cfg.resolveTopic(cfg.FairnessTimeIngest)
	if route.Topic != want {
		t.Fatalf("topic=%s want=%s", route.Topic, want)
	}
	if route.Key != "acme" && route.Partition == nil {
		t.Fatalf("expected tenant key or pinned partition, got %+v", route)
	}
}

func TestUnknownHandlerEnqueueManyJobs(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{}}}
	_, err := c.EnqueueManyJobs(context.Background(), "missing.job", []map[string]interface{}{{"x": 1}}, PushOptions{})
	if err == nil {
		t.Fatal("expected unknown handler error")
	}
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err type %T: %v", err, err)
	}
}
