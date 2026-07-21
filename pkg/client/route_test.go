package client

import (
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestRouteForFairnessLanes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TopicPrefix = "ship"
	batchID := "batch-9"
	c := &Client{cfg: cfg}

	tests := []struct {
		name     string
		entry    config.HandlerEntry
		jobID    string
		tenantID string
		batchID  *string
		wantKey  string
		wantTop  string
	}{
		{
			name:    "time with tenant",
			entry:   config.HandlerEntry{FairnessType: "time"},
			jobID:   "j1",
			tenantID: "t1",
			wantKey: "t1",
			wantTop: cfg.resolveTopic(cfg.FairnessTimeIngest),
		},
		{
			name:    "throughput falls back to batch",
			entry:   config.HandlerEntry{FairnessType: "throughput"},
			jobID:   "j1",
			batchID: &batchID,
			wantKey: batchID,
			wantTop: cfg.resolveTopic(cfg.FairnessThroughputIngest),
		},
		{
			name:    "unknown lane uses jobs topic and job id",
			entry:   config.HandlerEntry{FairnessType: "other"},
			jobID:   "j9",
			wantKey: "j9",
			wantTop: cfg.defaultJobsTopic(),
		},
		{
			name:    "plain topic override",
			entry:   config.HandlerEntry{Topic: "custom.jobs"},
			jobID:   "j2",
			wantKey: "j2",
			wantTop: "custom.jobs",
		},
		{
			name:    "plain default topic",
			entry:   config.HandlerEntry{},
			jobID:   "j3",
			wantKey: "j3",
			wantTop: cfg.defaultJobsTopic(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := c.routeFor(tt.entry, tt.jobID, tt.tenantID, tt.batchID)
			if route.Topic != tt.wantTop || route.Key != tt.wantKey {
				t.Fatalf("route=%+v want topic=%s key=%s", route, tt.wantTop, tt.wantKey)
			}
		})
	}
}

func TestResolveRouteAndManifest(t *testing.T) {
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"echo": {Runtime: "go", Topic: "jobs.echo"},
	}}
	c := &Client{cfg: DefaultConfig(), manifest: manifest}

	route, err := c.ResolveRoute("echo", "j1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if route.Topic != "jobs.echo" || route.Key != "j1" {
		t.Fatalf("route=%+v", route)
	}
	_, err = c.ResolveRoute("missing", "j1", "", nil)
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
	if got := c.Manifest(); len(got.Handlers) != 1 {
		t.Fatalf("manifest handlers=%d", len(got.Handlers))
	}
}
