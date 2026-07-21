package schedule

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
)

func TestDaemonRouterWorkerClassLookupAndDefault(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"hidden.go": {Runtime: "go", Topic: "jobs.hidden"},
		}},
		Default: "fallback.topic",
	}
	route, err := r.Route(map[string]interface{}{
		"worker_class": "go:hidden.go",
		"job_id":       "j1",
	})
	if err != nil || route.Topic != "jobs.hidden" || route.Key != "j1" {
		t.Fatalf("worker_class route %+v err=%v", route, err)
	}

	route, err = r.Route(map[string]interface{}{"job_id": "j2", "job_type": "missing"})
	if err != nil || route.Topic != "fallback.topic" || route.Key != "j2" {
		t.Fatalf("default route %+v err=%v", route, err)
	}
}

func TestDaemonRouterNoRoute(t *testing.T) {
	r := DaemonRouter{Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{}}}
	_, err := r.Route(map[string]interface{}{"job_type": "x", "worker_class": "go:x"})
	if err == nil {
		t.Fatal("expected no route error")
	}
}

func TestDaemonRouterFairThroughputAndKeyFallback(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.tp": {Runtime: "go", FairnessType: "throughput"},
		}},
		Cfg: config.Daemon{FairnessThroughputIngest: "tp.ingest"},
	}
	// No tenant → batch_id key.
	route, err := r.Route(map[string]interface{}{
		"job_type": "fair.tp", "job_id": "j1", "batch_id": "b1",
	})
	if err != nil || route.Topic != "tp.ingest" || route.Key != "b1" {
		t.Fatalf("batch key %+v err=%v", route, err)
	}
	// No tenant/batch → job_id key.
	route, err = r.Route(map[string]interface{}{
		"job_type": "fair.tp", "job_id": "j1",
	})
	if err != nil || route.Key != "j1" {
		t.Fatalf("job key %+v err=%v", route, err)
	}
}

func TestDaemonRouterUnknownFairnessType(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.x": {Runtime: "go", FairnessType: "weird"},
		}},
	}
	_, err := r.Route(map[string]interface{}{"job_type": "fair.x", "job_id": "j"})
	if err == nil {
		t.Fatal("expected unknown fairness_type")
	}
}

func TestDaemonRouterStaticTenantPartition(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.job": {Runtime: "go", FairnessType: "time"},
		}},
		Cfg: config.Daemon{
			FairnessTimeIngest: "fair.ingest",
			FairnessTenantPartitions: map[string]int32{
				"acme": 7,
			},
		},
	}
	route, err := r.Route(map[string]interface{}{
		"job_type": "fair.job", "job_id": "j1", "tenant_id": "acme",
	})
	if err != nil || route.Topic != "fair.ingest" || route.Key != "" || route.Partition == nil || *route.Partition != 7 {
		t.Fatalf("route %+v err=%v", route, err)
	}
}

func TestDaemonRouterTenantPartitionsResolve(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	tp := fairness.NewTenantPartitions(rdb, fairness.TenantPartitionsConfig{
		Static: map[string]int32{"dyn": 3},
	})
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.job": {Runtime: "go", FairnessType: "time"},
		}},
		Cfg:     config.Daemon{FairnessTimeIngest: "fair.ingest"},
		Tenants: tp,
	}
	route, err := r.Route(map[string]interface{}{
		"job_type": "fair.job", "job_id": "j1", "tenant_id": "dyn",
	})
	if err != nil || route.Partition == nil || *route.Partition != 3 || route.Key != "" {
		t.Fatalf("route %+v err=%v", route, err)
	}
}
