package fairness

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type memProducer struct {
	topic string
	key   string
	body  []byte
}

func (m *memProducer) Produce(_ context.Context, topic, key string, payload []byte) error {
	m.topic, m.key, m.body = topic, key, payload
	return nil
}

func TestForwarderForwardsReadyJob(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{
		Lane:                LaneTime,
		GlobalConcurrency:   10,
		ReadyWindow:         100,
		LeaseTTL:            60,
		DefaultWeight:       1,
		WeightedConcurrency: true,
	})
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "tenant_id": "acme", "worker_class": "go:fair",
	})
	if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
		t.Fatal(err)
	}

	prod := &memProducer{}
	fwd := &Forwarder{
		Lane:       LaneTime,
		Scheduler:  sched,
		ReadyTopic: "ready.time",
		Producer:   prod,
		Now:        time.Now,
	}
	if !fwd.ForwardOnce(ctx) {
		t.Fatal("expected forward")
	}
	if prod.topic != "ready.time" {
		t.Fatalf("topic=%q", prod.topic)
	}
}

func TestForwarderDropsExpiredJob(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{Lane: LaneTime, GlobalConcurrency: 10, ReadyWindow: 100, LeaseTTL: 60})
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": "j-exp", "tenant_id": "acme", "valid_till": "2000-01-01T00:00:00Z",
	})
	if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
		t.Fatal(err)
	}
	expired := 0
	fwd := &Forwarder{
		Lane:      LaneTime,
		Scheduler: sched,
		Producer:  &memProducer{},
		Now:       func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		OnExpired: func(context.Context, *CheckoutResult, []byte) error {
			expired++
			return nil
		},
	}
	if !fwd.ForwardOnce(ctx) {
		t.Fatal("expected expired handling")
	}
	if expired != 1 {
		t.Fatalf("expired=%d", expired)
	}
}

func TestForwarderResolveReadyTopicSplitRuntime(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{Lane: LaneTime, GlobalConcurrency: 10, ReadyWindow: 100, LeaseTTL: 60})
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "tenant_id": "acme", "job_type": "go.fair", "worker_class": "go:go.fair",
	})
	if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
		t.Fatal(err)
	}
	prod := &memProducer{}
	fwd := &Forwarder{
		Lane:      LaneTime,
		Scheduler: sched,
		ResolveReadyTopic: func(raw []byte) (string, error) {
			return "ready.go", nil
		},
		Producer: prod,
	}
	if !fwd.ForwardOnce(ctx) {
		t.Fatal("expected forward")
	}
	if prod.topic != "ready.go" {
		t.Fatalf("topic=%q", prod.topic)
	}
}
