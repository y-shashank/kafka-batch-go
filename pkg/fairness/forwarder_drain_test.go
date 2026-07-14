package fairness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type failOnceProducer struct {
	calls int
	inner memProducer
}

func (p *failOnceProducer) Produce(ctx context.Context, topic, key string, payload []byte) error {
	p.calls++
	if p.calls == 1 {
		return errors.New("broker down")
	}
	return p.inner.Produce(ctx, topic, key, payload)
}

func TestDrainBurstStopsOnEmpty(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{
		Lane: LaneTime, GlobalConcurrency: 10, ReadyWindow: 100, LeaseTTL: 60, DefaultWeight: 1,
	})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		payload, _ := json.Marshal(map[string]interface{}{
			"job_id": "j" + string(rune('1'+i)), "tenant_id": "acme", "worker_class": "go:fair",
		})
		if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
			t.Fatal(err)
		}
	}
	fwd := &Forwarder{
		Lane: LaneTime, Scheduler: sched, ReadyTopic: "ready.time",
		Producer: &memProducer{}, Now: time.Now,
	}
	n, empty, failed := fwd.drainBurst(ctx, 50)
	if failed {
		t.Fatal("unexpected failure")
	}
	if !empty {
		t.Fatal("expected empty after drain")
	}
	if n != 3 {
		t.Fatalf("forwarded=%d want 3", n)
	}
}

func TestDrainBurstReportsFailureWithoutCallingIdle(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{
		Lane: LaneTime, GlobalConcurrency: 10, ReadyWindow: 100, LeaseTTL: 60, DefaultWeight: 1,
	})
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "tenant_id": "acme", "worker_class": "go:fair",
	})
	if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
		t.Fatal(err)
	}
	fwd := &Forwarder{
		Lane: LaneTime, Scheduler: sched, ReadyTopic: "ready.time",
		Producer: &failOnceProducer{}, Now: time.Now,
	}
	n, empty, failed := fwd.drainBurst(ctx, 50)
	if !failed || empty || n != 0 {
		t.Fatalf("n=%d empty=%v failed=%v", n, empty, failed)
	}
}
