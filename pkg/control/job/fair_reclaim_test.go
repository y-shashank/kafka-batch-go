package job

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
)

func TestWithFairSlotClearsDedupOnReclaim(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := fairness.NewScheduler(rdb, fairness.Settings{
		Lane: fairness.LaneTime, GlobalConcurrency: 10, ReadyWindow: 10,
		LeaseTTL: 60, DefaultWeight: 1, WeightedConcurrency: true,
	})
	ctx := context.Background()
	ok, err := sched.ClaimSlotExecution(ctx, "slot-r1")
	if err != nil || !ok {
		t.Fatalf("seed claim ok=%v err=%v", ok, err)
	}
	ok, _ = sched.ClaimSlotExecution(ctx, "slot-r1")
	if ok {
		t.Fatal("expected dedup to block second claim")
	}

	p := &Processor{FairTime: sched, Now: time.Now}
	raw := []byte(`{"_fair_slot":true,"_fair_slot_id":"slot-r1","_fair_type":"time","_reclaim":true,"tenant_id":"t1"}`)
	ran := false
	err = p.withFairSlot(ctx, raw, func() error {
		ran = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("expected reclaim to clear dedup and run perform")
	}
}