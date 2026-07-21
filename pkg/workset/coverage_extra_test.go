package workset

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRenewTouchDeleteConsumer(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	var nilSt *Store
	ok, err := nilSt.Renew(ctx, "j", "c", "f", time.Minute)
	if err != nil || ok {
		t.Fatalf("nil renew ok=%v err=%v", ok, err)
	}
	if err := nilSt.TouchConsumer(ctx, "c", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := nilSt.DeleteConsumer(ctx, "c"); err != nil {
		t.Fatal(err)
	}

	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j-renew", Payload: []byte(`{"job_id":"j-renew"}`), Topic: "jobs",
		Partition: 0, Offset: 1, ConsumerID: "c1", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !res.Won {
		t.Fatalf("claim won=%v err=%v", res.Won, err)
	}
	ok, err = st.Renew(ctx, "j-renew", "c1", res.Fence, 0)
	if err != nil || !ok {
		t.Fatalf("renew ok=%v err=%v", ok, err)
	}
	ok, err = st.Renew(ctx, "j-renew", "other", res.Fence, time.Minute)
	if err != nil || ok {
		t.Fatalf("wrong consumer renew ok=%v err=%v", ok, err)
	}
	if err := st.TouchConsumer(ctx, "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := st.TouchConsumer(ctx, "c1", 0); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists(liveKey("c1")) {
		t.Fatal("live key missing")
	}
	if err := st.DeleteConsumer(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteConsumer(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(liveKey("c1")) {
		t.Fatal("live key should be gone")
	}
}

type stubProd struct{}

func (stubProd) Produce(context.Context, string, string, []byte) error { return nil }

func TestRunReclaimScheduler(t *testing.T) {
	RunReclaimScheduler(context.Background(), nil, stubProd{}, 0, 0, 0, nil)
	st, _ := testStore(t)
	RunReclaimScheduler(context.Background(), st, nil, 0, 0, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	var ticks int32
	RunReclaimScheduler(ctx, st, stubProd{}, time.Millisecond, 0, 0, func() {
		atomic.AddInt32(&ticks, 1)
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&ticks) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt32(&ticks) == 0 {
		t.Fatal("expected reclaim tick")
	}
}
