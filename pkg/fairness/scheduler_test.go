package fairness

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func testScheduler(t *testing.T) (*Scheduler, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := NewScheduler(rdb, Settings{
		Lane: LaneTime, ReadyWindow: 100, GlobalConcurrency: 4,
		LeaseTTL: 120, DefaultWeight: 1.0, WeightedConcurrency: false,
	})
	return s, mr
}

func TestEnqueueWindowCap(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	s.Settings.ReadyWindow = 2

	ok, _ := s.Enqueue(ctx, "t1", []byte(`{"job_id":"j1"}`))
	ok2, _ := s.Enqueue(ctx, "t1", []byte(`{"job_id":"j2"}`))
	ok3, _ := s.Enqueue(ctx, "t1", []byte(`{"job_id":"j3"}`))
	if !ok || !ok2 || ok3 {
		t.Fatalf("enqueue ok=%v %v %v", ok, ok2, ok3)
	}
	depth, _ := s.ReadyDepth(ctx, "t1")
	if depth != 2 {
		t.Fatalf("depth %d", depth)
	}
}

func TestCheckoutForwardCompleteReleasesSlot(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()

	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))

	job, err := s.Checkout(ctx)
	if err != nil || job == nil {
		t.Fatalf("checkout %+v err=%v", job, err)
	}
	stats, _ := s.Stats(ctx)
	if stats.InflightTotal != 1 {
		t.Fatalf("inflight %d", stats.InflightTotal)
	}

	ok, _ := s.ConfirmForward(ctx, job.SlotID)
	if !ok {
		t.Fatal("confirm")
	}
	if err := s.Complete(ctx, job.TenantID, job.SlotID, 1.5); err != nil {
		t.Fatal(err)
	}
	stats, _ = s.Stats(ctx)
	if stats.InflightTotal != 0 {
		t.Fatalf("inflight after complete %d", stats.InflightTotal)
	}
	vt, _ := s.Vtime(ctx, "acme")
	if vt <= 0 {
		t.Fatalf("vtime %f", vt)
	}
}

func TestCheckoutDoesNotAdvanceVtimeUntilComplete(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1"}))
	job, _ := s.Checkout(ctx)
	_, _ = s.ConfirmForward(ctx, job.SlotID)
	vt, _ := s.Vtime(ctx, "acme")
	if vt != 0 {
		t.Fatalf("vtime before complete %f", vt)
	}
}

func TestTwoTenantInterleaveBudget(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		_, _ = s.Enqueue(ctx, "A", mustJSON(t, map[string]interface{}{"job_id": "a" + string(rune('0'+i))}))
		_, _ = s.Enqueue(ctx, "B", mustJSON(t, map[string]interface{}{"job_id": "b" + string(rune('0'+i))}))
	}
	tenants := []string{}
	for i := 0; i < 4; i++ {
		job, err := s.Checkout(ctx)
		if err != nil || job == nil {
			t.Fatalf("checkout %d %+v err=%v", i, job, err)
		}
		tenants = append(tenants, job.TenantID)
		_, _ = s.ConfirmForward(ctx, job.SlotID)
		_ = s.Complete(ctx, job.TenantID, job.SlotID, 0.1)
	}
	counts := map[string]int{}
	for _, tnt := range tenants {
		counts[tnt]++
	}
	if counts["A"] != 2 || counts["B"] != 2 {
		t.Fatalf("first round %+v", counts)
	}
}

func TestClaimSlotExecutionDedup(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	ok1, _ := s.ClaimSlotExecution(ctx, "slot-1")
	ok2, _ := s.ClaimSlotExecution(ctx, "slot-1")
	if !ok1 || ok2 {
		t.Fatalf("claim %v %v", ok1, ok2)
	}
}

func TestThroughputCheckoutAdvancesVtimeAtDispatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := NewScheduler(rdb, Settings{
		Lane: LaneThroughput, ReadyWindow: 100, GlobalConcurrency: 4,
		LeaseTTL: 120, DefaultWeight: 1.0, WeightedConcurrency: false,
	})
	ctx := context.Background()
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))

	job, err := s.Checkout(ctx)
	if err != nil || job == nil {
		t.Fatalf("checkout %+v err=%v", job, err)
	}
	vt, _ := s.Vtime(ctx, "acme")
	if vt <= 0 {
		t.Fatalf("throughput vtime should advance at dispatch, got %f", vt)
	}
	_, _ = s.ConfirmForward(ctx, job.SlotID)
	if err := s.Complete(ctx, job.TenantID, job.SlotID, 0); err != nil {
		t.Fatal(err)
	}
	stats, _ := s.Stats(ctx)
	if stats.InflightTotal != 0 {
		t.Fatalf("inflight after complete %d", stats.InflightTotal)
	}
}

func TestDispatcherTenantFallback(t *testing.T) {
	s, _ := testScheduler(t)
	d := &Dispatcher{Lane: LaneTime, Scheduler: s}
	raw, _ := json.Marshal(map[string]interface{}{"job_id": "j1", "batch_id": "batch-x"})
	out, err := d.Process(context.Background(), raw, protocol.SourceCoords{Topic: "ingest"})
	if err != nil || !out.Enqueued || out.TenantID != "batch-x" {
		t.Fatalf("out %+v err=%v", out, err)
	}
}

func TestForwarderProducesMarkedPayload(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))

	var produced []byte
	fwd := &Forwarder{
		Lane: LaneTime, Scheduler: s, ReadyTopic: "ready.topic",
		Producer: producerFunc(func(_ context.Context, _, _ string, payload []byte) error {
			produced = append([]byte(nil), payload...)
			return nil
		}),
	}
	if !fwd.ForwardOnce(ctx) {
		t.Fatal("expected forward")
	}
	var m map[string]interface{}
	_ = json.Unmarshal(produced, &m)
	if m["_fair_slot"] != true || m["_fair_slot_id"] == "" {
		t.Fatalf("marked %+v", m)
	}
}

type producerFunc func(ctx context.Context, topic, key string, payload []byte) error

func (f producerFunc) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload)
}

// TestListStaleForwardsRecoversOrphanWithNoLeaseRecord is a regression test for the
// job-loss bug: a forwarding-buffer entry whose lease record has already been swept by
// ReclaimExpiredLeases (or was otherwise never/no-longer present) must still be reported
// as reclaimable. Checkout creates the forwarding entry and its lease atomically, and
// every legitimate teardown path clears the forwarding entry no later than the lease, so
// "forwarding entry present, lease entirely absent" can only mean a crashed producer that
// needs recovery — it must never be silently skipped.
func TestListStaleForwardsRecoversOrphanWithNoLeaseRecord(t *testing.T) {
	s, mr := testScheduler(t)
	ctx := context.Background()

	slotID := "slot-orphan-1"
	payload := mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"})
	mr.HSet(forwardingKey(s.Lane), slotID, string(payload))
	mr.HSet(forwardingMetaKey(s.Lane), slotID, "acme")
	// Deliberately no lease record for slotID in leasesKey — simulates
	// ReclaimExpiredLeases having already purged it.

	stale, err := s.ListStaleForwards(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].SlotID != slotID || stale[0].TenantID != "acme" {
		t.Fatalf("expected orphaned entry to be listed as stale, got %+v", stale)
	}
}

// TestReclaimStaleForwardConcurrentDoesNotDuplicateProduce is a regression test for the
// duplicate-delivery race: two forwarder replicas racing to reclaim the same stale slot
// must produce the job at most once.
func TestReclaimStaleForwardConcurrentDoesNotDuplicateProduce(t *testing.T) {
	s, mr := testScheduler(t)
	ctx := context.Background()

	slotID := "slot-orphan-2"
	payload := mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"})
	mr.HSet(forwardingKey(s.Lane), slotID, string(payload))
	mr.HSet(forwardingMetaKey(s.Lane), slotID, "acme")

	entry := StaleForward{SlotID: slotID, TenantID: "acme", Payload: payload}

	var mu sync.Mutex
	var produceCount int
	produce := func(_ []byte, _ string) error {
		mu.Lock()
		produceCount++
		mu.Unlock()
		return nil
	}

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_ = s.ReclaimStaleForward(ctx, entry, produce)
		}()
	}
	wg.Wait()

	if produceCount != 1 {
		t.Fatalf("expected exactly 1 produce across %d concurrent reclaims, got %d", n, produceCount)
	}
}

func mustJSON(t *testing.T, v map[string]interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
