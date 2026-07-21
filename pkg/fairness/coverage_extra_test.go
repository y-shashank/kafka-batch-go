package fairness

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestDefaultSettingsAndEffectiveHelpers(t *testing.T) {
	s := DefaultSettings(LaneTime)
	if s.Lane != LaneTime || s.ReadyWindow != 500 {
		t.Fatalf("%+v", s)
	}
	s.VtimeIdleResetDebounce = 0
	if s.EffectiveVtimeIdleResetDebounce() != 15*time.Second {
		t.Fatal("debounce floor")
	}
	s.VtimeIdleResetDebounce = 30 * time.Second
	if s.EffectiveVtimeIdleResetDebounce() != 30*time.Second {
		t.Fatal("debounce")
	}
	s.LeaseTTL = 10
	if s.EffectiveLeaseTTL() != LeaseTTLFloor {
		t.Fatal("lease floor")
	}
	s.LeaseTTL = 100
	if s.EffectiveLeaseTTL() != 100 {
		t.Fatal("lease")
	}
	s.GlobalConcurrency = 1
	if s.fetchN() != 60 {
		t.Fatal("fetchN floor")
	}
	s.GlobalConcurrency = 30
	if s.fetchN() != 90 {
		t.Fatal("fetchN")
	}
	s.SlotDedupTTL = 0
	s.LeaseTTL = 100
	if s.slotDedupTTL() != 100 {
		t.Fatal("dedup from lease")
	}
	s.SlotDedupTTL = 10
	if s.slotDedupTTL() != 60 {
		t.Fatal("dedup floor")
	}
	if ValidateLane(Lane("x")) == nil {
		t.Fatal("bad lane")
	}
	if ValidateLane(LaneThroughput) != nil {
		t.Fatal("throughput ok")
	}
	if reclaimLockKey(LaneTime) == "" || ReadyKey(LaneTime, "t") == "" {
		t.Fatal("keys")
	}
}

func TestCoordinatorEnsure(t *testing.T) {
	var n int32
	c := NewCoordinator(func(Lane) { atomic.AddInt32(&n, 1) })
	c.Ensure(LaneTime)
	c.Ensure(LaneTime)
	if atomic.LoadInt32(&n) != 1 {
		t.Fatalf("n=%d", n)
	}
	start := c.OnStart(LaneThroughput)
	start(LaneThroughput)
	start(LaneThroughput)
	if atomic.LoadInt32(&n) != 2 {
		t.Fatalf("n=%d", n)
	}
	NewCoordinator(nil).Ensure(LaneTime)
}

func TestSchedulerLeaseAndResetHelpers(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	if err := s.RenewLease(ctx, "t", ""); err != nil {
		t.Fatal(err)
	}
	active, _, err := s.SlotLeaseActive(ctx, "")
	if err != nil || active {
		t.Fatalf("empty slot active=%v err=%v", active, err)
	}
	if err := s.ClearSlotExecution(ctx, ""); err != nil {
		t.Fatal(err)
	}
	var nilS *Scheduler
	if err := nilS.ClearSlotExecution(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	ok, err := s.ClaimSlotExecution(ctx, "")
	if err != nil || !ok {
		t.Fatalf("empty claim ok=%v err=%v", ok, err)
	}
	ok, err = s.ClaimSlotExecution(ctx, "slot-1")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.ClearSlotExecution(ctx, "slot-1"); err != nil {
		t.Fatal(err)
	}
	n, err := s.ReclaimExpiredLeases(ctx)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// Second reclaim loses lock within TTL
	n, err = s.ReclaimExpiredLeases(ctx)
	if err != nil || n != 0 {
		t.Fatalf("locked reclaim n=%d err=%v", n, err)
	}
	ring, err := s.RingSize(ctx)
	if err != nil || ring != 0 {
		t.Fatalf("ring=%d err=%v", ring, err)
	}
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Enqueue(ctx, "acme", []byte(`{"job_id":"j1","tenant_id":"acme"}`))
	job, err := s.Checkout(ctx)
	if err != nil || job == nil {
		t.Fatalf("checkout %+v err=%v", job, err)
	}
	if err := s.RenewLease(ctx, job.TenantID, job.SlotID); err != nil {
		t.Fatal(err)
	}
	active, exp, err := s.SlotLeaseActive(ctx, job.SlotID)
	if err != nil || !active || exp <= 0 {
		t.Fatalf("active=%v exp=%v err=%v", active, exp, err)
	}
	_, _ = s.ConfirmForward(ctx, job.SlotID)
	_ = s.Complete(ctx, job.TenantID, job.SlotID, 1)
	if err := s.Reset(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTenantFromPayloadAndMessage(t *testing.T) {
	if tenantFromPayload([]byte(`{`)) != "" {
		t.Fatal("bad json")
	}
	if tenantFromPayload([]byte(`{"tenant_id":"t"}`)) != "t" {
		t.Fatal("tenant")
	}
	if tenantFromPayload([]byte(`{"batch_id":"b"}`)) != "b" {
		t.Fatal("batch")
	}
	if tenantFromPayload([]byte(`{"job_id":"j"}`)) != "j" {
		t.Fatal("job")
	}
	if TenantFromMessage(map[string]interface{}{}) != "default" {
		t.Fatal("default")
	}
	if TenantFromMessage(map[string]interface{}{"job_id": "j"}) != "j" {
		t.Fatal("job msg")
	}
}

func TestComputeActiveViewIngestLag(t *testing.T) {
	s, _ := testScheduler(t)
	s.Settings.ActiveCountSource = "ingest_lag"
	s.Settings.IngestLag = lagStub{n: 3}
	s.Settings.WeightedConcurrency = true
	s.Settings.DefaultWeight = 2
	view := s.computeActiveView(context.Background())
	if view.count != 3 || view.sumWeight != 6 {
		t.Fatalf("%+v", view)
	}
	s.Settings.IngestLag = lagStub{err: context.Canceled}
	view = s.computeActiveView(context.Background())
	if view.count != 0 {
		t.Fatalf("fallback %+v", view)
	}
}

type lagStub struct {
	n   int
	err error
}

func (l lagStub) IngestActiveCount(context.Context, string, string) (int, error) {
	return l.n, l.err
}

func TestForwarderRunAndMaybeReclaim(t *testing.T) {
	s, _ := testScheduler(t)
	ctx, cancel := context.WithCancel(context.Background())
	var ticks int32
	f := &Forwarder{
		Lane:      LaneTime,
		Scheduler: s,
		IdleSleep: time.Millisecond,
		Burst:     1,
		ResolveReadyTopic: func([]byte) (string, error) {
			return "ready", nil
		},
		Producer: produceStub{},
		RecordActivity: func() {
			atomic.AddInt32(&ticks, 1)
		},
	}
	done := make(chan struct{})
	go func() {
		f.Run(ctx)
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop")
	}
	if atomic.LoadInt32(&ticks) == 0 {
		t.Fatal("expected activity")
	}
	f.lastLeaseReclaim = time.Time{}
	f.lastForwardReclaim = time.Time{}
	f.maybeReclaim(context.Background())
}

type produceStub struct{}

func (produceStub) Produce(context.Context, string, string, []byte) error { return nil }

func TestDispatcherOnStartFwdAndNilScheduler(t *testing.T) {
	var started Lane
	d := &Dispatcher{
		Lane:       LaneTime,
		OnStartFwd: func(l Lane) { started = l },
	}
	raw, _ := json.Marshal(map[string]interface{}{"job_id": "j1", "tenant_id": "t1"})
	out, err := d.Process(context.Background(), raw, protocol.SourceCoords{})
	if err != nil || !out.CommitOffset || out.Enqueued {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if started != LaneTime {
		t.Fatalf("started=%q", started)
	}
	out, err = d.Process(context.Background(), []byte(`{`), protocol.SourceCoords{})
	if err != nil || !out.CommitOffset {
		t.Fatalf("bad json out=%+v err=%v", out, err)
	}
}
