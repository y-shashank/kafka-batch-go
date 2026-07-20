package fairness

import (
	"context"
	"testing"
	"time"
)

// buildVtime enqueues, checks out and completes one job so the tenant accrues a
// non-zero virtual time, then returns that vtime.
func buildVtime(t *testing.T, s *Scheduler, tenant string) float64 {
	t.Helper()
	ctx := context.Background()
	_, _ = s.Enqueue(ctx, tenant, mustJSON(t, map[string]interface{}{"job_id": "j-" + tenant, "tenant_id": tenant}))
	job, err := s.Checkout(ctx)
	if err != nil || job == nil {
		t.Fatalf("checkout %+v err=%v", job, err)
	}
	if _, err := s.ConfirmForward(ctx, job.SlotID); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, job.TenantID, job.SlotID, 2.0); err != nil {
		t.Fatal(err)
	}
	vt, _ := s.Vtime(ctx, tenant)
	if vt <= 0 {
		t.Fatalf("expected vtime>0 after complete, got %f", vt)
	}
	return vt
}

func TestResetVtimeIfQuiescentClearsWhenIdleKeepsWeights(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	if err := s.SetWeight(ctx, "acme", 3.5); err != nil {
		t.Fatal(err)
	}
	buildVtime(t, s, "acme")

	reset, err := s.ResetVtimeIfQuiescent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reset {
		t.Fatal("expected reset when lane fully idle")
	}
	if vt, _ := s.Vtime(ctx, "acme"); vt != 0 {
		t.Fatalf("vtime not cleared: %f", vt)
	}
	if w := s.WeightFor(ctx, "acme"); w != 3.5 {
		t.Fatalf("weight not preserved: %f", w)
	}
}

func TestResetVtimeIfQuiescentSkipsWithActiveRing(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	// Leave a job enqueued so the tenant sits in the ring (active backlog).
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))

	reset, err := s.ResetVtimeIfQuiescent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reset {
		t.Fatal("must not reset while a tenant is in the ring")
	}
}

func TestResetVtimeIfQuiescentSkipsWithLiveLease(t *testing.T) {
	s, _ := testScheduler(t)
	ctx := context.Background()
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))
	job, err := s.Checkout(ctx)
	if err != nil || job == nil {
		t.Fatalf("checkout %+v err=%v", job, err)
	}
	// Job is checked out but not confirmed/completed: a live lease + forwarding
	// entry exist, so the lane is NOT quiescent.
	reset, err := s.ResetVtimeIfQuiescent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reset {
		t.Fatal("must not reset while a lease/forwarding entry is in flight")
	}
}

func TestForwarderIdleResetFiresAfterDebounce(t *testing.T) {
	s, _ := testScheduler(t)
	s.Settings.ResetVtimeWhenIdle = true
	s.Settings.VtimeIdleResetDebounce = 10 * time.Millisecond
	ctx := context.Background()
	if err := s.SetWeight(ctx, "acme", 2.0); err != nil {
		t.Fatal(err)
	}
	buildVtime(t, s, "acme")

	fwd := &Forwarder{Lane: LaneTime, Scheduler: s}

	// First observation arms the debounce; nothing reset yet.
	fwd.maybeResetVtimeIdle(ctx)
	if fwd.quiescentSince.IsZero() {
		t.Fatal("expected debounce to be armed on first idle observation")
	}
	if vt, _ := s.Vtime(ctx, "acme"); vt == 0 {
		t.Fatal("vtime reset before debounce elapsed")
	}

	// Force the debounce window to have elapsed and allow the next idle check.
	fwd.quiescentSince = time.Now().Add(-time.Minute)
	fwd.lastIdleCheck = time.Now().Add(-time.Minute)
	fwd.maybeResetVtimeIdle(ctx)

	if vt, _ := s.Vtime(ctx, "acme"); vt != 0 {
		t.Fatalf("expected idle reset to clear vtime, got %f", vt)
	}
	if w := s.WeightFor(ctx, "acme"); w != 2.0 {
		t.Fatalf("weight not preserved across idle reset: %f", w)
	}
}

func TestForwarderIdleResetRearmsAfterActivity(t *testing.T) {
	s, _ := testScheduler(t)
	s.Settings.ResetVtimeWhenIdle = true
	s.Settings.VtimeIdleResetDebounce = 10 * time.Millisecond
	ctx := context.Background()

	fwd := &Forwarder{Lane: LaneTime, Scheduler: s}
	// Idle first: arms debounce.
	fwd.maybeResetVtimeIdle(ctx)
	if fwd.quiescentSince.IsZero() {
		t.Fatal("expected debounce armed")
	}

	// Activity appears (tenant enters the ring). Next check must re-arm (clear).
	_, _ = s.Enqueue(ctx, "acme", mustJSON(t, map[string]interface{}{"job_id": "j1", "tenant_id": "acme"}))
	fwd.lastIdleCheck = time.Now().Add(-time.Minute)
	fwd.maybeResetVtimeIdle(ctx)
	if !fwd.quiescentSince.IsZero() {
		t.Fatal("expected debounce to re-arm when activity observed")
	}
	if fwd.vtimeResetDone {
		t.Fatal("reset-done flag should clear on activity")
	}
}

func TestForwarderIdleResetDisabled(t *testing.T) {
	s, _ := testScheduler(t)
	s.Settings.ResetVtimeWhenIdle = false
	ctx := context.Background()
	vt := buildVtime(t, s, "acme")

	fwd := &Forwarder{Lane: LaneTime, Scheduler: s}
	fwd.lastIdleCheck = time.Now().Add(-time.Minute)
	fwd.quiescentSince = time.Now().Add(-time.Minute)
	fwd.maybeResetVtimeIdle(ctx)

	if got, _ := s.Vtime(ctx, "acme"); got != vt {
		t.Fatalf("vtime must be untouched when disabled: got %f want %f", got, vt)
	}
}
