package cron

import (
	"testing"
	"time"
)

// staleThreshold must be StaleFactor × the schedule's own interval.
func TestStaleThreshold(t *testing.T) {
	tk := &Ticker{StaleFactor: 2.0}
	sc := Schedule{
		Name: "x", CronExpr: "*/5 * * * *", Timezone: "UTC",
		NextRunAt: at("2026-07-18T10:00:00Z"),
	}
	got, ok := tk.staleThreshold(sc)
	if !ok {
		t.Fatal("staleThreshold not ok")
	}
	if got != 10*time.Minute { // 2 × 5min
		t.Errorf("threshold = %s, want 10m", got)
	}

	// Hourly schedule → 2h threshold.
	sc.CronExpr = "0 * * * *"
	got, _ = tk.staleThreshold(sc)
	if got != 2*time.Hour {
		t.Errorf("hourly threshold = %s, want 2h", got)
	}

	// Bad cron → not ok, never flags stale.
	sc.CronExpr = "nonsense"
	if _, ok := tk.staleThreshold(sc); ok {
		t.Error("bad cron should not yield a threshold")
	}
}

// TestHeartbeatIntegration exercises the stale sweep against a real MySQL.
// Gated on KBATCH_TEST_MYSQL (a go-sql DSN); skipped otherwise.
func TestHeartbeatIntegration(t *testing.T) {
	dsn := getenv("KBATCH_TEST_MYSQL")
	if dsn == "" {
		t.Skip("set KBATCH_TEST_MYSQL to run")
	}
	ctx := testCtx()
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("schema: %v", err)
	}
	// Two schedules: one fresh, one long-stale.
	freshID, err := store.Upsert(ctx, Schedule{Name: "hb-fresh", CronExpr: "*/5 * * * *", JobType: "hello.go", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	staleID, err := store.Upsert(ctx, Schedule{Name: "hb-stale", CronExpr: "*/5 * * * *", JobType: "hello.go", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = store.Delete(ctx, "hb-fresh")
		_ = store.Delete(ctx, "hb-stale")
	}()
	now := time.Now().UTC()
	// hb-fresh last fired 1 min ago (well within 10m threshold); hb-stale 30 min ago.
	setLastFire(t, store, freshID, now.Add(-1*time.Minute))
	setLastFire(t, store, staleID, now.Add(-30*time.Minute))

	events := captureEvents()
	defer events.stop()

	tk := &Ticker{Store: store, StaleFactor: 2.0}
	if err := tk.heartbeat(ctx, now); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	if events.count("cron.stale", "schedule", "hb-stale") != 1 {
		t.Errorf("expected cron.stale for hb-stale, got events: %v", events.names())
	}
	if events.count("cron.stale", "schedule", "hb-fresh") != 0 {
		t.Errorf("hb-fresh should not be stale")
	}
	if events.count("cron.heartbeat", "", "") != 1 {
		t.Errorf("expected one cron.heartbeat")
	}
}
