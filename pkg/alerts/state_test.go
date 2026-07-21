package alerts

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStateOpenClaimNotifyBaselineDLTCron(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewState(rdb)
	ctx := context.Background()

	if !st.TryLock(ctx, 1) { // bumps to min 2
		t.Fatal("lock")
	}
	if st.TryLock(ctx, 5) {
		t.Fatal("second lock")
	}
	mr.FastForward(3 * time.Second)
	if !st.TryLock(ctx, 5) {
		t.Fatal("lock after expiry")
	}

	inc := map[string]interface{}{"fingerprint": "fp1", "summary": "a"}
	if !st.ClaimOpen(ctx, "fp1", inc) {
		t.Fatal("claim open")
	}
	if st.ClaimOpen(ctx, "fp1", inc) {
		t.Fatal("duplicate claim")
	}
	st.TouchOpen(ctx, "fp1", "updated")
	open := st.GetOpen(ctx, "fp1")
	if open["summary"] != "updated" {
		t.Fatalf("%v", open)
	}
	alls := st.OpenAlerts(ctx)
	if len(alls) != 1 {
		t.Fatalf("%d", len(alls))
	}
	mr.HSet(openKey, "bad", "not-json")
	if len(st.OpenAlerts(ctx)) != 1 {
		t.Fatal("skip bad json")
	}

	if !st.ClaimNotify(ctx, "fp1", "fired", 10) { // floor 60
		t.Fatal("notify claim")
	}
	if st.ClaimNotify(ctx, "fp1", "fired", 60) {
		t.Fatal("notify dedupe")
	}

	st.IncrBreach(ctx, "fp1")
	st.IncrBreach(ctx, "fp1")
	if st.BreachCount(ctx, "fp1") != 2 {
		t.Fatal("breach")
	}
	st.ResetBreach(ctx, "fp1")
	st.IncrHealthy(ctx, "fp1")
	if st.HealthyCount(ctx, "fp1") != 1 || st.BreachCount(ctx, "fp1") != 0 {
		t.Fatal("healthy")
	}
	st.ResetHealthy(ctx, "fp1")

	base := map[string]map[string]interface{}{"g|t": {"lag": float64(1), "committed": float64(2)}}
	st.SaveBaseline(ctx, base)
	got := st.LoadBaseline(ctx)
	if got["g|t"]["lag"] != float64(1) {
		t.Fatalf("%v", got)
	}
	if len(st.LoadBaseline(ctx)) == 0 {
		// ok still loaded
	}
	_ = rdb.Set(ctx, baselineKey, "nope", 0).Err()
	if len(st.LoadBaseline(ctx)) != 0 {
		t.Fatal("bad baseline")
	}

	st.IncrDLT(ctx)
	st.IncrDLT(ctx)
	if st.DLTCountLastMinute(ctx) != 2 {
		t.Fatalf("dlt=%d", st.DLTCountLastMinute(ctx))
	}

	st.MarkCronStale(ctx, "nightly", "Job", 30)
	entries := st.CronStaleEntries(ctx)
	if len(entries) != 1 || entries[0]["schedule"] != "nightly" {
		t.Fatalf("%v", entries)
	}

	st.SaveLast(ctx, map[string]interface{}{"ok": true})
	if !st.ClearOpen(ctx, "fp1") || st.ClearOpen(ctx, "fp1") {
		t.Fatal("clear")
	}
	st.TouchOpen(ctx, "missing", "x") // no-op
}
