package cron

import (
	"testing"
	"time"
)

func at(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t.UTC()
}

func newSched(policy MisfirePolicy, next string) Schedule {
	return Schedule{
		ID: 1, Name: "test", CronExpr: "0 * * * *", Timezone: "UTC",
		JobType: "hello.go", Enabled: true, Misfire: policy, NextRunAt: at(next),
	}
}

func TestPlanFires_OnTime(t *testing.T) {
	expr := mustParse(t, "0 * * * *")
	now := at("2026-07-18T10:00:20Z") // 20s after the 10:00 fire — within grace
	for _, p := range []MisfirePolicy{MisfireFireOnce, MisfireSkip, MisfireBackfill} {
		sc := newSched(p, "2026-07-18T10:00:00Z")
		plan := PlanFires(sc, expr, time.UTC, now, 60*time.Second, 1000)
		if len(plan.Fires) != 1 || !plan.Fires[0].Equal(at("2026-07-18T10:00:00Z")) {
			t.Errorf("policy=%s on-time: got fires=%v", p, plan.Fires)
		}
		if !plan.NewNext.Equal(at("2026-07-18T11:00:00Z")) {
			t.Errorf("policy=%s on-time: newNext=%s", p, plan.NewNext.Format(time.RFC3339))
		}
	}
}

func TestPlanFires_MissedGap(t *testing.T) {
	expr := mustParse(t, "0 * * * *")
	// Was due at 07:00; scheduler comes back at 10:05 — missed 07,08,09,10.
	now := at("2026-07-18T10:05:00Z")
	grace := 60 * time.Second

	// fire_once: only the oldest missed instant, then skip ahead past now.
	fo := PlanFires(newSched(MisfireFireOnce, "2026-07-18T07:00:00Z"), expr, time.UTC, now, grace, 1000)
	if len(fo.Fires) != 1 || !fo.Fires[0].Equal(at("2026-07-18T07:00:00Z")) {
		t.Errorf("fire_once fires = %v", fo.Fires)
	}
	if !fo.NewNext.Equal(at("2026-07-18T11:00:00Z")) {
		t.Errorf("fire_once newNext = %s", fo.NewNext.Format(time.RFC3339))
	}

	// skip: nothing in the gap, jump straight to next future instant.
	sk := PlanFires(newSched(MisfireSkip, "2026-07-18T07:00:00Z"), expr, time.UTC, now, grace, 1000)
	if len(sk.Fires) != 0 {
		t.Errorf("skip fires = %v, want none", sk.Fires)
	}
	if !sk.NewNext.Equal(at("2026-07-18T11:00:00Z")) {
		t.Errorf("skip newNext = %s", sk.NewNext.Format(time.RFC3339))
	}

	// backfill: every missed instant 07,08,09,10.
	bf := PlanFires(newSched(MisfireBackfill, "2026-07-18T07:00:00Z"), expr, time.UTC, now, grace, 1000)
	want := []string{"2026-07-18T07:00:00Z", "2026-07-18T08:00:00Z", "2026-07-18T09:00:00Z", "2026-07-18T10:00:00Z"}
	if len(bf.Fires) != len(want) {
		t.Fatalf("backfill fires = %v, want %d", bf.Fires, len(want))
	}
	for i, w := range want {
		if !bf.Fires[i].Equal(at(w)) {
			t.Errorf("backfill fire[%d] = %s, want %s", i, bf.Fires[i].Format(time.RFC3339), w)
		}
	}
	if !bf.NewNext.Equal(at("2026-07-18T11:00:00Z")) {
		t.Errorf("backfill newNext = %s", bf.NewNext.Format(time.RFC3339))
	}
}

func TestPlanFires_BackfillCap(t *testing.T) {
	expr := mustParse(t, "0 * * * *")
	now := at("2026-07-18T10:00:00Z")
	bf := PlanFires(newSched(MisfireBackfill, "2026-07-18T00:00:00Z"), expr, time.UTC, now, 60*time.Second, 3)
	if len(bf.Fires) != 3 {
		t.Fatalf("capped backfill fires = %d, want 3", len(bf.Fires))
	}
	// NewNext must not skip past uncaught instants: it should be the 4th (03:00),
	// still <= now, so the remainder drains on the next tick.
	if !bf.NewNext.Equal(at("2026-07-18T03:00:00Z")) {
		t.Errorf("capped backfill newNext = %s, want 03:00", bf.NewNext.Format(time.RFC3339))
	}
}

func TestJobIDForFireDeterministic(t *testing.T) {
	f := at("2026-07-18T10:00:00Z")
	if JobIDForFire(7, f) != JobIDForFire(7, f) {
		t.Fatal("JobIDForFire not deterministic")
	}
	if JobIDForFire(7, f) == JobIDForFire(8, f) {
		t.Fatal("JobIDForFire collides across schedules")
	}
}
