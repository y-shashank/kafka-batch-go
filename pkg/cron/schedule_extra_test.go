package cron

import (
	"testing"
	"time"
)

func TestMisfirePolicyValid(t *testing.T) {
	for _, p := range []MisfirePolicy{MisfireSkip, MisfireFireOnce, MisfireBackfill} {
		if !p.Valid() {
			t.Errorf("%q should be valid", p)
		}
	}
	if MisfirePolicy("nope").Valid() {
		t.Fatal("unknown policy should be invalid")
	}
	if MisfirePolicy("").Valid() {
		t.Fatal("empty policy should be invalid")
	}
}

func TestScheduleLocation(t *testing.T) {
	utc, err := Schedule{Timezone: ""}.Location()
	if err != nil || utc != time.UTC {
		t.Fatalf("empty tz: loc=%v err=%v", utc, err)
	}
	utc2, err := Schedule{Timezone: "UTC"}.Location()
	if err != nil || utc2 != time.UTC {
		t.Fatalf("UTC: loc=%v err=%v", utc2, err)
	}
	ny, err := Schedule{Name: "ny", Timezone: "America/New_York"}.Location()
	if err != nil {
		t.Fatalf("NY: %v", err)
	}
	if ny.String() != "America/New_York" {
		t.Fatalf("NY name=%q", ny.String())
	}
	if _, err := (Schedule{Name: "bad", Timezone: "Not/AZone"}).Location(); err == nil {
		t.Fatal("expected bad timezone error")
	}
}

func TestPlanFires_MaxBackfillFloor(t *testing.T) {
	expr := mustParse(t, "0 * * * *")
	now := at("2026-07-18T10:00:00Z")
	p := PlanFires(newSched(MisfireBackfill, "2026-07-18T08:00:00Z"), expr, time.UTC, now, 60*time.Second, 0)
	if len(p.Fires) != 1 {
		t.Fatalf("maxBackfill<1 should floor to 1 fire, got %d", len(p.Fires))
	}
}

func TestPlanFires_BackfillExprExhausted(t *testing.T) {
	// Feb 30 never fires again after the seed instant — Next returns ok=false.
	expr := mustParse(t, "0 0 30 2 *")
	sc := Schedule{
		ID: 1, Name: "feb30", CronExpr: "0 0 30 2 *", Timezone: "UTC",
		Misfire: MisfireBackfill, NextRunAt: at("2026-02-01T00:00:00Z"),
	}
	now := at("2026-03-01T00:00:00Z")
	p := PlanFires(sc, expr, time.UTC, now, 60*time.Second, 100)
	if len(p.Fires) == 0 {
		t.Fatal("expected at least the seed fire")
	}
	// NewNext falls back to last fire + 1m when Next is exhausted.
	want := p.Fires[len(p.Fires)-1].Add(time.Minute)
	if !p.NewNext.Equal(want) {
		t.Errorf("NewNext=%s want %s", p.NewNext.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestPlanFires_UnknownPolicyFallsBackToFireOnce(t *testing.T) {
	expr := mustParse(t, "0 * * * *")
	sc := newSched(MisfirePolicy("weird"), "2026-07-18T10:00:00Z")
	now := at("2026-07-18T10:05:00Z")
	p := PlanFires(sc, expr, time.UTC, now, 60*time.Second, 1000)
	if len(p.Fires) != 1 || !p.Fires[0].Equal(at("2026-07-18T10:00:00Z")) {
		t.Errorf("unknown policy fires=%v", p.Fires)
	}
	if !p.NewNext.Equal(at("2026-07-18T11:00:00Z")) {
		t.Errorf("newNext=%s", p.NewNext.Format(time.RFC3339))
	}
}

func TestAdvancePast_WhenNextExhausted(t *testing.T) {
	expr := mustParse(t, "0 0 30 2 *")
	from := at("2026-01-01T00:00:00Z")
	now := at("2026-07-01T00:00:00Z")
	got := advancePast(expr, time.UTC, from, now)
	if !got.Equal(from.Add(time.Minute)) {
		t.Errorf("advancePast exhausted = %s, want from+1m", got.Format(time.RFC3339))
	}
}
