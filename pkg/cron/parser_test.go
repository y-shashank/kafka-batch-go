package cron

import (
	"testing"
	"time"
)

func mustParse(t *testing.T, expr string) Expr {
	t.Helper()
	e, err := Parse(expr)
	if err != nil {
		t.Fatalf("Parse(%q): %v", expr, err)
	}
	return e
}

func TestParseErrors(t *testing.T) {
	bad := []string{"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *",
		"* * 0 * *", "* * * 13 *", "*/0 * * * *", "a * * * *", "5-1 * * * *"}
	for _, b := range bad {
		if _, err := Parse(b); err == nil {
			t.Errorf("Parse(%q) expected error, got nil", b)
		}
	}
}

func TestNextBasic(t *testing.T) {
	utc := time.UTC
	cases := []struct {
		expr  string
		after string
		want  string
	}{
		{"*/15 * * * *", "2026-07-18T10:02:00Z", "2026-07-18T10:15:00Z"},
		{"0 * * * *", "2026-07-18T10:02:00Z", "2026-07-18T11:00:00Z"},
		{"0 2 * * *", "2026-07-18T10:00:00Z", "2026-07-19T02:00:00Z"},
		{"30 9 * * 1", "2026-07-18T00:00:00Z", "2026-07-20T09:30:00Z"}, // next Monday
		{"0 0 1 * *", "2026-07-18T00:00:00Z", "2026-08-01T00:00:00Z"},
		{"@daily", "2026-07-18T10:00:00Z", "2026-07-19T00:00:00Z"},
		{"@hourly", "2026-07-18T10:30:00Z", "2026-07-18T11:00:00Z"},
	}
	for _, c := range cases {
		e := mustParse(t, c.expr)
		after, _ := time.Parse(time.RFC3339, c.after)
		got, ok := e.Next(after, utc)
		if !ok {
			t.Errorf("%s Next(%s): no match", c.expr, c.after)
			continue
		}
		want, _ := time.Parse(time.RFC3339, c.want)
		if !got.Equal(want) {
			t.Errorf("%s Next(%s) = %s, want %s", c.expr, c.after, got.Format(time.RFC3339), c.want)
		}
	}
}

func TestNextDOMorDOW(t *testing.T) {
	// Both dom and dow restricted ⇒ OR semantics: fires on the 1st OR on Mondays.
	e := mustParse(t, "0 0 1 * 1")
	after, _ := time.Parse(time.RFC3339, "2026-07-18T00:00:00Z") // Sat
	got, _ := e.Next(after, time.UTC)
	// Next Monday is 2026-07-20; the 1st is 2026-08-01. Monday comes first.
	want, _ := time.Parse(time.RFC3339, "2026-07-20T00:00:00Z")
	if !got.Equal(want) {
		t.Errorf("Next = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestNextTimezone(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	// 02:00 local. From 2026-07-18T00:00Z (= 2026-07-17 20:00 EDT), next 02:00 EDT
	// is 2026-07-18 02:00 EDT = 06:00Z.
	e := mustParse(t, "0 2 * * *")
	after, _ := time.Parse(time.RFC3339, "2026-07-18T00:00:00Z")
	got, _ := e.Next(after, ny)
	want, _ := time.Parse(time.RFC3339, "2026-07-18T06:00:00Z")
	if !got.Equal(want) {
		t.Errorf("Next(NY) = %s, want %s", got.UTC().Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestNextNoImpossibleInfiniteLoop(t *testing.T) {
	// Feb 30 never exists ⇒ ok=false within horizon, no hang.
	e := mustParse(t, "0 0 30 2 *")
	after, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	if _, ok := e.Next(after, time.UTC); ok {
		t.Errorf("expected no match for Feb 30")
	}
}
