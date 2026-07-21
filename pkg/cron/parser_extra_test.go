package cron

import (
	"testing"
	"time"
)

func TestParseRawAndMacros(t *testing.T) {
	for _, macro := range []string{"@midnight", "@weekly", "@monthly", "@yearly", "@annually"} {
		e, err := Parse(macro)
		if err != nil {
			t.Fatalf("Parse(%q): %v", macro, err)
		}
		if e.Raw() != macro {
			t.Fatalf("Raw()=%q want %q", e.Raw(), macro)
		}
	}
}

func TestParseNamedMonthAndDow(t *testing.T) {
	e := mustParse(t, "0 0 1 jan mon")
	after, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z") // Thursday
	got, ok := e.Next(after, time.UTC)
	if !ok {
		t.Fatal("expected match")
	}
	// Next Monday in January is 2026-01-05 (OR: 1st already passed).
	want, _ := time.Parse(time.RFC3339, "2026-01-05T00:00:00Z")
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestParseDowSevenIsSunday(t *testing.T) {
	e := mustParse(t, "0 0 * * 7")
	// 2026-07-18 is Saturday; next Sunday is 2026-07-19.
	after, _ := time.Parse(time.RFC3339, "2026-07-18T00:00:00Z")
	got, ok := e.Next(after, time.UTC)
	if !ok {
		t.Fatal("expected match")
	}
	want, _ := time.Parse(time.RFC3339, "2026-07-19T00:00:00Z")
	if !got.Equal(want) {
		t.Fatalf("got %s want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestParseQuestionMarkAndLists(t *testing.T) {
	e := mustParse(t, "0,30 1-2 * * ?")
	after, _ := time.Parse(time.RFC3339, "2026-07-18T00:00:00Z")
	got, ok := e.Next(after, time.UTC)
	if !ok || !got.Equal(mustTime(t, "2026-07-18T01:00:00Z")) {
		t.Fatalf("got %v ok=%v", got, ok)
	}
}

func TestNextNilLocationDefaultsUTC(t *testing.T) {
	e := mustParse(t, "0 * * * *")
	after, _ := time.Parse(time.RFC3339, "2026-07-18T10:02:00Z")
	got, ok := e.Next(after, nil)
	if !ok || !got.Equal(mustTime(t, "2026-07-18T11:00:00Z")) {
		t.Fatalf("got %v ok=%v", got, ok)
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tt
}
