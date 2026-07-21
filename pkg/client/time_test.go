package client

import (
	"testing"
	"time"
)

func TestClampRunAt(t *testing.T) {
	horizon := 24 * time.Hour
	now := time.Now().UTC()

	past := now.Add(-time.Hour)
	if got := clampRunAt(past, horizon); got.Before(now.Add(-time.Second)) {
		t.Fatalf("past clamped to now, got %v", got)
	}

	future := now.Add(48 * time.Hour)
	if got := clampRunAt(future, horizon); got.After(now.Add(horizon).Add(time.Second)) {
		t.Fatalf("future clamped to horizon, got %v", got)
	}

	if got := clampRunAt(30*time.Minute, horizon); got.Before(now) || got.After(now.Add(horizon)) {
		t.Fatalf("duration clamp %+v", got)
	}

	if got := clampRunAt(now.Unix(), horizon); got.Sub(now) > time.Second {
		t.Fatalf("unix int clamp %+v", got)
	}
}

func TestClampRunAtInputKinds(t *testing.T) {
	horizon := 2 * time.Hour
	now := time.Now().UTC()
	mid := now.Add(30 * time.Minute)

	ptr := mid
	var nilPtr *time.Time

	tests := []struct {
		name string
		in   interface{}
		check func(t *testing.T, got time.Time)
	}{
		{
			name: "time.Time in window",
			in:   mid,
			check: func(t *testing.T, got time.Time) {
				if got.Sub(mid) > time.Second || mid.Sub(got) > time.Second {
					t.Fatalf("got=%v want~%v", got, mid)
				}
			},
		},
		{
			name: "*time.Time",
			in:   &ptr,
			check: func(t *testing.T, got time.Time) {
				if got.Sub(mid) > time.Second {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "nil *time.Time uses now",
			in:   nilPtr,
			check: func(t *testing.T, got time.Time) {
				if got.Before(now.Add(-time.Second)) || got.After(now.Add(time.Second)) {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "float64 unix",
			in:   float64(mid.Unix()),
			check: func(t *testing.T, got time.Time) {
				if got.Sub(mid.Truncate(time.Second)) > time.Second {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "int unix",
			in:   int(mid.Unix()),
			check: func(t *testing.T, got time.Time) {
				if got.Unix() != mid.Unix() {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "int64 unix",
			in:   mid.Unix(),
			check: func(t *testing.T, got time.Time) {
				if got.Unix() != mid.Unix() {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "rfc3339 string",
			in:   mid.Format(time.RFC3339),
			check: func(t *testing.T, got time.Time) {
				if got.Unix() != mid.Unix() {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "unix string",
			in:   "1700000000",
			check: func(t *testing.T, got time.Time) {
				want := time.Unix(1_700_000_000, 0).UTC()
				// past timestamps clamp to now
				if got.Before(now.Add(-2*time.Second)) {
					t.Fatalf("got=%v want clamped near now (past unix)", got)
				}
				_ = want
			},
		},
		{
			name: "garbage string uses now",
			in:   "not-a-time",
			check: func(t *testing.T, got time.Time) {
				if got.Before(now.Add(-2*time.Second)) || got.After(now.Add(2*time.Second)) {
					t.Fatalf("got=%v", got)
				}
			},
		},
		{
			name: "unsupported type uses now",
			in:   struct{}{},
			check: func(t *testing.T, got time.Time) {
				if got.Before(now.Add(-2*time.Second)) || got.After(now.Add(2*time.Second)) {
					t.Fatalf("got=%v", got)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, clampRunAt(tt.in, horizon))
		})
	}
}
