package dsn

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"native passthrough", "user:pass@tcp(mysql:3306)/sched?parseTime=true", "user:pass@tcp(mysql:3306)/sched?parseTime=true"},
		{"native no params", "user:pass@tcp(mysql)/sched", "user:pass@tcp(mysql)/sched"},
		{"url basic", "mysql2://user:pass@mysql:3306/sched", "user:pass@tcp(mysql:3306)/sched"},
		{"url default port", "mysql2://user:pass@mysql/sched", "user:pass@tcp(mysql:3306)/sched"},
		{"mysql scheme", "mysql://user:pass@db.internal:3307/kb", "user:pass@tcp(db.internal:3307)/kb"},
		{"url no password", "mysql2://user@mysql/sched", "user@tcp(mysql:3306)/sched"},
		{"whitespace trimmed", "  user:pass@tcp(mysql)/sched  ", "user:pass@tcp(mysql)/sched"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Normalize(c.in)
			if err != nil {
				t.Fatalf("Normalize(%q) error: %v", c.in, err)
			}
			if got != c.want {
				t.Fatalf("Normalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestNormalizeURLParamsRoundTrip(t *testing.T) {
	// Query params must survive so the driver still applies parseTime/loc.
	got, err := Normalize("mysql2://user:pass@mysql:3306/sched?parseTime=true&loc=UTC")
	if err != nil {
		t.Fatal(err)
	}
	want := "user:pass@tcp(mysql:3306)/sched?loc=UTC&parseTime=true"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeMissingHost(t *testing.T) {
	if _, err := Normalize("mysql2:///sched"); err == nil {
		t.Fatal("expected error for missing host")
	}
}
