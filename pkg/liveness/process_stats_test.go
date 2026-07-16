package liveness

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParsePSTime(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"0:01.50", time.Duration(1.5 * float64(time.Second))},
		{"1:02", 62 * time.Second},
		{"1:02:03", time.Hour + 2*time.Minute + 3*time.Second},
	}
	for _, tc := range cases {
		got, err := parsePSTime(tc.in)
		if err != nil {
			t.Fatalf("parsePSTime(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("parsePSTime(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

func TestConsumerHeartbeatJSONIncludesStats(t *testing.T) {
	s := newProcessSampler(time.Millisecond)
	// First sample may omit cpu_pct (needs a prior reading); force two samples.
	_ = s.sample()
	time.Sleep(20 * time.Millisecond)
	raw := ConsumerHeartbeatJSON("host:1:abc", "jobs", s)
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["runtime"] != "go" {
		t.Fatalf("runtime=%v", m["runtime"])
	}
	if m["consumer_id"] != "host:1:abc" {
		t.Fatalf("consumer_id=%v", m["consumer_id"])
	}
	if _, ok := m["rss_bytes"]; !ok {
		t.Fatalf("expected rss_bytes in %v", m)
	}
}
