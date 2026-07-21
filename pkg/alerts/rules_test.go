package alerts

import (
	"testing"
	"time"
)

func quietRules() map[string]RuleConf {
	r := defaultRules()
	for id := range r {
		if id != "lag_stuck_growing" {
			rc := r[id]
			rc.Enabled = false
			r[id] = rc
		}
	}
	return r
}

func TestRunRulesLagStuckGrowing(t *testing.T) {
	committed := int64(100)
	end := int64(2000)
	cfg := Config{
		LagThreshold: 1000,
		LagGrowthMin: 100,
		Rules:        quietRules(),
	}
	sample := Sample{
		LagTopics: []LagRow{{
			Group: "g", Topic: "jobs", Lag: 1500,
			CommittedSum: &committed, EndSum: &end,
		}},
		LagBaseline: map[string]map[string]interface{}{
			"g|jobs": {"committed": int64(100), "lag": int64(1000), "end_sum": int64(1800)},
		},
	}
	findings := runRules(cfg, sample)
	if len(findings) != 1 || findings[0].RuleID != "lag_stuck_growing" {
		t.Fatalf("findings=%+v", findings)
	}
	if findings[0].Severity != "critical" {
		t.Fatalf("severity=%s", findings[0].Severity)
	}

	// Below threshold — no finding.
	sample.LagTopics[0].Lag = 50
	if got := runRules(cfg, sample); len(got) != 0 {
		t.Fatalf("expected empty, got %+v", got)
	}

	// Paused topic skipped.
	sample.LagTopics[0].Lag = 1500
	sample.PausedKeys = []string{"g\x1fjobs"}
	if got := runRules(cfg, sample); len(got) != 0 {
		t.Fatalf("paused should skip: %+v", got)
	}

	// Disabled rule.
	sample.PausedKeys = nil
	cfg.Rules["lag_stuck_growing"] = RuleConf{Enabled: false, Severity: "critical"}
	if got := runRules(cfg, sample); len(got) != 0 {
		t.Fatalf("disabled: %+v", got)
	}
}

func TestRunRulesRedisRTTAndNoLive(t *testing.T) {
	rules := defaultRules()
	for _, id := range []string{"reconciler_stale", "dlt_rate_high", "schedule_depth_high", "fairness_ingest_backed_up", "cron_stale", "lag_stuck_growing"} {
		rc := rules[id]
		rc.Enabled = false
		rules[id] = rc
	}
	cfg := Config{
		PerformanceMetricsEnabled: true,
		LivenessEnabled:           true,
		RTTAvgMs:                  50,
		RTTMaxMs:                  200,
		RTTErrorRate:              0.25,
		Rules:                     rules,
	}
	sample := Sample{
		RTT: map[string]interface{}{
			"latest_avg_ms": 80.0, "latest_max_ms": 100.0, "errors": 0.0, "probes": 10.0,
		},
		PendingTotal:  5,
		LiveConsumers: 0,
	}
	findings := runRules(cfg, sample)
	ids := map[string]bool{}
	for _, f := range findings {
		ids[f.RuleID] = true
	}
	if !ids["redis_rtt_high"] || !ids["no_live_consumers"] {
		t.Fatalf("ids=%v findings=%+v", ids, findings)
	}

	cfg.PerformanceMetricsEnabled = false
	cfg.LivenessEnabled = false
	if got := runRules(cfg, sample); len(got) != 0 {
		t.Fatalf("gates off: %+v", got)
	}
}

func TestRunRulesReconcilerFairnessDLTScheduleCron(t *testing.T) {
	rules := defaultRules()
	rules["lag_stuck_growing"] = RuleConf{Enabled: false, Severity: "critical"}
	rules["redis_rtt_high"] = RuleConf{Enabled: false, Severity: "warning"}
	rules["no_live_consumers"] = RuleConf{Enabled: false, Severity: "critical"}
	cfg := Config{
		ReconcilerMaxAge:          900,
		FairnessIngestLag:         100,
		FairnessReadyMaxWhenStuck: 10,
		DLTPerMinute:              5,
		SchedulePendingMax:        100,
		RecurringEnabled:          true,
		Rules:                     rules,
	}
	old := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	sample := Sample{
		Reconciler: map[string]string{
			"ran_at": old, "produce_failed": "2", "found_stale": "12",
		},
		Fairness: []FairLane{
			{Lane: "time", IngestLag: 500, ReadyLag: 0},
			{Lane: "throughput", IngestLag: 10, ReadyLag: 0}, // below ingest threshold
		},
		DLTPerMinute:     20,
		SchedulePending:  150,
		ScheduleInflight: 3,
		CronStale: []map[string]interface{}{
			{"schedule": "nightly", "job_type": "X", "stale_seconds": int64(120)},
		},
	}
	findings := runRules(cfg, sample)
	ids := map[string]int{}
	for _, f := range findings {
		ids[f.RuleID]++
	}
	for _, want := range []string{
		"reconciler_stale", "fairness_ingest_backed_up", "dlt_rate_high",
		"schedule_depth_high", "cron_stale",
	} {
		if ids[want] == 0 {
			t.Fatalf("missing %s in %v (%+v)", want, ids, findings)
		}
	}
	// reconciler: age + failures
	if ids["reconciler_stale"] < 2 {
		t.Fatalf("expected age+failures reconciler findings, got %d", ids["reconciler_stale"])
	}

	// Missing reconciler summary.
	sample.Reconciler = nil
	sample.Fairness = nil
	sample.DLTPerMinute = 0
	sample.SchedulePending = 0
	sample.CronStale = nil
	got := runRules(cfg, sample)
	if len(got) != 1 || got[0].Fingerprint != "reconciler_stale:missing" {
		t.Fatalf("missing reconciler: %+v", got)
	}
}

func TestRuleSeverityAndAsConverters(t *testing.T) {
	cfg := Config{Rules: map[string]RuleConf{"x": {Enabled: true, Severity: "info"}}}
	if ruleSeverity(cfg, "x", "warning") != "info" {
		t.Fatal("override")
	}
	if ruleSeverity(cfg, "y", "warning") != "warning" {
		t.Fatal("default")
	}
	if !ruleEnabled(cfg, "missing") {
		t.Fatal("missing rule defaults enabled")
	}

	for _, tc := range []struct {
		in   interface{}
		want int64
		ok   bool
	}{
		{int64(3), 3, true}, {int(4), 4, true}, {float64(5), 5, true}, {"6", 6, true}, {"x", 0, false}, {true, 0, false},
	} {
		got, ok := asInt64(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("asInt64(%v)=%d,%v want %d,%v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
	for _, tc := range []struct {
		in   interface{}
		want float64
		ok   bool
	}{
		{float64(1.5), 1.5, true}, {int(2), 2, true}, {int64(3), 3, true}, {"4.5", 4.5, true}, {true, 0, false},
	} {
		got, ok := asFloat(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("asFloat(%v)=%v,%v", tc.in, got, ok)
		}
	}
}

func TestPausedSet(t *testing.T) {
	m := pausedSet([]string{"g\x1ft", "a|b"})
	if !m["g\x1ft"] || !m["g|t"] || !m["a|b"] {
		t.Fatalf("%v", m)
	}
}

func TestFairnessLanes(t *testing.T) {
	cfg := Config{
		FairnessTimeIngest:       "t-ingest",
		FairnessThroughputIngest: "th-ingest",
		FairnessTimeReady:        []string{"t-ready"},
		FairnessThroughputReady:  []string{"th-ready-a", "th-ready-b"},
	}
	rows := []LagRow{
		{Topic: "t-ingest", Lag: 10},
		{Topic: "t-ready", Lag: 2},
		{Topic: "th-ingest", Lag: 7},
		{Topic: "th-ready-a", Lag: 1},
		{Topic: "th-ready-b", Lag: 3},
	}
	lanes := fairnessLanes(nil, cfg, rows)
	if len(lanes) != 2 {
		t.Fatalf("%+v", lanes)
	}
	if lanes[0].IngestLag != 10 || lanes[0].ReadyLag != 2 {
		t.Fatalf("time %+v", lanes[0])
	}
	if lanes[1].IngestLag != 7 || lanes[1].ReadyLag != 4 {
		t.Fatalf("throughput %+v", lanes[1])
	}
}
