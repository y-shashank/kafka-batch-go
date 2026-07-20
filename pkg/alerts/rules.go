package alerts

import (
	"fmt"
	"strconv"
	"time"
)

// Finding is one rule breach (Ruby Alerts::Rules::Finding).
type Finding struct {
	RuleID      string
	Fingerprint string
	Title       string
	Summary     string
	Severity    string
	Link        string
	Sample      map[string]interface{}
}

func runRules(cfg Config, sample Sample) []Finding {
	var out []Finding
	out = append(out, ruleLagStuck(cfg, sample)...)
	out = append(out, ruleRedisRTT(cfg, sample)...)
	out = append(out, ruleNoLive(cfg, sample)...)
	out = append(out, ruleReconciler(cfg, sample)...)
	out = append(out, ruleFairness(cfg, sample)...)
	out = append(out, ruleDLT(cfg, sample)...)
	out = append(out, ruleSchedule(cfg, sample)...)
	out = append(out, ruleCron(cfg, sample)...)
	return out
}

func ruleEnabled(cfg Config, id string) bool {
	rc, ok := cfg.Rules[id]
	if !ok {
		return true
	}
	return rc.Enabled
}

func ruleSeverity(cfg Config, id, def string) string {
	if rc, ok := cfg.Rules[id]; ok && rc.Severity != "" {
		return rc.Severity
	}
	return def
}

func ruleLagStuck(cfg Config, sample Sample) []Finding {
	const id = "lag_stuck_growing"
	if !ruleEnabled(cfg, id) {
		return nil
	}
	paused := pausedSet(sample.PausedKeys)
	var out []Finding
	for _, row := range sample.LagTopics {
		if row.Lag < int64(cfg.LagThreshold) {
			continue
		}
		if paused[row.Group+"\x1f"+row.Topic] || paused[row.Group+"|"+row.Topic] {
			continue
		}
		prev := sample.LagBaseline[row.Group+"|"+row.Topic]
		if prev == nil {
			continue
		}
		prevCommitted, okC := asInt64(prev["committed"])
		prevLag, _ := asInt64(prev["lag"])
		prevEnd, okE := asInt64(prev["end_sum"])
		if !okC || row.CommittedSum == nil {
			continue
		}
		committedStuck := prevCommitted == *row.CommittedSum
		lagGrew := row.Lag-prevLag >= int64(cfg.LagGrowthMin)
		endGrew := okE && row.EndSum != nil && *row.EndSum > prevEnd
		if !(committedStuck && (lagGrew || endGrew)) {
			continue
		}
		out = append(out, Finding{
			RuleID: id, Fingerprint: fmt.Sprintf("%s:%s:%s", id, row.Group, row.Topic),
			Title: "Lag growing without consumption", Severity: ruleSeverity(cfg, id, "critical"),
			Summary: fmt.Sprintf("%s (%s) lag=%d committed stuck; backlog still growing.", row.Topic, row.Group, row.Lag),
			Link:    "/lag",
		})
	}
	return out
}

func ruleRedisRTT(cfg Config, sample Sample) []Finding {
	const id = "redis_rtt_high"
	if !ruleEnabled(cfg, id) || !cfg.PerformanceMetricsEnabled || sample.RTT == nil {
		return nil
	}
	avg, _ := asFloat(sample.RTT["latest_avg_ms"])
	max, _ := asFloat(sample.RTT["latest_max_ms"])
	errors, _ := asFloat(sample.RTT["errors"])
	probes, _ := asFloat(sample.RTT["probes"])
	errRate := 0.0
	if probes > 0 {
		errRate = errors / probes
	}
	if avg < cfg.RTTAvgMs && max < cfg.RTTMaxMs && errRate < cfg.RTTErrorRate {
		return nil
	}
	return []Finding{{
		RuleID: id, Fingerprint: id, Title: "Redis RTT elevated",
		Severity: ruleSeverity(cfg, id, "warning"),
		Summary:  fmt.Sprintf("Redis RTT avg=%.1fms max=%.1fms errors=%.0f/%.0f", avg, max, errors, probes),
		Link:     "/performance",
	}}
}

func ruleNoLive(cfg Config, sample Sample) []Finding {
	const id = "no_live_consumers"
	if !ruleEnabled(cfg, id) || !cfg.LivenessEnabled {
		return nil
	}
	if sample.PendingTotal <= 0 || sample.LiveConsumers > 0 {
		return nil
	}
	return []Finding{{
		RuleID: id, Fingerprint: id, Title: "No live consumers with lag",
		Severity: ruleSeverity(cfg, id, "critical"),
		Summary:  fmt.Sprintf("topic_pending=%d but live consumers=0", sample.PendingTotal),
		Link:     "/live",
	}}
}

func ruleReconciler(cfg Config, sample Sample) []Finding {
	const id = "reconciler_stale"
	if !ruleEnabled(cfg, id) {
		return nil
	}
	last := sample.Reconciler
	if len(last) == 0 {
		return []Finding{{
			RuleID: id, Fingerprint: id + ":missing", Title: "Reconciler stale or failing",
			Severity: ruleSeverity(cfg, id, "warning"),
			Summary:  "No reconciler run summary in Redis (never ran or Redis unavailable).",
			Link:     "/reconciler",
		}}
	}
	var out []Finding
	if ranAt := last["ran_at"]; ranAt != "" {
		if t, err := time.Parse(time.RFC3339, ranAt); err == nil {
			age := int(time.Since(t).Seconds())
			if age > cfg.ReconcilerMaxAge {
				out = append(out, Finding{
					RuleID: id, Fingerprint: id + ":age", Title: "Reconciler stale or failing",
					Severity: ruleSeverity(cfg, id, "warning"),
					Summary:  fmt.Sprintf("Last reconciler run %ds ago (max %ds).", age, cfg.ReconcilerMaxAge),
					Link:     "/reconciler",
				})
			}
		}
	}
	pf, _ := strconv.Atoi(last["produce_failed"])
	fs, _ := strconv.Atoi(last["found_stale"])
	if pf > 0 || fs >= 10 {
		out = append(out, Finding{
			RuleID: id, Fingerprint: id + ":failures", Title: "Reconciler stale or failing",
			Severity: ruleSeverity(cfg, id, "warning"),
			Summary:  fmt.Sprintf("Reconciler produce_failed=%d found_stale=%d.", pf, fs),
			Link:     "/reconciler",
		})
	}
	return out
}

func ruleFairness(cfg Config, sample Sample) []Finding {
	const id = "fairness_ingest_backed_up"
	if !ruleEnabled(cfg, id) {
		return nil
	}
	var out []Finding
	for _, lane := range sample.Fairness {
		if lane.IngestLag < int64(cfg.FairnessIngestLag) {
			continue
		}
		if lane.ReadyLag > int64(cfg.FairnessReadyMaxWhenStuck) {
			continue
		}
		out = append(out, Finding{
			RuleID: id, Fingerprint: id + ":" + lane.Lane,
			Title: "Fairness ingest backed up", Severity: ruleSeverity(cfg, id, "warning"),
			Summary: fmt.Sprintf("Fair %s ingest_lag=%d ready_lag=%d (forwarder may be stuck).", lane.Lane, lane.IngestLag, lane.ReadyLag),
			Link:    "/fairness/" + lane.Lane,
		})
	}
	return out
}

func ruleDLT(cfg Config, sample Sample) []Finding {
	const id = "dlt_rate_high"
	if !ruleEnabled(cfg, id) || sample.DLTPerMinute < cfg.DLTPerMinute {
		return nil
	}
	return []Finding{{
		RuleID: id, Fingerprint: id, Title: "Dead-letter rate high",
		Severity: ruleSeverity(cfg, id, "warning"),
		Summary:  fmt.Sprintf("DLT publishes last minute=%d (threshold %d).", sample.DLTPerMinute, cfg.DLTPerMinute),
		Link:     "/dead_letter",
	}}
}

func ruleSchedule(cfg Config, sample Sample) []Finding {
	const id = "schedule_depth_high"
	if !ruleEnabled(cfg, id) || sample.SchedulePending < int64(cfg.SchedulePendingMax) {
		return nil
	}
	return []Finding{{
		RuleID: id, Fingerprint: id, Title: "Delayed-job schedule depth high",
		Severity: ruleSeverity(cfg, id, "warning"),
		Summary:  fmt.Sprintf("sched:pending=%d (threshold %d), inflight=%d.", sample.SchedulePending, cfg.SchedulePendingMax, sample.ScheduleInflight),
		Link:     "/scheduled",
	}}
}

func ruleCron(cfg Config, sample Sample) []Finding {
	const id = "cron_stale"
	if !ruleEnabled(cfg, id) || !cfg.RecurringEnabled {
		return nil
	}
	var out []Finding
	for _, e := range sample.CronStale {
		sched, _ := e["schedule"].(string)
		jt, _ := e["job_type"].(string)
		stale, _ := asInt64(e["stale_seconds"])
		out = append(out, Finding{
			RuleID: id, Fingerprint: id + ":" + sched,
			Title: "Recurring schedule stale", Severity: ruleSeverity(cfg, id, "warning"),
			Summary: fmt.Sprintf("Recurring schedule %s stale for %ds (job_type=%s).", sched, stale, jt),
			Link:    "/recurring",
		})
	}
	return out
}

func asInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func asFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		n, err := strconv.ParseFloat(t, 64)
		return n, err == nil
	default:
		return 0, false
	}
}
