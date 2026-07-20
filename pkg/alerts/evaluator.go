package alerts

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type transition struct {
	event   string
	finding Finding
	firedAt string
}

// EvaluateOnce runs one NX-locked evaluation tick (Ruby Evaluator parity).
// Notifications fire at most once per open and once per resolve — no reminders.
func EvaluateOnce(ctx context.Context, rdb *redis.Client, cfg Config) map[string]interface{} {
	if !cfg.Enabled {
		return map[string]interface{}{"ok": false, "reason": "disabled"}
	}
	st := NewState(rdb)
	ttl := cfg.Interval
	if ttl < 30 {
		ttl = 30
	}
	if !st.TryLock(ctx, ttl) {
		return map[string]interface{}{"ok": false, "reason": "lock"}
	}

	sample := collectSample(ctx, rdb, st, cfg)
	findings := runRules(cfg, sample)
	transitions := applyHysteresis(ctx, st, cfg, findings)
	notifyTransitions(ctx, st, cfg, transitions)
	persistBaseline(ctx, st, sample)

	summary := map[string]interface{}{
		"ran_at":           nowISO(),
		"findings":         len(findings),
		"open":             len(st.OpenAlerts(ctx)),
		"fired":            countEvent(transitions, "fired"),
		"resolved":         countEvent(transitions, "resolved"),
		"settings_version": cfg.Version,
		"runtime":          "go",
	}
	st.SaveLast(ctx, summary)
	summary["ok"] = true
	return summary
}

func applyHysteresis(ctx context.Context, st *State, cfg Config, findings []Finding) []transition {
	forTicks := cfg.ForTicks
	if forTicks < 1 {
		forTicks = 1
	}
	resolveTicks := cfg.ResolveTicks
	if resolveTicks < 1 {
		resolveTicks = 1
	}
	now := time.Now().UTC()
	active := map[string]Finding{}
	for _, f := range findings {
		active[f.Fingerprint] = f
	}
	var transitions []transition

	for fp, finding := range active {
		st.IncrBreach(ctx, fp)
		st.ResetHealthy(ctx, fp)
		count := st.BreachCount(ctx, fp)
		open := st.GetOpen(ctx, fp)

		if open != nil {
			// Still open — refresh summary only; do not re-notify.
			st.TouchOpen(ctx, fp, finding.Summary)
			continue
		}
		if count < forTicks {
			continue
		}
		incident := map[string]interface{}{
			"fingerprint":    fp,
			"rule_id":        finding.RuleID,
			"title":          finding.Title,
			"summary":        finding.Summary,
			"severity":      finding.Severity,
			"link":           finding.Link,
			"fired_at":       now.Format(time.RFC3339),
			"last_notify_at": now.Format(time.RFC3339),
			"runtime":        "go",
		}
		if !st.ClaimOpen(ctx, fp, incident) {
			continue
		}
		transitions = append(transitions, transition{
			event: "fired", finding: finding, firedAt: incident["fired_at"].(string),
		})
	}

	for _, incident := range st.OpenAlerts(ctx) {
		fp := str(incident["fingerprint"])
		if _, ok := active[fp]; ok {
			continue
		}
		st.IncrHealthy(ctx, fp)
		st.ResetBreach(ctx, fp)
		if st.HealthyCount(ctx, fp) < resolveTicks {
			continue
		}
		if !st.ClearOpen(ctx, fp) {
			continue
		}
		st.ResetHealthy(ctx, fp)
		finding := Finding{
			RuleID: str(incident["rule_id"]), Fingerprint: fp,
			Title: str(incident["title"]), Summary: str(incident["summary"]),
			Severity: str(incident["severity"]), Link: str(incident["link"]),
		}
		transitions = append(transitions, transition{
			event: "resolved", finding: finding, firedAt: str(incident["fired_at"]),
		})
	}
	return transitions
}

func notifyTransitions(ctx context.Context, st *State, cfg Config, transitions []transition) {
	multi := NewMulti(cfg)
	dedupeTTL := cfg.Interval * 3
	if dedupeTTL < 120 {
		dedupeTTL = 120
	}
	if dedupeTTL > 3600 {
		dedupeTTL = 3600
	}
	for _, t := range transitions {
		firedAt := t.firedAt
		dedupeEvent := t.event
		if firedAt != "" {
			dedupeEvent = t.event + ":" + firedAt
		}
		if !st.ClaimNotify(ctx, t.finding.Fingerprint, dedupeEvent, dedupeTTL) {
			continue
		}
		p := Payload{
			Event: t.event, RuleID: t.finding.RuleID, Title: t.finding.Title,
			Summary: t.finding.Summary, Severity: t.finding.Severity,
			Fingerprint: t.finding.Fingerprint, Link: t.finding.Link,
			Sample: t.finding.Sample, Source: "kafka-batch-go",
		}
		if t.event == "fired" {
			p.FiredAt = firedAt
			if p.FiredAt == "" {
				p.FiredAt = nowISO()
			}
		} else {
			p.FiredAt = firedAt
			p.ResolvedAt = nowISO()
		}
		multi.Deliver(p)
	}
}

func countEvent(ts []transition, event string) int {
	n := 0
	for _, t := range ts {
		if t.event == event {
			n++
		}
	}
	return n
}

func str(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
