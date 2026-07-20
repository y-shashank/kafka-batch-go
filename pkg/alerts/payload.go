package alerts

import "time"

// Payload is the normalized notification body (Ruby Alerts::Payload parity).
type Payload struct {
	Event       string                 `json:"event"`
	RuleID      string                 `json:"rule_id"`
	Title       string                 `json:"title"`
	Summary     string                 `json:"summary"`
	Severity    string                 `json:"severity"`
	Fingerprint string                 `json:"fingerprint"`
	Link        string                 `json:"link,omitempty"`
	Sample      map[string]interface{} `json:"sample,omitempty"`
	FiredAt     string                 `json:"fired_at,omitempty"`
	ResolvedAt  string                 `json:"resolved_at,omitempty"`
	Source      string                 `json:"source"`
}

func (p Payload) withDefaults() Payload {
	if p.Source == "" {
		p.Source = "kafka-batch"
	}
	if p.Severity == "" {
		p.Severity = "warning"
	}
	return p
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}
