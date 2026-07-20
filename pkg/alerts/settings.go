package alerts

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

const (
	settingsKey = "kafka_batch:alerts:settings"
	versionKey  = "kafka_batch:alerts:settings:version"
)

// Config is the effective evaluator config (library defaults ← Redis).
type Config struct {
	Enabled                   bool
	Interval                  int
	ForTicks                  int
	ResolveTicks              int
	CooldownSeconds           int
	LagThreshold              int
	LagGrowthMin              int
	RTTAvgMs                  float64
	RTTMaxMs                  float64
	RTTErrorRate              float64
	ReconcilerMaxAge          int
	SchedulePendingMax        int
	DLTPerMinute              int
	FairnessIngestLag         int
	FairnessReadyMaxWhenStuck int
	ChannelSlack              bool
	ChannelWebhook            bool
	ChannelEmail              bool
	ChannelMetrics            bool
	SlackWebhookURL           string
	WebhookURLs               []string
	EmailTo                   string
	EmailFrom                 string
	EmailSMTPAddress          string
	EmailSMTPPort             int
	EmailSMTPUser             string
	EmailSMTPPassword         string
	Rules                     map[string]RuleConf
	Version                   string
	EncryptionSalt            string
	MetricsEnabled            bool
	PerformanceMetricsEnabled bool
	LivenessEnabled           bool
	RecurringEnabled          bool
	ConsumerGroup             string
	Brokers                   []string
	FairnessTimeIngest        string
	FairnessThroughputIngest  string
	FairnessTimeReady         []string
	FairnessThroughputReady   []string
}

// RuleConf is per-rule enable + severity.
type RuleConf struct {
	Enabled  bool
	Severity string
}

func loadEffective(ctx context.Context, rdb *redis.Client, cfg config.Daemon) Config {
	raw, _ := rdb.HGetAll(ctx, settingsKey).Result()
	if raw == nil {
		raw = map[string]string{}
	}
	ver, _ := rdb.Get(ctx, versionKey).Result()

	out := Config{
		Enabled:                   boolField(raw, "enabled", cfg.AlertsEnabled),
		Interval:                  intField(raw, "interval", positiveOr(cfg.AlertsIntervalSec, 60)),
		ForTicks:                  intField(raw, "for_ticks", positiveOr(cfg.AlertsForTicks, 3)),
		ResolveTicks:              intField(raw, "resolve_ticks", positiveOr(cfg.AlertsResolveTicks, 2)),
		CooldownSeconds:           intField(raw, "cooldown_seconds", positiveOr(cfg.AlertsCooldownSeconds, 900)),
		LagThreshold:              intField(raw, "lag_threshold", positiveOr(cfg.AlertsLagThreshold, 1000)),
		LagGrowthMin:              intField(raw, "lag_growth_min", positiveOr(cfg.AlertsLagGrowthMin, 100)),
		RTTAvgMs:                  floatField(raw, "rtt_avg_ms", positiveOrF(cfg.AlertsRTTAvgMs, 50)),
		RTTMaxMs:                  floatField(raw, "rtt_max_ms", positiveOrF(cfg.AlertsRTTMaxMs, 200)),
		RTTErrorRate:              floatField(raw, "rtt_error_rate", positiveOrF(cfg.AlertsRTTErrorRate, 0.25)),
		ReconcilerMaxAge:          intField(raw, "reconciler_max_age", positiveOr(cfg.AlertsReconcilerMaxAge, 900)),
		SchedulePendingMax:        intField(raw, "schedule_pending_max", positiveOr(cfg.AlertsSchedulePendingMax, 10000)),
		DLTPerMinute:              intField(raw, "dlt_per_minute", positiveOr(cfg.AlertsDLTPerMinute, 50)),
		FairnessIngestLag:         intField(raw, "fairness_ingest_lag", positiveOr(cfg.AlertsFairnessIngestLag, 5000)),
		FairnessReadyMaxWhenStuck: intField(raw, "fairness_ready_max_when_stuck", positiveOr(cfg.AlertsFairnessReadyMaxWhenStuck, 10)),
		ChannelSlack:              boolField(raw, "channel_slack", false),
		ChannelWebhook:            boolField(raw, "channel_webhook", false),
		ChannelEmail:              boolField(raw, "channel_email", false),
		ChannelMetrics:            boolField(raw, "channel_metrics", true),
		EmailTo:                   raw["email_to"],
		EmailFrom:                 raw["email_from"],
		EmailSMTPAddress:          raw["email_smtp_address"],
		EmailSMTPPort:             intField(raw, "email_smtp_port", 587),
		EmailSMTPUser:             raw["email_smtp_user"],
		Rules:                     parseRules(raw["rules_json"]),
		Version:                   ver,
		EncryptionSalt:            cfg.AIEncryptionSalt,
		MetricsEnabled:            cfg.MetricsEnabled,
		PerformanceMetricsEnabled: cfg.PerformanceMetricsEnabled,
		LivenessEnabled:           cfg.LivenessEnabled,
		RecurringEnabled:          cfg.RecurringSchedulerEnabled,
		ConsumerGroup:             cfg.ConsumerGroup,
		Brokers:                   cfg.Brokers,
		FairnessTimeIngest:        cfg.FairnessTimeIngest,
		FairnessThroughputIngest:  cfg.FairnessThroughputIngest,
	}
	tr := cfg.FairReadyTopics("time")
	out.FairnessTimeReady = []string{tr.Go, tr.Ruby}
	thr := cfg.FairReadyTopics("throughput")
	out.FairnessThroughputReady = []string{thr.Go, thr.Ruby}

	salt := cfg.AIEncryptionSalt
	if salt != "" {
		if s, err := Decrypt(salt, raw["slack_webhook_url_ciphertext"]); err == nil {
			out.SlackWebhookURL = s
		}
		if s, err := Decrypt(salt, raw["webhook_urls_ciphertext"]); err == nil {
			out.WebhookURLs = splitURLs(s)
		}
		if s, err := Decrypt(salt, raw["email_smtp_password_ciphertext"]); err == nil {
			out.EmailSMTPPassword = s
		}
	}
	return out
}

func parseRules(jsonStr string) map[string]RuleConf {
	defaults := defaultRules()
	if jsonStr == "" {
		return defaults
	}
	var raw map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return defaults
	}
	for id, conf := range raw {
		rc := defaults[id]
		if v, ok := conf["enabled"]; ok {
			rc.Enabled = truthy(v)
		}
		if v, ok := conf["severity"].(string); ok && v != "" {
			rc.Severity = v
		}
		defaults[id] = rc
	}
	return defaults
}

func defaultRules() map[string]RuleConf {
	ids := []string{
		"lag_stuck_growing", "redis_rtt_high", "no_live_consumers", "reconciler_stale",
		"fairness_ingest_backed_up", "dlt_rate_high", "schedule_depth_high", "cron_stale",
	}
	m := make(map[string]RuleConf, len(ids))
	for _, id := range ids {
		sev := "warning"
		if id == "lag_stuck_growing" || id == "no_live_consumers" {
			sev = "critical"
		}
		m[id] = RuleConf{Enabled: true, Severity: sev}
	}
	return m
}

func splitURLs(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' })
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func boolField(raw map[string]string, key string, def bool) bool {
	v, ok := raw[key]
	if !ok || v == "" {
		return def
	}
	return truthy(v)
}

func intField(raw map[string]string, key string, def int) int {
	v, ok := raw[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func floatField(raw map[string]string, key string, def float64) float64 {
	v, ok := raw[key]
	if !ok || v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func truthy(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "1" || s == "true" || s == "yes" || s == "on"
	case float64:
		return t != 0
	default:
		return false
	}
}

func positiveOr(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func positiveOrF(v, def float64) float64 {
	if v > 0 {
		return v
	}
	return def
}
