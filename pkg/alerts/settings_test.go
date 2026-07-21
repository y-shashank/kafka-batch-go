package alerts

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestParseRulesAndDefaults(t *testing.T) {
	d := defaultRules()
	if !d["lag_stuck_growing"].Enabled || d["lag_stuck_growing"].Severity != "critical" {
		t.Fatalf("%+v", d["lag_stuck_growing"])
	}
	if d["dlt_rate_high"].Severity != "warning" {
		t.Fatalf("%+v", d["dlt_rate_high"])
	}

	merged := parseRules(`{"dlt_rate_high":{"enabled":false,"severity":"info"},"no_live_consumers":{"enabled":true}}`)
	if merged["dlt_rate_high"].Enabled || merged["dlt_rate_high"].Severity != "info" {
		t.Fatalf("%+v", merged["dlt_rate_high"])
	}
	if !merged["no_live_consumers"].Enabled {
		t.Fatal("enabled merge")
	}
	if parseRules("")["lag_stuck_growing"].Enabled != true {
		t.Fatal("empty json")
	}
	if parseRules("{")["lag_stuck_growing"].Enabled != true {
		t.Fatal("bad json falls back")
	}
}

func TestSettingsHelpers(t *testing.T) {
	if !truthy(true) || !truthy("YES") || !truthy("1") || !truthy(1.0) || truthy(0.0) || truthy("no") {
		t.Fatal("truthy")
	}
	raw := map[string]string{"a": "true", "b": "12", "c": "1.5", "bad": "x"}
	if !boolField(raw, "a", false) || boolField(raw, "missing", true) != true {
		t.Fatal("boolField")
	}
	if intField(raw, "b", 0) != 12 || intField(raw, "bad", 7) != 7 {
		t.Fatal("intField")
	}
	if floatField(raw, "c", 0) != 1.5 || floatField(raw, "bad", 9) != 9 {
		t.Fatal("floatField")
	}
	if positiveOr(0, 5) != 5 || positiveOr(3, 5) != 3 {
		t.Fatal("positiveOr")
	}
	if positiveOrF(0, 1.5) != 1.5 || positiveOrF(2, 1.5) != 2 {
		t.Fatal("positiveOrF")
	}
	urls := splitURLs(" https://a.com ,\nhttps://b.com\r\n")
	if len(urls) != 2 || urls[0] != "https://a.com" || urls[1] != "https://b.com" {
		t.Fatalf("%v", urls)
	}
}

func TestLoadEffectiveDecryptsSecrets(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	salt := "unit-test-salt"
	_ = rdb.HSet(ctx, settingsKey, map[string]interface{}{
		"enabled":                      "true",
		"interval":                     "45",
		"channel_slack":                "1",
		"slack_webhook_url_ciphertext": encryptBlob(t, salt, "https://hooks.example/slack"),
		"webhook_urls_ciphertext":      encryptBlob(t, salt, "https://a.example\nhttps://b.example"),
		"email_smtp_password_ciphertext": encryptBlob(t, salt, "secret"),
		"rules_json":                   `{"dlt_rate_high":{"enabled":false}}`,
	}).Err()
	_ = rdb.Set(ctx, versionKey, "v9", 0).Err()

	cfg := loadEffective(ctx, rdb, config.Daemon{
		AIEncryptionSalt:            salt,
		AlertsEnabled:               false,
		AlertsIntervalSec:           60,
		MetricsEnabled:              true,
		PerformanceMetricsEnabled:   true,
		LivenessEnabled:             true,
		RecurringSchedulerEnabled:   true,
		ConsumerGroup:               "kb",
		Brokers:                     []string{"localhost:9092"},
		FairnessTimeIngest:          "t-in",
		FairnessThroughputIngest:    "th-in",
	})
	if !cfg.Enabled || cfg.Interval != 45 || cfg.Version != "v9" {
		t.Fatalf("%+v", cfg)
	}
	if cfg.SlackWebhookURL != "https://hooks.example/slack" {
		t.Fatalf("slack=%q", cfg.SlackWebhookURL)
	}
	if len(cfg.WebhookURLs) != 2 || cfg.EmailSMTPPassword != "secret" {
		t.Fatalf("webhooks=%v pass=%q", cfg.WebhookURLs, cfg.EmailSMTPPassword)
	}
	if cfg.Rules["dlt_rate_high"].Enabled {
		t.Fatal("rules merge")
	}
	if !cfg.ChannelSlack || !cfg.ChannelMetrics {
		t.Fatal("channels")
	}
}

func TestPayloadDefaults(t *testing.T) {
	p := Payload{Title: "t"}.withDefaults()
	if p.Source != "kafka-batch" || p.Severity != "warning" {
		t.Fatalf("%+v", p)
	}
	if nowISO() == "" {
		t.Fatal("nowISO")
	}
}
