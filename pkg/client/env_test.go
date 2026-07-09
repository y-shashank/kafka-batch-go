package client

import (
	"testing"
)

func TestApplyEnvOverrides(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "b1:9092,b2:9092")
	t.Setenv("KAFKA_PREFIX", "ship")
	t.Setenv("REDIS_URL", "redis://redis:6379/1")
	t.Setenv("KAFKA_BATCH_HANDLER_MANIFEST", "/etc/handlers.yml")
	t.Setenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN", "user:pass@tcp(mysql)/sched")

	cfg := DefaultConfig()
	ApplyEnv(&cfg)

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "b1:9092" {
		t.Fatalf("brokers=%v", cfg.Brokers)
	}
	if cfg.TopicPrefix != "ship" {
		t.Fatalf("prefix=%q", cfg.TopicPrefix)
	}
	if cfg.RedisURL != "redis://redis:6379/1" {
		t.Fatalf("redis=%q", cfg.RedisURL)
	}
	if cfg.ManifestPath != "/etc/handlers.yml" {
		t.Fatalf("manifest=%q", cfg.ManifestPath)
	}
	if cfg.ScheduleMySQLDSN != "user:pass@tcp(mysql)/sched" {
		t.Fatalf("schedule dsn=%q", cfg.ScheduleMySQLDSN)
	}
}

func TestApplyEnvEmptyLeavesDefaults(t *testing.T) {
	for _, key := range []string{
		"KAFKA_BROKERS", "KAFKA_PREFIX", "REDIS_URL",
		"KAFKA_BATCH_HANDLER_MANIFEST", "KAFKA_BATCH_SCHEDULE_MYSQL_DSN",
	} {
		t.Setenv(key, "")
	}
	cfg := DefaultConfig()
	cfg.Brokers = []string{"keep"}
	ApplyEnv(&cfg)
	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "keep" {
		t.Fatalf("brokers changed: %v", cfg.Brokers)
	}
}
