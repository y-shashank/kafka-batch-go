package client

import (
	"os"
	"strings"
)

// ApplyEnv overlays deployment environment variables onto cfg (same names as daemon/worker).
// YAML values are the base; env wins when set.
func ApplyEnv(cfg *Config) {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_PREFIX"); v != "" {
		cfg.TopicPrefix = strings.TrimSpace(v)
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("KAFKA_BATCH_HANDLER_MANIFEST"); v != "" {
		cfg.ManifestPath = v
	}
	if v := os.Getenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN"); v != "" {
		cfg.ScheduleMySQLDSN = v
	}
}
