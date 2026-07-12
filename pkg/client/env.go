package client

import (
	"os"
	"strings"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
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
	// Expand ${VAR} / ${VAR:-default} refs so the client can share the same
	// env-referenced DSN/URL as the control and execution roles.
	cfg.ScheduleMySQLDSN = config.ExpandEnv(cfg.ScheduleMySQLDSN)
}
