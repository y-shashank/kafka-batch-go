package config

import (
	"fmt"
	"strings"
)

// ValidateMySQLConfig ensures MySQL-backed settings have a DSN before the daemon
// starts consumers. Callers should also open/ping the store (daemon does this
// when wiring the schedule poller and optional store backends).
func (c Daemon) ValidateMySQLConfig() error {
	if c.SchedulePollerEnabled && strings.EqualFold(strings.TrimSpace(c.ScheduleStore), "mysql") {
		if strings.TrimSpace(c.ScheduleMySQLDSN) == "" {
			return fmt.Errorf("schedule_mysql_dsn is required when schedule_poller_enabled and schedule_store is mysql")
		}
	}
	if strings.EqualFold(strings.TrimSpace(c.Store), "mysql") {
		if strings.TrimSpace(c.StoreMySQLDSN) == "" {
			return fmt.Errorf("store_mysql_dsn is required when store is mysql")
		}
	}
	return nil
}
