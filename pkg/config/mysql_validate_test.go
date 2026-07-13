package config

import "testing"

func TestValidateMySQLConfigScheduleDSNRequired(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.SchedulePollerEnabled = true
	cfg.ScheduleStore = "mysql"
	if err := cfg.ValidateMySQLConfig(); err == nil {
		t.Fatal("expected error for missing schedule_mysql_dsn")
	}
}

func TestValidateMySQLConfigStoreDSNRequired(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.Store = "mysql"
	if err := cfg.ValidateMySQLConfig(); err == nil {
		t.Fatal("expected error for missing store_mysql_dsn")
	}
}

func TestValidateMySQLConfigOK(t *testing.T) {
	cfg := DefaultDaemon()
	cfg.SchedulePollerEnabled = true
	cfg.ScheduleStore = "mysql"
	cfg.ScheduleMySQLDSN = "mysql2://root:@127.0.0.1:3306/kb"
	cfg.Store = "mysql"
	cfg.StoreMySQLDSN = "mysql2://root:@127.0.0.1:3306/kb"
	if err := cfg.ValidateMySQLConfig(); err != nil {
		t.Fatal(err)
	}
}
