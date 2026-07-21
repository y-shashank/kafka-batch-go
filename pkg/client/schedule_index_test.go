package client

import (
	"testing"
)

func TestOpenScheduleIndex(t *testing.T) {
	idx, ms, err := openScheduleIndex(Config{ScheduleStore: ""})
	if err != nil || idx != nil || ms != nil {
		t.Fatalf("redis default idx=%v ms=%v err=%v", idx, ms, err)
	}
	idx, ms, err = openScheduleIndex(Config{ScheduleStore: "redis"})
	if err != nil || idx != nil || ms != nil {
		t.Fatalf("redis explicit idx=%v ms=%v err=%v", idx, ms, err)
	}

	_, _, err = openScheduleIndex(Config{ScheduleStore: "mysql"})
	if err == nil {
		t.Fatal("expected missing dsn")
	}
	if _, ok := err.(ConfigurationError); !ok {
		t.Fatalf("err=%v", err)
	}

	_, _, err = openScheduleIndex(Config{ScheduleStore: "mysql", ScheduleMySQLDSN: "bad-dsn"})
	if err == nil {
		t.Fatal("expected mysql open error")
	}

	_, _, err = openScheduleIndex(Config{ScheduleStore: "postgres"})
	if err == nil {
		t.Fatal("expected invalid store")
	}
	if _, ok := err.(ConfigurationError); !ok {
		t.Fatalf("err=%v", err)
	}
}
