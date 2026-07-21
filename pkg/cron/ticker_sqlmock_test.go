package cron

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHeartbeatFlagsStale(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := at("2026-07-18T10:30:00Z")
	freshLast := at("2026-07-18T10:25:00Z") // within 10m for */5
	staleLast := at("2026-07-18T10:00:00Z") // 30m stale
	disabledLast := at("2026-07-18T09:00:00Z")

	rows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).
		AddRow(1, "fresh", "*/5 * * * *", "UTC", "j", nil, nil, 1, "fire_once", now, freshLast).
		AddRow(2, "stale", "*/5 * * * *", "UTC", "j", nil, nil, 1, "fire_once", now, staleLast).
		AddRow(3, "off", "*/5 * * * *", "UTC", "j", nil, nil, 0, "fire_once", now, disabledLast).
		AddRow(4, "badcron", "nope", "UTC", "j", nil, nil, 1, "fire_once", now, nil)

	mock.ExpectQuery("ORDER BY name").WillReturnRows(rows)

	events := captureEvents()
	defer events.stop()

	tk := &Ticker{Store: NewStoreDB(db), StaleFactor: 2.0}
	if err := tk.heartbeat(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if events.count("cron.stale", "schedule", "stale") != 1 {
		t.Errorf("stale events: %v", events.names())
	}
	if events.count("cron.stale", "schedule", "fresh") != 0 {
		t.Error("fresh should not be stale")
	}
	if events.count("cron.heartbeat", "", "") != 1 {
		t.Error("expected heartbeat")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchDueAndRecoverSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewStoreDB(db)
	enq := &stubEnqueuer{}
	tk := &Ticker{
		Store: store, Enqueuer: enq,
		BatchSize: 10, MisfireGrace: time.Minute, MaxBackfill: 10,
		RecoverGrace: time.Minute,
	}
	tk.applyDefaults()
	ctx := context.Background()
	now := at("2026-07-18T10:00:20Z")
	due := at("2026-07-18T10:00:00Z")

	// dispatchDue → ClaimAndAdvance
	mock.ExpectBegin()
	dueRows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).AddRow(9, "n", "0 * * * *", "UTC", "hello.go", nil, "t", 1, "fire_once", due, nil)
	mock.ExpectQuery("FROM kafka_batch_recurring_schedules").WillReturnRows(dueRows)
	mock.ExpectExec("INSERT IGNORE INTO kafka_batch_recurring_fires").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE kafka_batch_recurring_schedules SET next_run_at").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectExec("UPDATE kafka_batch_recurring_fires").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := tk.dispatchDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	if enq.calls.Load() != 1 {
		t.Fatalf("enqueue calls=%d", enq.calls.Load())
	}

	// recover → RecoverPending + enqueue
	fireAt := at("2026-07-18T09:00:00Z")
	recRows := sqlmock.NewRows([]string{
		"schedule_id", "fire_at", "name", "job_type", "tenant_id", "args_json",
	}).AddRow(9, fireAt, "n", "hello.go", "t", nil)
	mock.ExpectQuery("FROM kafka_batch_recurring_fires").WillReturnRows(recRows)
	mock.ExpectExec("UPDATE kafka_batch_recurring_fires").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := tk.recover(ctx, now); err != nil {
		t.Fatal(err)
	}
	if enq.calls.Load() != 2 {
		t.Fatalf("after recover calls=%d", enq.calls.Load())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
