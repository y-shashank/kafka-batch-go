package cron

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestClaimAndAdvanceWithSqlmock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx := context.Background()
	now := at("2026-07-18T10:00:00Z")
	nextRun := at("2026-07-18T10:00:00Z")

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).AddRow(7, "hourly", "0 * * * *", "UTC", "hello.go", `{"x":1}`, "ten",
		1, "fire_once", nextRun, nil)
	mock.ExpectQuery("FROM kafka_batch_recurring_schedules").
		WithArgs(now.UTC(), 10).
		WillReturnRows(rows)
	mock.ExpectExec("INSERT IGNORE INTO kafka_batch_recurring_fires").
		WithArgs(int64(7), nextRun.UTC(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE kafka_batch_recurring_schedules SET next_run_at").
		WithArgs(sqlmock.AnyArg(), nextRun.UTC(), sqlmock.AnyArg(), int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	claimed, err := s.ClaimAndAdvance(ctx, now, 10, func(sc Schedule) (Plan, error) {
		return Plan{
			Fires:   []time.Time{sc.NextRunAt},
			NewNext: at("2026-07-18T11:00:00Z"),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ScheduleID != 7 || claimed[0].Name != "hourly" {
		t.Fatalf("claimed=%+v", claimed)
	}
	if claimed[0].TenantID != "ten" || claimed[0].Args["x"] != float64(1) {
		t.Fatalf("args/tenant %+v", claimed[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimAndAdvancePoisonSchedule(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx := context.Background()
	now := at("2026-07-18T10:00:00Z")

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).AddRow(3, "poison", "bad", "UTC", "j", nil, nil, 1, "fire_once", now, nil)
	mock.ExpectQuery("FROM kafka_batch_recurring_schedules").
		WillReturnRows(rows)
	mock.ExpectExec("SET enabled = 0").
		WithArgs(sqlmock.AnyArg(), int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	claimed, err := s.ClaimAndAdvance(ctx, now, 5, func(Schedule) (Plan, error) {
		return Plan{}, errPoison
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed=%+v", claimed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

var errPoison = errString("poison")

type errString string

func (e errString) Error() string { return string(e) }

func TestClaimAndAdvanceSkipPolicyNoFires(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx := context.Background()
	now := at("2026-07-18T10:00:00Z")

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).AddRow(1, "skip", "0 * * * *", "UTC", "j", "", nil, 1, "skip", now, nil)
	mock.ExpectQuery("FROM kafka_batch_recurring_schedules").WillReturnRows(rows)
	// No INSERT — only advance next_run_at.
	mock.ExpectExec("UPDATE kafka_batch_recurring_schedules SET next_run_at = \\?, updated_at").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	claimed, err := s.ClaimAndAdvance(ctx, now, 5, func(Schedule) (Plan, error) {
		return Plan{Fires: nil, NewNext: at("2026-07-18T11:00:00Z")}, nil
	})
	if err != nil || len(claimed) != 0 {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListAndRecoverPendingSqlmock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx := context.Background()
	last := at("2026-07-18T09:00:00Z")

	listRows := sqlmock.NewRows([]string{
		"id", "name", "cron_expr", "timezone", "job_type", "args_json", "tenant_id",
		"enabled", "misfire_policy", "next_run_at", "last_fire_at",
	}).AddRow(1, "a", "*/5 * * * *", "UTC", "j", nil, nil, 1, "fire_once", last, last)
	mock.ExpectQuery("ORDER BY name").WillReturnRows(listRows)

	got, err := s.List(ctx)
	if err != nil || len(got) != 1 || got[0].LastFire == nil {
		t.Fatalf("list=%+v err=%v", got, err)
	}

	recRows := sqlmock.NewRows([]string{
		"schedule_id", "fire_at", "name", "job_type", "tenant_id", "args_json",
	}).AddRow(1, last, "a", "j", "t", `{"k":true}`)
	mock.ExpectQuery("FROM kafka_batch_recurring_fires").
		WithArgs(sqlmock.AnyArg(), 50).
		WillReturnRows(recRows)

	pending, err := s.RecoverPending(ctx, time.Now().UTC(), 50)
	if err != nil || len(pending) != 1 || pending[0].TenantID != "t" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertInsertSqlmock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx := context.Background()

	mock.ExpectExec("INSERT INTO kafka_batch_recurring_schedules").
		WillReturnResult(sqlmock.NewResult(42, 1))

	id, err := s.Upsert(ctx, Schedule{
		Name: "n", CronExpr: "0 * * * *", JobType: "hello.go",
		Enabled: true, Args: map[string]interface{}{"a": 1}, TenantID: "t",
	})
	if err != nil || id != 42 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRealDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s := NewStoreDB(db)
	mock.ExpectClose()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
