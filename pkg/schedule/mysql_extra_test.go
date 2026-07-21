package schedule

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewMysqlStoreErrorsAndClose(t *testing.T) {
	if _, err := NewMysqlStore("mysql://%zz", 10); err == nil {
		t.Fatal("expected bad URL error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewMysqlStore("nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s", 0)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ping failure")
		}
	case <-ctx.Done():
		t.Fatal("NewMysqlStore hung")
	}

	st := &MysqlStore{}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMysqlScheduleManyEmptyAndAckEmpty(t *testing.T) {
	st := &MysqlStore{}
	if err := st.ScheduleMany(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestMysqlStoreSqlmockPaths(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := &MysqlStore{db: db, limit: 100}
	ctx := context.Background()
	now := time.Unix(2000, 0)

	mock.ExpectExec("INSERT INTO kafka_batch_scheduled_jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := st.Schedule(ctx, "j1", now, 0, 5, "b1"); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO kafka_batch_scheduled_jobs").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	if err := st.ScheduleMany(ctx, []ScheduleEntry{{
		JobID: "j2", RunAt: now, Partition: 1, Offset: 2, BatchID: "b2",
	}}); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	rows := sqlmock.NewRows([]string{"job_id", "partition_id", "kafka_offset"}).
		AddRow("j1", 0, 5)
	mock.ExpectQuery("FROM kafka_batch_scheduled_jobs").WillReturnRows(rows)
	mock.ExpectExec("UPDATE kafka_batch_scheduled_jobs SET lease_until").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	claimed, err := st.ClaimDue(ctx, now, 60, 10)
	if err != nil || len(claimed) != 1 || claimed[0] != "j1:0:5" {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}

	mock.ExpectBegin()
	empty := sqlmock.NewRows([]string{"job_id", "partition_id", "kafka_offset"})
	mock.ExpectQuery("FROM kafka_batch_scheduled_jobs").WillReturnRows(empty)
	mock.ExpectCommit()
	claimed, err = st.ClaimDue(ctx, now, 60, 10)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("empty claim=%v err=%v", claimed, err)
	}

	mock.ExpectExec("DELETE FROM kafka_batch_scheduled_jobs").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := st.Ack(ctx, []string{"bad-member", "j1:0:5"}); err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec("UPDATE kafka_batch_scheduled_jobs SET lease_until = NULL").
		WillReturnResult(sqlmock.NewResult(0, 3))
	n, err := st.Reclaim(ctx, now)
	if err != nil || n != 3 {
		t.Fatalf("reclaim n=%d err=%v", n, err)
	}

	mock.ExpectClose()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMysqlStoreDeadDBErrors(t *testing.T) {
	db, err := sql.Open("mysql", "nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := &MysqlStore{db: db, limit: 10}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := st.Schedule(ctx, "j", time.Now(), 0, 1, ""); err == nil {
		t.Fatal("Schedule")
	}
}
