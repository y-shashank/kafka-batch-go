package cron

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestDecodeArgs(t *testing.T) {
	if m := decodeArgs(sql.NullString{}); len(m) != 0 {
		t.Fatalf("invalid: %v", m)
	}
	if m := decodeArgs(sql.NullString{Valid: true, String: ""}); len(m) != 0 {
		t.Fatalf("empty: %v", m)
	}
	if m := decodeArgs(sql.NullString{Valid: true, String: "not-json"}); len(m) != 0 {
		t.Fatalf("bad json: %v", m)
	}
	if m := decodeArgs(sql.NullString{Valid: true, String: "null"}); len(m) != 0 {
		t.Fatalf("json null: %v", m)
	}
	m := decodeArgs(sql.NullString{Valid: true, String: `{"k":"v"}`})
	if m["k"] != "v" {
		t.Fatalf("got %v", m)
	}
}

func TestNewStoreDBAndClose(t *testing.T) {
	s := NewStoreDB(nil)
	if s == nil || s.db != nil {
		t.Fatal("NewStoreDB(nil)")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close nil db: %v", err)
	}
}

func TestNewStoreErrors(t *testing.T) {
	if _, err := NewStore("mysql://%zz"); err == nil {
		t.Fatal("expected Normalize/Open error for bad URL")
	}
	// Unreachable host: Open succeeds, Ping fails quickly with a refused/timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewStore("nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s&readTimeout=1s&writeTimeout=1s")
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ping failure")
		}
	case <-ctx.Done():
		t.Fatal("NewStore ping hung")
	}
}

func TestUpsertValidation(t *testing.T) {
	s := NewStoreDB(nil) // validation runs before any DB call
	ctx := context.Background()

	if _, err := s.Upsert(ctx, Schedule{}); err == nil {
		t.Fatal("expected required-fields error")
	}
	if _, err := s.Upsert(ctx, Schedule{Name: "n", CronExpr: "0 * * * *"}); err == nil {
		t.Fatal("expected job_type required")
	}
	if _, err := s.Upsert(ctx, Schedule{
		Name: "n", CronExpr: "0 * * * *", JobType: "j", Misfire: "nope",
	}); err == nil {
		t.Fatal("expected invalid misfire")
	}
	if _, err := s.Upsert(ctx, Schedule{
		Name: "n", CronExpr: "not a cron", JobType: "j",
	}); err == nil {
		t.Fatal("expected bad cron")
	}
	if _, err := s.Upsert(ctx, Schedule{
		Name: "n", CronExpr: "0 * * * *", JobType: "j", Timezone: "Not/AZone",
	}); err == nil {
		t.Fatal("expected bad timezone")
	}
	if _, err := s.Upsert(ctx, Schedule{
		Name: "n", CronExpr: "0 0 30 2 *", JobType: "j",
	}); err == nil {
		t.Fatal("expected never-fires error")
	}
}

func TestClaimAndAdvanceDefaultLimit(t *testing.T) {
	// sql.Open without a live server: BeginTx fails → exercises limit<=0 branch then DB error.
	db, err := sql.Open("mysql", "nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(time.Second)
	s := NewStoreDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = s.ClaimAndAdvance(ctx, time.Now().UTC(), 0, func(Schedule) (Plan, error) {
		return Plan{}, nil
	})
	if err == nil {
		t.Fatal("expected ClaimAndAdvance error against dead MySQL")
	}
}

func TestRecoverPendingDefaultLimit(t *testing.T) {
	db, err := sql.Open("mysql", "nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = s.RecoverPending(ctx, time.Now().UTC(), 0)
	if err == nil {
		t.Fatal("expected RecoverPending error")
	}
}

func TestStoreMethodsErrorOnDeadDB(t *testing.T) {
	db, err := sql.Open("mysql", "nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewStoreDB(db)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.EnsureSchema(ctx); err == nil {
		t.Fatal("EnsureSchema")
	}
	if _, err := s.List(ctx); err == nil {
		t.Fatal("List")
	}
	if err := s.SetEnabled(ctx, "x", true); err == nil {
		t.Fatal("SetEnabled true")
	}
	if err := s.SetEnabled(ctx, "x", false); err == nil {
		t.Fatal("SetEnabled false")
	}
	if err := s.Delete(ctx, "x"); err == nil {
		t.Fatal("Delete")
	}
	if err := s.MarkDispatched(ctx, 1, time.Now().UTC(), "jid"); err == nil {
		t.Fatal("MarkDispatched")
	}
	if _, err := s.Prune(ctx, time.Now().UTC()); err == nil {
		t.Fatal("Prune")
	}
}
