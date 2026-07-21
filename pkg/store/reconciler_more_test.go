package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStaleBatchesReturnsRunning(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: "stale-1", Sealed: true}); err != nil {
		t.Fatal(err)
	}
	// Score in the past so olderThan=now includes it.
	mr.ZAdd(runningIndex, 1, "stale-1")

	stale, err := st.StaleBatches(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].ID != "stale-1" {
		t.Fatalf("stale=%+v", stale)
	}

	var nilStore *RedisStore
	if _, err := nilStore.StaleBatches(ctx, time.Now()); err == nil {
		t.Fatal("expected nil store error")
	}
}

func TestDoneBatchesWithoutCallback(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	id := "done-pending"
	// Finished batch awaiting callback (no claim stamps).
	mr.HSet("kafka_batch:b:"+id,
		"id", id, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0", "touched_count", "1",
	)
	mr.ZAdd(doneIndex, 1, id)

	pending, err := st.DoneBatchesWithoutCallback(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("pending=%+v", pending)
	}

	mr.HSet("kafka_batch:b:"+id, "callback_dispatched_at", time.Now().UTC().Format(time.RFC3339))
	mr.ZAdd(doneIndex, 1, id)
	pending, err = st.DoneBatchesWithoutCallback(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected prune of claimed, got %+v", pending)
	}
}

func TestWithReconcilerLock(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()

	ran := false
	ok, err := st.WithReconcilerLock(ctx, time.Second, func() error {
		ran = true
		return nil
	})
	if err != nil || !ok || !ran {
		t.Fatalf("first lock ok=%v ran=%v err=%v", ok, ran, err)
	}

	// Hold lock and ensure second caller loses.
	held := make(chan struct{})
	release := make(chan struct{})
	released := make(chan struct{})
	go func() {
		_, _ = st.WithReconcilerLock(ctx, 5*time.Second, func() error {
			close(held)
			<-release
			return nil
		})
		close(released)
	}()
	<-held
	ok, err = st.WithReconcilerLock(ctx, time.Second, func() error {
		t.Fatal("should not run under contended lock")
		return nil
	})
	if err != nil || ok {
		t.Fatalf("contended lock ok=%v err=%v", ok, err)
	}
	close(release)
	<-released

	// fn error is returned with acquired=true.
	ok, err = st.WithReconcilerLock(ctx, time.Second, func() error {
		return errors.New("boom")
	})
	if !ok || err == nil || err.Error() != "boom" {
		t.Fatalf("fn error ok=%v err=%v", ok, err)
	}

	var nilStore *RedisStore
	if _, err := nilStore.WithReconcilerLock(ctx, time.Second, func() error { return nil }); err == nil {
		t.Fatal("expected nil store error")
	}
}

func TestReconcileBatchCounts(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: "c1", Sealed: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: "c2", Sealed: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkFinishedIfRunning(ctx, "c2", "success"); err != nil {
		t.Fatal(err)
	}
	// Orphan index entry with no hash.
	mr.ZAdd(allIndex, 1, "ghost")

	if err := st.ReconcileBatchCounts(ctx); err != nil {
		t.Fatal(err)
	}
	running, _ := rdb.HGet(ctx, countsKey, "running").Int64()
	success, _ := rdb.HGet(ctx, countsKey, "success").Int64()
	if running != 1 || success != 1 {
		t.Fatalf("counts running=%d success=%d", running, success)
	}
	if mr.Exists(allIndex) {
		members, _ := rdb.ZRange(ctx, allIndex, 0, -1).Result()
		for _, m := range members {
			if m == "ghost" {
				t.Fatal("ghost should be pruned from all index")
			}
		}
	}

	var nilStore *RedisStore
	if err := nilStore.ReconcileBatchCounts(ctx); err == nil {
		t.Fatal("expected nil store error")
	}
}

func TestSealBatchDoneAndNotFound(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()

	seal, err := st.SealBatch(ctx, "missing")
	if err != nil || seal.Status != "not_found" {
		t.Fatalf("seal=%+v err=%v", seal, err)
	}

	id := "seal-done"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: id, TotalJobs: 0, Sealed: false}); err != nil {
		t.Fatal(err)
	}
	// Empty batch (total 0) seals to done when sealed with zero jobs.
	seal, err = st.SealBatch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if seal.Status != "done" && seal.Status != "sealed" {
		t.Fatalf("unexpected seal status %+v", seal)
	}
}

func TestAddJobsNotFoundAndCancelled(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()

	add, err := st.AddJobs(ctx, "ghost", 1)
	if err != nil || add.Status != "not_found" {
		t.Fatalf("add=%+v err=%v", add, err)
	}

	id := "cancel-add"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: id, Sealed: false}); err != nil {
		t.Fatal(err)
	}
	if err := st.CancelBatch(ctx, id); err != nil {
		t.Fatal(err)
	}
	add, err = st.AddJobs(ctx, id, 1)
	if err != nil || add.Status != "cancelled" {
		t.Fatalf("add cancelled=%+v err=%v", add, err)
	}
}

func TestCreateBatchDuplicate(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()
	ok, err := st.CreateBatch(ctx, CreateBatchParams{ID: "dup", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("first create ok=%v err=%v", ok, err)
	}
	ok, err = st.CreateBatch(ctx, CreateBatchParams{ID: "dup", Sealed: true})
	if err != nil || ok {
		t.Fatalf("duplicate create ok=%v err=%v", ok, err)
	}
}

func TestReleaseUniqLockNilStore(t *testing.T) {
	var nilStore *RedisStore
	if err := nilStore.ReleaseUniqLock(context.Background(), "aa", "j1"); err != nil {
		t.Fatal(err)
	}
}

func TestHashToBatchCallbackClaimed(t *testing.T) {
	b := hashToBatch(map[string]string{
		"id": "x", "status": "success", "total_jobs": "1",
		"complete_callback_dispatched_at": "2020-01-01T00:00:00Z",
	})
	if b == nil || !b.CallbackClaimed {
		t.Fatalf("expected claimed: %+v", b)
	}
	if hashToBatch(nil) != nil {
		t.Fatal("empty hash")
	}
}

func TestRandomToken(t *testing.T) {
	a, err := randomToken(8)
	if err != nil || len(a) != 16 {
		t.Fatalf("token=%q err=%v", a, err)
	}
}
