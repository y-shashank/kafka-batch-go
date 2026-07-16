package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCancelBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	created, err := st.CreateBatch(ctx, CreateBatchParams{ID: "b1", Sealed: true})
	if err != nil || !created {
		t.Fatalf("create %v %v", created, err)
	}
	if err := st.CancelBatch(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	row, err := st.FindBatch(ctx, "b1")
	if err != nil || row == nil || row.Status != "cancelled" {
		t.Fatalf("row %+v err %v", row, err)
	}
	cancelled, err := st.BatchCancelled(ctx, "b1")
	if err != nil || !cancelled {
		t.Fatalf("cancelled=%v err=%v", cancelled, err)
	}
}

func TestCancelBatchNotFound(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	if err := st.CancelBatch(context.Background(), "missing"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCancelledBatchIDs(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	for _, id := range []string{"b1", "b2"} {
		if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: id, Sealed: true}); err != nil {
			t.Fatal(err)
		}
		if err := st.CancelBatch(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := st.CancelledBatchIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if !got["b1"] || !got["b2"] {
		t.Fatalf("ids=%v", ids)
	}
}
