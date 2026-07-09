package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStaleBatchesPrunesAdvanced(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	created, _ := st.CreateBatch(ctx, CreateBatchParams{ID: "done1", Sealed: true})
	if !created {
		t.Fatal("create failed")
	}
	_, _ = st.MarkFinishedIfRunning(ctx, "done1", "success")

	stale, err := st.StaleBatches(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected prune, got %+v", stale)
	}
}
