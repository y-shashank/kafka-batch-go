package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCompositeFailuresClearFailureRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	comp := &CompositeFailures{Redis: st}
	ctx := context.Background()

	_ = comp.RecordFailure(ctx, FailureEntry{
		BatchID: "b1", JobID: "j1", Status: "retrying", WorkerClass: "w",
	})
	if err := comp.ClearFailure(ctx, "b1", "j1"); err != nil {
		t.Fatal(err)
	}
	n, err := rdb.HLen(ctx, "kafka_batch:b:b1:failures").Result()
	if err != nil || n != 0 {
		t.Fatalf("expected empty failures hash, n=%d err=%v", n, err)
	}
}
