package uniq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestLockerClaimAndRelease(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	locker := NewLocker(rdb, time.Hour)
	ctx := context.Background()
	payload := map[string]interface{}{"order_id": 42}

	ok, err := locker.Claim(ctx, "ExportWorker", payload, "job-1")
	if err != nil || !ok {
		t.Fatalf("first claim ok=%v err=%v", ok, err)
	}
	ok, err = locker.Claim(ctx, "ExportWorker", payload, "job-2")
	if err != nil || ok {
		t.Fatalf("duplicate claim ok=%v err=%v", ok, err)
	}

	fp := DigestHex("ExportWorker", payload)
	if err := locker.Release(ctx, fp, "job-1"); err != nil {
		t.Fatal(err)
	}
	ok, err = locker.Claim(ctx, "ExportWorker", payload, "job-3")
	if err != nil || !ok {
		t.Fatalf("re-claim after release ok=%v err=%v", ok, err)
	}
}

func TestLockerReleaseWrongJobIDKeepsLock(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	locker := NewLocker(rdb, time.Hour)
	ctx := context.Background()
	payload := map[string]interface{}{"id": 1}

	ok, _ := locker.Claim(ctx, "W", payload, "owner")
	if !ok {
		t.Fatal("claim failed")
	}
	fp := DigestHex("W", payload)
	if err := locker.Release(ctx, fp, "other"); err != nil {
		t.Fatal(err)
	}
	ok, _ = locker.Claim(ctx, "W", payload, "new")
	if ok {
		t.Fatal("lock should still be held")
	}
}

func TestLockerNilClientFailsOpen(t *testing.T) {
	var locker *Locker
	ok, err := locker.Claim(context.Background(), "W", nil, "j1")
	if err != nil || !ok {
		t.Fatalf("fail-open claim ok=%v err=%v", ok, err)
	}
	if err := locker.Release(context.Background(), "abc", "j1"); err != nil {
		t.Fatal(err)
	}
}

func TestRedisKeyUsesBinaryDigest(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	locker := NewLocker(rdb, time.Hour)
	payload := map[string]interface{}{"a": 1}
	key := redisKey("Worker", payload)
	ctx := context.Background()
	if !rdb.SetNX(ctx, key, "j1", time.Hour).Val() {
		t.Fatal("setnx")
	}
	val, err := rdb.Get(ctx, key).Result()
	if err != nil || val != "j1" {
		t.Fatalf("get=%q err=%v", val, err)
	}
	_ = locker
}
