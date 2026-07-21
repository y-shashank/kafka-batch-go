package cron

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testLockRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return rdb, mr
}

func TestNewLockDefaultTTL(t *testing.T) {
	rdb, _ := testLockRedis(t)
	lock := NewLock(rdb, 0)
	if lock.ttl != 60*time.Second {
		t.Fatalf("default ttl = %s, want 60s", lock.ttl)
	}
	lockNeg := NewLock(rdb, -time.Second)
	if lockNeg.ttl != 60*time.Second {
		t.Fatalf("negative ttl = %s, want 60s", lockNeg.ttl)
	}
}

func TestLockAcquireAndRelease(t *testing.T) {
	rdb, _ := testLockRedis(t)
	ctx := context.Background()
	lock := NewLock(rdb, time.Minute)

	token, ok, err := lock.Acquire(ctx)
	if err != nil || !ok || token == "" {
		t.Fatalf("first Acquire ok=%v token=%q err=%v", ok, token, err)
	}

	_, ok2, err := lock.Acquire(ctx)
	if err != nil {
		t.Fatalf("second Acquire err=%v", err)
	}
	if ok2 {
		t.Fatal("second Acquire should fail while lock held")
	}

	lock.Release(ctx, "wrong-token")
	_, ok3, err := lock.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after wrong Release err=%v", err)
	}
	if ok3 {
		t.Fatal("wrong-token Release should keep the lock")
	}

	lock.Release(ctx, token)
	token2, ok4, err := lock.Acquire(ctx)
	if err != nil || !ok4 || token2 == "" {
		t.Fatalf("re-acquire after correct Release ok=%v token=%q err=%v", ok4, token2, err)
	}

	lock.Release(ctx, "") // empty token is a no-op
	_, ok5, err := lock.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire after empty Release err=%v", err)
	}
	if ok5 {
		t.Fatal("empty-token Release should leave the lock held")
	}
}
