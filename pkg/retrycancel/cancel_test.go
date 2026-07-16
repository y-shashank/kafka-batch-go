package retrycancel

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCancelAcknowledge(t *testing.T) {
	mr := miniredis.RunT(t)
	s := &Store{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()}), TTL: time.Hour}
	ctx := context.Background()

	n, err := s.Cancel(ctx, []string{"j1", "j2", "j1"})
	if err != nil || n != 2 {
		t.Fatalf("Cancel n=%d err=%v", n, err)
	}
	if !s.Cancelled(ctx, "j1") || s.Cancelled(ctx, "j3") {
		t.Fatal("membership")
	}
	s.Acknowledge(ctx, "j1")
	if s.Cancelled(ctx, "j1") {
		t.Fatal("expected acknowledged")
	}
}

func TestSkipWatermarkAndClearCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	s := &Store{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	ctx := context.Background()

	_, _ = s.Cancel(ctx, []string{"a", "b"})
	if err := s.SetSkipWatermarks(ctx, map[string]map[int32]int64{
		"retry.short": {0: 10, 1: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearCancelSet(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Cancelled(ctx, "a") {
		t.Fatal("cancel set should be cleared")
	}
	if !s.ShouldSkip(ctx, "retry.short", 0, 10, "x") {
		t.Fatal("offset 10 should skip")
	}
	if s.ShouldSkip(ctx, "retry.short", 0, 11, "x") {
		t.Fatal("offset 11 should not skip via watermark")
	}
	_, _ = s.Cancel(ctx, []string{"z"})
	if !s.ShouldSkip(ctx, "retry.short", 0, 11, "z") {
		t.Fatal("cancelled job should skip")
	}
}
