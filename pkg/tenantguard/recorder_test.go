package tenantguard

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestRecordAndWindowCounts(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)
	at := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 7; i++ {
		Record(ctx, rdb, "acme", OK, 300, at)
	}
	for i := 0; i < 3; i++ {
		Record(ctx, rdb, "acme", Fail, 300, at)
	}
	Record(ctx, rdb, "acme", Retry, 300, at)
	// A spread-out fail in an earlier minute, still inside the 300s window.
	Record(ctx, rdb, "acme", Fail, 300, at.Add(-2*time.Minute))
	// A different tenant must not bleed in.
	Record(ctx, rdb, "globex", Fail, 300, at)

	c := WindowCounts(ctx, rdb, "acme", 300, at)
	if c.OK != 7 || c.Fail != 4 || c.Retry != 1 {
		t.Fatalf("window counts = %+v, want ok=7 fail=4 retry=1", c)
	}

	// A bucket outside the window is excluded.
	old := WindowCounts(ctx, rdb, "acme", 60, at.Add(10*time.Minute))
	if old.OK != 0 || old.Fail != 0 {
		t.Fatalf("expected empty window far in the future, got %+v", old)
	}
}

func TestErrorRate(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)
	at := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 3; i++ {
		Record(ctx, rdb, "acme", OK, 300, at)
	}
	Record(ctx, rdb, "acme", Fail, 300, at)

	// 1 fail / 4 samples = 25%.
	rate, samples, ok := ErrorRate(ctx, rdb, "acme", 300, 4, false, at)
	if !ok || samples != 4 || rate < 24.9 || rate > 25.1 {
		t.Fatalf("rate=%v samples=%d ok=%v, want 25%% over 4", rate, samples, ok)
	}

	// Below min-samples ⇒ not actionable.
	if _, _, ok := ErrorRate(ctx, rdb, "acme", 300, 5, false, at); ok {
		t.Fatalf("expected not-ok below min_samples")
	}
}

func TestActiveTenants(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedis(t)
	at := time.Unix(1_700_000_000, 0).UTC()

	Record(ctx, rdb, "acme", OK, 300, at)
	Record(ctx, rdb, "globex", OK, 300, at.Add(-10*time.Minute)) // outside a 300s window

	got := ActiveTenants(ctx, rdb, 300, at)
	if len(got) != 1 || got[0] != "acme" {
		t.Fatalf("active tenants = %v, want [acme]", got)
	}
}

func TestMinuteStampMatchesRubyStrftime(t *testing.T) {
	// Ruby: Time.at(1700000000).utc.strftime("%Y%m%d%H%M") => "202311142213"
	if got := minuteStamp(1_700_000_000); got != "202311142213" {
		t.Fatalf("minuteStamp(1700000000) = %q, want 202311142213", got)
	}
	// Ruby: bucket_epoch(1700000059) => 1700000040 (22:14:00) => "202311142214"
	if got := bucketEpoch(1_700_000_059); got != 1_700_000_040 {
		t.Fatalf("bucketEpoch = %d, want 1700000040", got)
	}
	if got := minuteStamp(bucketEpoch(1_700_000_059)); got != "202311142214" {
		t.Fatalf("minuteStamp(bucketEpoch(...)) = %q, want 202311142214", got)
	}
}
