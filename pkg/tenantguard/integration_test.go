package tenantguard

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Gated integration test: writes real per-tenant error counters into a live
// Redis so the Ruby recorder can read the SAME keys back (cross-runtime parity).
// Run with:  TG_REDIS_ADDR=localhost:6379 go test ./pkg/tenantguard/ -run Integration -v
func TestIntegration_WriteForRubyToRead(t *testing.T) {
	addr := os.Getenv("TG_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TG_REDIS_ADDR to run the cross-runtime integration test")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}

	tenant := os.Getenv("TG_TENANT")
	if tenant == "" {
		tenant = "acme"
	}
	// Fixed instant so Ruby (reading with the same `at`) sums the same buckets.
	atUnix := int64(1_700_000_000)
	if v := os.Getenv("TG_AT"); v != "" {
		// keep simple: only override via env when provided as unix seconds
		if n, err := time.Parse(time.RFC3339, v); err == nil {
			atUnix = n.Unix()
		}
	}
	at := time.Unix(atUnix, 0).UTC()

	// Clean any prior run for this tenant/minute so counts are deterministic.
	rdb.Del(ctx, bucketKey(tenant, at))

	for i := 0; i < 7; i++ {
		Record(ctx, rdb, tenant, OK, 300, at)
	}
	for i := 0; i < 3; i++ {
		Record(ctx, rdb, tenant, Fail, 300, at)
	}
	Record(ctx, rdb, tenant, Retry, 300, at)

	c := WindowCounts(ctx, rdb, tenant, 300, at)
	if c.OK != 7 || c.Fail != 3 || c.Retry != 1 {
		t.Fatalf("go readback = %+v, want ok=7 fail=3 retry=1", c)
	}
	t.Logf("wrote tenant=%s at=%d key=%s ok=7 fail=3 retry=1 (Ruby should read the same)",
		tenant, atUnix, bucketKey(tenant, at))
}
