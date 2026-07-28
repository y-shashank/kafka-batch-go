// Package tenantguard implements the per-tenant error-rate window that backs
// the tenant guard (per-tenant pause/throttle for fairness jobs). It is the Go
// side of the shared Redis contract documented in the Ruby kafka-batch README
// ("Tenant guard"): both runtimes read/write the same minute-bucket hashes so
// error rates aggregate across a mixed Ruby+Go fleet.
//
// Everything here is fire-and-forget — a Redis error never propagates into the
// job execution path.
package tenantguard

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	errorPrefix = "kafka_batch:tenant_errors:"
	// activeKey is a ZSET of tenants seen recently (scored by last-seen epoch)
	// so the evaluator can enumerate candidates cheaply instead of SCANning.
	activeKey = "kafka_batch:tenant_errors:active"
	// skewSeconds keeps a bucket alive a bit past the window so the tick that
	// reads it can still sum it (clock skew + evaluator interval slack).
	skewSeconds = 120
)

// Outcome is the classification recorded for a job. Must match the Ruby
// recorder field names ("ok" | "fail" | "retry").
type Outcome string

const (
	OK    Outcome = "ok"
	Fail  Outcome = "fail"
	Retry Outcome = "retry"
)

// Counts is a windowed total across the minute buckets overlapping a window.
type Counts struct {
	OK    int64
	Fail  int64
	Retry int64
}

// Record increments the per-(tenant, minute) bucket and marks the tenant active.
// No-op on a nil client or empty tenant (plain jobs carry no tenant).
func Record(ctx context.Context, rdb *redis.Client, tenantID string, outcome Outcome, windowSeconds int, at time.Time) {
	if rdb == nil || tenantID == "" {
		return
	}
	switch outcome {
	case OK, Fail, Retry:
	default:
		return
	}
	ttl := time.Duration(normalizedWindow(windowSeconds)+skewSeconds) * time.Second
	key := bucketKey(tenantID, at)
	pipe := rdb.Pipeline()
	pipe.HIncrBy(ctx, key, string(outcome), 1)
	pipe.Expire(ctx, key, ttl)
	pipe.ZAdd(ctx, activeKey, redis.Z{Score: float64(at.Unix()), Member: tenantID})
	_, _ = pipe.Exec(ctx)
}

// WindowCounts sums every minute bucket overlapping the window ending at `at`.
func WindowCounts(ctx context.Context, rdb *redis.Client, tenantID string, windowSeconds int, at time.Time) Counts {
	var c Counts
	if rdb == nil || tenantID == "" {
		return c
	}
	win := normalizedWindow(windowSeconds)
	start := bucketEpoch(at.Unix() - int64(win))
	end := bucketEpoch(at.Unix())

	pipe := rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, 0)
	for b := start; b <= end; b += 60 {
		cmds = append(cmds, pipe.HGetAll(ctx, errorPrefix+tenantID+":"+minuteStamp(b)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return c
	}
	for _, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil || len(m) == 0 {
			continue
		}
		c.OK += parseInt(m["ok"])
		c.Fail += parseInt(m["fail"])
		c.Retry += parseInt(m["retry"])
	}
	return c
}

// ActiveTenants returns tenants with recorded activity within `withinSeconds`,
// pruning stale members as it reads so the index stays bounded.
func ActiveTenants(ctx context.Context, rdb *redis.Client, withinSeconds int, at time.Time) []string {
	if rdb == nil {
		return nil
	}
	win := normalizedWindow(withinSeconds)
	floor := at.Unix() - int64(win)
	pruneBelow := floor - skewSeconds
	if pruneBelow < 0 {
		pruneBelow = 0
	}
	_ = rdb.ZRemRangeByScore(ctx, activeKey, "0", strconv.FormatInt(pruneBelow, 10)).Err()
	res, err := rdb.ZRangeByScore(ctx, activeKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(floor, 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil
	}
	return res
}

// ErrorRate returns fail/(ok+fail)*100 for the window. ok=false when there are
// fewer than minSamples in the denominator (not enough signal to act on).
func ErrorRate(ctx context.Context, rdb *redis.Client, tenantID string, windowSeconds, minSamples int, includeRetries bool, at time.Time) (rate float64, samples int64, ok bool) {
	c := WindowCounts(ctx, rdb, tenantID, windowSeconds, at)
	fails := c.Fail
	if includeRetries {
		fails += c.Retry
	}
	denom := c.OK + fails
	if denom == 0 || denom < int64(minSamples) {
		return 0, denom, false
	}
	return float64(fails) / float64(denom) * 100.0, denom, true
}

func bucketKey(tenantID string, at time.Time) string {
	return errorPrefix + tenantID + ":" + minuteStamp(bucketEpoch(at.Unix()))
}

func bucketEpoch(epoch int64) int64 { return (epoch / 60) * 60 }

// minuteStamp is yyyymmddHHmm in UTC — MUST match the Ruby recorder
// (strftime "%Y%m%d%H%M").
func minuteStamp(epoch int64) string {
	return time.Unix(epoch, 0).UTC().Format("200601021504")
}

func normalizedWindow(windowSeconds int) int {
	if windowSeconds < 60 {
		return 60
	}
	return windowSeconds
}

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
