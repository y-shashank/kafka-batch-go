package perfmetrics

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	rttStatus   = "rtt"
	rttLockKey  = "kafka_batch:perf:rtt:probe_lock"
	defaultRTTI = 15 * time.Second
	defaultRTTTO = 200 * time.Millisecond
)

// Ruby PerformanceMetrics::RECORD_RTT_LUA parity.
const recordRTTLua = `
redis.call('HINCRBY', KEYS[1], 'count', 1)
redis.call('HINCRBY', KEYS[1], 'sum_us', tonumber(ARGV[1]))
local cur = tonumber(redis.call('HGET', KEYS[1], 'max_us') or '0')
if tonumber(ARGV[1]) > cur then
  redis.call('HSET', KEYS[1], 'max_us', ARGV[1])
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`

// RTTProbeConfig configures the cluster-wide Redis RTT sampler.
type RTTProbeConfig struct {
	Enabled  bool
	Interval time.Duration
	Timeout  time.Duration
	// Retention / BucketSeconds mirror the Writer so RTT keys share EXPIRE
	// and bucket alignment with throughput hashes.
	Retention     time.Duration
	BucketSeconds time.Duration
}

var (
	rttMu     sync.Mutex
	rttCancel context.CancelFunc
	rttRunning bool
)

// StartRTTProbe runs a background ticker. Only the NX lock winner issues a
// timed PING and writes kafka_batch:perf:min:<bucket>:rtt each interval.
func StartRTTProbe(cfg RTTProbeConfig, client *redis.Client) {
	if !cfg.Enabled || client == nil {
		return
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultRTTI
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRTTTO
	}
	retention := cfg.Retention
	if retention <= 0 {
		retention = defaultRetain
	}
	bucket := cfg.BucketSeconds
	if bucket <= 0 {
		bucket = defaultBucket
	}

	rttMu.Lock()
	defer rttMu.Unlock()
	if rttRunning {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rttCancel = cancel
	rttRunning = true

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Immediate first attempt so the UI is not empty for a full interval.
		rttTick(client, timeout, retention, bucket, interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rttTick(client, timeout, retention, bucket, interval)
			}
		}
	}()
	log.Printf("[kbatch-perfmetrics] rtt probe started interval=%s timeout=%s", interval, timeout)
}

// StopRTTProbe cancels the background probe (tests / shutdown).
func StopRTTProbe() {
	rttMu.Lock()
	defer rttMu.Unlock()
	if rttCancel != nil {
		rttCancel()
		rttCancel = nil
	}
	rttRunning = false
}

// RTTProbeRunning reports whether StartRTTProbe has an active loop (tests).
func RTTProbeRunning() bool {
	rttMu.Lock()
	defer rttMu.Unlock()
	return rttRunning
}

func rttTick(client *redis.Client, timeout, retention, bucket, interval time.Duration) {
	lockTTL := interval * 2
	if lockTTL < 2*time.Second {
		lockTTL = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+50*time.Millisecond)
	defer cancel()

	ok, err := client.SetNX(ctx, rttLockKey, "1", lockTTL).Result()
	if err != nil || !ok {
		return
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), timeout)
	defer pingCancel()
	start := time.Now()
	if err := client.Ping(pingCtx).Err(); err != nil {
		recordRTTError(client, retention, bucket)
		return
	}
	elapsed := time.Since(start)
	us := elapsed.Microseconds()
	if us <= 0 {
		us = 1
	}
	if elapsed > timeout {
		recordRTTError(client, retention, bucket)
		return
	}
	recordRTT(client, us, retention, bucket)
}

func recordRTT(client *redis.Client, us int64, retention, bucket time.Duration) {
	key := rttBucketKey(bucket, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	retainSecs := int64(retention.Seconds())
	if retainSecs <= 0 {
		retainSecs = int64(defaultRetain.Seconds())
	}
	if err := client.Eval(ctx, recordRTTLua, []string{key}, us, retainSecs).Err(); err != nil {
		log.Printf("[kbatch-perfmetrics] rtt write failed: %v", err)
	}
}

func recordRTTError(client *redis.Client, retention, bucket time.Duration) {
	key := rttBucketKey(bucket, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HIncrBy(ctx, key, "errors", 1)
		pipe.Expire(ctx, key, retention)
		return nil
	})
	if err != nil {
		log.Printf("[kbatch-perfmetrics] rtt error write failed: %v", err)
	}
}

func rttBucketKey(bucket time.Duration, at time.Time) string {
	secs := int64(bucket.Seconds())
	if secs <= 0 {
		secs = 60
	}
	start := (at.Unix() / secs) * secs
	return KeyPrefix + strconv.FormatInt(start, 10) + ":" + rttStatus
}
