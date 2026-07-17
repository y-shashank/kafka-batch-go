package perfmetrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/perfmetrics"
)

func newTestWriter(t *testing.T) (*perfmetrics.Writer, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	w := perfmetrics.NewWriter(rdb, perfmetrics.Config{
		Enabled:       true,
		Retention:     time.Hour,
		BucketSeconds: 60 * time.Second,
		MaxJobTypes:   50,
		SampleRate:    1.0,
	})
	return w, rdb, mr
}

func TestRecordWritesAllAndJobTypeFields(t *testing.T) {
	w, rdb, _ := newTestWriter(t)
	ctx := context.Background()

	w.Record("processed", "FooWorker", 1)
	w.Record("processed", "FooWorker", 1)
	w.Record("failed", "FooWorker", 1)

	key := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":processed"
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	if vals[perfmetrics.AllField] != "2" {
		t.Fatalf("_all = %q, want 2", vals[perfmetrics.AllField])
	}
	if vals["FooWorker"] != "2" {
		t.Fatalf("FooWorker = %q, want 2", vals["FooWorker"])
	}

	failedKey := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":failed"
	fvals, err := rdb.HGetAll(ctx, failedKey).Result()
	if err != nil {
		t.Fatalf("hgetall failed: %v", err)
	}
	if fvals[perfmetrics.AllField] != "1" || fvals["FooWorker"] != "1" {
		t.Fatalf("failed bucket = %v", fvals)
	}

	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("ttl = %v, want (0, 1h]", ttl)
	}
}

func TestRecordWithoutJobTypeOnlyIncrementsAllField(t *testing.T) {
	w, rdb, _ := newTestWriter(t)
	ctx := context.Background()

	w.Record("reclaimed", "", 3)

	key := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":reclaimed"
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	if vals[perfmetrics.AllField] != "3" {
		t.Fatalf("_all = %q, want 3", vals[perfmetrics.AllField])
	}
	if len(vals) != 1 {
		t.Fatalf("vals = %v, want only _all field", vals)
	}
}

func TestFieldOverflowsToOtherPastMaxJobTypes(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	w := perfmetrics.NewWriter(rdb, perfmetrics.Config{
		Enabled:       true,
		Retention:     time.Hour,
		BucketSeconds: 60 * time.Second,
		MaxJobTypes:   1,
		SampleRate:    1.0,
	})
	ctx := context.Background()

	w.Record("processed", "FirstWorker", 1)
	w.Record("processed", "SecondWorker", 1)
	w.Record("processed", "FirstWorker", 1) // already known, still tracked by name

	key := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":processed"
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	if vals["FirstWorker"] != "2" {
		t.Fatalf("FirstWorker = %q, want 2", vals["FirstWorker"])
	}
	if vals["SecondWorker"] != "" {
		t.Fatalf("SecondWorker should have overflowed to _other, got %q", vals["SecondWorker"])
	}
	if vals[perfmetrics.OtherField] != "1" {
		t.Fatalf("_other = %q, want 1", vals[perfmetrics.OtherField])
	}
}

func TestHandleRoutesInstrumentationEventsToRecord(t *testing.T) {
	w, rdb, _ := newTestWriter(t)
	ctx := context.Background()

	w.Handle("job.processed", map[string]interface{}{"job_id": "j1", "worker_class": "FooWorker"}, 0)
	w.Handle("job.failed", map[string]interface{}{"job_id": "j2", "worker_class": "FooWorker"}, 0)
	w.Handle("job.retried", map[string]interface{}{"job_id": "j3", "worker_class": "FooWorker"}, 0)
	w.Handle("workset.reclaimed", map[string]interface{}{"checked": 5, "reclaimed": 2, "failed": 0, "skipped": 3}, 0)
	w.Handle("job.cancelled", map[string]interface{}{"job_id": "j4"}, 0) // untracked event: no-op

	for _, status := range []string{"processed", "failed", "retried"} {
		key := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":" + status
		vals, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatalf("hgetall %s: %v", status, err)
		}
		if vals[perfmetrics.AllField] != "1" {
			t.Fatalf("%s _all = %q, want 1", status, vals[perfmetrics.AllField])
		}
	}

	reclaimedKey := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":reclaimed"
	rvals, err := rdb.HGetAll(ctx, reclaimedKey).Result()
	if err != nil {
		t.Fatalf("hgetall reclaimed: %v", err)
	}
	if rvals[perfmetrics.AllField] != "2" {
		t.Fatalf("reclaimed _all = %q, want 2", rvals[perfmetrics.AllField])
	}
}

func TestHandlePrefersJobTypeOverWorkerClass(t *testing.T) {
	w, rdb, _ := newTestWriter(t)
	ctx := context.Background()

	w.Handle("job.processed", map[string]interface{}{
		"job_id":       "j1",
		"job_type":     "hello.go",
		"worker_class": "go:hello.go",
	}, 0)

	key := "kafka_batch:perf:min:" + itoa(w.BucketStart(time.Now())) + ":processed"
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	if vals["hello.go"] != "1" {
		t.Fatalf("hello.go = %q, want 1 (job_type should win); vals=%v", vals["hello.go"], vals)
	}
	if vals["go:hello.go"] != "" {
		t.Fatalf("should not write worker_class when job_type present; vals=%v", vals)
	}
}

func TestInstallAddsHandlerAndResetRemovesIt(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer perfmetrics.Reset()

	if err := perfmetrics.Install(perfmetrics.Config{Enabled: true, SampleRate: 1.0}, rdb); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !perfmetrics.Installed() {
		t.Fatalf("expected Installed() true after Install")
	}

	instrument.Emit("job.processed", map[string]interface{}{"job_id": "j1", "worker_class": "FooWorker"}, 0)

	ctx := context.Background()
	key := "kafka_batch:perf:min:" + itoa((time.Now().Unix()/60)*60) + ":processed"
	vals, err := rdb.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("hgetall: %v", err)
	}
	if vals[perfmetrics.AllField] != "1" {
		t.Fatalf("_all = %q, want 1 (Install should subscribe via instrument.AddHandler)", vals[perfmetrics.AllField])
	}

	perfmetrics.Reset()
	if perfmetrics.Installed() {
		t.Fatalf("expected Installed() false after Reset")
	}
}

func TestInstallIsNoOpWhenDisabled(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer perfmetrics.Reset()

	if err := perfmetrics.Install(perfmetrics.Config{Enabled: false}, rdb); err != nil {
		t.Fatalf("install: %v", err)
	}
	if perfmetrics.Installed() {
		t.Fatalf("expected Installed() false when Enabled=false")
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
