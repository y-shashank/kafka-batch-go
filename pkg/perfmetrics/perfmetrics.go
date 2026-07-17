// Package perfmetrics is the Go counterpart of Ruby's
// KafkaBatch::PerformanceMetrics: an opt-in, best-effort Redis-backed
// throughput/error-rate history for the Web UI's Performance page.
//
// It subscribes to the job.processed / job.retried / job.failed /
// workset.reclaimed instrumentation events via instrument.AddHandler (so it
// coexists with metrics.Install rather than replacing it — see
// pkg/instrument) and writes HINCRBY counters into the exact same per-bucket
// Redis hash layout the Ruby writer uses, so a mixed Ruby+Go deployment
// shares one Performance dashboard:
//
//	kafka_batch:perf:min:<bucket_start_epoch>:<processed|failed|retried|reclaimed>
//
// Hash field = job_type (worker_class), plus "_all" (system total) and
// "_other" (overflow once MaxJobTypes distinct job types have been seen).
//
// Disabled by default. Enable via config.PerformanceMetricsEnabled (YAML
// performance_metrics_enabled, or KAFKA_BATCH_PERFORMANCE_METRICS_ENABLED).
package perfmetrics

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

const (
	KeyPrefix       = "kafka_batch:perf:min:"
	AllField        = "_all"
	OtherField      = "_other"
	maxFieldBytes   = 200
	writeTimeout    = 500 * time.Millisecond
	defaultRetain   = 24 * time.Hour
	defaultBucket   = 60 * time.Second
	defaultMaxTypes = 50
)

// Statuses mirrors Ruby PerformanceMetrics::STATUSES.
var Statuses = []string{"processed", "failed", "retried", "reclaimed"}

// Config configures the perf writer. Ruby parity: config.performance_metrics_*.
type Config struct {
	Enabled       bool
	Retention     time.Duration
	BucketSeconds time.Duration
	MaxJobTypes   int
	SampleRate    float64
}

// FromDaemon maps daemon YAML/env settings to a perfmetrics Config.
func FromDaemon(cfg config.Daemon) Config {
	return Config{
		Enabled:       cfg.PerformanceMetricsEnabled,
		Retention:     cfg.PerformanceMetricsRetention,
		BucketSeconds: cfg.PerformanceMetricsBucketSeconds,
		MaxJobTypes:   cfg.PerformanceMetricsMaxJobTypes,
		SampleRate:    cfg.PerformanceMetricsSampleRate,
	}
}

// Writer is the best-effort event handler; never panics or blocks the
// caller on Redis errors (writes use a short timeout and are fire-and-log).
type Writer struct {
	client        *redis.Client
	retention     time.Duration
	bucketSeconds time.Duration
	maxJobTypes   int
	sampleRate    float64

	mu    sync.Mutex
	known map[string]struct{}
}

// NewWriter builds a Writer, normalizing zero-value Config fields to the
// same defaults as Ruby (retention 24h, bucket 60s, max_job_types 50, sample_rate 1.0).
func NewWriter(client *redis.Client, cfg Config) *Writer {
	retention := cfg.Retention
	if retention <= 0 {
		retention = defaultRetain
	}
	bucket := cfg.BucketSeconds
	if bucket <= 0 {
		bucket = defaultBucket
	}
	maxTypes := cfg.MaxJobTypes
	if maxTypes <= 0 {
		maxTypes = defaultMaxTypes
	}
	rate := cfg.SampleRate
	if rate <= 0 || rate > 1.0 {
		rate = 1.0
	}
	return &Writer{
		client:        client,
		retention:     retention,
		bucketSeconds: bucket,
		maxJobTypes:   maxTypes,
		sampleRate:    rate,
		known:         make(map[string]struct{}),
	}
}

var (
	mu         sync.Mutex
	installed  bool
	removeFunc func()
)

// Install registers the perf writer as an instrument.AddHandler subscriber
// when cfg.Enabled and client are both present. It coexists with
// metrics.Install (both subscribe independently via AddHandler). Safe to
// call multiple times — subsequent calls while already installed are no-ops.
func Install(cfg Config, client *redis.Client) error {
	mu.Lock()
	defer mu.Unlock()
	if !cfg.Enabled || client == nil {
		return nil
	}
	if installed {
		return nil
	}
	w := NewWriter(client, cfg)
	removeFunc = instrument.AddHandler(w.Handle)
	installed = true
	log.Printf("[kbatch-perfmetrics] installed retention=%s bucket=%s max_job_types=%d sample_rate=%.2f",
		w.retention, w.bucketSeconds, w.maxJobTypes, w.sampleRate)
	return nil
}

// Reset unregisters the perf writer handler (tests / graceful shutdown).
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	if installed && removeFunc != nil {
		removeFunc()
	}
	installed = false
	removeFunc = nil
}

// Installed reports whether Install has registered a handler (tests).
func Installed() bool {
	mu.Lock()
	defer mu.Unlock()
	return installed
}

// Handle is the instrument.AddHandler callback. instrument.Emit already
// recovers panicking handlers, but writes here are also individually
// best-effort so a Redis outage never affects the hot path.
func (w *Writer) Handle(event string, payload map[string]interface{}, _ float64) {
	if w == nil || w.client == nil || !w.sampled() {
		return
	}
	switch event {
	case "job.processed":
		w.Record("processed", stringField(payload, "worker_class"), 1)
	case "job.retried":
		w.Record("retried", stringField(payload, "worker_class"), 1)
	case "job.failed":
		w.Record("failed", stringField(payload, "worker_class"), 1)
	case "workset.reclaimed":
		if n := intField(payload, "reclaimed"); n > 0 {
			w.Record("reclaimed", "", n)
		}
	}
}

// Record writes one HINCRBY (+ pipelined EXPIRE refresh) for a status/job_type
// at the current bucket. jobType == "" (e.g. workset.reclaimed) only
// increments the system "_all" total. Best-effort: logs and returns on error.
func (w *Writer) Record(status, jobType string, count int) {
	if w == nil || w.client == nil || count == 0 {
		return
	}
	field := w.fieldFor(jobType)
	key := w.bucketKey(status, time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	_, err := w.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HIncrBy(ctx, key, AllField, int64(count))
		if field != AllField {
			pipe.HIncrBy(ctx, key, field, int64(count))
		}
		pipe.Expire(ctx, key, w.retention)
		return nil
	})
	if err != nil {
		log.Printf("[kbatch-perfmetrics] redis write failed key=%s: %v", key, err)
	}
}

// BucketStart returns the start-of-bucket epoch second containing at.
func (w *Writer) BucketStart(at time.Time) int64 {
	secs := int64(w.bucketSeconds.Seconds())
	if secs <= 0 {
		secs = 60
	}
	return (at.Unix() / secs) * secs
}

func (w *Writer) bucketKey(status string, at time.Time) string {
	return KeyPrefix + strconv.FormatInt(w.BucketStart(at), 10) + ":" + status
}

// fieldFor mirrors Ruby PerformanceMetrics.field_for: the raw job_type once
// seen (up to maxJobTypes distinct names per process), "_all" when there is
// no job_type, or "_other" once the cap is reached.
func (w *Writer) fieldFor(jobType string) string {
	jt := strings.TrimSpace(jobType)
	if jt == "" {
		return AllField
	}
	if len(jt) > maxFieldBytes {
		jt = jt[:maxFieldBytes]
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.known[jt]; ok {
		return jt
	}
	if len(w.known) >= w.maxJobTypes {
		return OtherField
	}
	w.known[jt] = struct{}{}
	return jt
}

func (w *Writer) sampled() bool {
	if w.sampleRate >= 1.0 {
		return true
	}
	return rand.Float64() < w.sampleRate
}

func stringField(payload map[string]interface{}, key string) string {
	if s, ok := payload[key].(string); ok {
		return s
	}
	return ""
}

func intField(payload map[string]interface{}, key string) int {
	switch v := payload[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}
