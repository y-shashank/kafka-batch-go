package liveness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	consumerPrefix = "kafka_batch:live:consumer:"
	jobPrefix      = "kafka_batch:live:job:"
	defaultTTL     = 180 * time.Second
	defaultInterval = 20 * time.Second
)

// JobMeta is written when a job starts executing.
type JobMeta struct {
	JobID       string
	BatchID     string
	WorkerClass string
	Topic       string
	Partition   int32
}

// Reporter writes Redis consumer heartbeats for the Ruby /live dashboard and
// SuperFetch reclaim (EXISTS on live:consumer:*).
type Reporter struct {
	Client           *redis.Client
	TTL              time.Duration
	HeartbeatEvery   time.Duration
	StatsInterval    time.Duration // CPU/RSS sample throttle (default 15s)
	ConsumerID       string
	TrackRunningJobs bool

	mu        sync.Mutex
	lastTopic string
	stats     *processSampler
}

func NewReporter(client *redis.Client, ttl time.Duration) *Reporter {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	host, _ := os.Hostname()
	return &Reporter{
		Client:           client,
		TTL:              ttl,
		HeartbeatEvery:   defaultInterval,
		StatsInterval:    defaultStatsInterval,
		ConsumerID:       fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString()[:6]),
		TrackRunningJobs: true,
		stats:            newProcessSampler(defaultStatsInterval),
	}
}

// StartHeartbeatLoop refreshes the Redis heartbeat on a fixed interval so
// CPU-heavy #perform work that starves the poll path cannot miss enough cycles
// to look dead (default: every 20s with TTL 180s ≈ 9 misses).
func (r *Reporter) StartHeartbeatLoop(ctx context.Context) {
	if r == nil || r.Client == nil {
		return
	}
	interval := r.HeartbeatEvery
	if interval <= 0 {
		interval = defaultInterval
	}
	go func() {
		r.Heartbeat(ctx, "")
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.mu.Lock()
				topic := r.lastTopic
				r.mu.Unlock()
				r.Heartbeat(ctx, topic)
			}
		}
	}()
}

// JobStarted records a running job (Ruby Liveness.job_started).
func (r *Reporter) JobStarted(ctx context.Context, meta JobMeta) {
	if r == nil || r.Client == nil || !r.TrackRunningJobs || meta.JobID == "" {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"job_id":       meta.JobID,
		"batch_id":     meta.BatchID,
		"worker_class": meta.WorkerClass,
		"consumer_id":  r.ConsumerID,
		"topic":        meta.Topic,
		"partition":    meta.Partition,
		"started_at":   time.Now().UTC().Format(time.RFC3339),
		"runtime":      "go",
	})
	_ = r.Client.Set(ctx, jobPrefix+r.ConsumerID+":"+meta.JobID, payload, r.TTL).Err()
}

// JobFinished clears a running job marker (Ruby Liveness.job_finished).
func (r *Reporter) JobFinished(ctx context.Context, jobID string) {
	if r == nil || r.Client == nil || !r.TrackRunningJobs || jobID == "" {
		return
	}
	_ = r.Client.Del(ctx, jobPrefix+r.ConsumerID+":"+jobID).Err()
}

func (r *Reporter) Heartbeat(ctx context.Context, topic string) {
	if r == nil || r.Client == nil {
		return
	}
	if topic != "" {
		r.mu.Lock()
		r.lastTopic = topic
		r.mu.Unlock()
	}
	r.mu.Lock()
	if topic == "" {
		topic = r.lastTopic
	}
	if r.stats == nil {
		r.stats = newProcessSampler(r.StatsInterval)
	}
	sampler := r.stats
	r.mu.Unlock()
	payload := ConsumerHeartbeatJSON(r.ConsumerID, topic, sampler)
	_ = r.Client.Set(ctx, consumerPrefix+r.ConsumerID, payload, r.TTL).Err()
}

// ConsumerHeartbeatJSON builds the Redis live:consumer payload (Ruby Liveness parity),
// including throttled rss_bytes / cpu_pct when sampler is non-nil.
func ConsumerHeartbeatJSON(consumerID, topic string, sampler *processSampler) []byte {
	m := map[string]interface{}{
		"consumer_id": consumerID,
		"hostname":    hostname(),
		"pid":         os.Getpid(),
		"topic":       topic,
		"last_seen":   time.Now().UTC().Format(time.RFC3339),
		"runtime":     "go",
	}
	if sampler != nil {
		for k, v := range sampler.sample() {
			m[k] = v
		}
	}
	raw, _ := json.Marshal(m)
	return raw
}

// DefaultProcessSampler returns a shared sampler for SuperFetch heartbeats.
func DefaultProcessSampler() *processSampler {
	return sharedSampler
}

var sharedSampler = newProcessSampler(defaultStatsInterval)

func hostname() string {
	h, _ := os.Hostname()
	return h
}
