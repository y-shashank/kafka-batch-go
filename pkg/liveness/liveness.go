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
	ConsumerID       string
	TrackRunningJobs bool

	mu        sync.Mutex
	lastTopic string
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
		ConsumerID:       fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString()[:6]),
		TrackRunningJobs: true,
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
	r.mu.Unlock()
	payload, _ := json.Marshal(map[string]interface{}{
		"consumer_id": r.ConsumerID,
		"hostname":    hostname(),
		"pid":         os.Getpid(),
		"topic":       topic,
		"last_seen":   time.Now().UTC().Format(time.RFC3339),
		"runtime":     "go",
	})
	_ = r.Client.Set(ctx, consumerPrefix+r.ConsumerID, payload, r.TTL).Err()
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
