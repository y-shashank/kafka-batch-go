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
)

// JobMeta is written when a job starts executing.
type JobMeta struct {
	JobID       string
	BatchID     string
	WorkerClass string
	Topic       string
	Partition   int32
}

// Reporter writes Redis consumer heartbeats for the Ruby /live dashboard.
type Reporter struct {
	Client            *redis.Client
	TTL               time.Duration
	ConsumerID        string
	TrackRunningJobs  bool

	mu sync.Mutex
}

func NewReporter(client *redis.Client, ttl time.Duration) *Reporter {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	host, _ := os.Hostname()
	return &Reporter{
		Client:           client,
		TTL:              ttl,
		ConsumerID:       fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString()[:6]),
		TrackRunningJobs: true,
	}
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
