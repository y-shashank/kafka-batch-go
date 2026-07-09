package liveness

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestJobStartedAndFinished(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, time.Minute)
	r.TrackRunningJobs = true

	ctx := context.Background()
	r.JobStarted(ctx, JobMeta{
		JobID: "j1", BatchID: "b1", WorkerClass: "go:test.echo",
		Topic: "jobs", Partition: 2,
	})

	keys := mr.Keys()
	if len(keys) != 1 {
		t.Fatalf("keys %v", keys)
	}
	if !mr.Exists(jobPrefix + r.ConsumerID + ":j1") {
		t.Fatal("expected job key")
	}

	r.JobFinished(ctx, "j1")
	if mr.Exists(jobPrefix + r.ConsumerID + ":j1") {
		t.Fatal("expected job key removed")
	}
}

func TestJobStartedDisabledWhenTrackRunningJobsOff(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, time.Minute)
	r.TrackRunningJobs = false

	r.JobStarted(context.Background(), JobMeta{JobID: "j1"})
	if len(mr.Keys()) != 0 {
		t.Fatalf("expected no keys, got %v", mr.Keys())
	}
}
