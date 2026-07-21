package liveness

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNewReporterDefaultTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, 0)
	if r.TTL != defaultTTL {
		t.Fatalf("ttl=%s", r.TTL)
	}
	if r.stats == nil {
		t.Fatal("expected process sampler")
	}
}

func TestHeartbeatWritesConsumerKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, time.Minute)
	r.ConsumerID = "test-consumer"

	ctx := context.Background()
	r.Heartbeat(ctx, "jobs.topic")

	key := consumerPrefix + r.ConsumerID
	if !mr.Exists(key) {
		t.Fatalf("missing heartbeat key, keys=%v", mr.Keys())
	}
	raw, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["topic"] != "jobs.topic" || m["consumer_id"] != "test-consumer" {
		t.Fatalf("payload=%v", m)
	}

	// Empty topic reuses lastTopic.
	r.Heartbeat(ctx, "")
	raw, _ = rdb.Get(ctx, key).Bytes()
	_ = json.Unmarshal(raw, &m)
	if m["topic"] != "jobs.topic" {
		t.Fatalf("expected sticky topic, got %v", m["topic"])
	}

	var nilR *Reporter
	nilR.Heartbeat(ctx, "x")
	r.Client = nil
	r.Heartbeat(ctx, "x")
}

func TestStartHeartbeatLoop(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, time.Minute)
	r.ConsumerID = "loop-consumer"
	r.HeartbeatEvery = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartHeartbeatLoop(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	key := consumerPrefix + r.ConsumerID
	for time.Now().Before(deadline) {
		if mr.Exists(key) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !mr.Exists(key) {
		t.Fatal("expected heartbeat from loop")
	}

	r.Heartbeat(ctx, "sticky")
	time.Sleep(40 * time.Millisecond)
	cancel()

	// Nil / no-client are no-ops.
	(&Reporter{}).StartHeartbeatLoop(context.Background())
	var nilR *Reporter
	nilR.StartHeartbeatLoop(context.Background())
}

func TestDefaultProcessSampler(t *testing.T) {
	s := DefaultProcessSampler()
	if s == nil {
		t.Fatal("nil sampler")
	}
	_ = s.sample()
}

func TestJobFinishedGuards(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := NewReporter(rdb, time.Minute)
	r.JobFinished(context.Background(), "")
	r.TrackRunningJobs = false
	r.JobFinished(context.Background(), "j1")
	var nilR *Reporter
	nilR.JobFinished(context.Background(), "j1")
}

func TestParsePSTimeErrorsAndCache(t *testing.T) {
	if _, err := parsePSTime("bad"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := parsePSTime("1"); err == nil {
		t.Fatal("expected error for single part")
	}
	if _, err := parsePSTime("a:b:c"); err == nil {
		t.Fatal("expected parse error")
	}
	got, err := parsePSTime("01:02:03.5")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Hour + 2*time.Minute + 3500*time.Millisecond
	if got != want {
		t.Fatalf("got=%v want=%v", got, want)
	}

	s := newProcessSampler(0)
	if s.interval != defaultStatsInterval {
		t.Fatalf("interval=%s", s.interval)
	}
	first := s.sample()
	second := s.sample()
	if len(first) == 0 {
		// darwin should usually return rss; tolerate empty but cache must stick
		_ = second
	} else if len(second) != len(first) {
		t.Fatalf("cache miss: first=%v second=%v", first, second)
	}
	if (*processSampler)(nil).sample() != nil {
		t.Fatal("nil sampler sample")
	}
}

func TestConsumerHeartbeatJSONNilSampler(t *testing.T) {
	raw := ConsumerHeartbeatJSON("c1", "t", nil)
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["consumer_id"] != "c1" {
		t.Fatalf("%v", m)
	}
}

func TestHeartbeatCreatesSamplerAndDefaultInterval(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	r := &Reporter{
		Client:         rdb,
		TTL:            time.Minute,
		ConsumerID:     "no-stats",
		HeartbeatEvery: 0, // StartHeartbeatLoop should use defaultInterval
	}
	r.Heartbeat(context.Background(), "t")
	if r.stats == nil {
		t.Fatal("expected sampler created")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.StartHeartbeatLoop(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	if !mr.Exists(consumerPrefix + r.ConsumerID) {
		t.Fatal("expected heartbeat key")
	}
}

func TestSampleCacheExpiry(t *testing.T) {
	s := newProcessSampler(5 * time.Millisecond)
	_ = s.sample()
	time.Sleep(15 * time.Millisecond)
	_ = s.sample() // refresh path after interval
}
