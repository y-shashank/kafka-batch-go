package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/consumption"
)

func TestPingRedisSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if err := PingRedis(context.Background(), rdb); err != nil {
		t.Fatal(err)
	}
}

func TestPingRedisNilClient(t *testing.T) {
	if err := PingRedis(context.Background(), nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestPingRedisUnreachable(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := PingRedis(ctx, rdb); err == nil {
		t.Fatal("expected ping failure")
	}
}

func TestBuildPauseControlRedisOnly(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := config.DefaultDaemon()
	ctl, mysql, closeFn := BuildPauseControl(cfg, rdb)
	defer closeFn()
	if mysql != nil {
		t.Fatal("expected nil mysql pause store")
	}
	if ctl == nil {
		t.Fatal("expected control")
	}
	c := consumption.NewControl(rdb, time.Nanosecond)
	ctx := context.Background()
	if err := c.PauseTopic(ctx, "g1", "topic-a"); err != nil {
		t.Fatal(err)
	}
	if !ctl.Paused(ctx, "g1", "topic-a", 0) {
		t.Fatal("expected paused topic")
	}
	_ = c
}

func TestNewConsumerHealthFromConfig(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.LivenessTTL = 20 * time.Second
	h := NewConsumerHealth(cfg)
	if h == nil {
		t.Fatal("nil health")
	}
	h.Register("g-test")
	h.RecordPoll("g-test")
	ok, detail := h.Healthy(context.Background())
	if !ok {
		t.Fatalf("expected healthy: %s", detail)
	}
}

func TestNewLivenessReporterRespectsConfig(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := config.DefaultDaemon()
	cfg.LivenessEnabled = true
	cfg.TrackRunningJobs = false
	r := NewLivenessReporter(cfg, rdb)
	if r == nil {
		t.Fatal("expected reporter")
	}
	if r.TrackRunningJobs {
		t.Fatal("track running jobs should be false")
	}
}

func TestNewLivenessReporterDisabled(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.LivenessEnabled = false
	if r := NewLivenessReporter(cfg, redis.NewClient(&redis.Options{Addr: "127.0.0.1:9"})); r != nil {
		t.Fatal("expected nil when liveness disabled")
	}
}
