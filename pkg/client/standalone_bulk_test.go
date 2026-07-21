package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func TestEnqueueManyJobsAtEmptyAndUnknown(t *testing.T) {
	c := &Client{
		cfg: DefaultConfig(),
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"echo": {Runtime: "go"},
		}},
	}
	ids, err := c.EnqueueManyJobsAt(context.Background(), time.Now(), "echo", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	_, err = c.EnqueueManyJobsAt(context.Background(), time.Now(), "missing", []map[string]interface{}{{"x": 1}}, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestEnqueueManyJobsInUnknown(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}
	_, err := c.EnqueueManyJobsIn(context.Background(), time.Second, "missing", []map[string]interface{}{{}}, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestEnqueueManyJobsAllUniqSkipped(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq: uniq.NewLocker(rdb, time.Hour),
	}
	payload := map[string]interface{}{"n": 1}
	ok, err := c.uniq.Claim(context.Background(), "go:uniq.job", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	ids, err := c.EnqueueManyJobs(context.Background(), "uniq.job", []map[string]interface{}{payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "" {
		t.Fatalf("ids=%v", ids)
	}

	ids, err = c.EnqueueManyJobsAt(context.Background(), time.Now().Add(time.Minute), "uniq.job", []map[string]interface{}{payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "" {
		t.Fatalf("at ids=%v", ids)
	}
}

func TestRollbackStandalonePlans(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := &Client{cfg: DefaultConfig(), uniq: uniq.NewLocker(rdb, time.Hour)}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"a": 1}
	_, _ = c.uniq.Claim(context.Background(), "go:echo", payload, "j1")
	c.rollbackStandalonePlans(entry, "echo", []pushPlan{{jobID: "j1", payload: payload, fp: ""}}, 0)
	ok, err := c.uniq.Claim(context.Background(), "go:echo", payload, "j2")
	if err != nil || !ok {
		t.Fatalf("reclaim ok=%v err=%v", ok, err)
	}
}
