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

func TestClaimAndReleaseUniqWorker(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.UniqOnDuplicate = "raise"
	c := &Client{cfg: cfg, uniq: uniq.NewLocker(rdb, time.Hour)}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"id": 1}

	skipped, err := c.claimUniqWorker(context.Background(), entry, "W", payload, "j1", "")
	if err != nil || skipped {
		t.Fatalf("first skipped=%v err=%v", skipped, err)
	}
	skipped, err = c.claimUniqWorker(context.Background(), entry, "W", payload, "j2", "")
	if skipped {
		t.Fatal("raise should not skip")
	}
	if _, ok := err.(DuplicateJobError); !ok {
		t.Fatalf("err=%v", err)
	}

	cfg.UniqOnDuplicate = "skip"
	c.cfg = cfg
	skipped, err = c.claimUniqWorker(context.Background(), entry, "W", payload, "j3", "")
	if err != nil || !skipped {
		t.Fatalf("skip skipped=%v err=%v", skipped, err)
	}

	c.releaseUniqWorker(entry, "W", payload, "j1", "")
	ok, err := c.uniq.Claim(context.Background(), "W", payload, "j4")
	if err != nil || !ok {
		t.Fatalf("reclaim after digest release ok=%v err=%v", ok, err)
	}
	fp := uniq.DigestHex("W", payload)
	c.releaseUniqWorker(entry, "W", payload, "j4", fp)

	c.cfg.UniqEnabled = false
	skipped, err = c.claimUniqWorker(context.Background(), entry, "W", payload, "j5", "")
	if err != nil || skipped {
		t.Fatalf("disabled skipped=%v err=%v", skipped, err)
	}
	c.releaseUniqWorker(entry, "W", payload, "j5", fp)
}

func TestBuildWorkerMessageBranches(t *testing.T) {
	c := &Client{cfg: DefaultConfig()}
	batchID := "b1"
	seq := int64(3)
	entry := config.HandlerEntry{Uniq: true, RetryTier: "fast", MaxRetries: 1}
	msg := c.buildWorkerMessage(entry, "jt", "W", nil, "j1", &batchID, PushOptions{
		TenantID:  "t1",
		ValidTill: "2026-06-01T00:00:00Z",
	}, &seq)
	if msg.TenantID == nil || *msg.TenantID != "t1" {
		t.Fatalf("tenant=%v", msg.TenantID)
	}
	if msg.BatchSeq == nil || *msg.BatchSeq != 3 {
		t.Fatalf("seq=%v", msg.BatchSeq)
	}
	if msg.RetryTier != "fast" || msg.UniqFP == "" || msg.ValidTill == "" {
		t.Fatalf("msg=%+v", msg)
	}
}

func TestEnqueueUnknownWorkerClass(t *testing.T) {
	c := &Client{cfg: DefaultConfig()}
	c.buildWorkerIndex()
	_, err := c.Enqueue(context.Background(), "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueAt(context.Background(), time.Now(), "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueIn(context.Background(), time.Second, "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	b := &Batch{client: c, id: "b"}
	_, err = b.Push(context.Background(), "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = b.PushAt(context.Background(), time.Now(), "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = b.PushIn(context.Background(), time.Second, "Missing::W", nil, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
}
