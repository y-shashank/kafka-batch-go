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

func TestWorkerClassName(t *testing.T) {
	if got := workerClassName(config.HandlerEntry{WorkerClass: "MyApp::W"}, "jt"); got != "MyApp::W" {
		t.Fatalf("got=%q", got)
	}
	if got := workerClassName(config.HandlerEntry{}, "echo"); got != "go:echo" {
		t.Fatalf("got=%q", got)
	}
}

func TestMaxRetries(t *testing.T) {
	tests := []struct {
		name  string
		entry config.HandlerEntry
		cfg   Config
		want  int
	}{
		{name: "entry wins", entry: config.HandlerEntry{MaxRetries: 3}, cfg: Config{MaxRetries: 9}, want: 3},
		{name: "cfg fallback", entry: config.HandlerEntry{}, cfg: Config{MaxRetries: 4}, want: 4},
		{name: "default seven", entry: config.HandlerEntry{}, cfg: Config{}, want: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{cfg: tt.cfg}
			if got := c.maxRetries(tt.entry); got != tt.want {
				t.Fatalf("got=%d want=%d", got, tt.want)
			}
		})
	}
}

func TestBuildMessageBranches(t *testing.T) {
	cfg := DefaultConfig()
	c := &Client{cfg: cfg}
	batchID := "batch-1"
	seq := int64(7)
	entry := config.HandlerEntry{
		Runtime:     "go",
		WorkerClass: "Custom::Worker",
		MaxRetries:  2,
		RetryTier:   "slow",
		Uniq:        true,
	}
	msg, err := c.buildMessage(entry, "echo", nil, "job-1", &batchID, PushOptions{
		TenantID:  "acme",
		ValidTill: "2026-01-02T03:04:05Z",
	}, &seq)
	if err != nil {
		t.Fatal(err)
	}
	if msg.WorkerClass != "Custom::Worker" {
		t.Fatalf("worker=%q", msg.WorkerClass)
	}
	if msg.Payload == nil {
		t.Fatal("nil payload should become empty map")
	}
	if msg.TenantID == nil || *msg.TenantID != "acme" {
		t.Fatalf("tenant=%v", msg.TenantID)
	}
	if msg.BatchSeq == nil || *msg.BatchSeq != 7 {
		t.Fatalf("batch_seq=%v", msg.BatchSeq)
	}
	if msg.RetryTier != "slow" {
		t.Fatalf("retry_tier=%q", msg.RetryTier)
	}
	if msg.ValidTill != "2026-01-02T03:04:05Z" {
		t.Fatalf("valid_till=%q", msg.ValidTill)
	}
	if msg.UniqFP == "" {
		t.Fatal("expected uniq fingerprint")
	}

	// Uniq disabled → no fingerprint.
	c.cfg.UniqEnabled = false
	msg2, err := c.buildMessage(entry, "echo", map[string]interface{}{"a": 1}, "job-2", nil, PushOptions{ValidTill: "not-a-time"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if msg2.UniqFP != "" {
		t.Fatalf("uniq fp should be empty, got %q", msg2.UniqFP)
	}
	if msg2.ValidTill != "" {
		t.Fatalf("invalid valid_till should be ignored, got %q", msg2.ValidTill)
	}
	if msg2.BatchSeq != nil {
		t.Fatal("batch_seq without batch id should be nil")
	}
}

func TestClaimUniqRaiseAndDisabled(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.UniqOnDuplicate = "raise"
	c := &Client{
		cfg: cfg,
		uniq: uniq.NewLocker(rdb, time.Hour),
	}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"k": 1}

	skipped, err := c.claimUniq(context.Background(), entry, "echo", payload, "j1", "")
	if err != nil || skipped {
		t.Fatalf("first claim skipped=%v err=%v", skipped, err)
	}
	skipped, err = c.claimUniq(context.Background(), entry, "echo", payload, "j2", "")
	if skipped {
		t.Fatal("raise mode should not skip")
	}
	if _, ok := err.(DuplicateJobError); !ok {
		t.Fatalf("err=%v", err)
	}

	c.cfg.UniqEnabled = false
	skipped, err = c.claimUniq(context.Background(), entry, "echo", payload, "j3", "")
	if err != nil || skipped {
		t.Fatalf("disabled uniq skipped=%v err=%v", skipped, err)
	}
}

func TestReleaseUniq(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	locker := uniq.NewLocker(rdb, time.Hour)
	c := &Client{cfg: cfg, uniq: locker}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"id": 9}

	ok, err := locker.Claim(context.Background(), "go:echo", payload, "j1")
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	fp := uniq.DigestHex("go:echo", payload)
	c.releaseUniq(entry, "echo", payload, "j1", fp)

	ok, err = locker.Claim(context.Background(), "go:echo", payload, "j2")
	if err != nil || !ok {
		t.Fatalf("reclaim after release by fp ok=%v err=%v", ok, err)
	}

	c.releaseUniq(entry, "echo", payload, "j2", "") // digest path
	ok, err = locker.Claim(context.Background(), "go:echo", payload, "j3")
	if err != nil || !ok {
		t.Fatalf("reclaim after release by digest ok=%v err=%v", ok, err)
	}

	c.cfg.UniqEnabled = false
	c.releaseUniq(entry, "echo", payload, "j3", fp) // no-op
}
