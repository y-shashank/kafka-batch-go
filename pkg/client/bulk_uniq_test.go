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

func TestBulkUniqClaims(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	c := &Client{cfg: cfg, uniq: uniq.NewLocker(rdb, time.Hour)}

	claimed, err := c.bulkUniqClaims(context.Background(), config.HandlerEntry{}, "W", nil, nil, "")
	if err != nil || len(claimed) != 0 {
		t.Fatalf("empty claimed=%v err=%v", claimed, err)
	}

	claimed, err = c.bulkUniqClaims(context.Background(), config.HandlerEntry{Uniq: false}, "W",
		[]map[string]interface{}{{"a": 1}}, []string{"j1"}, "")
	if err != nil || len(claimed) != 1 || !claimed[0] {
		t.Fatalf("disabled claimed=%v err=%v", claimed, err)
	}

	entry := config.HandlerEntry{Uniq: true}
	payloads := []map[string]interface{}{{"n": 1}, {"n": 1}, nil}
	ids := []string{"a", "b", "c"}
	claimed, err = c.bulkUniqClaims(context.Background(), entry, "W", payloads, ids, "batch")
	if err != nil {
		t.Fatal(err)
	}
	if !claimed[0] || claimed[1] || !claimed[2] {
		t.Fatalf("claimed=%v", claimed)
	}

	c.cfg.UniqOnDuplicate = "raise"
	_, err = c.bulkUniqClaims(context.Background(), entry, "W", []map[string]interface{}{{"n": 1}}, []string{"d"}, "")
	if _, ok := err.(DuplicateJobError); !ok {
		t.Fatalf("err=%v", err)
	}
}
