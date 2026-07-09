package fairness

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type stubCounter struct {
	count int
}

func (s *stubCounter) TopicPartitionCount(_ context.Context, _ string) (int, error) {
	return s.count, nil
}

func TestTenantPartitionsCheckout(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	tp := NewTenantPartitions(rdb, TenantPartitionsConfig{
		Dynamic: true,
		Counter: &stubCounter{count: 4},
		IngestTopic: func(lane string) string {
			return "kafka_batch.fair_time_ingest"
		},
	})
	ctx := context.Background()
	if err := tp.Warm(ctx, "time"); err != nil {
		t.Fatal(err)
	}
	p1 := tp.Resolve(ctx, "tenant-a", "time")
	p2 := tp.Resolve(ctx, "tenant-b", "time")
	if p1 == nil || p2 == nil || *p1 == *p2 {
		t.Fatalf("partitions %v %v", p1, p2)
	}
	p1Again := tp.Resolve(ctx, "tenant-a", "time")
	if p1Again == nil || *p1Again != *p1 {
		t.Fatalf("cache miss %v vs %v", p1Again, p1)
	}
}

func TestTenantPartitionsStaticWins(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	static := int32(2)
	tp := NewTenantPartitions(rdb, TenantPartitionsConfig{
		Static:  map[string]int32{"tenant-a": static},
		Dynamic: true,
		Counter: &stubCounter{count: 4},
		CacheTTL: time.Second,
	})
	p := tp.Resolve(context.Background(), "tenant-a", "time")
	if p == nil || *p != static {
		t.Fatalf("got %v", p)
	}
}
