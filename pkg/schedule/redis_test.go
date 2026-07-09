package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisStoreClaimDueAndAck(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, 100)
	ctx := context.Background()

	now := time.Unix(100, 0)
	if err := st.Schedule(ctx, "j1", now.Add(-time.Second), 0, 42); err != nil {
		t.Fatal(err)
	}

	claimed, err := st.ClaimDue(ctx, now, 60, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0] != "j1:0:42" {
		t.Fatalf("claimed %+v", claimed)
	}

	// still in inflight until ack
	n, err := rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || n != 1 {
		t.Fatalf("expected inflight entry, zcard=%d err=%v", n, err)
	}
	if err := st.Ack(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	n, err = rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || n != 0 {
		t.Fatalf("expected inflight cleared, zcard=%d err=%v", n, err)
	}
}

func TestRedisStoreReclaim(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, 100)
	ctx := context.Background()

	now := time.Unix(200, 0)
	mr.ZAdd(inflightKey, float64(now.Unix()-10), "j2:1:7")

	n, err := st.Reclaim(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d", n)
	}
	pending, err := rdb.ZCard(ctx, pendingKey).Result()
	if err != nil || pending != 1 {
		t.Fatalf("expected pending after reclaim, zcard=%d err=%v", pending, err)
	}
}

func TestParseMember(t *testing.T) {
	m, ok := ParseMember("abc:2:99")
	if !ok || m.JobID != "abc" || m.Partition != 2 || m.Offset != 99 {
		t.Fatalf("parsed %+v ok=%v", m, ok)
	}
	if BuildMember("abc", 2, 99) != "abc:2:99" {
		t.Fatal("build roundtrip")
	}
	m2, ok := ParseMember("uuid:with:colons:1:5")
	if !ok || m2.JobID != "uuid:with:colons" || m2.Partition != 1 || m2.Offset != 5 {
		t.Fatalf("colon job_id parsed %+v ok=%v", m2, ok)
	}
}
