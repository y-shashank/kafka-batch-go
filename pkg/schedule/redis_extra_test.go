package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNewRedisStoreDefaultReclaimLimit(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, 0)
	if st.reclaimLimit != 500 {
		t.Fatalf("reclaimLimit=%d", st.reclaimLimit)
	}
	stNeg := NewRedisStore(rdb, -1)
	if stNeg.reclaimLimit != 500 {
		t.Fatalf("neg reclaimLimit=%d", stNeg.reclaimLimit)
	}
}

func TestRedisStoreScheduleManyAndAckEmpty(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, 50)
	ctx := context.Background()

	if err := st.ScheduleMany(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, nil); err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1000, 0)
	err := st.ScheduleMany(ctx, []ScheduleEntry{
		{JobID: "a", RunAt: now.Add(-time.Second), Partition: 0, Offset: 1},
		{JobID: "b", RunAt: now.Add(-2 * time.Second), Partition: 1, Offset: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := rdb.ZCard(ctx, pendingKey).Result()
	if err != nil || n != 2 {
		t.Fatalf("pending zcard=%d err=%v", n, err)
	}

	claimed, err := st.ClaimDue(ctx, now, 60, 10)
	if err != nil || len(claimed) != 2 {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
}

func TestRedisStoreReadMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, 50)
	ctx := context.Background()

	n1, err := st.RecordReadMiss(ctx, "j1:0:1")
	if err != nil || n1 != 1 {
		t.Fatalf("n1=%d err=%v", n1, err)
	}
	n2, err := st.RecordReadMiss(ctx, "j1:0:1")
	if err != nil || n2 != 2 {
		t.Fatalf("n2=%d err=%v", n2, err)
	}
	if err := st.ClearReadMiss(ctx, "j1:0:1"); err != nil {
		t.Fatal(err)
	}
	n3, err := st.RecordReadMiss(ctx, "j1:0:1")
	if err != nil || n3 != 1 {
		t.Fatalf("n3=%d err=%v", n3, err)
	}
}

func TestParseMemberFailures(t *testing.T) {
	for _, bad := range []string{"", "nocolon", "only:one", ":1:2", "job::3", "job:x:3", "job:1:x"} {
		if _, ok := ParseMember(bad); ok {
			t.Errorf("ParseMember(%q) should fail", bad)
		}
	}
}
