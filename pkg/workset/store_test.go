package workset

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewStore(rdb), mr
}

func TestClaimCompleteFence(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j1", Payload: []byte(`{"job_id":"j1"}`), Topic: "jobs",
		Partition: 0, Offset: 1, ConsumerID: "c1", LeaseTTL: time.Minute,
	})
	if err != nil || !res.Won {
		t.Fatalf("claim won=%v err=%v", res.Won, err)
	}
	ok, err := st.StillOwned(ctx, "j1", "c1", res.Fence)
	if err != nil || !ok {
		t.Fatalf("still owned=%v err=%v", ok, err)
	}
	if err := st.Complete(ctx, "j1", "c1", res.Fence); err != nil {
		t.Fatal(err)
	}
	ok, err = st.StillOwned(ctx, "j1", "c1", res.Fence)
	if err != nil || ok {
		t.Fatalf("expected not owned after complete, ok=%v err=%v", ok, err)
	}
}

func TestClaimLostWhenOtherLive(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j2", Payload: []byte(`{}`), Topic: "jobs",
		ConsumerID: "alive", LeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	mr.Set(liveConsumerPrefix+"alive", "1")
	mr.SetTTL(liveConsumerPrefix+"alive", time.Minute)

	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j2", Payload: []byte(`{}`), Topic: "jobs",
		ConsumerID: "other", LeaseTTL: time.Minute,
	})
	if err != nil || res.Won {
		t.Fatalf("expected lost claim, won=%v err=%v", res.Won, err)
	}
}

func TestClaimStealsFromDeadConsumer(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j3", Payload: []byte(`{"job_id":"j3"}`), Topic: "jobs",
		ConsumerID: "dead", LeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	// no live:consumer:dead key
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j3", Payload: []byte(`{"job_id":"j3"}`), Topic: "jobs",
		ConsumerID: "alive", LeaseTTL: time.Minute,
	})
	if err != nil || !res.Won {
		t.Fatalf("expected steal, won=%v err=%v", res.Won, err)
	}
}

func TestClaimResumeSameConsumer(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	first, err := st.Claim(ctx, ClaimParams{
		JobID: "j4", Payload: []byte(`{"job_id":"j4"}`), Topic: "jobs",
		ConsumerID: "c1", LeaseTTL: time.Minute,
	})
	if err != nil || !first.Won {
		t.Fatal(err)
	}
	second, err := st.Claim(ctx, ClaimParams{
		JobID: "j4", Payload: []byte(`{"job_id":"j4"}`), Topic: "jobs",
		ConsumerID: "c1", LeaseTTL: time.Minute,
	})
	if err != nil || !second.Won {
		t.Fatalf("resume won=%v err=%v", second.Won, err)
	}
	if second.Fence != first.Fence {
		t.Fatalf("fence changed on resume: %s vs %s", first.Fence, second.Fence)
	}
}

func TestListOrphansAndReclaim(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j5", Payload: []byte(`{"job_id":"j5","worker_class":"W"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	orphans, err := st.ListOrphans(ctx, 10)
	if err != nil || len(orphans) != 1 || orphans[0].JobID != "j5" {
		t.Fatalf("orphans=%+v err=%v", orphans, err)
	}

	var produced []struct{ topic, key string; body []byte }
	prod := producerFunc(func(_ context.Context, topic, key string, body []byte) error {
		produced = append(produced, struct {
			topic, key string
			body       []byte
		}{topic, key, append([]byte(nil), body...)})
		return nil
	})
	res, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute)
	if err != nil || res.Reclaimed != 1 {
		t.Fatalf("reclaim=%+v err=%v", res, err)
	}
	if len(produced) != 1 || produced[0].topic != "jobs.x" {
		t.Fatalf("produced=%+v", produced)
	}
	owned, _ := st.StillOwned(ctx, "j5", "gone", orphans[0].Fence)
	if owned {
		t.Fatal("expected entry removed after reclaim")
	}
}

type producerFunc func(context.Context, string, string, []byte) error

func (f producerFunc) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload)
}
