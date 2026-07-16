package workset

import (
	"context"
	"encoding/json"
	"errors"
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

func ageClaim(t *testing.T, st *Store, mr *miniredis.Miniredis, jobID string, age time.Duration) {
	t.Helper()
	ctx := context.Background()
	e, err := st.getEntry(ctx, jobID)
	if err != nil || e == nil {
		t.Fatalf("getEntry: %v %#v", err, e)
	}
	e.ClaimedAtUnix = time.Now().Add(-age).Unix()
	e.ClaimedAt = time.Unix(e.ClaimedAtUnix, 0).UTC().Format(time.RFC3339Nano)
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := mr.Set(jobKey(jobID), string(raw)); err != nil {
		t.Fatal(err)
	}
	mr.ZAdd(indexKey, float64(e.ClaimedAtUnix), jobID)
}

func killConsumer(t *testing.T, mr *miniredis.Miniredis, consumerID string) {
	t.Helper()
	mr.Del(liveConsumerPrefix + consumerID)
}

func TestClaimCompleteFence(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j1", Payload: []byte(`{"job_id":"j1"}`), Topic: "jobs",
		Partition: 0, Offset: 1, ConsumerID: "c1", LeaseTTL: time.Minute,
		StealGrace: -1,
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

func TestClaimSetsLiveKey(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j-live", Payload: []byte(`{}`), Topic: "jobs",
		ConsumerID: "c-live", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !mr.Exists(liveConsumerPrefix + "c-live") {
		t.Fatal("expected live:consumer key after claim")
	}
}

func TestClaimLostWhenOtherLive(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j2", Payload: []byte(`{}`), Topic: "jobs",
		ConsumerID: "alive", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j2", Payload: []byte(`{}`), Topic: "jobs",
		ConsumerID: "other", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || res.Won {
		t.Fatalf("expected lost claim, won=%v err=%v", res.Won, err)
	}
}

func TestClaimStealsFromDeadConsumer(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j3", Payload: []byte(`{"job_id":"j3"}`), Topic: "jobs",
		ConsumerID: "dead", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "dead")
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j3", Payload: []byte(`{"job_id":"j3"}`), Topic: "jobs",
		ConsumerID: "alive", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !res.Won {
		t.Fatalf("expected steal, won=%v err=%v", res.Won, err)
	}
}

func TestClaimDoesNotStealInsideGrace(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j3g", Payload: []byte(`{"job_id":"j3g"}`), Topic: "jobs",
		ConsumerID: "dead", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "dead")
	res, err := st.Claim(ctx, ClaimParams{
		JobID: "j3g", Payload: []byte(`{"job_id":"j3g"}`), Topic: "jobs",
		ConsumerID: "alive", LeaseTTL: time.Minute, StealGrace: 40 * time.Second,
	})
	if err != nil || res.Won {
		t.Fatalf("expected no steal inside grace, won=%v err=%v", res.Won, err)
	}
}

func TestClaimResumeSameConsumer(t *testing.T) {
	st, _ := testStore(t)
	ctx := context.Background()
	first, err := st.Claim(ctx, ClaimParams{
		JobID: "j4", Payload: []byte(`{"job_id":"j4"}`), Topic: "jobs",
		ConsumerID: "c1", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !first.Won {
		t.Fatal(err)
	}
	second, err := st.Claim(ctx, ClaimParams{
		JobID: "j4", Payload: []byte(`{"job_id":"j4"}`), Topic: "jobs",
		ConsumerID: "c1", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !second.Won {
		t.Fatalf("resume won=%v err=%v", second.Won, err)
	}
	if second.Fence != first.Fence {
		t.Fatalf("fence changed on resume: %s vs %s", first.Fence, second.Fence)
	}
}

func TestListOrphansRespectsGrace(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j5g", Payload: []byte(`{"job_id":"j5g"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "gone")
	orphans, err := st.ListOrphans(ctx, 10, 40*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("expected no orphans inside grace, got %+v", orphans)
	}
	ageClaim(t, st, mr, "j5g", time.Minute)
	orphans, err = st.ListOrphans(ctx, 10, 40*time.Second)
	if err != nil || len(orphans) != 1 || orphans[0].JobID != "j5g" {
		t.Fatalf("orphans=%+v err=%v", orphans, err)
	}
}

func TestListOrphansPipelinesMixedOwners(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	for _, c := range []struct {
		job, consumer string
	}{
		{"o1", "dead-pod"},
		{"o2", "dead-pod"},
		{"o3", "live-pod"},
	} {
		if _, err := st.Claim(ctx, ClaimParams{
			JobID: c.job, Payload: []byte(`{"job_id":"` + c.job + `"}`), Topic: "jobs",
			ConsumerID: c.consumer, LeaseTTL: time.Minute, StealGrace: -1,
		}); err != nil {
			t.Fatal(err)
		}
		ageClaim(t, st, mr, c.job, time.Minute)
	}
	killConsumer(t, mr, "dead-pod")
	// Stale index member with missing job key should be pruned.
	if err := rdb.ZAdd(ctx, indexKey, redis.Z{
		Score: float64(time.Now().Add(-time.Minute).Unix()), Member: "ghost",
	}).Err(); err != nil {
		t.Fatal(err)
	}

	orphans, err := st.ListOrphans(ctx, 10, 40*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, e := range orphans {
		got[e.JobID] = true
	}
	if !got["o1"] || !got["o2"] || got["o3"] || got["ghost"] {
		t.Fatalf("orphans=%v (want o1,o2 only)", got)
	}
	if n, err := rdb.ZScore(ctx, indexKey, "ghost").Result(); err == nil {
		t.Fatalf("expected ghost pruned from index, score=%v", n)
	}
}

func TestListOrphansAndReclaim(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j5", Payload: []byte(`{"job_id":"j5","worker_class":"W"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "gone")
	ageClaim(t, st, mr, "j5", time.Minute)

	orphans, err := st.ListOrphans(ctx, 10, -1)
	if err != nil || len(orphans) != 1 || orphans[0].JobID != "j5" {
		t.Fatalf("orphans=%+v err=%v", orphans, err)
	}

	var produced []struct {
		topic, key string
		body       []byte
	}
	prod := producerFunc(func(_ context.Context, topic, key string, body []byte) error {
		produced = append(produced, struct {
			topic, key string
			body       []byte
		}{topic, key, append([]byte(nil), body...)})
		return nil
	})
	res, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute, -1)
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

func TestReclaimIdempotentAfterFinishFailure(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	claim, err := st.Claim(ctx, ClaimParams{
		JobID: "j-idem", Payload: []byte(`{"job_id":"j-idem"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatal(err)
	}
	killConsumer(t, mr, "gone")
	ageClaim(t, st, mr, "j-idem", time.Minute)

	var produces int
	prod := producerFunc(func(_ context.Context, topic, key string, body []byte) error {
		produces++
		return nil
	})

	// Simulate: produce + mark succeeded, Finish never ran (entry still present).
	body, err := markReclaimPayload([]byte(`{"job_id":"j-idem"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := prod.Produce(ctx, "jobs.x", "j-idem", body); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkProduced(ctx, "j-idem", claim.Fence, time.Hour); err != nil {
		t.Fatal(err)
	}
	// Drop reclaim lock from a prior attempt.
	_ = st.AbortReclaim(ctx, "j-idem")

	res, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute, -1)
	if err != nil {
		t.Fatal(err)
	}
	if produces != 1 {
		t.Fatalf("expected exactly 1 produce, got %d", produces)
	}
	if res.Reclaimed != 1 {
		t.Fatalf("expected finish-only reclaim, got %+v", res)
	}
	owned, _ := st.StillOwned(ctx, "j-idem", "gone", claim.Fence)
	if owned {
		t.Fatal("expected entry removed after finish-only")
	}
}

func TestReclaimSecondSweepAfterFinishOnlyIsEmpty(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	claim, err := st.Claim(ctx, ClaimParams{
		JobID: "j-twice", Payload: []byte(`{"job_id":"j-twice"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "gone")
	ageClaim(t, st, mr, "j-twice", time.Minute)

	// Prior sweep: produce + mark, Finish never ran.
	var produces int
	_ = producerFunc(func(ctx context.Context, topic, key string, body []byte) error {
		produces++
		return nil
	}).Produce(ctx, "jobs.x", "j-twice", []byte(`{"job_id":"j-twice","_reclaim":true}`))
	_ = st.MarkProduced(ctx, "j-twice", claim.Fence, time.Hour)
	_ = st.AbortReclaim(ctx, "j-twice")

	prod := producerFunc(func(_ context.Context, topic, key string, body []byte) error {
		produces++
		return nil
	})
	res1, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute, -1)
	if err != nil {
		t.Fatal(err)
	}
	res2, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute, -1)
	if err != nil {
		t.Fatal(err)
	}
	if produces != 1 {
		t.Fatalf("produces=%d want 1 (finish-only then empty)", produces)
	}
	if res1.Reclaimed != 1 {
		t.Fatalf("res1=%+v", res1)
	}
	if res2.Checked != 0 || res2.Reclaimed != 0 {
		t.Fatalf("res2=%+v", res2)
	}
}

func TestReclaimProduceErrorAborts(t *testing.T) {
	st, mr := testStore(t)
	ctx := context.Background()
	_, err := st.Claim(ctx, ClaimParams{
		JobID: "j-fail", Payload: []byte(`{"job_id":"j-fail"}`), Topic: "jobs.x",
		ConsumerID: "gone", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	killConsumer(t, mr, "gone")
	ageClaim(t, st, mr, "j-fail", time.Minute)

	prod := producerFunc(func(context.Context, string, string, []byte) error {
		return errors.New("broker down")
	})
	res, err := st.ReclaimOrphans(ctx, prod, 10, time.Minute, -1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || res.Reclaimed != 0 {
		t.Fatalf("res=%+v", res)
	}
	// Entry still owned for retry.
	orphans, _ := st.ListOrphans(ctx, 10, -1)
	if len(orphans) != 1 {
		t.Fatalf("expected orphan retained, got %d", len(orphans))
	}
}

type producerFunc func(context.Context, string, string, []byte) error

func (f producerFunc) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload)
}
