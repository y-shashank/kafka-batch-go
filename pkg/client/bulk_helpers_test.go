package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func TestScheduleEntriesFrom(t *testing.T) {
	runAt := time.Unix(1_700_000_000, 0).UTC()
	msgs := []protocol.JobMessage{{JobID: "a"}, {JobID: "b"}}
	dels := []kafkaclient.Delivery{{Partition: 1, Offset: 10}, {Partition: 2, Offset: 20}}
	got := scheduleEntriesFrom(msgs, dels, runAt, "batch")
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
	if got[0].JobID != "a" || got[0].Partition != 1 || got[0].Offset != 10 || got[0].BatchID != "batch" {
		t.Fatalf("entry0=%+v", got[0])
	}
	if !got[1].RunAt.Equal(runAt) || got[1].JobID != "b" {
		t.Fatalf("entry1=%+v", got[1])
	}
}

func TestNextBatchSeq(t *testing.T) {
	b := &Batch{}
	if _, err := b.nextBatchSeq(); err == nil {
		t.Fatal("expected no reserved slots")
	}
	b.seqCursor, b.seqEnd = 1, 2
	seq, err := b.nextBatchSeq()
	if err != nil || seq != 1 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	seq, err = b.nextBatchSeq()
	if err != nil || seq != 2 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	if _, err := b.nextBatchSeq(); err == nil {
		t.Fatal("expected too few slots")
	}
}

func TestPlanPushesAndUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.UniqOnDuplicate = "skip"
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq: uniq.NewLocker(rdb, time.Hour),
	}
	b := &Batch{client: c, id: "batch-u"}
	payloads := []map[string]interface{}{{"n": 1}, {"n": 1}, {"n": 1}}
	entry, plans, jobIDs, err := b.planPushes(context.Background(), "uniq.job", payloads)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Uniq != true {
		t.Fatalf("entry=%+v", entry)
	}
	if len(plans) != 1 {
		t.Fatalf("plans=%d want 1 unique", len(plans))
	}
	if len(jobIDs) != 3 || jobIDs[0] == "" || jobIDs[1] != "" || jobIDs[2] != "" {
		t.Fatalf("jobIDs=%v", jobIDs)
	}
	if plans[0].fp == "" {
		t.Fatal("expected fingerprint on claimed plan")
	}
}

func TestPlanPushesUnknownHandler(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}, id: "b"}
	_, _, _, err := b.planPushes(context.Background(), "missing", []map[string]interface{}{{"x": 1}})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPushManyJobsEmptyAndAllSkipped(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq:  uniq.NewLocker(rdb, time.Hour),
		store: store.NewRedisStore(rdb, time.Hour),
	}
	b := &Batch{client: c, id: "b1"}

	ids, err := b.PushManyJobs(context.Background(), "uniq.job", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("empty ids=%v err=%v", ids, err)
	}

	// Pre-claim so both bulk slots are skipped → no Kafka needed.
	payload := map[string]interface{}{"n": 1}
	ok, err := c.uniq.Claim(context.Background(), "go:uniq.job", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	ids, err = b.PushManyJobs(context.Background(), "uniq.job", []map[string]interface{}{payload, payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "" || ids[1] != "" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestPushManyJobsAtEmpty(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig()}, id: "b"}
	ids, err := b.PushManyJobsAt(context.Background(), time.Now(), "x", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}

func TestRollbackPlans(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	cfg := DefaultConfig()
	c := &Client{cfg: cfg, uniq: uniq.NewLocker(rdb, time.Hour), store: st}
	ctx := context.Background()
	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "rb", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := st.AddJobs(ctx, "rb", 2); err != nil {
		t.Fatal(err)
	}
	b := &Batch{client: c, id: "rb"}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"x": 1}
	fp := uniq.DigestHex("go:echo", payload)
	_, _ = c.uniq.Claim(ctx, "go:echo", payload, "j-keep")
	_, _ = c.uniq.Claim(ctx, "go:echo", map[string]interface{}{"x": 2}, "j-drop")
	plans := []pushPlan{
		{jobID: "j-keep", payload: payload, fp: fp},
		{jobID: "j-drop", payload: map[string]interface{}{"x": 2}, fp: ""},
	}
	b.rollbackPlans(ctx, entry, "echo", plans, 1)
	row, _ := st.FindBatch(ctx, "rb")
	if row == nil || row.TotalJobs != 1 {
		t.Fatalf("row=%+v", row)
	}
}

func TestEnqueueJobInUnknownHandler(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}
	_, err := c.EnqueueJobIn(context.Background(), time.Second, "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPushJobInUnknownHandler(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}, id: "b"}
	_, err := b.PushJobIn(context.Background(), time.Second, "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestReserveStatuses(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}
	ctx := context.Background()

	b := &Batch{client: c, id: "missing"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected not_found")
	} else if _, ok := err.(BatchNotFoundError); !ok {
		t.Fatalf("err=%v", err)
	}

	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "open", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	b = &Batch{client: c, id: "open"}
	seq, err := b.reserve(ctx, 1)
	if err != nil || seq < 1 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	if b.seqCursor == 0 || b.seqEnd == 0 {
		t.Fatalf("seq window cursor=%d end=%d", b.seqCursor, b.seqEnd)
	}

	if _, err := b.reserve(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if b.seqEnd-b.seqCursor+1 < 0 {
		t.Fatalf("bad window cursor=%d end=%d", b.seqCursor, b.seqEnd)
	}

	ok, err = st.CreateBatch(ctx, store.CreateBatchParams{ID: "sealed", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("sealed create ok=%v err=%v", ok, err)
	}
	// Terminal status is what AddJobs treats as closed (matches store tests).
	mr.HSet("kafka_batch:b:sealed", "status", "success")
	b = &Batch{client: c, id: "sealed"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected closed")
	} else if ce, ok := err.(BatchClosedError); !ok || ce.Reason != "closed" {
		t.Fatalf("err=%v", err)
	}

	ok, err = st.CreateBatch(ctx, store.CreateBatchParams{ID: "canc", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("canc create ok=%v err=%v", ok, err)
	}
	if err := st.CancelBatch(ctx, "canc"); err != nil {
		t.Fatal(err)
	}
	b = &Batch{client: c, id: "canc"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected cancelled")
	} else if ce, ok := err.(BatchClosedError); !ok || ce.Reason != "cancelled" {
		t.Fatalf("err=%v", err)
	}
}
