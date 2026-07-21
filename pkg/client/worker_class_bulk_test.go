package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func workerClient(t *testing.T) *Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.Workers = map[string]WorkerClassConfig{
		"Orders::ProcessWorker": {Topic: "jobs.ruby", Uniq: true},
	}
	c := &Client{
		cfg:   cfg,
		uniq:  uniq.NewLocker(rdb, time.Hour),
		store: store.NewRedisStore(rdb, time.Hour),
	}
	c.buildWorkerIndex()
	return c
}

func TestEnqueueManyAtEmptyAndUnknown(t *testing.T) {
	c := workerClient(t)
	ids, err := c.EnqueueManyAt(context.Background(), time.Now(), "Orders::ProcessWorker", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	_, err = c.EnqueueManyAt(context.Background(), time.Now(), "Missing::W", []map[string]interface{}{{}}, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueManyIn(context.Background(), time.Second, "Missing::W", []map[string]interface{}{{}}, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPlanWorkerPushesAndAllSkipped(t *testing.T) {
	c := workerClient(t)
	payload := map[string]interface{}{"n": 1}
	ok, err := c.uniq.Claim(context.Background(), "Orders::ProcessWorker", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}

	jt, entry, plans, jobIDs, err := c.planWorkerPushes(context.Background(), "Orders::ProcessWorker", []map[string]interface{}{payload, payload}, "")
	if err != nil {
		t.Fatal(err)
	}
	if jt == "" || !entry.Uniq {
		t.Fatalf("jt=%s entry=%+v", jt, entry)
	}
	if len(plans) != 0 {
		t.Fatalf("plans=%d", len(plans))
	}
	if len(jobIDs) != 2 || jobIDs[0] != "" || jobIDs[1] != "" {
		t.Fatalf("jobIDs=%v", jobIDs)
	}

	ids, err := c.EnqueueMany(context.Background(), "Orders::ProcessWorker", []map[string]interface{}{payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestPushManyEmptyAndAllSkipped(t *testing.T) {
	c := workerClient(t)
	b := &Batch{client: c, id: "b1"}
	ids, err := b.PushMany(context.Background(), "Orders::ProcessWorker", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	ids, err = b.PushManyAt(context.Background(), time.Now(), "Orders::ProcessWorker", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("at ids=%v err=%v", ids, err)
	}

	payload := map[string]interface{}{"n": 2}
	ok, err := c.uniq.Claim(context.Background(), "Orders::ProcessWorker", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	ids, err = b.PushMany(context.Background(), "Orders::ProcessWorker", []map[string]interface{}{payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "" {
		t.Fatalf("ids=%v", ids)
	}
	ids, err = b.PushManyAt(context.Background(), time.Now().Add(time.Minute), "Orders::ProcessWorker", []map[string]interface{}{payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "" {
		t.Fatalf("at ids=%v", ids)
	}
	_, err = b.PushManyIn(context.Background(), time.Second, "Missing::W", []map[string]interface{}{{}}, PushOptions{})
	if _, ok := err.(UnknownWorkerClassError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPushManyReserveFails(t *testing.T) {
	c := workerClient(t)
	ctx := context.Background()
	b := &Batch{client: c, id: "open-w"}
	ok, err := c.store.CreateBatch(ctx, store.CreateBatchParams{ID: "open-w", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if err := c.store.CancelBatch(ctx, "open-w"); err != nil {
		t.Fatal(err)
	}
	_, err = b.PushMany(ctx, "Orders::ProcessWorker", []map[string]interface{}{{"n": 9}}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = b.PushManyAt(ctx, time.Now().Add(time.Minute), "Orders::ProcessWorker", []map[string]interface{}{{"n": 10}}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("at err=%v", err)
	}
}

func TestRollbackWorkerPlans(t *testing.T) {
	c := workerClient(t)
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"z": 1}
	_, _ = c.uniq.Claim(context.Background(), "Orders::ProcessWorker", payload, "j1")
	c.rollbackWorkerPlans(entry, "Orders::ProcessWorker", []workerPushPlan{
		{jobID: "j1", payload: payload, fp: ""},
	}, 0)
	ok, err := c.uniq.Claim(context.Background(), "Orders::ProcessWorker", payload, "j2")
	if err != nil || !ok {
		t.Fatalf("reclaim ok=%v err=%v", ok, err)
	}

	ctx := context.Background()
	ok, err = c.store.CreateBatch(ctx, store.CreateBatchParams{ID: "wb", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := c.store.AddJobs(ctx, "wb", 2); err != nil {
		t.Fatal(err)
	}
	b := &Batch{client: c, id: "wb"}
	_, _ = c.uniq.Claim(ctx, "Orders::ProcessWorker", map[string]interface{}{"z": 2}, "j-drop")
	b.rollbackWorkerPlans(ctx, entry, "Orders::ProcessWorker", []workerPushPlan{
		{jobID: "kept", payload: map[string]interface{}{"z": 9}, fp: "x"},
		{jobID: "j-drop", payload: map[string]interface{}{"z": 2}, fp: ""},
	}, 1)
	row, _ := c.store.FindBatch(ctx, "wb")
	if row == nil || row.TotalJobs != 1 {
		t.Fatalf("row=%+v", row)
	}
}
