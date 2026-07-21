package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*miniredis.Miniredis, *RedisStore) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, NewRedisStore(rdb, time.Hour)
}

func TestRawClientAndNilGuards(t *testing.T) {
	mr, st := newTestStore(t)
	if st.RawClient() == nil {
		t.Fatal("expected raw client")
	}
	_ = mr
	var nilStore *RedisStore
	if nilStore.RawClient() != nil {
		t.Fatal("nil store RawClient")
	}
	ok, err := nilStore.CompletionRecorded(context.Background(), "b", 1)
	if err != nil || ok {
		t.Fatalf("nil CompletionRecorded ok=%v err=%v", ok, err)
	}
	if err := nilStore.RecordCallbackRunner(context.Background(), "b", "n"); err != nil {
		t.Fatal(err)
	}
	batches, err := nilStore.FindReplayCallbackBatches(context.Background(), []string{"b"})
	if err != nil || batches != nil {
		t.Fatalf("nil FindReplay %+v err=%v", batches, err)
	}
	if err := nilStore.MarkReconcilerRefired(context.Background(), "b"); err == nil {
		t.Fatal("expected error for nil store MarkReconcilerRefired")
	}
}

func TestFindBatchMissing(t *testing.T) {
	_, st := newTestStore(t)
	b, err := st.FindBatch(context.Background(), "missing")
	if err != nil || b != nil {
		t.Fatalf("batch=%v err=%v", b, err)
	}
}

func TestCompletionRecorded(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()
	batchID := "rec-1"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: batchID, TotalJobs: 1, Sealed: true}); err != nil {
		t.Fatal(err)
	}
	ok, err := st.CompletionRecorded(ctx, batchID, 0)
	if err != nil || ok {
		t.Fatalf("seq<1: ok=%v err=%v", ok, err)
	}
	ok, err = st.CompletionRecorded(ctx, batchID, 1)
	if err != nil || ok {
		t.Fatalf("before complete: ok=%v err=%v", ok, err)
	}
	if _, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{
		{BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1},
	}); err != nil {
		t.Fatal(err)
	}
	ok, err = st.CompletionRecorded(ctx, batchID, 1)
	if err != nil || !ok {
		t.Fatalf("after complete: ok=%v err=%v", ok, err)
	}
}

func TestClaimCallbackAndDispatched(t *testing.T) {
	mr, st := newTestStore(t)
	ctx := context.Background()
	// Seed a finished batch without preclaimed callback stamps (finalize Lua
	// HSETNXs those on RecordCompletionsBatch).
	batchID := "cb-1"
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0", "touched_count", "1",
	)
	mr.ZAdd(doneIndex, 1, batchID)

	dispatched, err := st.CallbackDispatched(ctx, batchID)
	if err != nil || dispatched {
		t.Fatalf("before claim: dispatched=%v err=%v", dispatched, err)
	}

	won, err := st.ClaimCallback(ctx, batchID, "node-a", "complete")
	if err != nil || !won {
		t.Fatalf("first claim won=%v err=%v", won, err)
	}
	won, err = st.ClaimCallback(ctx, batchID, "node-b", "complete")
	if err != nil || won {
		t.Fatalf("second claim should lose: won=%v err=%v", won, err)
	}
	dispatched, err = st.CallbackDispatched(ctx, batchID)
	if err != nil || !dispatched {
		t.Fatalf("after claim: dispatched=%v err=%v", dispatched, err)
	}

	batchID2 := "cb-2"
	mr.HSet("kafka_batch:b:"+batchID2, "id", batchID2, "status", "complete")
	won, err = st.ClaimCallback(ctx, batchID2, "node-c")
	if err != nil || !won {
		t.Fatalf("any claim won=%v err=%v", won, err)
	}
	won, err = st.ClaimCallback(ctx, "missing", "node")
	if err != nil || won {
		t.Fatalf("missing claim won=%v err=%v", won, err)
	}
}

func TestRecordCallbackRunner(t *testing.T) {
	mr, st := newTestStore(t)
	ctx := context.Background()
	batchID := "runner-1"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: batchID, Sealed: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordCallbackRunner(ctx, "", "node"); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordCallbackRunner(ctx, batchID, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordCallbackRunner(ctx, batchID, "node-x"); err != nil {
		t.Fatal(err)
	}
	if got := mr.HGet("kafka_batch:b:"+batchID, "callback_dispatched_by"); got != "node-x" {
		t.Fatalf("callback_dispatched_by=%q", got)
	}
}

func TestFindReplayCallbackBatches(t *testing.T) {
	mr, st := newTestStore(t)
	ctx := context.Background()

	empty, err := st.FindReplayCallbackBatches(ctx, nil)
	if err != nil || empty != nil {
		t.Fatalf("empty ids: %+v err=%v", empty, err)
	}

	needReplay := "replay-need"
	already := "replay-done"
	running := "replay-running"
	mr.HSet("kafka_batch:b:"+needReplay,
		"id", needReplay, "status", "success", "total_jobs", "1",
	)
	mr.HSet("kafka_batch:b:"+already,
		"id", already, "status", "success",
		"callback_dispatched_at", "t",
		"complete_callback_dispatched_at", "t",
		"success_callback_dispatched_at", "t",
	)
	mr.HSet("kafka_batch:b:"+running, "id", running, "status", "running")

	got, err := st.FindReplayCallbackBatches(ctx, []string{needReplay, already, running, "ghost"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != needReplay {
		t.Fatalf("replay batches=%+v", got)
	}
}

func TestMarkReconcilerRefired(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()
	id := "refire-1"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: id, Sealed: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkReconcilerRefired(ctx, id); err != nil {
		t.Fatal(err)
	}
	b, err := st.FindBatch(ctx, id)
	if err != nil || b == nil || b.ReconcilerRefiredAt == "" {
		t.Fatalf("batch=%+v err=%v", b, err)
	}
}

func TestBatchCancelledFalse(t *testing.T) {
	_, st := newTestStore(t)
	cancelled, err := st.BatchCancelled(context.Background(), "nope")
	if err != nil || cancelled {
		t.Fatalf("cancelled=%v err=%v", cancelled, err)
	}
}

func TestRecordCompletionsBatchEmpty(t *testing.T) {
	_, st := newTestStore(t)
	res, err := st.RecordCompletionsBatch(context.Background(), nil)
	if err != nil || len(res.Finished) != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestRecordCompletionsUnknownStatusMapsToFailed(t *testing.T) {
	_, st := newTestStore(t)
	ctx := context.Background()
	id := "bad-status"
	if _, err := st.CreateBatch(ctx, CreateBatchParams{ID: id, TotalJobs: 1, Sealed: true}); err != nil {
		t.Fatal(err)
	}
	res, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{
		{BatchID: id, JobID: "j1", Status: "weird", BatchSeq: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := st.FindBatch(ctx, id)
	if b == nil || b.FailedCount != 1 {
		t.Fatalf("expected failed_count=1, batch=%+v finished=%+v", b, res.Finished)
	}
}
