package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// No per-job failure metadata is ever written to Redis: exhausted jobs land
// on the dead-letter topic and retrying jobs are listed live from the retry
// topics (see BuildFailureRecorder / Processor.recordFailure). Only the
// batch-level aggregate counter (failed_count, part of the batch hash
// itself) is maintained.
func TestRedisStoreNeverWritesAPerJobFailuresHash(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	batchID := "b1"
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "total_jobs", "1", "completed_count", "0", "failed_count", "0",
		"touched_count", "0", "status", "running", "locked_at", time.Now().UTC().Format(time.RFC3339),
	)
	mr.ZAdd("kafka_batch:index:running", 1, batchID)

	if _, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{
		{BatchID: batchID, JobID: "j1", Status: "failed", BatchSeq: 1},
	}); err != nil {
		t.Fatal(err)
	}

	batch, err := st.FindBatch(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.FailedCount != 1 {
		t.Fatalf("expected the batch aggregate failed_count to still update, got %d", batch.FailedCount)
	}

	exists, err := rdb.Exists(ctx, "kafka_batch:b:"+batchID+":failures").Result()
	if err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatal("expected no per-job failures hash to be written to Redis")
	}
}
