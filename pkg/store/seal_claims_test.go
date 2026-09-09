package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Seal must mirror batch_done_job's claim-based outcomes: a callback claim this
// call did not win was already dispatched elsewhere (concurrent completion or
// the early-complete path), and returning a fire signal for it invokes the same
// callback twice — e.g. campaign.settle_billing running twice.
func TestSealDoesNotRefireAlreadyClaimedCallbacks(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	ok, err := s.CreateBatch(ctx, CreateBatchParams{ID: "sc", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := s.AddJobs(ctx, "sc", 1); err != nil {
		t.Fatal(err)
	}

	// Simulate a concurrent actor (the wire-shared Ruby runtime, or the
	// non-atomic cancel path) that counted the completion and stamped the
	// complete claim while the batch is still 'running'. Go's own scripts
	// can't produce this state atomically, but the Lua contract must never
	// double-fire regardless of which runtime wrote the hash.
	mr.HSet("kafka_batch:b:sc", "completed_count", "1")
	mr.HSet("kafka_batch:b:sc", "complete_callback_dispatched_at", "2026-08-01T00:00:00Z")
	mr.HSet("kafka_batch:b:sc", "callback_dispatched_at", "2026-08-01T00:00:00Z")

	seal, err := s.SealBatch(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if seal.Status != "done" {
		t.Fatalf("status=%q want done", seal.Status)
	}
	if seal.Outcome != "success_only" {
		t.Fatalf("outcome=%q want success_only — anything else refires the already-dispatched on_complete", seal.Outcome)
	}

	// Both claims already taken → nothing to fire at all.
	ok, err = s.CreateBatch(ctx, CreateBatchParams{ID: "sc2", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := s.AddJobs(ctx, "sc2", 1); err != nil {
		t.Fatal(err)
	}
	mr.HSet("kafka_batch:b:sc2", "completed_count", "1")
	mr.HSet("kafka_batch:b:sc2", "complete_callback_dispatched_at", "2026-08-01T00:00:00Z")
	mr.HSet("kafka_batch:b:sc2", "callback_dispatched_at", "2026-08-01T00:00:00Z")
	mr.HSet("kafka_batch:b:sc2", "success_callback_dispatched_at", "2026-08-01T00:00:00Z")

	seal2, err := s.SealBatch(ctx, "sc2")
	if err != nil {
		t.Fatal(err)
	}
	if seal2.Status != "sealed" {
		t.Fatalf("status=%q want sealed — %q refires callbacks already dispatched elsewhere", seal2.Status, seal2.Status)
	}

	// Idempotence: a second seal finds the batch terminal and fires nothing.
	again, err := s.SealBatch(ctx, "sc")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != "sealed" {
		t.Fatalf("re-seal status=%q want sealed", again.Status)
	}
}

// Happy path unchanged: a seal that finalizes an untouched-claims batch fires
// both callbacks exactly once with outcome success.
func TestSealNormalTerminalStillFiresSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	// Unsealed batch: completions count but never finalize or stamp claims.
	ok, err := s.CreateBatch(ctx, CreateBatchParams{ID: "sn", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := s.AddJobs(ctx, "sn", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordCompletionsBatch(ctx, []CompletionEvent{
		{BatchID: "sn", JobID: "j1", Status: "success", BatchSeq: 1},
		{BatchID: "sn", JobID: "j2", Status: "success", BatchSeq: 2},
	}); err != nil {
		t.Fatal(err)
	}

	seal, err := s.SealBatch(ctx, "sn")
	if err != nil {
		t.Fatal(err)
	}
	if seal.Status != "done" || seal.Outcome != "success" {
		t.Fatalf("status=%q outcome=%q want done/success", seal.Status, seal.Outcome)
	}
}
