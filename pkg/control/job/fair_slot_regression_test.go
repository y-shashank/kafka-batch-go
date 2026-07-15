package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Regression for Finding #1: the fair-slot dedup key is set at claim time (SetNX,
// before perform) and is never deleted — it only expires after slotDedupTTL
// (default == lease TTL == 1800s). If a worker claims a slot and then dies mid-
// perform (OOM / SIGKILL / rolling deploy) *before* committing the Kafka offset
// and *before* emitting a completion event, Kafka redelivers the same message
// (same _fair_slot_id) to another worker. That worker's ClaimSlotExecution now
// fails, Process returns errFairSkipped and commits the message with NO event
// (processor.go:136-139).
//
// Net effect: the job never ran and never counted. Its batch is stuck at
// (total-1) forever and the reconciler keeps classifying it "in_progress"
// because done < total. The callback never fires.
//
// This test models "worker A already claimed the slot" by pre-setting the dedup
// key WITHOUT setting the completion bitmap bit for the seq (A crashed before
// emitting). The invariant a correct implementation must uphold: a fair-slot
// skip for a batch_seq that was never counted must still make the batch advance
// — i.e. Process must emit a completion event (or re-run), not silently drop it.
//
// EXPECTED TODAY: FAILS (out.Event == nil). After the fix it should pass.
func TestFairSlotSkipAfterCrashStillAdvancesBatch(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.fairskip", func(ctx *kbatch.Context) error { return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	st := store.NewRedisStore(rdb, time.Hour)
	sched := fairness.NewScheduler(rdb, fairness.Settings{
		Lane:                fairness.LaneTime,
		GlobalConcurrency:   10,
		ReadyWindow:         100,
		LeaseTTL:            60,
		DefaultWeight:       1,
		WeightedConcurrency: true,
	})

	const slotID = "slot-from-crashed-worker"
	// Worker A claimed this slot and then crashed before completing. The dedup
	// key survives; the completion bit for the seq was never set.
	claimed, err := sched.ClaimSlotExecution(ctx, slotID)
	if err != nil {
		t.Fatal(err)
	}
	if !claimed {
		t.Fatal("precondition: expected first claim to succeed")
	}

	batchID := "b-stuck"
	seq := int64(7)
	msg := map[string]interface{}{
		"job_id":                 "j-crash",
		"batch_id":               batchID,
		"job_type":               "test.fairskip",
		"worker_class":           "go:test.fairskip",
		"payload":                map[string]interface{}{},
		"attempt":                0,
		"max_retries":            3,
		"batch_seq": seq,
		// fair-slot metadata as stamped by the forwarder (markSlot).
		"_fair_slot":    true,
		"_fair_slot_id": slotID,
		"_fair_type":    "time",
		"tenant_id":     "acme",
	}
	raw, _ := json.Marshal(msg)

	p := &Processor{
		Cfg:      config.DefaultDaemon(),
		Store:    st,
		Producer: &memProducer{},
		FairTime: sched,
		Now:      time.Now,
	}

	out, err := p.Process(ctx, raw, protocol.SourceCoords{Topic: "ready.time", Partition: 0, Offset: 42})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	// The redelivered copy of a never-completed job must not be silently dropped.
	if out.Event == nil {
		t.Fatalf("BUG (Finding #1): fair-slot skip committed the job (CommitOffset=%v) " +
			"with no completion event — batch %q seq %d is now permanently stuck",
			out.CommitOffset, batchID, seq)
	}
	if out.Event.BatchID != batchID || out.Event.BatchSeq != seq {
		t.Fatalf("completion event targets wrong job: %+v", out.Event)
	}
}

// Companion to the above: when the slot's holder is still ALIVE (its lease is
// live) but hasn't recorded the completion yet, the redelivered copy must NOT be
// re-run — that would double-execute the handler. Instead it is deferred via the
// delayed retry topic (routed back to the source topic) to be re-checked after
// the lease expires. This is the conservative "be sure before re-running" path.
func TestFairSlotSkipDefersWhileHolderLeaseIsLive(t *testing.T) {
	kbatch.Reset()
	ran := 0
	kbatch.Register("test.fairskip", func(ctx *kbatch.Context) error { ran++; return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	st := store.NewRedisStore(rdb, time.Hour)
	sched := fairness.NewScheduler(rdb, fairness.Settings{
		Lane: fairness.LaneTime, GlobalConcurrency: 10, ReadyWindow: 100,
		LeaseTTL: 60, DefaultWeight: 1, WeightedConcurrency: true,
	})

	const slotID = "slot-live-holder"
	if _, err := sched.ClaimSlotExecution(ctx, slotID); err != nil {
		t.Fatal(err)
	}
	// Holder A is alive: it holds a live lease for this slot (expiry in the future).
	// Key layout mirrors fairness/keys.go: kafka_batch:fair_<lane>:leases.
	future := float64(time.Now().Add(45*time.Second).UnixNano()) / 1e9
	if err := rdb.ZAdd(ctx, "kafka_batch:fair_time:leases", redis.Z{Score: future, Member: slotID}).Err(); err != nil {
		t.Fatal(err)
	}

	batchID := "b-live"
	seq := int64(3)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j-dup", "batch_id": batchID, "job_type": "test.fairskip",
		"worker_class": "go:test.fairskip", "payload": map[string]interface{}{},
		"attempt": 0, "max_retries": 3, "batch_seq": seq,
		"_fair_slot": true, "_fair_slot_id": slotID, "_fair_type": "time", "tenant_id": "acme",
	})

	cfg := config.DefaultDaemon()
	p := &Processor{Cfg: cfg, Store: st, Producer: &memProducer{}, FairTime: sched, Now: time.Now}

	out, err := p.Process(ctx, raw, protocol.SourceCoords{Topic: "ready.time", Partition: 0, Offset: 7})
	if err != nil {
		t.Fatalf("Process error: %v", err)
	}
	if ran != 0 {
		t.Fatalf("handler re-ran %d time(s) while the holder's lease was live — double execution", ran)
	}
	if out.Event != nil {
		t.Fatalf("unexpected completion event emitted while holder is alive: %+v", out.Event)
	}
	if out.RetryPayload == nil {
		t.Fatal("expected the redelivery to be deferred to the retry topic, got none")
	}
	// The deferred copy must carry retry routing back to the source topic and keep
	// its fair-slot identity so it re-checks the same slot on return.
	var deferred map[string]interface{}
	if err := json.Unmarshal(out.RetryPayload, &deferred); err != nil {
		t.Fatal(err)
	}
	if deferred["retry_to"] != "ready.time" {
		t.Fatalf("retry_to = %v, want ready.time", deferred["retry_to"])
	}
	if deferred["_fair_slot_id"] != slotID {
		t.Fatalf("deferred payload dropped _fair_slot_id: %v", deferred["_fair_slot_id"])
	}
	if deferred["retry_after"] == nil || deferred["retry_after"] == "" {
		t.Fatal("deferred payload missing retry_after delay")
	}
}
