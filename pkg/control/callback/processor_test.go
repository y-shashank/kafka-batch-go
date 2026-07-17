package callback

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type spyInvoker struct {
	calls []protocol.CallbackMessage
}

func (s *spyInvoker) Invoke(_ context.Context, cb protocol.CallbackMessage) error {
	s.calls = append(s.calls, cb)
	return nil
}

type spyDLT struct {
	calls int32
}

func (s *spyDLT) ProduceDLT(_ context.Context, _ string, _ []byte) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

func TestProcessClaimsAndInvokes(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-1"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0", "locked_at", now,
	)
	mr.ZAdd("kafka_batch:index:done", 1, batchID)

	inv := &spyInvoker{}
	p := &Processor{Store: st, Invoker: inv, NodeID: "node-1"}

	raw, _ := json.Marshal(protocol.CallbackMessage{
		BatchID: batchID, Outcome: "success", TotalJobs: 1, CompletedCount: 1,
	})
	out, err := p.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if !out.CommitOffset {
		t.Fatal("expected commit")
	}
  if len(inv.calls) != 1 || inv.calls[0].BatchID != batchID {
		t.Fatalf("invoker calls %+v", inv.calls)
	}
	dispatched, err := st.CallbackDispatched(context.Background(), batchID)
	if err != nil || !dispatched {
		t.Fatalf("dispatched=%v err=%v", dispatched, err)
	}
	by, err := rdb.HGet(context.Background(), "kafka_batch:b:"+batchID, "callback_dispatched_by").Result()
	if err != nil || by != "node-1" {
		t.Fatalf("callback_dispatched_by=%q err=%v", by, err)
	}
}

func TestProcessRecordsRunnerWhenPreclaimed(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-pre"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0",
		"callback_dispatched_at", now,
		"complete_callback_dispatched_at", now,
	)

	inv := &spyInvoker{}
	p := &Processor{Store: st, Invoker: inv, NodeID: "runner-pod"}
	raw, _ := json.Marshal(protocol.CallbackMessage{
		BatchID: batchID, Outcome: "complete", Preclaimed: true, OnComplete: "MyCb",
	})
	_, err := p.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 1 {
		t.Fatalf("expected invoke, got %+v", inv.calls)
	}
	by, err := rdb.HGet(context.Background(), "kafka_batch:b:"+batchID, "callback_dispatched_by").Result()
	if err != nil || by != "runner-pod" {
		t.Fatalf("callback_dispatched_by=%q err=%v want runner-pod", by, err)
	}
}

func TestProcessSkipsWhenAlreadyDispatched(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-dup"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success",
		"callback_dispatched_at", now,
		"success_callback_dispatched_at", now,
	)

	inv := &spyInvoker{}
	p := &Processor{Store: st, Invoker: inv, NodeID: "node-1"}
	raw, _ := json.Marshal(protocol.CallbackMessage{BatchID: batchID, Outcome: "success"})
	_, err := p.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 0 {
		t.Fatalf("expected no invoke, got %+v", inv.calls)
	}
}

type countingInvoker struct {
	n int32
}

func (c *countingInvoker) Invoke(_ context.Context, _ protocol.CallbackMessage) error {
	atomic.AddInt32(&c.n, 1)
	return nil
}

func TestProcessClaimBeforeInvokeSingleWinner(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-race"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0", "locked_at", now,
	)
	mr.ZAdd("kafka_batch:index:done", 1, batchID)

	inv := &countingInvoker{}
	raw, _ := json.Marshal(protocol.CallbackMessage{
		BatchID: batchID, Outcome: "success", TotalJobs: 1, CompletedCount: 1,
	})

	const n = 8
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			p := &Processor{Store: st, Invoker: inv, NodeID: fmt.Sprintf("node-%d", i)}
			_, err := p.Process(context.Background(), raw)
			done <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&inv.n); got != 1 {
		t.Fatalf("expected exactly one invoke, got %d", got)
	}
	dispatched, err := st.CallbackDispatched(context.Background(), batchID)
	if err != nil || !dispatched {
		t.Fatalf("dispatched=%v err=%v", dispatched, err)
	}
}

func TestProcessMalformedCallbackJSON(t *testing.T) {
	var failed, dlt int32
	instrument.SetHandler(func(event string, _ map[string]interface{}, _ float64) {
		switch event {
		case "callback.failed":
			atomic.AddInt32(&failed, 1)
		case "dlt.published":
			atomic.AddInt32(&dlt, 1)
		}
	})
	defer instrument.SetHandler(nil)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	dltSpy := &spyDLT{}
	p := &Processor{Store: st, Invoker: LogInvoker{}, DLT: dltSpy, NodeID: "n1"}

	out, err := p.Process(context.Background(), []byte("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	if !out.CommitOffset {
		t.Fatal("expected commit")
	}
	if failed != 1 || dlt != 1 || dltSpy.calls != 1 {
		t.Fatalf("failed=%d dlt_events=%d dlt_calls=%d", failed, dlt, dltSpy.calls)
	}
}
