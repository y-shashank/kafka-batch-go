package event

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type memProducer struct {
	msgs []struct {
		topic string
		key   string
		body  []byte
	}
}

func (m *memProducer) Produce(_ context.Context, topic, key string, payload []byte) error {
	m.msgs = append(m.msgs, struct {
		topic string
		key   string
		body  []byte
	}{topic, key, payload})
	return nil
}

func (m *memProducer) ProduceMany(_ context.Context, reqs ...kafkaclient.ProduceRequest) error {
	for _, r := range reqs {
		if err := m.Produce(context.Background(), r.Topic, r.Key, r.Value); err != nil {
			return err
		}
	}
	return nil
}

func seedRunningBatch(t *testing.T, mr *miniredis.Miniredis, batchID string, total int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID,
		"total_jobs", strconv.Itoa(total),
		"completed_count", "0",
		"failed_count", "0",
		"status", "running",
		"locked_at", now,
		"on_success", "MyCb",
		"on_complete", "MyCb",
	)
	mr.ZAdd("kafka_batch:index:running", 1, batchID)
}

func TestProcessBatchFinalizesAndProducesCallback(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	prod := &memProducer{}

	batchID := "batch-ev-1"
	seedRunningBatch(t, mr, batchID, 1)

	ev, _ := json.Marshal(protocol.EventMessage{
		BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1,
		WorkerClass: "go:integration.go_daemon",
	})

	cfg := config.DefaultDaemon()
	cfg.CallbacksTopic = "kafka_batch.callbacks.test"
	p := &Processor{Cfg: cfg, Store: st, Producer: prod}

	out, err := p.ProcessBatch(context.Background(), [][]byte{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Callbacks) != 1 {
		t.Fatalf("callbacks %+v", out.Callbacks)
	}
	if out.Callbacks[0].Outcome != "success" {
		t.Fatalf("outcome %q", out.Callbacks[0].Outcome)
	}
	if len(prod.msgs) != 1 || prod.msgs[0].topic != cfg.CallbacksTopic {
		t.Fatalf("produced %+v", prod.msgs)
	}

	batch, err := st.FindBatch(context.Background(), batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != "success" {
		t.Fatalf("status %q", batch.Status)
	}
}

func TestProcessBatchMultipleEventsOnePoll(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	prod := &memProducer{}

	batchID := "batch-multi"
	seedRunningBatch(t, mr, batchID, 2)

	ev1, _ := json.Marshal(protocol.EventMessage{BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1})
	ev2, _ := json.Marshal(protocol.EventMessage{BatchID: batchID, JobID: "j2", Status: "success", BatchSeq: 2})

	cfg := config.DefaultDaemon()
	cfg.CallbacksTopic = "callbacks.multi"
	p := &Processor{Cfg: cfg, Store: st, Producer: prod}

	_, err := p.ProcessBatch(context.Background(), [][]byte{ev1, ev2})
	if err != nil {
		t.Fatal(err)
	}
	if len(prod.msgs) != 1 {
		t.Fatalf("expected 1 batched callback, got %d", len(prod.msgs))
	}
	batch, err := st.FindBatch(context.Background(), batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != "success" {
		t.Fatalf("status %q", batch.Status)
	}
}

func TestProcessBatchReplayProducesCallbackWhenNotDispatched(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	prod := &memProducer{}

	batchID := "batch-replay"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID,
		"total_jobs", "1",
		"completed_count", "1",
		"failed_count", "0",
		"status", "success",
		"locked_at", now,
		"finished_at", now,
		"on_success", "Cb",
		"on_complete", "Cb",
	)
	mr.ZAdd("kafka_batch:index:done", 1, batchID)

	ev, _ := json.Marshal(protocol.EventMessage{
		BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1,
	})

	cfg := config.DefaultDaemon()
	cfg.CallbacksTopic = "callbacks.replay"
	p := &Processor{Cfg: cfg, Store: st, Producer: prod}

	out, err := p.ProcessBatch(context.Background(), [][]byte{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Callbacks) != 1 {
		t.Fatalf("callbacks %+v", out.Callbacks)
	}
	if len(prod.msgs) != 1 {
		t.Fatalf("produced %+v", prod.msgs)
	}
}

func TestProcessBatchSkipsInvalidWithoutFailing(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	prod := &memProducer{}
	cfg := config.DefaultDaemon()
	cfg.CallbacksTopic = "callbacks"
	p := &Processor{Cfg: cfg, Store: st, Producer: prod}

	out, err := p.ProcessBatch(context.Background(), [][]byte{
		[]byte(`not-json`),
		[]byte(`{"batch_id":"","batch_seq":1,"job_id":"j","status":"success"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Callbacks) != 0 {
		t.Fatalf("callbacks %+v", out.Callbacks)
	}
}

// Regression (#1): a completion event for a nonexistent batch must be surfaced
// via instrumentation (completion.dropped), not silently swallowed, and the
// batch is still acknowledged (no poison loop).
func TestProcessBatchInstrumentsDroppedCompletion(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	var dropped []map[string]interface{}
	remove := instrument.AddHandler(func(event string, payload map[string]interface{}, _ float64) {
		if event == "completion.dropped" {
			dropped = append(dropped, payload)
		}
	})
	defer remove()

	p := &Processor{Cfg: config.Daemon{CallbacksTopic: "cb"}, Store: st, Producer: &memProducer{}}
	ev := protocol.EventMessage{BatchID: "ghost", JobID: "j1", Status: "success", BatchSeq: 2}
	raw, _ := json.Marshal(ev)
	out, err := p.ProcessBatch(context.Background(), [][]byte{raw})
	if err != nil {
		t.Fatalf("ProcessBatch returned error (should ack): %v", err)
	}
	if len(out.Callbacks) != 0 {
		t.Fatalf("expected no callbacks, got %d", len(out.Callbacks))
	}
	if len(dropped) != 1 {
		t.Fatalf("expected 1 completion.dropped event, got %d", len(dropped))
	}
	if dropped[0]["reason"] != "not_found" || dropped[0]["batch_id"] != "ghost" {
		t.Fatalf("unexpected dropped payload: %+v", dropped[0])
	}
}

// failCallbacksProducer errors on any produce to failTopic (and on ProduceMany
// containing it), but succeeds for every other topic (e.g. the DLT).
type failCallbacksProducer struct {
	failTopic string
	produced  []struct{ topic, key string }
}

func (f *failCallbacksProducer) Produce(_ context.Context, topic, key string, _ []byte) error {
	if topic == f.failTopic {
		return errProduce
	}
	f.produced = append(f.produced, struct{ topic, key string }{topic, key})
	return nil
}

var errProduce = errors.New("simulated produce failure")

// Regression (#4): when the callbacks topic can't be produced, the callback must
// be parked on the dead-letter topic (never silently lost), ProcessBatch must
// still ack, and it must NOT double-produce to the callbacks topic.
func TestProcessBatchDeadLettersUnproducibleCallback(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	// A sealed 1-job batch that finalizes on the incoming success event.
	if _, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "b1", TotalJobs: 0, Sealed: false}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddJobs(ctx, "b1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SealBatch(ctx, "b1"); err != nil {
		t.Fatal(err)
	}

	var failedEv []map[string]interface{}
	remove := instrument.AddHandler(func(event string, payload map[string]interface{}, _ float64) {
		if event == "callback.produce_failed" {
			failedEv = append(failedEv, payload)
		}
	})
	defer remove()

	prod := &failCallbacksProducer{failTopic: "cb"}
	p := &Processor{Cfg: config.Daemon{CallbacksTopic: "cb", DeadLetterTopic: "dlt"}, Store: st, Producer: prod}

	ev := protocol.EventMessage{BatchID: "b1", JobID: "j1", Status: "success", BatchSeq: 1}
	raw, _ := json.Marshal(ev)
	out, err := p.ProcessBatch(ctx, [][]byte{raw})
	if err != nil {
		t.Fatalf("ProcessBatch returned error (should ack): %v", err)
	}
	if len(out.Callbacks) != 1 {
		t.Fatalf("expected 1 finalized callback, got %d", len(out.Callbacks))
	}
	if len(failedEv) != 1 {
		t.Fatalf("expected 1 callback.produce_failed instrument, got %d", len(failedEv))
	}
	// It must have been parked on the DLT (and only the DLT, never the cb topic).
	dltCount := 0
	for _, pm := range prod.produced {
		if pm.topic == "dlt" && pm.key == "b1" {
			dltCount++
		}
		if pm.topic == "cb" {
			t.Fatalf("callback must not be produced to cb topic on failure")
		}
	}
	if dltCount != 1 {
		t.Fatalf("expected callback parked on dlt once, got %d", dltCount)
	}
}
