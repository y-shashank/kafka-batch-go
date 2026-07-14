package event

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
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
