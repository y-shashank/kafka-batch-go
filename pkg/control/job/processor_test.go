package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
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

func TestProcessSuccessEmitsEvent(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.echo", func(ctx *kbatch.Context) error { return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "b1"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.echo", WorkerClass: "go:test.echo",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, CompleteAfterRetries: 3,
		BatchSeq: &seq,
	})

	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out.Event == nil || out.Event.Status != "success" {
		t.Fatalf("event %+v", out.Event)
	}
	if out.Event.BatchSeq != 1 {
		t.Fatalf("batch_seq %d", out.Event.BatchSeq)
	}
}

func TestProcessHandlerErrorSchedulesRetry(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.fail", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Boom", Message: "boom"}
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "test.fail", WorkerClass: "go:test.fail",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})

	cfg := config.DefaultDaemon()
	p := &Processor{Cfg: cfg, Store: st, Producer: &memProducer{}, Now: func() time.Time { return time.Unix(0, 0) }}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if out.RetryTopic != cfg.RetryTopic("short") {
		t.Fatalf("retry topic %q", out.RetryTopic)
	}
	if out.RetryPayload == nil {
		t.Fatal("expected retry payload")
	}
}

func TestProcessExpiredJobDLT(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "any", ValidTill: "2000-01-01T00:00:00Z",
		Payload: map[string]interface{}{}, Attempt: 0,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{},
		Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
	var dlt map[string]interface{}
	_ = json.Unmarshal(out.DLTPayload, &dlt)
	if dlt["dlt_type"] != "expired" {
		t.Fatalf("dlt_type %v", dlt["dlt_type"])
	}
}

func TestProcessRubyRuntimeJobDLTWithoutRetry(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "ruby.missing", WorkerClass: "Missing",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"ruby.missing": {Runtime: "ruby", Topic: "jobs"},
	}}
	p := &Processor{
		Cfg: config.DefaultDaemon(), Manifest: manifest, Store: st, Producer: &memProducer{},
	}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 4})
	if err != nil {
		t.Fatal(err)
	}
	if out.RetryPayload != nil {
		t.Fatal("expected DLT not retry")
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
}

func TestProcessRetriesExhaustedHookBeforeDLT(t *testing.T) {
	kbatch.Reset()
	var hookCalled bool
	kbatch.OnRetriesExhausted("test.exhaust", func(s kbatch.RetriesExhaustedSummary, err error) {
		hookCalled = true
		if s.Attempt != 3 {
			t.Fatalf("attempt %d", s.Attempt)
		}
	})

	kbatch.Register("test.exhaust", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Boom", Message: "boom"}
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "test.exhaust", WorkerClass: "go:test.exhaust",
		Payload: map[string]interface{}{}, Attempt: 3, MaxRetries: 3,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 9})
	if err != nil {
		t.Fatal(err)
	}
	if !hookCalled {
		t.Fatal("expected retries_exhausted hook")
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT after hook")
	}
}

func TestProcessRecordsRetryFailureAndClearsOnSuccess(t *testing.T) {
	kbatch.Reset()
	attempts := 0
	kbatch.Register("test.flip", func(ctx *kbatch.Context) error {
		attempts++
		if ctx.Attempt == 0 {
			return &kbatch.HandlerError{Class: "Boom", Message: "boom"}
		}
		return nil
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "b1"
	seq := int64(1)
	rawFail, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.flip", WorkerClass: "go:test.flip",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
		BatchSeq: &seq, CompleteAfterRetries: 3,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{},
		Now: func() time.Time { return time.Unix(0, 0) }}
	out, err := p.Process(context.Background(), rawFail, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 10})
	if err != nil || out.RetryPayload == nil {
		t.Fatalf("retry out=%+v err=%v", out, err)
	}

	raw, err := rdb.HGet(context.Background(), "kafka_batch:b:"+batchID+":failures", "j1").Result()
	if err != nil || raw == "" {
		t.Fatalf("expected failure row, err=%v raw=%q", err, raw)
	}
	var row map[string]interface{}
	_ = json.Unmarshal([]byte(raw), &row)
	if row["status"] != "retrying" {
		t.Fatalf("status %v", row["status"])
	}

	rawOK, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.flip", WorkerClass: "go:test.flip",
		Payload: map[string]interface{}{}, Attempt: 1, MaxRetries: 3,
		BatchSeq: &seq, CompleteAfterRetries: 3,
	})
	out, err = p.Process(context.Background(), rawOK, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 11})
	if err != nil || out.Event == nil {
		t.Fatalf("success out=%+v err=%v", out, err)
	}
	n, err := rdb.HLen(context.Background(), "kafka_batch:b:"+batchID+":failures").Result()
	if err != nil || n != 0 {
		t.Fatalf("expected failure cleared, n=%d err=%v", n, err)
	}
}

func TestProcessJobLiveness(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.live", func(ctx *kbatch.Context) error { return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	rep := liveness.NewReporter(rdb, time.Minute)
	rep.TrackRunningJobs = true

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "test.live", WorkerClass: "go:test.live",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}, Liveness: rep}
	_, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 1, Offset: 12})
	if err != nil {
		t.Fatal(err)
	}
	if len(mr.Keys()) != 0 {
		t.Fatalf("expected job key cleaned up, keys=%v", mr.Keys())
	}
}

