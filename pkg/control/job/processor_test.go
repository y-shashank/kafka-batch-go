package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/cancellation"
	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/retrycancel"
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
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, BatchSeq: &seq,
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

func TestProcessHandlerErrorEmitsExecutedForBatch(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.fail2", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Boom", Message: "boom"}
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "b-exec"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.fail2", WorkerClass: "go:test.fail2",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, BatchSeq: &seq,
	})

	cfg := config.DefaultDaemon()
	p := &Processor{Cfg: cfg, Store: st, Producer: &memProducer{}, Now: func() time.Time { return time.Unix(0, 0) }}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if out.Event == nil || out.Event.Status != "executed" {
		t.Fatalf("expected executed event, got %+v", out.Event)
	}
	if out.RetryPayload == nil {
		t.Fatal("expected retry payload")
	}
	var retry map[string]interface{}
	_ = json.Unmarshal(out.RetryPayload, &retry)
	if retry["batch_counted"] != true {
		t.Fatalf("expected batch_counted on retry payload, got %#v", retry["batch_counted"])
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

func TestProcessCancelledBatchSkipsHandlerViaCache(t *testing.T) {
	kbatch.Reset()
	ran := false
	kbatch.Register("test.cancel", func(ctx *kbatch.Context) error {
		ran = true
		return nil
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()
	if _, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "b-cancel", Sealed: true}); err != nil {
		t.Fatal(err)
	}
	if err := st.CancelBatch(ctx, "b-cancel"); err != nil {
		t.Fatal(err)
	}

	batchID := "b-cancel"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.cancel", WorkerClass: "go:test.cancel",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, BatchSeq: &seq,
	})

	cache := cancellation.New(2*time.Minute, st.CancelledBatchIDs)
	p := &Processor{
		Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}, CancelCache: cache,
	}
	out, err := p.Process(ctx, raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 5})
	if err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("handler should not run for cancelled batch")
	}
	if out.Event != nil || out.RetryPayload != nil || out.DLTPayload != nil {
		t.Fatalf("expected silent skip, got %+v", out)
	}
	// Second job must hit cache, not require another Redis list (still correct).
	out2, err := p.Process(ctx, raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 6})
	if err != nil {
		t.Fatal(err)
	}
	if out2.Event != nil {
		t.Fatal("second cancelled job should also skip")
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

func TestProcessRetryDoesNotCacheRetryingInRedis(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.flip", func(ctx *kbatch.Context) error {
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
		BatchSeq: &seq,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{},
		Now: func() time.Time { return time.Unix(0, 0) }}
	out, err := p.Process(context.Background(), rawFail, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 10})
	if err != nil || out.RetryPayload == nil {
		t.Fatalf("retry out=%+v err=%v", out, err)
	}
	// Retrying jobs are listed from Kafka; no per-job failure metadata is
	// ever written to Redis (p.Failures is nil in this test, matching the
	// default Redis-only store).
	n, err := rdb.HLen(context.Background(), "kafka_batch:b:"+batchID+":failures").Result()
	if err != nil || n != 0 {
		t.Fatalf("expected no retrying cache row, n=%d err=%v", n, err)
	}

	rawOK, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.flip", WorkerClass: "go:test.flip",
		Payload: map[string]interface{}{}, Attempt: 1, MaxRetries: 3,
		BatchSeq: &seq,
	})
	out, err = p.Process(context.Background(), rawOK, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 11})
	if err != nil || out.Event == nil || out.Event.Status != "success" {
		t.Fatalf("success out=%+v err=%v", out, err)
	}
}

func TestProcessSkipsRetryCancelJobID(t *testing.T) {
	kbatch.Reset()
	ran := false
	kbatch.Register("test.skip", func(ctx *kbatch.Context) error {
		ran = true
		return nil
	})
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	cancel := &retrycancel.Store{Client: rdb}
	ctx := context.Background()
	if _, err := cancel.Cancel(ctx, []string{"j-skip"}); err != nil {
		t.Fatal(err)
	}
	batchID := "b-skip"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j-skip", BatchID: &batchID, JobType: "test.skip", WorkerClass: "go:test.skip",
		Payload: map[string]interface{}{}, BatchSeq: &seq,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}, RetryCancel: cancel}
	out, err := p.Process(ctx, raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("handler should not run")
	}
	if out.Event == nil || out.Event.Status != "failed" {
		t.Fatalf("event %+v", out.Event)
	}
	if cancel.Cancelled(ctx, "j-skip") {
		t.Fatal("expected acknowledge")
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

