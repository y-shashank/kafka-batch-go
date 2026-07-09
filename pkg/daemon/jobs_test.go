package daemon

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type captureProducer struct {
	topics []string
}

func (p *captureProducer) Produce(_ context.Context, topic, _ string, _ []byte) error {
	p.topics = append(p.topics, topic)
	return nil
}

func runJobHandler(t *testing.T, raw []byte, cfg config.Daemon, st *store.RedisStore) (*captureProducer, error) {
	t.Helper()
	prod := &captureProducer{}
	proc := &job.Processor{Cfg: cfg, Store: st, Producer: prod}
	rec := &kgo.Record{Topic: "jobs", Partition: 0, Offset: 1, Value: raw}
	out, err := proc.Process(context.Background(), rec.Value, protocol.SourceCoords{
		Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset,
	})
	if err != nil {
		return prod, err
	}
	return prod, applyJobOutcome(context.Background(), cfg, prod, out)
}

func TestJobHandlerPathBatchSuccessEmitsEvent(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.echo", func(ctx *kbatch.Context) error { return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, 0)
	ctx := context.Background()
	batchID := "b-success"
	if ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: batchID, Sealed: true}); err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := st.AddJobs(ctx, batchID, 1); err != nil {
		t.Fatal(err)
	}

	cfg := config.DefaultDaemon()
	cfg.EventsTopic = "events.test"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, BatchSeq: &seq,
		JobType: "test.echo", WorkerClass: "go:test.echo",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})
	prod, err := runJobHandler(t, raw, cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(prod.topics) != 1 || prod.topics[0] != "events.test" {
		t.Fatalf("topics=%v", prod.topics)
	}
}

func TestJobHandlerPathFailureProducesRetry(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.fail", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Err", Message: "fail"}
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, 0)
	cfg := config.DefaultDaemon()
	cfg.RetryTopicBase = "retry"
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j2", "job_type": "test.fail", "worker_class": "go:test.fail",
		"payload": map[string]interface{}{}, "attempt": 0, "max_retries": 3,
	})
	prod, err := runJobHandler(t, raw, cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	if len(prod.topics) != 1 || prod.topics[0] != "retry.short" {
		t.Fatalf("topics=%v", prod.topics)
	}
}
