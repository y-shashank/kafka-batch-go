package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

func TestPollerTickSkipsCancelledBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	redisStore := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(500, 0)

	if err := redisStore.Schedule(ctx, "j-cancel", now.Add(-time.Second), 0, 9); err != nil {
		t.Fatal(err)
	}
	if created, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "batch-cancel", Sealed: true}); err != nil || !created {
		t.Fatalf("create batch err=%v created=%v", err, created)
	}
	if err := st.CancelBatch(ctx, "batch-cancel"); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"job_id":"j-cancel","batch_id":"batch-cancel","job_type":"x","worker_class":"go:x"}`)
	reader := &stubReader{found: map[string][]byte{"0:9": payload}}
	produced := 0
	poller := &Poller{
		Cfg: config.Daemon{SkipCancelledJobs: true, ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store: redisStore,
		Reader: reader,
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			produced++
			return nil
		}),
		Router: DaemonRouter{
			Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
				"x": {Runtime: "go", Topic: "jobs.x"},
			}},
		},
		Cancelled: st.BatchCancelled,
		Now:       func() time.Time { return now },
	}
	n, err := poller.Tick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("tick n=%d err=%v", n, err)
	}
	if produced != 0 {
		t.Fatal("cancelled job should not be produced")
	}
}

func TestPollerTickProduceFailureLeavesMember(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	redisStore := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(600, 0)
	if err := redisStore.Schedule(ctx, "j-fail", now.Add(-time.Second), 1, 3); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"job_id":"j-fail","job_type":"x","worker_class":"go:x"}`)
	reader := &stubReader{found: map[string][]byte{"1:3": payload}}
	poller := &Poller{
		Cfg:    config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  redisStore,
		Reader: reader,
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			return errors.New("kafka unavailable")
		}),
		Router: DaemonRouter{
			Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
				"x": {Runtime: "go", Topic: "jobs.x"},
			}},
		},
		Now: func() time.Time { return now },
	}
	n, err := poller.Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("tick n=%d err=%v", n, err)
	}
	inflight, err := rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || inflight != 1 {
		t.Fatalf("expected inflight lease retained, zcard=%d err=%v", inflight, err)
	}
}

func TestPollerTickDropsLostPayload(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	redisStore := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(700, 0)
	if err := redisStore.Schedule(ctx, "j-lost", now.Add(-time.Second), 2, 8); err != nil {
		t.Fatal(err)
	}
	reader := &stubReader{found: map[string][]byte{}, lost: []string{"2:8"}}
	poller := &Poller{
		Cfg:    config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  redisStore,
		Reader: reader,
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			t.Fatal("should not produce lost payload")
			return nil
		}),
		Router: DaemonRouter{Default: "fallback"},
		Now:    func() time.Time { return now },
	}
	n, err := poller.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("lost payload should not count as dispatched, n=%d", n)
	}
	inflight, err := rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || inflight != 0 {
		t.Fatalf("expected ack after drop, inflight=%d err=%v", inflight, err)
	}
}

func TestPollerProduceDuePartitioned(t *testing.T) {
	part := int32(4)
	var gotPart int32
	p := &Poller{
		Cfg: config.Daemon{},
		Producer: partitionedProducerFunc(func(_ context.Context, _, _ string, _ []byte, partition int32) error {
			gotPart = partition
			return nil
		}),
		Router: staticRouter{route: Route{Topic: "t", Key: "k", Partition: &part}},
	}
	raw := []byte(`{"job_id":"j1","job_type":"x"}`)
	if !p.produceDue(context.Background(), raw, "j1") {
		t.Fatal("expected success")
	}
	if gotPart != 4 {
		t.Fatalf("partition=%d", gotPart)
	}
}

type staticRouter struct{ route Route }

func (s staticRouter) Route(map[string]interface{}) (Route, error) { return s.route, nil }

type partitionedProducerFunc func(context.Context, string, string, []byte, int32) error

func (f partitionedProducerFunc) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload, 0)
}

func (f partitionedProducerFunc) ProducePartition(ctx context.Context, topic, key string, payload []byte, partition int32) error {
	return f(ctx, topic, key, payload, partition)
}
