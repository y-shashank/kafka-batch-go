package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

type fakeProducer struct {
	count int
}

func (f *fakeProducer) Produce(_ context.Context, _ string, _ string, _ []byte) error {
	f.count++
	return nil
}

func TestReconcileStuckRunningBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	created, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "b1", TotalJobs: 0, Sealed: false})
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	_, _ = st.AddJobs(ctx, "b1", 1)
	_, _ = st.SealBatch(ctx, "b1")
	_ = rdb.HSet(ctx, "kafka_batch:b:b1", "completed_count", 1)
	pastScore := float64(time.Now().Add(-time.Hour).UnixNano()) / 1e9
	_ = rdb.ZAdd(ctx, "kafka_batch:index:running", redis.Z{Score: pastScore, Member: "b1"})

	prod := &fakeProducer{}
	cfg := config.DefaultDaemon()
	cfg.ReconciliationInterval = time.Second
	cfg.CallbacksTopic = "kafka_batch.callbacks"
	cfg.RedisURL = "redis://" + mr.Addr()

	ResetScheduler()
	result := Run(ctx, cfg, st, prod, "test")
	if result != ResultCompleted {
		t.Fatalf("result %v", result)
	}
	if prod.count != 1 {
		t.Fatalf("callbacks produced=%d", prod.count)
	}
	row, _ := st.FindBatch(ctx, "b1")
	if row == nil || row.Status != "success" {
		t.Fatalf("batch %+v", row)
	}
}

func TestWithReconcilerLockSkipsSecond(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	var runs int
	ok1, _ := st.WithReconcilerLock(ctx, time.Minute, func() error {
		runs++
		ok2, _ := st.WithReconcilerLock(ctx, time.Minute, func() error {
			runs++
			return nil
		})
		if ok2 {
			t.Fatal("expected lock skip")
		}
		return nil
	})
	if !ok1 || runs != 1 {
		t.Fatalf("ok1=%v runs=%d", ok1, runs)
	}
}
