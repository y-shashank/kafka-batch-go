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

func TestRefireCallbackSkipsRecentRefire(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_ = rdb.HSet(ctx, "kafka_batch:b:b1",
		"id", "b1", "status", "success", "finished_at", now, "reconciler_refired_at", now)
	pastScore := float64(time.Now().Add(-time.Hour).UnixNano()) / 1e9
	_ = rdb.ZAdd(ctx, "kafka_batch:index:done", redis.Z{Score: pastScore, Member: "b1"})

	prod := &fakeProducer{}
	cfg := config.DefaultDaemon()
	cfg.ReconciliationInterval = time.Minute
	cfg.CallbacksTopic = "kafka_batch.callbacks"

	outcome := refireCallback(ctx, st, prod, cfg, cfg.ReconciliationInterval, &store.Batch{ID: "b1"})
	if outcome != outcomeSkippedRecentRefire {
		t.Fatalf("outcome=%q want skipped_recent_refire", outcome)
	}
	if prod.count != 0 {
		t.Fatalf("expected no callback produce, got %d", prod.count)
	}
}
