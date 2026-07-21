package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/cancellation"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

func TestCancelBatchAndBatchCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}
	ctx := context.Background()

	cache := cancellation.New(time.Hour, func(context.Context) ([]string, error) { return nil, nil })
	cancellation.SetProcessCache(cache)
	t.Cleanup(func() { cancellation.SetProcessCache(nil) })
	// Prime snapshot so AddToProcess keeps a non-stale fetchedAt window.
	if _, err := cache.Cancelled(ctx, "prime"); err != nil {
		t.Fatal(err)
	}

	err := c.CancelBatch(ctx, "missing")
	if _, ok := err.(BatchNotFoundError); !ok {
		t.Fatalf("err=%v", err)
	}

	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "b-cancel", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if err := c.CancelBatch(ctx, "b-cancel"); err != nil {
		t.Fatal(err)
	}
	row, err := st.FindBatch(ctx, "b-cancel")
	if err != nil || row == nil || row.Status != "cancelled" {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	cancelled, err := cache.Cancelled(ctx, "b-cancel")
	if err != nil || !cancelled {
		t.Fatalf("process cache cancelled=%v err=%v", cancelled, err)
	}

	b := &Batch{client: c, id: "b-cancel-2"}
	ok, err = st.CreateBatch(ctx, store.CreateBatchParams{ID: "b-cancel-2", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create2 ok=%v err=%v", ok, err)
	}
	if err := b.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
}
