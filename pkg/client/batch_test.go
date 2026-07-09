package client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

func TestCreateBatchPersistsLedger(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{
		cfg:      DefaultConfig(),
		manifest: config.Manifest{},
		store:    st,
	}

	b, err := c.CreateBatch(context.Background(), BatchOptions{
		OnSuccess: "MyCallback#done",
		Meta:      map[string]interface{}{"source": "test"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.ID() == "" {
		t.Fatal("expected batch id")
	}
	row, err := st.FindBatch(context.Background(), b.ID())
	if err != nil || row == nil {
		t.Fatalf("find err=%v row=%v", err, row)
	}
	if row.OnSuccess != "MyCallback#done" {
		t.Fatalf("on_success=%q", row.OnSuccess)
	}
}

func TestCreateBatchBlockFormSealsAfterPopulate(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}

	var capturedID string
	b, err := c.CreateBatch(context.Background(), BatchOptions{Description: "block batch"}, func(b *Batch) error {
		capturedID = b.ID()
		add, err := st.AddJobs(context.Background(), b.ID(), 2)
		if err != nil || add.Status != "ok" {
			t.Fatalf("add %+v err=%v", add, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.ID() != capturedID {
		t.Fatalf("ids differ b=%s captured=%s", b.ID(), capturedID)
	}
	row, err := st.FindBatch(context.Background(), b.ID())
	if err != nil || row == nil || row.TotalJobs != 2 {
		t.Fatalf("row=%+v err=%v", row, err)
	}
	if row.Status != "running" {
		t.Fatalf("status=%q", row.Status)
	}
}

func TestOpenBatchRehydratesExisting(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	ctx := context.Background()
	if ok, err := st.CreateBatch(ctx, store.CreateBatchParams{
		ID: "existing-1", OnSuccess: "Cb", Meta: map[string]interface{}{"k": "v"}, Sealed: true,
	}); err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}

	c := &Client{cfg: DefaultConfig(), store: st}
	b, err := c.OpenBatch(ctx, "existing-1")
	if err != nil {
		t.Fatal(err)
	}
	if b.onSuccess != "Cb" {
		t.Fatalf("on_success=%q", b.onSuccess)
	}
	if b.meta["k"] != "v" {
		t.Fatalf("meta=%v", b.meta)
	}
}

func TestOpenBatchNotFound(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := &Client{cfg: DefaultConfig(), store: store.NewRedisStore(rdb, time.Hour)}
	_, err := c.OpenBatch(context.Background(), "missing")
	if _, ok := err.(BatchNotFoundError); !ok {
		t.Fatalf("err=%v", err)
	}
}
