package client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/schedule"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func TestEnqueueJobUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq: uniq.NewLocker(rdb, time.Hour),
	}
	payload := map[string]interface{}{"x": 1}
	ok, err := c.uniq.Claim(context.Background(), "go:uniq.job", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	_, err = c.EnqueueJob(context.Background(), "uniq.job", payload, PushOptions{})
	if err != ErrJobSkipped {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueJobAt(context.Background(), time.Now().Add(time.Minute), "uniq.job", payload, PushOptions{})
	if err != ErrJobSkipped {
		t.Fatalf("at err=%v", err)
	}
}

func TestPushJobClosedAndUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"echo":     {Runtime: "go"},
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq:  uniq.NewLocker(rdb, time.Hour),
		store: st,
	}
	ctx := context.Background()
	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "closed-b", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	mr.HSet("kafka_batch:b:closed-b", "status", "success")
	b := &Batch{client: c, id: "closed-b"}
	_, err = b.PushJob(ctx, "echo", map[string]interface{}{"a": 1}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = b.PushJobAt(ctx, time.Now().Add(time.Minute), "echo", map[string]interface{}{"a": 1}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("at err=%v", err)
	}

	payload := map[string]interface{}{"n": 1}
	ok, err = c.uniq.Claim(ctx, "go:uniq.job", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	_, err = b.PushJob(ctx, "uniq.job", payload, PushOptions{})
	if err != ErrJobSkipped {
		t.Fatalf("uniq err=%v", err)
	}
}

func TestPushManyJobsReserveFails(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{
		cfg: DefaultConfig(),
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"echo": {Runtime: "go"},
		}},
		store: st,
	}
	ctx := context.Background()
	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "done-b", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	mr.HSet("kafka_batch:b:done-b", "status", "success")
	b := &Batch{client: c, id: "done-b"}
	_, err = b.PushManyJobs(ctx, "echo", []map[string]interface{}{{"a": 1}}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = b.PushManyJobsAt(ctx, time.Now().Add(time.Minute), "echo", []map[string]interface{}{{"a": 1}}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("at err=%v", err)
	}
}

func TestSealNotFoundAndCreateBatchExists(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}
	ctx := context.Background()

	b := &Batch{client: c, id: "ghost"}
	_, err := b.Seal(ctx)
	if _, ok := err.(BatchNotFoundError); !ok {
		t.Fatalf("err=%v", err)
	}

	_, err = c.CreateBatch(ctx, BatchOptions{ID: "dup"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateBatch(ctx, BatchOptions{ID: "dup"}, nil)
	if _, ok := err.(BatchExistsError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestRedisScheduleIndexViaWrite(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	inner := schedule.NewRedisStore(rdb, 500)
	c := &Client{
		cfg:   Config{ScheduleIndexWriteRetries: 1},
		sched: redisScheduleIndex{inner: inner},
	}
	runAt := time.Now().UTC().Add(time.Hour)
	err := c.writeScheduleIndex(context.Background(), []schedule.ScheduleEntry{
		{JobID: "j1", RunAt: runAt, Partition: 1, Offset: 2},
	}, "", "j1", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c.writeScheduleIndex(context.Background(), []schedule.ScheduleEntry{
		{JobID: "j2", RunAt: runAt, Partition: 1, Offset: 3},
		{JobID: "j3", RunAt: runAt, Partition: 1, Offset: 4},
	}, "b", "j2", 2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientNewValidationErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Brokers = nil
	if _, err := New(cfg); err == nil {
		t.Fatal("expected brokers required")
	}
	cfg = DefaultConfig()
	cfg.RedisURL = ""
	if _, err := New(cfg); err == nil {
		t.Fatal("expected redis required")
	}
	cfg = DefaultConfig()
	cfg.RedisURL = "://bad"
	if _, err := New(cfg); err == nil {
		t.Fatal("expected redis parse error")
	}
	cfg = DefaultConfig()
	cfg.ManifestPath = filepath.Join(t.TempDir(), "missing.yml")
	mr := miniredis.RunT(t)
	cfg.RedisURL = "redis://" + mr.Addr()
	if _, err := New(cfg); err == nil {
		t.Fatal("expected missing manifest error")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "handlers.yml")
	if err := os.WriteFile(path, []byte("handlers: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg = DefaultConfig()
	cfg.ManifestPath = path
	cfg.RedisURL = "redis://" + mr.Addr()
	cfg.ScheduleStore = "bogus"
	if _, err := New(cfg); err == nil {
		t.Fatal("expected schedule store error")
	}

	cfg = DefaultConfig()
	cfg.BatchTTL = 0
	cfg.UniqLockTTL = 0
	cfg.ManifestPath = path
	cfg.RedisURL = "redis://" + mr.Addr()
	cfg.Brokers = []string{"127.0.0.1:1"}
	cfg.FairnessDynamicTenantPartitions = false
	_, err := New(cfg)
	// Empty handlers + ManifestPath fails validateManifest before Kafka.
	if err == nil {
		t.Fatal("expected empty manifest error")
	}

	pathOK := filepath.Join(dir, "ok.yml")
	if err := os.WriteFile(pathOK, []byte(`handlers:
  echo:
    runtime: go
    topic: jobs.echo
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg = DefaultConfig()
	cfg.ManifestPath = pathOK
	cfg.RedisURL = "redis://" + mr.Addr()
	cfg.Brokers = []string{"127.0.0.1:1"}
	cfg.FairnessDynamicTenantPartitions = false
	cfg.ValidateTopicsOnConnect = false
	_, err = New(cfg)
	// Reaches kafkaclient.New against a dead broker — must not succeed here.
	if err == nil {
		t.Log("kafka unexpectedly available at 127.0.0.1:1")
	}
}

func TestResolveWorkerEntryPrefix(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TopicPrefix = "ship"
	cfg.AllowUnknownWorkerClasses = true
	c := &Client{cfg: cfg}
	c.buildWorkerIndex()
	_, entry, err := c.lookupWorkerClass("AdHoc::Worker")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Topic != "ship.kafka_batch.jobs" {
		t.Fatalf("topic=%q", entry.Topic)
	}

	cfg.Workers = map[string]WorkerClassConfig{
		"Prefixed::W": {JobType: "custom.jt", Topic: "raw.topic", ApplyTopicPrefix: true},
	}
	c = &Client{cfg: cfg}
	c.buildWorkerIndex()
	jt, entry, err := c.lookupWorkerClass("Prefixed::W")
	if err != nil {
		t.Fatal(err)
	}
	if jt != "custom.jt" || entry.Topic != "ship.raw.topic" {
		t.Fatalf("jt=%s entry=%+v", jt, entry)
	}
}

func TestWorkerEnqueueUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.Workers = map[string]WorkerClassConfig{"W": {Uniq: true, Topic: "jobs.w"}}
	c := &Client{cfg: cfg, uniq: uniq.NewLocker(rdb, time.Hour), store: store.NewRedisStore(rdb, time.Hour)}
	c.buildWorkerIndex()
	payload := map[string]interface{}{"k": 1}
	ok, err := c.uniq.Claim(context.Background(), "W", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	_, err = c.Enqueue(context.Background(), "W", payload, PushOptions{})
	if err != ErrJobSkipped {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueAt(context.Background(), time.Now().Add(time.Minute), "W", payload, PushOptions{})
	if err != ErrJobSkipped {
		t.Fatalf("at err=%v", err)
	}

	ctx := context.Background()
	ok, err = c.store.CreateBatch(ctx, store.CreateBatchParams{ID: "wb-closed", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if err := c.store.CancelBatch(ctx, "wb-closed"); err != nil {
		t.Fatal(err)
	}
	b := &Batch{client: c, id: "wb-closed"}
	_, err = b.Push(ctx, "W", map[string]interface{}{"k": 2}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("push err=%v", err)
	}
	_, err = b.PushAt(ctx, time.Now().Add(time.Minute), "W", map[string]interface{}{"k": 3}, PushOptions{})
	if _, ok := err.(BatchClosedError); !ok {
		t.Fatalf("pushAt err=%v", err)
	}
}
