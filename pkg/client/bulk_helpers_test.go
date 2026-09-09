package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

func TestProducedFlags(t *testing.T) {
	if got := producedFlags(context.Canceled); got != nil {
		t.Fatalf("non-partial error flags=%v", got)
	}
	if got := producedFlags(&PartialProduceError{Message: "x"}); got != nil {
		t.Fatalf("nil-flag partial error flags=%v", got)
	}
	flags := []bool{true, false, true}
	pe := &PartialProduceError{Message: "x", ProducedCount: 2, Produced: flags}
	if got := producedFlags(pe); len(got) != 3 || !got[0] || got[1] || !got[2] {
		t.Fatalf("flags=%v", got)
	}
}

func TestSeqWindowTake(t *testing.T) {
	var empty seqWindow
	if _, err := empty.take(); err == nil {
		t.Fatal("expected no reserved slots")
	}
	w := seqWindow{next: 1, end: 2}
	seq, err := w.take()
	if err != nil || seq != 1 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	seq, err = w.take()
	if err != nil || seq != 2 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	if _, err := w.take(); err == nil {
		t.Fatal("expected too few slots")
	}
}

// Concurrent pushes into one *Batch must receive disjoint batch_seq values —
// the old struct-level cursor was overwritten by every reserve, silently
// reusing/skipping seqs and corrupting the completion bitmap.
func TestConcurrentReserveWindowsAreDisjoint(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}
	ctx := context.Background()
	if ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "cw", Sealed: false}); err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	b := &Batch{client: c, id: "cw"}

	const goroutines = 8
	const perG = 25
	seqs := make(chan int64, goroutines*perG)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			win, err := b.reserve(ctx, perG)
			if err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < perG; i++ {
				s, err := win.take()
				if err != nil {
					t.Error(err)
					return
				}
				seqs <- s
			}
		}()
	}
	wg.Wait()
	close(seqs)
	seen := map[int64]bool{}
	for s := range seqs {
		if seen[s] {
			t.Fatalf("batch_seq %d issued twice", s)
		}
		seen[s] = true
	}
	if len(seen) != goroutines*perG {
		t.Fatalf("issued %d seqs, want %d", len(seen), goroutines*perG)
	}
}

func TestPlanPushesAndUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.UniqOnDuplicate = "skip"
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq: uniq.NewLocker(rdb, time.Hour),
	}
	b := &Batch{client: c, id: "batch-u"}
	payloads := []map[string]interface{}{{"n": 1}, {"n": 1}, {"n": 1}}
	entry, plans, jobIDs, err := b.planPushes(context.Background(), "uniq.job", payloads)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Uniq != true {
		t.Fatalf("entry=%+v", entry)
	}
	if len(plans) != 1 {
		t.Fatalf("plans=%d want 1 unique", len(plans))
	}
	if len(jobIDs) != 3 || jobIDs[0] == "" || jobIDs[1] != "" || jobIDs[2] != "" {
		t.Fatalf("jobIDs=%v", jobIDs)
	}
	if plans[0].fp == "" {
		t.Fatal("expected fingerprint on claimed plan")
	}
}

func TestPlanPushesUnknownHandler(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}, id: "b"}
	_, _, _, err := b.planPushes(context.Background(), "missing", []map[string]interface{}{{"x": 1}})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPushManyJobsEmptyAndAllSkipped(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq:  uniq.NewLocker(rdb, time.Hour),
		store: store.NewRedisStore(rdb, time.Hour),
	}
	b := &Batch{client: c, id: "b1"}

	ids, err := b.PushManyJobs(context.Background(), "uniq.job", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("empty ids=%v err=%v", ids, err)
	}

	// Pre-claim so both bulk slots are skipped → no Kafka needed.
	payload := map[string]interface{}{"n": 1}
	ok, err := c.uniq.Claim(context.Background(), "go:uniq.job", payload, "pre")
	if err != nil || !ok {
		t.Fatalf("preclaim ok=%v err=%v", ok, err)
	}
	ids, err = b.PushManyJobs(context.Background(), "uniq.job", []map[string]interface{}{payload, payload}, PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "" || ids[1] != "" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestPushManyJobsAtEmpty(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig()}, id: "b"}
	ids, err := b.PushManyJobsAt(context.Background(), time.Now(), "x", nil, PushOptions{})
	if err != nil || ids != nil {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}

func TestRollbackPlans(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	cfg := DefaultConfig()
	c := &Client{cfg: cfg, uniq: uniq.NewLocker(rdb, time.Hour), store: st}
	ctx := context.Background()
	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "rb", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := st.AddJobs(ctx, "rb", 2); err != nil {
		t.Fatal(err)
	}
	b := &Batch{client: c, id: "rb"}
	entry := config.HandlerEntry{Uniq: true}
	payload := map[string]interface{}{"x": 1}
	fp := uniq.DigestHex("go:echo", payload)
	_, _ = c.uniq.Claim(ctx, "go:echo", payload, "j-keep")
	_, _ = c.uniq.Claim(ctx, "go:echo", map[string]interface{}{"x": 2}, "j-drop")
	plans := []pushPlan{
		{jobID: "j-keep", payload: payload, fp: fp},
		{jobID: "j-drop", payload: map[string]interface{}{"x": 2}, fp: ""},
	}
	// Non-prefix flags: the produced plan is FIRST — rollback must skip it and
	// only roll back the failed one (deliveries complete out of input order).
	b.rollbackPlans(ctx, entry, "echo", plans, []bool{true, false})
	row, _ := st.FindBatch(ctx, "rb")
	if row == nil || row.TotalJobs != 1 {
		t.Fatalf("row=%+v", row)
	}
	// The produced job's uniq lock must survive rollback: releasing it would
	// let a duplicate enqueue while the job is still pending.
	if ok, _ := c.uniq.Claim(ctx, "go:echo", payload, "j-other"); ok {
		t.Fatal("produced plan's uniq lock was released during rollback")
	}
}

func TestRollbackPlansNilRollsBackAll(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), uniq: uniq.NewLocker(rdb, time.Hour), store: st}
	ctx := context.Background()
	if ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "rb2", Sealed: false}); err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	if _, err := st.AddJobs(ctx, "rb2", 2); err != nil {
		t.Fatal(err)
	}
	b := &Batch{client: c, id: "rb2"}
	plans := []pushPlan{{jobID: "a"}, {jobID: "b"}}
	b.rollbackPlans(ctx, config.HandlerEntry{}, "echo", plans, nil)
	row, _ := st.FindBatch(ctx, "rb2")
	if row == nil || row.TotalJobs != 0 {
		t.Fatalf("row=%+v", row)
	}
}

func TestEnqueueJobInUnknownHandler(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}
	_, err := c.EnqueueJobIn(context.Background(), time.Second, "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestPushJobInUnknownHandler(t *testing.T) {
	b := &Batch{client: &Client{cfg: DefaultConfig(), manifest: config.Manifest{}}, id: "b"}
	_, err := b.PushJobIn(context.Background(), time.Second, "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}

func TestReserveStatuses(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	c := &Client{cfg: DefaultConfig(), store: st}
	ctx := context.Background()

	b := &Batch{client: c, id: "missing"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected not_found")
	} else if _, ok := err.(BatchNotFoundError); !ok {
		t.Fatalf("err=%v", err)
	}

	ok, err := st.CreateBatch(ctx, store.CreateBatchParams{ID: "open", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}
	b = &Batch{client: c, id: "open"}
	win, err := b.reserve(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := win.take()
	if err != nil || seq < 1 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}

	win3, err := b.reserve(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if win3.end-win3.next+1 != 3 {
		t.Fatalf("bad window next=%d end=%d", win3.next, win3.end)
	}
	if win3.next <= seq {
		t.Fatalf("windows overlap: first seq=%d second window starts at %d", seq, win3.next)
	}

	ok, err = st.CreateBatch(ctx, store.CreateBatchParams{ID: "sealed", Sealed: true})
	if err != nil || !ok {
		t.Fatalf("sealed create ok=%v err=%v", ok, err)
	}
	// Terminal status is what AddJobs treats as closed (matches store tests).
	mr.HSet("kafka_batch:b:sealed", "status", "success")
	b = &Batch{client: c, id: "sealed"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected closed")
	} else if ce, ok := err.(BatchClosedError); !ok || ce.Reason != "closed" {
		t.Fatalf("err=%v", err)
	}

	ok, err = st.CreateBatch(ctx, store.CreateBatchParams{ID: "canc", Sealed: false})
	if err != nil || !ok {
		t.Fatalf("canc create ok=%v err=%v", ok, err)
	}
	if err := st.CancelBatch(ctx, "canc"); err != nil {
		t.Fatal(err)
	}
	b = &Batch{client: c, id: "canc"}
	if _, err := b.reserve(ctx, 1); err == nil {
		t.Fatal("expected cancelled")
	} else if ce, ok := err.(BatchClosedError); !ok || ce.Reason != "cancelled" {
		t.Fatalf("err=%v", err)
	}
}
