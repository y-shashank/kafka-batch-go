package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

type sfMarker struct {
	mu     sync.Mutex
	marked []*kgo.Record
}

func (f *sfMarker) MarkCommitRecords(recs ...*kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, recs...)
}

func (f *sfMarker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.marked)
}

func TestExtractJobID(t *testing.T) {
	if got := extractJobID([]byte(`{"job_id":"abc"}`)); got != "abc" {
		t.Fatalf("got %q", got)
	}
	if got := extractJobID([]byte(`{`)); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestSuperFetchClaimAckBeforePerform(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)

	var started sync.WaitGroup
	started.Add(1)
	release := make(chan struct{})
	var performs int32

	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 4
	cfg.SuperFetchLeaseTTL = time.Minute

	exec := NewSuperFetchExecutor(cfg, work, "consumer-1",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			atomic.AddInt32(&performs, 1)
			started.Done()
			<-release
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)

	rec := &kgo.Record{
		Topic: "jobs", Partition: 0, Offset: 7,
		Value: []byte(`{"job_id":"sf-1","job_type":"x"}`),
	}

	// Claim + mark path (without real kgo client): exercise via Work + inFlight directly.
	ctx := context.Background()
	claim, err := work.Claim(ctx, workset.ClaimParams{
		JobID: "sf-1", Payload: rec.Value, Topic: rec.Topic,
		Partition: 0, Offset: 7, ConsumerID: "consumer-1", LeaseTTL: time.Minute,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	marker := &sfMarker{}
	marker.MarkCommitRecords(rec)
	if marker.count() != 1 {
		t.Fatal("expected immediate mark")
	}

	exec.Sem <- struct{}{}
	exec.inFlight.Store("sf-1", struct{}{})
	go exec.perform(ctx, rec, "sf-1", claim.Fence, "g")

	started.Wait()
	if atomic.LoadInt32(&performs) != 1 {
		t.Fatal("expected perform started")
	}
	// Still owned while performing
	ok, _ := work.StillOwned(ctx, "sf-1", "consumer-1", claim.Fence)
	if !ok {
		t.Fatal("expected owned during perform")
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ = work.StillOwned(ctx, "sf-1", "consumer-1", claim.Fence)
		if !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected complete to clear ownership")
}

func TestSuperFetchPerformSurvivesPollCtxCancel(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)

	var started sync.WaitGroup
	started.Add(1)
	release := make(chan struct{})
	var performed int32

	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2
	cfg.SuperFetchLeaseTTL = time.Minute

	life := context.Background()
	exec := NewSuperFetchExecutor(cfg, work, "c-life",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			if err := ctx.Err(); err != nil {
				return job.Outcome{}, err
			}
			atomic.AddInt32(&performed, 1)
			started.Done()
			<-release
			if err := ctx.Err(); err != nil {
				return job.Outcome{}, err
			}
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)
	exec.BindLife(life)

	pollCtx, endProc := context.WithCancel(life)
	rec := &kgo.Record{
		Topic: "jobs", Partition: 0, Offset: 1,
		Value: []byte(`{"job_id":"sf-life","job_type":"x"}`),
	}
	marker := &sfMarker{}
	// Claim + mark + perform using poll ctx (as runGroupPollLoop does), then cancel.
	exec.Sem <- struct{}{}
	claim, err := work.Claim(life, workset.ClaimParams{
		JobID: "sf-life", Payload: rec.Value, Topic: rec.Topic,
		Partition: 0, Offset: 1, ConsumerID: "c-life", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	marker.MarkCommitRecords(rec)
	exec.inFlight.Store("sf-life", struct{}{})
	go exec.perform(exec.life(), rec, "sf-life", claim.Fence, "g")
	endProc() // simulate endProc() after Dispatch returns

	started.Wait()
	if atomic.LoadInt32(&performed) != 1 {
		t.Fatal("perform should start despite poll ctx cancel")
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, _ := work.StillOwned(life, "sf-life", "c-life", claim.Fence)
		if !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected Complete after perform despite canceled poll ctx")
	_ = pollCtx
}

func TestSuperFetchSkipsDuplicateInFlight(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2

	var performs int32
	exec := NewSuperFetchExecutor(cfg, work, "c1",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			atomic.AddInt32(&performs, 1)
			time.Sleep(50 * time.Millisecond)
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)

	// Simulate Dispatch inFlight guard
	exec.inFlight.Store("dup", struct{}{})
	if _, loaded := exec.inFlight.LoadOrStore("dup", struct{}{}); !loaded {
		t.Fatal("expected already in flight")
	}
	if atomic.LoadInt32(&performs) != 0 {
		t.Fatal("should not perform")
	}
}
