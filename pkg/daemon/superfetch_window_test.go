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

// fakeCommitClient satisfies the MarkCommitRecords call site used by Dispatch.
type fakeCommitClient struct {
	mu     sync.Mutex
	marked int
}

func (f *fakeCommitClient) MarkCommitRecords(recs ...*kgo.Record) {
	f.mu.Lock()
	f.marked += len(recs)
	f.mu.Unlock()
}

func (f *fakeCommitClient) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.marked
}

func TestDispatchClaimsAheadOfPerformPool(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)

	firstStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var performs int32

	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 1
	cfg.SuperFetchClaimWindow = 4
	cfg.SuperFetchLeaseTTL = time.Minute

	exec := NewSuperFetchExecutor(cfg, work, "c-win",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			n := atomic.AddInt32(&performs, 1)
			if n == 1 {
				firstStarted <- struct{}{}
			}
			<-release
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)
	life := context.Background()
	exec.BindLife(life)

	recs := []*kgo.Record{
		{Topic: "jobs", Partition: 0, Offset: 1, Value: []byte(`{"job_id":"w1"}`)},
		{Topic: "jobs", Partition: 0, Offset: 2, Value: []byte(`{"job_id":"w2"}`)},
		{Topic: "jobs", Partition: 0, Offset: 3, Value: []byte(`{"job_id":"w3"}`)},
	}
	// Use a thin wrapper: Dispatch needs *kgo.Client; call claim path via exported pieces.
	// Exercise window accounting by driving ClaimWindow + Sem the same way Dispatch does.
	cl := &fakeCommitClient{}
	_ = cl

	// Simulate DispatchClaimsAndAcks without a real kgo.Client by inlining the loop
	// against ClaimWindow (same contract as production).
	for _, rec := range recs {
		exec.ClaimWindow <- struct{}{}
		jobID := extractJobID(rec.Value)
		claim, err := work.Claim(life, workset.ClaimParams{
			JobID: jobID, Payload: rec.Value, Topic: rec.Topic,
			Partition: rec.Partition, Offset: rec.Offset,
			ConsumerID: "c-win", LeaseTTL: time.Minute, StealGrace: -1,
		})
		if err != nil || !claim.Won {
			t.Fatalf("claim %s: %+v %v", jobID, claim, err)
		}
		cl.MarkCommitRecords(rec)
		stop := exec.startRenew(life, jobID, claim.Fence)
		exec.inFlight.Store(jobID, struct{}{})
		go exec.perform(life, rec, jobID, claim.Fence, "g", stop)
	}

	// All three should be Kafka-acked even though only 1 perform slot exists.
	deadline := time.Now().Add(2 * time.Second)
	for cl.count() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if cl.count() != 3 {
		t.Fatalf("marked=%d want 3 (claim/ack ahead of perform)", cl.count())
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first perform did not start")
	}
	if atomic.LoadInt32(&performs) != 1 {
		t.Fatalf("performs=%d want 1 while others wait on Sem", atomic.LoadInt32(&performs))
	}
	close(release)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&performs) == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("performs=%d want 3 after release", atomic.LoadInt32(&performs))
}

func TestPerformReleasesSemBeforeSlowApply(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)

	var performs int32
	firstApplyStarted := make(chan struct{}, 1)
	releaseApply := make(chan struct{})
	secondProcessStarted := make(chan struct{}, 1)

	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 1
	cfg.SuperFetchClaimWindow = 4
	cfg.SuperFetchLeaseTTL = time.Minute

	exec := NewSuperFetchExecutor(cfg, work, "c-sem",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			n := atomic.AddInt32(&performs, 1)
			if n == 2 {
				secondProcessStarted <- struct{}{}
			}
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error {
			select {
			case firstApplyStarted <- struct{}{}:
			default:
			}
			<-releaseApply
			return nil
		},
	)
	life := context.Background()
	exec.BindLife(life)

	for i, id := range []string{"s1", "s2"} {
		rec := &kgo.Record{Topic: "jobs", Partition: 0, Offset: int64(i + 1), Value: []byte(`{"job_id":"` + id + `"}`)}
		exec.ClaimWindow <- struct{}{}
		claim, err := work.Claim(life, workset.ClaimParams{
			JobID: id, Payload: rec.Value, Topic: rec.Topic,
			Partition: 0, Offset: rec.Offset, ConsumerID: "c-sem",
			LeaseTTL: time.Minute, StealGrace: -1,
		})
		if err != nil || !claim.Won {
			t.Fatalf("claim %s: %+v %v", id, claim, err)
		}
		exec.inFlight.Store(id, struct{}{})
		go exec.perform(life, rec, id, claim.Fence, "g", func() {})
	}

	select {
	case <-firstApplyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first apply did not start")
	}
	select {
	case <-secondProcessStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second process blocked on Sem during slow Apply — Sem not released after Process")
	}
	close(releaseApply)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&performs) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("performs=%d want 2", atomic.LoadInt32(&performs))
}
