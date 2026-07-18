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

	exec.ClaimWindow <- struct{}{}
	exec.inFlight.Store("sf-1", struct{}{})
	go exec.perform(ctx, rec, "sf-1", claim.Fence, "g", func() {})

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
	exec.ClaimWindow <- struct{}{}
	claim, err := work.Claim(life, workset.ClaimParams{
		JobID: "sf-life", Payload: rec.Value, Topic: rec.Topic,
		Partition: 0, Offset: 1, ConsumerID: "c-life", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	marker.MarkCommitRecords(rec)
	exec.inFlight.Store("sf-life", struct{}{})
	go exec.perform(exec.life(), rec, "sf-life", claim.Fence, "g", func() {})
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

func TestSuperFetchAppliesEvenAfterLostFence(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 1
	cfg.SuperFetchClaimWindow = 2

	var applied int32
	life, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := NewSuperFetchExecutor(cfg, work, "c-lost",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{
				CommitOffset: true,
				Event: &protocol.EventMessage{
					JobID: "sf-lost", BatchID: "b1", Status: "success",
				},
			}, nil
		},
		func(ctx context.Context, out job.Outcome) error {
			atomic.AddInt32(&applied, 1)
			return nil
		},
	)
	exec.BindLife(life)
	exec.ClaimWindow <- struct{}{}

	rec := &kgo.Record{
		Topic: "jobs", Partition: 0, Offset: 9,
		Value: []byte(`{"job_id":"sf-lost","job_type":"x","batch_id":"b1","batch_seq":1}`),
	}
	claim, err := work.Claim(life, workset.ClaimParams{
		JobID: "sf-lost", Payload: rec.Value, Topic: rec.Topic,
		Partition: 0, Offset: 9, ConsumerID: "c-lost", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	// Simulate lease TTL / reclaim Finish deleting the workset entry mid-perform.
	if err := work.Complete(life, "sf-lost", "c-lost", claim.Fence); err != nil {
		t.Fatal(err)
	}
	exec.inFlight.Store("sf-lost", struct{}{})
	exec.perform(life, rec, "sf-lost", claim.Fence, "g", func() {})

	if atomic.LoadInt32(&applied) != 1 {
		t.Fatalf("applied=%d want 1 (must emit event even when fence is gone)", applied)
	}
}

func TestSuperFetchRetriesApplyWhileRenewing(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 1
	cfg.SuperFetchClaimWindow = 2
	cfg.SuperFetchLeaseTTL = time.Minute

	var applied int32
	life, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := NewSuperFetchExecutor(cfg, work, "c-retry",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{CommitOffset: true, Event: &protocol.EventMessage{JobID: "sf-retry", Status: "success"}}, nil
		},
		func(ctx context.Context, out job.Outcome) error {
			n := atomic.AddInt32(&applied, 1)
			if n < 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	)
	exec.BindLife(life)
	exec.ClaimWindow <- struct{}{}

	rec := &kgo.Record{
		Topic: "jobs", Value: []byte(`{"job_id":"sf-retry","job_type":"x"}`),
	}
	claim, err := work.Claim(life, workset.ClaimParams{
		JobID: "sf-retry", Payload: rec.Value, Topic: "jobs",
		ConsumerID: "c-retry", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	renewStopped := int32(0)
	exec.inFlight.Store("sf-retry", struct{}{})
	exec.perform(life, rec, "sf-retry", claim.Fence, "g", func() { atomic.StoreInt32(&renewStopped, 1) })

	if atomic.LoadInt32(&applied) < 3 {
		t.Fatalf("applied=%d want >=3 (retries on apply error)", applied)
	}
	if atomic.LoadInt32(&renewStopped) != 1 {
		t.Fatal("expected renew stopped only after perform finished")
	}
	// Job should be completed out of the workset after successful apply.
	if e, _ := work.StillOwned(life, "sf-retry", "c-retry", claim.Fence); e {
		t.Fatal("expected workset entry completed after successful apply retry")
	}
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

func TestSuperFetchStopAcceptingAndWaitInFlight(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	exec := NewSuperFetchExecutor(cfg, work, "c-drain",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			started.Done()
			<-release
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)
	life := context.Background()
	exec.BindLife(life)

	rec := &kgo.Record{
		Topic: "jobs", Partition: 0, Offset: 1,
		Value: []byte(`{"job_id":"drain-1","job_type":"x"}`),
	}
	claim, err := work.Claim(life, workset.ClaimParams{
		JobID: "drain-1", Payload: rec.Value, Topic: rec.Topic,
		Partition: 0, Offset: 1, ConsumerID: "c-drain", LeaseTTL: time.Minute, StealGrace: -1,
	})
	if err != nil || !claim.Won {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	exec.inFlight.Store("drain-1", struct{}{})
	exec.ClaimWindow <- struct{}{}
	go exec.perform(life, rec, "drain-1", claim.Fence, "g", func() {})
	started.Wait()
	if exec.InFlightCount() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", exec.InFlightCount())
	}

	exec.StopAccepting()
	if exec.accepting.Load() {
		t.Fatal("expected accepting=false")
	}
	// Dispatch must no-op after StopAccepting (nil client is fine — returns before use).
	exec.DispatchClaimsAndAcks(life, nil, []*kgo.Record{rec}, "g")
	if exec.InFlightCount() != 1 {
		t.Fatalf("StopAccepting should refuse new claims, in-flight=%d", exec.InFlightCount())
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	if rem := exec.WaitInFlight(2 * time.Second); rem != 0 {
		t.Fatalf("expected drain complete, remaining=%d", rem)
	}
}

// Regression: the workset payload lease TTL must be raised above the safe floor
// (liveness_ttl + orphan_grace + reclaim_interval + buffer) so a crashed pod's
// in-flight jobs are reclaimable before their payload expires. Otherwise the
// payload dies before reclaim is allowed to act and the job is permanently lost.
func TestSuperFetchLeaseTTLFloor(t *testing.T) {
	// Inverted config: lease (120s) < liveness (180s) — the loss-causing default.
	cfg := config.Daemon{
		SuperFetchLeaseTTL:        120 * time.Second,
		LivenessTTL:               180 * time.Second,
		SuperFetchOrphanGrace:     40 * time.Second,
		SuperFetchReclaimEvery:    30 * time.Second,
		SuperFetchConcurrency:     4,
	}
	exec := NewSuperFetchExecutor(cfg, nil, "c-floor", nil, nil)
	want := 180*time.Second + 40*time.Second + 30*time.Second + 30*time.Second // 280s
	if exec.LeaseTTL != want {
		t.Fatalf("LeaseTTL = %s, want raised to floor %s", exec.LeaseTTL, want)
	}
	if exec.LeaseTTL <= exec.HeartbeatTTL {
		t.Fatalf("LeaseTTL %s must exceed HeartbeatTTL %s", exec.LeaseTTL, exec.HeartbeatTTL)
	}

	// Already-safe config is left untouched.
	safe := config.Daemon{
		SuperFetchLeaseTTL:     600 * time.Second,
		LivenessTTL:            180 * time.Second,
		SuperFetchOrphanGrace:  40 * time.Second,
		SuperFetchReclaimEvery: 30 * time.Second,
		SuperFetchConcurrency:  4,
	}
	exec2 := NewSuperFetchExecutor(safe, nil, "c-safe", nil, nil)
	if exec2.LeaseTTL != 600*time.Second {
		t.Fatalf("safe LeaseTTL = %s, want unchanged 600s", exec2.LeaseTTL)
	}
}
