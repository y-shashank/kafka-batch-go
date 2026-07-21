package schedule

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestPollerNowAndMinJittered(t *testing.T) {
	fixed := time.Unix(42, 0)
	p := &Poller{Now: func() time.Time { return fixed }}
	if !p.now().Equal(fixed) {
		t.Fatal("Now override")
	}
	p2 := &Poller{}
	if p2.now().IsZero() {
		t.Fatal("wall clock")
	}

	if min(time.Second, 2*time.Second) != time.Second {
		t.Fatal("min a")
	}
	if min(3*time.Second, time.Second) != time.Second {
		t.Fatal("min b")
	}

	p3 := &Poller{Cfg: config.Daemon{SchedulePollJitter: 0}}
	if p3.jittered(time.Second) != time.Second {
		t.Fatal("no jitter")
	}
	p3.Cfg.SchedulePollJitter = 0.5
	d := p3.jittered(time.Second)
	if d < 500*time.Millisecond || d > 1500*time.Millisecond {
		t.Fatalf("jittered=%s", d)
	}
}

func TestPollerDrainDue(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(1100, 0)
	if err := store.Schedule(ctx, "j1", now.Add(-time.Second), 0, 1); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"job_id":"j1","job_type":"x"}`)
	var activity atomic.Int32
	poller := &Poller{
		Cfg:    config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  store,
		Reader: &stubReader{found: map[string][]byte{"0:1": payload}},
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			return nil
		}),
		Router:         DaemonRouter{Default: "jobs"},
		Now:            func() time.Time { return now },
		RecordActivity: func() { activity.Add(1) },
	}
	drained, err := poller.drainDue(ctx)
	if err != nil || !drained {
		t.Fatalf("drained=%v err=%v", drained, err)
	}
	if activity.Load() < 1 {
		t.Fatal("expected RecordActivity")
	}
	// Second drain: nothing due.
	drained, err = poller.drainDue(ctx)
	if err != nil || drained {
		t.Fatalf("empty drain drained=%v err=%v", drained, err)
	}
}

func TestPollerDrainDueError(t *testing.T) {
	p := &Poller{
		Cfg:   config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store: errStore{err: errors.New("redis down")},
		Now:   func() time.Time { return time.Unix(1, 0) },
	}
	_, err := p.drainDue(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPollerRunCancels(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p := &Poller{
		Cfg: config.Daemon{
			ScheduleLeaseSeconds:   60,
			ScheduleBatchSize:      10,
			SchedulePollInterval:   20 * time.Millisecond,
			SchedulePollMaxInterval: 50 * time.Millisecond,
		},
		Store:  NewRedisStore(rdb, 100),
		Reader: &stubReader{},
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			return nil
		}),
		Router: DaemonRouter{Default: "jobs"},
		Now:    func() time.Time { return time.Unix(1200, 0) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPollerTickReadMissThreshold(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(1300, 0)
	member := "j-miss:0:5"

	// Pre-seed so this Tick is the threshold hit (10th miss).
	for i := 0; i < maxReadMisses-1; i++ {
		if _, err := store.RecordReadMiss(ctx, member); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Schedule(ctx, "j-miss", now.Add(-time.Second), 0, 5); err != nil {
		t.Fatal(err)
	}

	poller := &Poller{
		Cfg:    config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  store,
		Reader: &stubReader{found: map[string][]byte{}}, // not lost, just missing
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			t.Fatal("should not produce")
			return nil
		}),
		Router: DaemonRouter{Default: "jobs"},
		Now:    func() time.Time { return now },
	}
	n, err := poller.Tick(ctx)
	if err != nil || n != 0 {
		t.Fatalf("tick n=%d err=%v", n, err)
	}
	inflight, err := rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || inflight != 0 {
		t.Fatalf("expected ack after threshold, inflight=%d err=%v", inflight, err)
	}
	if mr.HGet(readMissKey, member) != "" {
		t.Fatalf("read miss counter should be cleared, got %q", mr.HGet(readMissKey, member))
	}
}

func TestPollerTickReclaimAndInvalidPayload(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(1500, 0)
	if err := store.Schedule(ctx, "j-bad", now.Add(-time.Second), 0, 7); err != nil {
		t.Fatal(err)
	}
	var activity atomic.Int32
	poller := &Poller{
		Cfg: config.Daemon{
			ScheduleLeaseSeconds: 60,
			ScheduleBatchSize:    10,
			ScheduleReclaimEvery: time.Second,
		},
		Store:  store,
		Reader: &stubReader{found: map[string][]byte{"0:7": []byte("not-json")}},
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			t.Fatal("invalid payload should not produce")
			return nil
		}),
		Router:         DaemonRouter{Default: "jobs"},
		Now:            func() time.Time { return now },
		RecordActivity: func() { activity.Add(1) },
		lastReclaim:    now.Add(-time.Hour), // force reclaim branch
	}
	n, err := poller.Tick(ctx)
	if err != nil || n != 1 {
		// invalid JSON is treated as success (acked) — produceDue returns true
		t.Fatalf("tick n=%d err=%v", n, err)
	}
}

func TestPollerTickReadMissBelowThresholdKeepsLease(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(1400, 0)
	if err := store.Schedule(ctx, "j-miss2", now.Add(-time.Second), 1, 2); err != nil {
		t.Fatal(err)
	}
	poller := &Poller{
		Cfg:    config.Daemon{ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  store,
		Reader: &stubReader{found: map[string][]byte{}},
		Producer: producerFunc(func(context.Context, string, string, []byte) error {
			return nil
		}),
		Router: DaemonRouter{Default: "jobs"},
		Now:    func() time.Time { return now },
	}
	if _, err := poller.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	inflight, err := rdb.ZCard(ctx, inflightKey).Result()
	if err != nil || inflight != 1 {
		t.Fatalf("expected leased member retained, inflight=%d err=%v", inflight, err)
	}
	n, err := store.RecordReadMiss(ctx, "j-miss2:1:2")
	if err != nil || n != 2 {
		t.Fatalf("miss counter n=%d err=%v", n, err)
	}
}

type errStore struct{ err error }

func (e errStore) Schedule(context.Context, string, time.Time, int32, int64) error { return e.err }
func (e errStore) ScheduleMany(context.Context, []ScheduleEntry) error             { return e.err }
func (e errStore) ClaimDue(context.Context, time.Time, int, int) ([]string, error) {
	return nil, e.err
}
func (e errStore) Ack(context.Context, []string) error                   { return e.err }
func (e errStore) Reclaim(context.Context, time.Time) (int, error)       { return 0, e.err }
func (e errStore) RecordReadMiss(context.Context, string) (int64, error) { return 0, e.err }
func (e errStore) ClearReadMiss(context.Context, string) error           { return e.err }
