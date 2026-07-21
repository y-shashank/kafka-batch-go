package cron

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type stubEnqueuer struct {
	err   error
	calls atomic.Int32
	last  struct {
		jobType string
		opts    EnqueueOpts
	}
}

func (s *stubEnqueuer) Enqueue(_ context.Context, jobType string, _ map[string]interface{}, opts EnqueueOpts) (string, error) {
	s.calls.Add(1)
	s.last.jobType = jobType
	s.last.opts = opts
	if s.err != nil {
		return "", s.err
	}
	return opts.JobID, nil
}

func deadStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open("mysql", "nobody:nopass@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStoreDB(db)
}

func TestApplyDefaults(t *testing.T) {
	tk := &Ticker{}
	tk.applyDefaults()
	if tk.Window != 30*time.Second || tk.BatchSize != 100 || tk.MisfireGrace != 60*time.Second {
		t.Fatalf("defaults window=%s batch=%d grace=%s", tk.Window, tk.BatchSize, tk.MisfireGrace)
	}
	if tk.MaxBackfill != 1000 || tk.RecoverEvery != 5*time.Minute || tk.RecoverGrace != 2*time.Minute {
		t.Fatal("recover/backfill defaults")
	}
	if tk.PruneEvery != time.Hour || tk.PruneRetention != 7*24*time.Hour {
		t.Fatal("prune defaults")
	}
	if tk.HeartbeatEvery != time.Minute || tk.StaleFactor != 2.0 {
		t.Fatal("heartbeat defaults")
	}

	tk2 := &Ticker{Window: time.Second, BatchSize: 1, MisfireGrace: time.Second,
		MaxBackfill: 2, RecoverEvery: time.Second, RecoverGrace: time.Second,
		PruneEvery: time.Second, PruneRetention: time.Second,
		HeartbeatEvery: time.Second, StaleFactor: 3}
	tk2.applyDefaults()
	if tk2.Window != time.Second || tk2.StaleFactor != 3 {
		t.Fatal("explicit values should be kept")
	}
}

func TestTickerNow(t *testing.T) {
	fixed := at("2026-07-18T12:00:00Z")
	tk := &Ticker{Now: func() time.Time { return fixed }}
	if !tk.now().Equal(fixed) {
		t.Fatalf("now=%s", tk.now())
	}
	tk2 := &Ticker{}
	if tk2.now().IsZero() {
		t.Fatal("wall clock now")
	}
}

func TestPlanFor(t *testing.T) {
	tk := &Ticker{MisfireGrace: time.Minute, MaxBackfill: 10}
	now := at("2026-07-18T10:00:20Z")
	plan := tk.planFor(now)

	p, err := plan(Schedule{
		Name: "ok", CronExpr: "0 * * * *", Timezone: "UTC",
		Misfire: MisfireFireOnce, NextRunAt: at("2026-07-18T10:00:00Z"),
	})
	if err != nil || len(p.Fires) != 1 {
		t.Fatalf("plan=%+v err=%v", p, err)
	}

	if _, err := plan(Schedule{Name: "bad", CronExpr: "nonsense"}); err == nil {
		t.Fatal("expected bad cron")
	}
	if _, err := plan(Schedule{
		Name: "tz", CronExpr: "0 * * * *", Timezone: "Not/AZone",
	}); err == nil {
		t.Fatal("expected bad tz")
	}
}

func TestEnqueueFailAndSuccess(t *testing.T) {
	store := deadStore(t)
	fail := &stubEnqueuer{err: errors.New("produce down")}
	tk := &Ticker{Store: store, Enqueuer: fail}
	cf := ClaimedFire{
		ScheduleID: 9, Name: "n", JobType: "hello.go",
		TenantID: "t1", Args: map[string]interface{}{"a": 1},
		FireAt: at("2026-07-18T10:00:00Z"),
	}
	tk.enqueue(context.Background(), cf)
	if fail.calls.Load() != 1 {
		t.Fatal("expected enqueue call")
	}

	ok := &stubEnqueuer{}
	tk.Enqueuer = ok
	// MarkDispatched will fail against dead DB — still covers success + mark-error path.
	tk.enqueue(context.Background(), cf)
	if ok.calls.Load() != 1 || ok.last.opts.JobID != JobIDForFire(9, cf.FireAt) {
		t.Fatalf("calls=%d jobID=%q", ok.calls.Load(), ok.last.opts.JobID)
	}
	if ok.last.opts.TenantID != "t1" || ok.last.jobType != "hello.go" {
		t.Fatalf("last=%+v", ok.last)
	}
}

func TestTickLockPaths(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	lock := NewLock(rdb, time.Minute)
	store := deadStore(t)
	enq := &stubEnqueuer{}

	var activity atomic.Int32
	fixed := at("2026-07-18T10:00:00Z")
	tk := &Ticker{
		Store: store, Lock: lock, Enqueuer: enq,
		Window: time.Second, BatchSize: 10,
		RecoverEvery: time.Hour, PruneEvery: time.Hour, HeartbeatEvery: time.Hour,
		Now:            func() time.Time { return fixed },
		RecordActivity: func() { activity.Add(1) },
	}
	tk.applyDefaults()

	// First tick: acquires lock, dispatch/recover/prune/hb hit dead DB (logged, no panic).
	tk.lastRecover = fixed
	tk.lastPrune = fixed
	tk.lastHeartbeat = fixed
	tk.tick(context.Background())
	if activity.Load() != 1 {
		t.Fatalf("activity=%d", activity.Load())
	}

	// Hold the lock with another token so this tick is not leader.
	ctx := context.Background()
	if err := rdb.Set(ctx, leaderLockKey, "other", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	tk.tick(ctx)
	if activity.Load() != 2 {
		t.Fatalf("activity after non-leader=%d", activity.Load())
	}

	// Lock error path: closed redis client.
	_ = rdb.Close()
	tk.tick(ctx)
	if activity.Load() != 3 {
		t.Fatalf("activity after lock err=%d", activity.Load())
	}
}

func TestTickNilLockAndPeriodicSweeps(t *testing.T) {
	store := deadStore(t)
	fixed := at("2026-07-18T10:00:00Z")
	tk := &Ticker{
		Store: store, Enqueuer: &stubEnqueuer{},
		Window: time.Second, BatchSize: 5,
		RecoverEvery: time.Minute, RecoverGrace: time.Second,
		PruneEvery: time.Minute, PruneRetention: time.Hour,
		HeartbeatEvery: time.Minute, StaleFactor: 2,
		Now: func() time.Time { return fixed },
	}
	tk.applyDefaults()
	// Zero last* times ⇒ all sweeps run.
	tk.tick(context.Background())
	if tk.lastRecover.IsZero() || tk.lastPrune.IsZero() || tk.lastHeartbeat.IsZero() {
		t.Fatalf("expected sweep timestamps set recover=%v prune=%v hb=%v",
			tk.lastRecover, tk.lastPrune, tk.lastHeartbeat)
	}
}

func TestTickerRunCancels(t *testing.T) {
	store := deadStore(t)
	tk := &Ticker{
		Store: store, Enqueuer: &stubEnqueuer{},
		Window: 50 * time.Millisecond, BatchSize: 1,
		RecoverEvery: time.Hour, PruneEvery: time.Hour, HeartbeatEvery: time.Hour,
		Now: func() time.Time { return time.Now().UTC() },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tk.Run(ctx)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestStaleThresholdBadLocation(t *testing.T) {
	tk := &Ticker{StaleFactor: 2}
	sc := Schedule{
		Name: "x", CronExpr: "*/5 * * * *", Timezone: "Not/AZone",
		NextRunAt: at("2026-07-18T10:00:00Z"),
	}
	if _, ok := tk.staleThreshold(sc); ok {
		t.Fatal("bad tz should not yield threshold")
	}
}

func TestHeartbeatListError(t *testing.T) {
	tk := &Ticker{Store: deadStore(t), StaleFactor: 2}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tk.heartbeat(ctx, time.Now().UTC()); err == nil {
		t.Fatal("expected list error")
	}
}

func TestDispatchDueAndRecoverErrors(t *testing.T) {
	tk := &Ticker{
		Store: deadStore(t), Enqueuer: &stubEnqueuer{},
		BatchSize: 10, RecoverGrace: time.Minute,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if err := tk.dispatchDue(ctx, now); err == nil {
		t.Fatal("dispatchDue")
	}
	if err := tk.recover(ctx, now); err == nil {
		t.Fatal("recover")
	}
}
