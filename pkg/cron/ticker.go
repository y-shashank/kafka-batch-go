package cron

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// Ticker drives the recurring scheduler. Once per Window it (on the elected
// leader) claims due schedules, enqueues their jobs, and periodically recovers
// fires that were claimed but never dispatched.
type Ticker struct {
	Store    *Store
	Lock     *Lock // optional; nil ⇒ every node ticks (correctness still holds)
	Enqueuer Enqueuer

	// Window is the resolution/poll interval. Fires are accurate to within one
	// Window (default 30s) — deliberately coarse; sub-second accuracy is not a
	// goal.
	Window time.Duration
	// BatchSize caps schedules processed per tick.
	BatchSize int
	// MisfireGrace: an instant within this of now is "on time" and always fires.
	MisfireGrace time.Duration
	// MaxBackfill caps fires emitted per schedule per tick (backfill policy).
	MaxBackfill int
	// RecoverEvery / RecoverGrace govern the crash-recovery sweep of pending fires.
	RecoverEvery time.Duration
	RecoverGrace time.Duration
	// PruneEvery / PruneRetention govern deletion of old dispatched ledger rows.
	PruneEvery     time.Duration
	PruneRetention time.Duration
	// HeartbeatEvery is how often the stale-schedule sweep runs (emitting
	// cron.heartbeat + cron.stale). StaleFactor multiplies a schedule's own
	// interval to get its staleness threshold (default 2×).
	HeartbeatEvery time.Duration
	StaleFactor    float64

	// Now is overridable in tests.
	Now func() time.Time
	// RecordActivity is an optional liveness hook (like the poller's).
	RecordActivity func()

	lastRecover   time.Time
	lastPrune     time.Time
	lastHeartbeat time.Time
}

func (t *Ticker) now() time.Time {
	if t.Now != nil {
		return t.Now().UTC()
	}
	return time.Now().UTC()
}

func (t *Ticker) applyDefaults() {
	if t.Window <= 0 {
		t.Window = 30 * time.Second
	}
	if t.BatchSize <= 0 {
		t.BatchSize = 100
	}
	if t.MisfireGrace <= 0 {
		t.MisfireGrace = 2 * t.Window
	}
	if t.MaxBackfill <= 0 {
		t.MaxBackfill = 1000
	}
	if t.RecoverEvery <= 0 {
		t.RecoverEvery = 5 * time.Minute
	}
	if t.RecoverGrace <= 0 {
		t.RecoverGrace = 2 * time.Minute
	}
	if t.PruneEvery <= 0 {
		t.PruneEvery = time.Hour
	}
	if t.PruneRetention <= 0 {
		t.PruneRetention = 7 * 24 * time.Hour
	}
	if t.HeartbeatEvery <= 0 {
		t.HeartbeatEvery = time.Minute
	}
	if t.StaleFactor <= 0 {
		t.StaleFactor = 2.0
	}
}

// Run blocks until ctx is cancelled, ticking every Window.
func (t *Ticker) Run(ctx context.Context) {
	t.applyDefaults()
	log.Printf("[kbatch-cron] recurring scheduler started window=%s batch=%d misfire_grace=%s",
		t.Window, t.BatchSize, t.MisfireGrace)
	ticker := time.NewTicker(t.Window)
	defer ticker.Stop()
	t.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[kbatch-cron] recurring scheduler stopped")
			return
		case <-ticker.C:
			t.tick(ctx)
		}
	}
}

// tick runs one leader-gated pass: dispatch due fires, then (occasionally)
// recover pending and prune.
func (t *Ticker) tick(ctx context.Context) {
	if t.RecordActivity != nil {
		t.RecordActivity()
	}
	token, leader := "", true
	if t.Lock != nil {
		var err error
		token, leader, err = t.Lock.Acquire(ctx)
		if err != nil {
			log.Printf("[kbatch-cron] leader lock error: %v", err)
			return
		}
		if !leader {
			return
		}
		defer t.Lock.Release(ctx, token)
	}

	now := t.now()
	if err := t.dispatchDue(ctx, now); err != nil {
		log.Printf("[kbatch-cron] dispatch error: %v", err)
	}
	if now.Sub(t.lastRecover) >= t.RecoverEvery {
		t.lastRecover = now
		if err := t.recover(ctx, now); err != nil {
			log.Printf("[kbatch-cron] recover error: %v", err)
		}
	}
	if now.Sub(t.lastPrune) >= t.PruneEvery {
		t.lastPrune = now
		if n, err := t.Store.Prune(ctx, now.Add(-t.PruneRetention)); err != nil {
			log.Printf("[kbatch-cron] prune error: %v", err)
		} else if n > 0 {
			log.Printf("[kbatch-cron] pruned %d dispatched fire rows", n)
		}
	}
	if now.Sub(t.lastHeartbeat) >= t.HeartbeatEvery {
		t.lastHeartbeat = now
		if err := t.heartbeat(ctx, now); err != nil {
			log.Printf("[kbatch-cron] heartbeat error: %v", err)
		}
	}
}

// heartbeat sweeps enabled schedules, flags any that have not fired within their
// staleness threshold (StaleFactor × the schedule's own interval), and emits
// cron.stale per stale schedule plus a cron.heartbeat pulse. This is the
// "the recurring scheduler silently stopped" alarm.
func (t *Ticker) heartbeat(ctx context.Context, now time.Time) error {
	schedules, err := t.Store.List(ctx)
	if err != nil {
		return err
	}
	enabled, stale := 0, 0
	var maxStale float64
	for _, sc := range schedules {
		if !sc.Enabled {
			continue
		}
		enabled++
		threshold, ok := t.staleThreshold(sc)
		if !ok {
			continue
		}
		// Reference point: last successful fire, else the schedule's currently
		// due instant (a never-fired schedule that is already overdue is stale).
		ref := sc.NextRunAt
		if sc.LastFire != nil {
			ref = *sc.LastFire
		}
		staleness := now.Sub(ref)
		if staleness > threshold {
			stale++
			s := staleness.Seconds()
			if s > maxStale {
				maxStale = s
			}
			log.Printf("[kbatch-cron] STALE schedule=%s job_type=%s stale=%s threshold=%s",
				sc.Name, sc.JobType, staleness.Round(time.Second), threshold.Round(time.Second))
			instrument.CronStale(sc.Name, sc.JobType, s, threshold.Seconds())
		}
	}
	instrument.CronHeartbeat(enabled, stale, maxStale)
	return nil
}

// staleThreshold returns StaleFactor × the schedule's interval, derived from the
// gap between its next two cron instants. ok is false for unparseable schedules.
func (t *Ticker) staleThreshold(sc Schedule) (time.Duration, bool) {
	expr, err := Parse(sc.CronExpr)
	if err != nil {
		return 0, false
	}
	loc, err := sc.Location()
	if err != nil {
		return 0, false
	}
	a, ok := expr.Next(sc.NextRunAt, loc)
	if !ok {
		return 0, false
	}
	b, ok := expr.Next(a, loc)
	if !ok {
		return 0, false
	}
	interval := b.Sub(a)
	if interval <= 0 {
		return 0, false
	}
	return time.Duration(float64(interval) * t.StaleFactor), true
}

func (t *Ticker) dispatchDue(ctx context.Context, now time.Time) error {
	claimed, err := t.Store.ClaimAndAdvance(ctx, now, t.BatchSize, t.planFor(now))
	if err != nil {
		return err
	}
	for _, cf := range claimed {
		t.enqueue(ctx, cf)
	}
	return nil
}

func (t *Ticker) recover(ctx context.Context, now time.Time) error {
	pending, err := t.Store.RecoverPending(ctx, now.Add(-t.RecoverGrace), t.BatchSize)
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		log.Printf("[kbatch-cron] recovering %d pending fires", len(pending))
	}
	for _, cf := range pending {
		t.enqueue(ctx, cf)
	}
	return nil
}

// enqueue pushes one fire and, on success, marks it dispatched. On failure the
// row stays 'pending' so the recovery sweep retries it. The deterministic job
// id makes that retry idempotent for uniq handlers.
func (t *Ticker) enqueue(ctx context.Context, cf ClaimedFire) {
	jobID := JobIDForFire(cf.ScheduleID, cf.FireAt)
	if _, err := t.Enqueuer.Enqueue(ctx, cf.JobType, cf.Args, EnqueueOpts{JobID: jobID, TenantID: cf.TenantID}); err != nil {
		log.Printf("[kbatch-cron] enqueue schedule=%s job_type=%s fire_at=%s: %v — left pending for recovery",
			cf.Name, cf.JobType, cf.FireAt.Format(time.RFC3339), err)
		instrument.CronEnqueueFailed(cf.Name, cf.JobType, err.Error())
		return
	}
	instrument.CronFired(cf.Name, cf.JobType, jobID, cf.TenantID)
	if err := t.Store.MarkDispatched(ctx, cf.ScheduleID, cf.FireAt, jobID); err != nil {
		// Job is enqueued; failing to mark only risks a benign recovery re-enqueue
		// (same job id ⇒ deduped by uniq handlers).
		log.Printf("[kbatch-cron] mark dispatched schedule=%s fire_at=%s: %v",
			cf.Name, cf.FireAt.Format(time.RFC3339), err)
	}
}

// planFor returns the ClaimAndAdvance planner closure for a given "now".
func (t *Ticker) planFor(now time.Time) func(Schedule) (Plan, error) {
	return func(sc Schedule) (Plan, error) {
		expr, err := Parse(sc.CronExpr)
		if err != nil {
			return Plan{}, fmt.Errorf("schedule %q: %w", sc.Name, err)
		}
		loc, err := sc.Location()
		if err != nil {
			return Plan{}, err
		}
		return PlanFires(sc, expr, loc, now, t.MisfireGrace, t.MaxBackfill), nil
	}
}

// JobIDForFire is the deterministic job id for a (schedule, instant) pair.
func JobIDForFire(scheduleID int64, fireAt time.Time) string {
	return fmt.Sprintf("sched-%d-%d", scheduleID, fireAt.UTC().Unix())
}
