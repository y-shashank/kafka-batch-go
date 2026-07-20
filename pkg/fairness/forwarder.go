package fairness

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
)

// Producer publishes to Kafka.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Forwarder checks out fairly-selected jobs and produces to the ready topic.
type Forwarder struct {
	Lane              Lane
	Scheduler         *Scheduler
	ReadyTopic        string
	ResolveReadyTopic func(payload []byte) (string, error)
	Producer          Producer
	IdleSleep      time.Duration
	Burst          int
	Now            func() time.Time
	OnExpired      func(ctx context.Context, job *CheckoutResult, raw []byte) error
	RecordActivity func() // optional hook for liveness probes

	lastLeaseReclaim   time.Time
	lastForwardReclaim time.Time

	lastIdleCheck  time.Time
	quiescentSince time.Time
	vtimeResetDone bool
}

const (
	defaultIdleSleep  = 50 * time.Millisecond
	defaultBurst      = 50
	reclaimInterval   = 30 * time.Second
	idleCheckInterval = 5 * time.Second
)

// forwardOutcome distinguishes idle ready-list from transient forward failures so
// Run can drain backlog without treating produce errors as "no work".
type forwardOutcome int

const (
	forwardDidWork forwardOutcome = iota
	forwardIdle
	forwardFailed
)

// ForwardOnce forwards one job when available.
func (f *Forwarder) ForwardOnce(ctx context.Context) bool {
	return f.forwardOnceOutcome(ctx) == forwardDidWork
}

func (f *Forwarder) forwardOnceOutcome(ctx context.Context) forwardOutcome {
	if f.Scheduler == nil {
		return forwardIdle
	}
	job, err := f.Scheduler.Checkout(ctx)
	if err != nil {
		log.Printf("[kbatch-fair-forwarder] checkout error lane=%s: %v", f.Lane, err)
		return forwardFailed
	}
	if job == nil {
		return forwardIdle
	}
	if f.handleExpired(ctx, job) {
		return forwardDidWork
	}
	if err := f.forwardJob(ctx, job); err != nil {
		log.Printf("[kbatch-fair-forwarder] forward error lane=%s: %v", f.Lane, err)
		return forwardFailed
	}
	return forwardDidWork
}

func (f *Forwarder) handleExpired(ctx context.Context, job *CheckoutResult) bool {
	var m map[string]interface{}
	if err := json.Unmarshal(job.Payload, &m); err != nil {
		return false
	}
	validTill, _ := m["valid_till"].(string)
	if !jobexpiry.Expired(validTill, f.now()) {
		return false
	}
	// The payload was already LPOP'd from the ready list during Checkout, so the
	// forwarding entry (+ its lease) is now the ONLY durable record of this job.
	// Emit the completion/DLT drop first; only release the slot and confirm the
	// forward once it succeeds. If the drop fails we leave the forwarding entry
	// and lease intact so stale-forward recovery re-produces the job instead of
	// losing it (and stranding its batch). Mirrors forwardJob's produce→confirm.
	if f.OnExpired != nil {
		if err := f.OnExpired(ctx, job, job.Payload); err != nil {
			log.Printf("[kbatch-fair-forwarder] expired drop lane=%s: %v (retained for stale-forward recovery)", f.Lane, err)
			return true
		}
	}
	_ = f.Scheduler.Complete(ctx, job.TenantID, job.SlotID, 0)
	_, _ = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	return true
}

func (f *Forwarder) forwardJob(ctx context.Context, job *CheckoutResult) error {
	marked, key, err := markSlot(job.Payload, job.TenantID, job.SlotID, f.Lane)
	if err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	if key == "" {
		key = job.TenantID
	}
	readyTopic, err := f.readyTopicFor(job.Payload)
	if err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	if err := f.Producer.Produce(ctx, readyTopic, key, marked); err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	_, err = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	return err
}

// Run blocks until ctx is cancelled.
//
// While Redis has ready jobs (worker backlog), Run drains them in bursts with no
// idle sleep — produce → ready topic continuously. Only after a burst finds the
// ready list empty (or makes no progress) does it resume the idle wait cycle.
func (f *Forwarder) Run(ctx context.Context) {
	idle := f.IdleSleep
	if idle <= 0 {
		idle = defaultIdleSleep
	}
	burst := f.Burst
	if burst <= 0 {
		burst = defaultBurst
	}
	log.Printf("[kbatch-fair-forwarder] started lane=%s ready=%s", f.Lane, f.ReadyTopic)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[kbatch-fair-forwarder] stopped lane=%s", f.Lane)
			return
		default:
		}
		if f.RecordActivity != nil {
			f.RecordActivity()
		}
		n, empty, failed := f.drainBurst(ctx, burst)
		f.maybeReclaim(ctx)
		f.maybeResetVtimeIdle(ctx)
		switch {
		case n == burst && !empty && !failed:
			// Full burst with more ready work likely remaining — keep draining.
			continue
		case n > 0 && failed:
			// Partial progress then error — retry immediately, do not idle-sleep.
			continue
		default:
			// Ready backlog cleared (empty) or no progress — resume poll wait.
			time.Sleep(idle)
		}
	}
}

// drainBurst forwards up to burst jobs. empty means Checkout reported no ready
// work; failed means a checkout/produce error stopped the burst early.
func (f *Forwarder) drainBurst(ctx context.Context, burst int) (n int, empty, failed bool) {
	for n < burst {
		select {
		case <-ctx.Done():
			return n, false, false
		default:
		}
		switch f.forwardOnceOutcome(ctx) {
		case forwardDidWork:
			n++
		case forwardIdle:
			return n, true, false
		case forwardFailed:
			return n, false, true
		}
	}
	return n, false, false
}

func (f *Forwarder) maybeReclaim(ctx context.Context) {
	now := time.Now()
	if now.Sub(f.lastLeaseReclaim) >= reclaimInterval {
		f.lastLeaseReclaim = now
		if n, err := f.Scheduler.ReclaimExpiredLeases(ctx); err != nil {
			log.Printf("[kbatch-fair-forwarder] reclaim leases lane=%s: %v", f.Lane, err)
		} else if n > 0 {
			log.Printf("[kbatch-fair-forwarder] reclaimed %d expired lease(s) lane=%s", n, f.Lane)
		}
	}
	if now.Sub(f.lastForwardReclaim) >= reclaimInterval {
		f.lastForwardReclaim = now
		stale, err := f.Scheduler.ListStaleForwards(ctx)
		if err != nil {
			log.Printf("[kbatch-fair-forwarder] list stale forwards lane=%s: %v", f.Lane, err)
			return
		}
		for _, e := range stale {
			if err := f.Scheduler.ReclaimStaleForward(ctx, e, func(payload []byte, key string) error {
				readyTopic, err := f.readyTopicFor(payload)
				if err != nil {
					return err
				}
				return f.Producer.Produce(ctx, readyTopic, key, payload)
			}); err != nil {
				log.Printf("[kbatch-fair-forwarder] reclaim stale forward lane=%s slot=%s: %v", f.Lane, e.SlotID, err)
			}
		}
	}
}

// maybeResetVtimeIdle clears the lane's virtual-time ledger (weights preserved)
// once the lane has been fully quiescent for the configured debounce window.
//
// "Quiescent" means no active or in-flight work anywhere: no active tenants (empty
// ring), nothing in flight (no live leases), no in-progress forwards, and — when an
// ingest-lag counter is wired — zero ingest backlog. Because the ready ring is
// continuously fed from ingest, a ring that stays empty across the debounce already
// implies ingest has drained; the ingest-lag gate is an extra guard.
//
// The check is cheap (a pooled Redis snapshot + optional lag read) and rate-limited
// to idleCheckInterval. The final DEL is done atomically under a ring-empty guard in
// ResetVtimeIfQuiescent, so this can never wipe a tenant that just re-enqueued. The
// reset fires at most once per idle period (vtimeResetDone) and re-arms as soon as
// any activity is observed.
func (f *Forwarder) maybeResetVtimeIdle(ctx context.Context) {
	if f.Scheduler == nil || !f.Scheduler.Settings.ResetVtimeWhenIdle {
		return
	}
	now := time.Now()
	if now.Sub(f.lastIdleCheck) < idleCheckInterval {
		return
	}
	f.lastIdleCheck = now

	// Cheap Redis snapshot only: any active/in-flight work re-arms the debounce for
	// the next idle period (and clears the once-per-idle reset latch).
	redisIdle, err := f.redisIdle(ctx)
	if err != nil || !redisIdle {
		f.quiescentSince = time.Time{}
		f.vtimeResetDone = false
		return
	}
	// Already reset for this idle period — nothing to do until activity resumes.
	if f.vtimeResetDone {
		return
	}
	if f.quiescentSince.IsZero() {
		f.quiescentSince = now
		return
	}
	if now.Sub(f.quiescentSince) < f.Scheduler.Settings.EffectiveVtimeIdleResetDebounce() {
		return
	}
	// Final gate, evaluated only at the moment of reset (not every idle tick): if the
	// ingest topic still has backlog the lane is not truly idle — keep waiting.
	pending, err := f.Scheduler.IngestPending(ctx)
	if err != nil {
		return
	}
	if pending {
		f.quiescentSince = time.Time{}
		return
	}
	reset, err := f.Scheduler.ResetVtimeIfQuiescent(ctx)
	if err != nil {
		log.Printf("[kbatch-fair-forwarder] idle vtime reset lane=%s: %v", f.Lane, err)
		return
	}
	f.vtimeResetDone = true
	if reset {
		log.Printf("[kbatch-fair-forwarder] reset virtual-time ledger on idle lane=%s (weights preserved)", f.Lane)
	}
}

// redisIdle reports whether the lane currently shows no active tenants, no in-flight
// leases, and no in-progress forwards. It is a fast, pooled Redis snapshot; the
// authoritative atomic recheck happens inside ResetVtimeIfQuiescent.
func (f *Forwarder) redisIdle(ctx context.Context) (bool, error) {
	stats, err := f.Scheduler.Stats(ctx)
	if err != nil {
		return false, err
	}
	return stats.ActiveTenants == 0 && stats.InflightTotal == 0 && stats.ForwardingDepth == 0, nil
}

func (f *Forwarder) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Forwarder) readyTopicFor(payload []byte) (string, error) {
	if f.ResolveReadyTopic != nil {
		return f.ResolveReadyTopic(payload)
	}
	if f.ReadyTopic != "" {
		return f.ReadyTopic, nil
	}
	return "", fmt.Errorf("forwarder has no ready topic router configured")
}
