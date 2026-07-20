package daemon

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

// SuperFetchExecutor claims Redis ownership, Kafka-acks immediately, then runs
// #perform on a bounded goroutine pool without blocking the poll loop on perform.
//
// Two limits:
//   - ClaimWindow: max jobs Claimed∨Queued∨Performing (gates Claim+Mark)
//   - Sem: max concurrent #perform
//
// ClaimWindow ≥ Sem so ack can run ahead of perform; renew starts at Claim so
// leases stay alive while waiting for a perform slot.
type SuperFetchExecutor struct {
	Work             *workset.Store
	ConsumerID       string
	LeaseTTL         time.Duration // working-set job key TTL (renewed during perform)
	HeartbeatTTL     time.Duration // live:consumer:* TTL used for pod-alive checks
	HeartbeatEvery   time.Duration // how often to refresh the heartbeat key
	OrphanGrace      time.Duration // steal grace aligned with daemon reclaim
	ClaimWindow      chan struct{} // outstanding claimed∨queued∨performing
	Sem              chan struct{} // concurrent #perform
	Process          func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error)
	Apply            func(ctx context.Context, out job.Outcome) error
	heartbeatStarted sync.Once

	// lifeCtx is the process/member lifetime (not the poll-scoped procCtx).
	// #perform must outlive DispatchClaimsAndAcks — endProc cancels procCtx.
	// On SIGTERM, runCtx is cancelled first; lifeCtx stays alive until drain ends
	// so renew/heartbeat/#perform can finish or leave work in Redis for reclaim.
	lifeMu  sync.Mutex
	lifeCtx context.Context

	accepting atomic.Bool // false after StopAccepting — no new claims
	inFlight  sync.Map    // job_id → struct{} while claimed/queued/performing locally
}

func NewSuperFetchExecutor(cfg config.Daemon, work *workset.Store, consumerID string,
	process func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error),
	apply func(ctx context.Context, out job.Outcome) error,
) *SuperFetchExecutor {
	n := cfg.SuperFetchWorkers()
	win := cfg.SuperFetchClaimWindowSize()
	lease := cfg.SuperFetchLeaseTTL
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	grace := cfg.SuperFetchOrphanGrace
	if grace <= 0 {
		grace = workset.DefaultOrphanGrace
	}
	hbTTL := cfg.LivenessTTLDuration()
	// The workset payload lease MUST outlive death-detection plus one reclaim
	// cycle. Reclaim only re-produces a crashed pod's jobs once its heartbeat
	// (live:consumer:*, TTL=liveness_ttl) has expired; if the payload lease
	// (super_fetch_lease_ttl) expires first, ListOrphans finds the index entry
	// with no payload and can only clean it — the job is permanently lost (its
	// Kafka offset was already committed at claim). Enforce a safe floor so this
	// TTL inversion cannot silently drop in-flight jobs on an ungraceful crash.
	reclaimEvery := cfg.SuperFetchReclaimEvery
	if reclaimEvery <= 0 {
		reclaimEvery = 30 * time.Second
	}
	if minLease := hbTTL + grace + reclaimEvery + 30*time.Second; lease < minLease {
		log.Printf("[kbatch-superfetch] super_fetch_lease_ttl=%s is below the safe floor %s "+
			"(liveness_ttl=%s + orphan_grace=%s + reclaim_interval=%s + 30s buffer) — raising it so a "+
			"crashed pod's in-flight jobs are reclaimable before their payload expires (else they are lost)",
			lease, minLease, hbTTL, grace, reclaimEvery)
		lease = minLease
	}
	e := &SuperFetchExecutor{
		Work:           work,
		ConsumerID:     consumerID,
		LeaseTTL:       lease,
		HeartbeatTTL:   hbTTL,
		HeartbeatEvery: cfg.LivenessHeartbeatIntervalDuration(),
		OrphanGrace:    grace,
		ClaimWindow:    make(chan struct{}, win),
		Sem:            make(chan struct{}, n),
		Process:        process,
		Apply:          apply,
	}
	e.accepting.Store(true)
	return e
}

// StopAccepting refuses new Claim+ack work (graceful shutdown step 1).
func (e *SuperFetchExecutor) StopAccepting() {
	if e == nil {
		return
	}
	e.accepting.Store(false)
}

// InFlightCount returns jobs claimed/queued/performing in this executor.
func (e *SuperFetchExecutor) InFlightCount() int {
	if e == nil {
		return 0
	}
	n := 0
	e.inFlight.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// WaitInFlight blocks until in-flight is empty or timeout elapses.
// Returns the remaining in-flight count (0 = drained cleanly).
func (e *SuperFetchExecutor) WaitInFlight(timeout time.Duration) int {
	if e == nil {
		return 0
	}
	if timeout <= 0 {
		return e.InFlightCount()
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.InFlightCount() == 0 {
			return 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	return e.InFlightCount()
}

// BindLife pins the member lifetime context used by #perform / renew / heartbeat.
// Must be called with the supervised consumer ctx (not a poll-scoped procCtx).
func (e *SuperFetchExecutor) BindLife(ctx context.Context) {
	if e == nil || ctx == nil {
		return
	}
	e.lifeMu.Lock()
	if e.lifeCtx == nil {
		e.lifeCtx = ctx
	}
	life := e.lifeCtx
	e.lifeMu.Unlock()
	e.StartHeartbeatLoop(life)
}

func (e *SuperFetchExecutor) life() context.Context {
	e.lifeMu.Lock()
	defer e.lifeMu.Unlock()
	if e.lifeCtx != nil {
		return e.lifeCtx
	}
	return context.Background()
}

// StartHeartbeatLoop keeps the SuperFetch member id alive even when idle or
// during long performs (independent of the Kafka poll path).
func (e *SuperFetchExecutor) StartHeartbeatLoop(ctx context.Context) {
	if e == nil || e.Work == nil {
		return
	}
	e.heartbeatStarted.Do(func() {
		interval := e.HeartbeatEvery
		if interval <= 0 {
			interval = 20 * time.Second
		}
		go func() {
			if err := e.Work.TouchConsumer(ctx, e.ConsumerID, e.HeartbeatTTL); err != nil {
				log.Printf("[kbatch-superfetch] heartbeat touch consumer=%s: %v", e.ConsumerID, err)
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := e.Work.TouchConsumer(ctx, e.ConsumerID, e.HeartbeatTTL); err != nil {
						log.Printf("[kbatch-superfetch] heartbeat touch consumer=%s: %v", e.ConsumerID, err)
					}
				}
			}
		}()
	})
}

// DispatchClaimsAndAcks claims each record, marks the Kafka offset, and starts
// perform in the background. Blocks only on ClaimWindow (not perform Sem) so
// rebalance is not held for the full #perform duration when window > Sem.
//
// ctx may be the poll-scoped procCtx (canceled when this function returns).
// #perform uses BindLife's context so it is not canceled by endProc.
func (e *SuperFetchExecutor) DispatchClaimsAndAcks(ctx context.Context, cl *kgo.Client, recs []*kgo.Record, group string) {
	if e == nil || e.Work == nil {
		return
	}
	if !e.accepting.Load() {
		return
	}
	life := e.life()
	e.StartHeartbeatLoop(life)
	for i, rec := range recs {
		if !e.accepting.Load() {
			rewindUndispatched(cl, recs[i:])
			return
		}
		select {
		case <-ctx.Done():
			// Aborted mid-batch (rebalance / stall). PollFetches already advanced
			// franz-go's fetch cursor past every record in this batch; for a
			// partition this member KEEPS through a cooperative rebalance the
			// un-dispatched tail would otherwise never be re-fetched, and later
			// marks would commit past it — a silent drop. Rewind the consume
			// position to the first un-dispatched offset per partition so those
			// records are redelivered. (For revoked partitions this is a no-op;
			// the new owner resumes from the committed marks.)
			rewindUndispatched(cl, recs[i:])
			return
		case e.ClaimWindow <- struct{}{}:
		}
		jobID := extractJobID(rec.Value)
		if jobID == "" {
			// Malformed — process synchronously for DLT then ack (no Redis claim).
			e.processMissingJobID(ctx, cl, rec, group)
			<-e.ClaimWindow
			continue
		}
		if _, loaded := e.inFlight.LoadOrStore(jobID, struct{}{}); loaded {
			// Already claimed/performing in this process (kafka redelivery).
			cl.MarkCommitRecords(rec)
			<-e.ClaimWindow
			continue
		}
		claim, err := e.Work.Claim(life, workset.ClaimParams{
			JobID: jobID, Payload: rec.Value, Topic: rec.Topic,
			Partition: rec.Partition, Offset: rec.Offset,
			ConsumerID: e.ConsumerID, LeaseTTL: e.LeaseTTL,
			HeartbeatTTL: e.HeartbeatTTL, StealGrace: e.OrphanGrace,
		})
		if err != nil {
			log.Printf("[kbatch-superfetch] claim error group=%s job_id=%s: %v — leaving unacked",
				group, jobID, err)
			e.inFlight.Delete(jobID)
			<-e.ClaimWindow
			continue
		}
		if !claim.Won {
			log.Printf("[kbatch-superfetch] claim lost group=%s job_id=%s — acking duplicate",
				group, jobID)
			cl.MarkCommitRecords(rec)
			e.inFlight.Delete(jobID)
			<-e.ClaimWindow
			continue
		}
		// Durability: Redis owns the job before Kafka forgets it.
		cl.MarkCommitRecords(rec)
		// Renew from claim time so lease cannot expire while waiting for Sem.
		stopRenew := e.startRenew(life, jobID, claim.Fence)
		go e.perform(life, rec, jobID, claim.Fence, group, stopRenew)
	}
}

func (e *SuperFetchExecutor) perform(ctx context.Context, rec *kgo.Record, jobID, fence, group string, stopRenew func()) {
	defer func() {
		if stopRenew != nil {
			stopRenew()
		}
		e.inFlight.Delete(jobID)
		<-e.ClaimWindow
	}()

	// Sem gates concurrent #perform only. Apply (event/retry/DLT produce) and
	// Complete run after release so slow Kafka emit does not starve the pool.
	// ClaimWindow still covers the full lifetime (durability / renew).
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}

	// Retry process/apply while healthy. Returning early used to stop renew while
	// the consumer heartbeat stayed alive — reclaim only steals from *dead*
	// consumers, so the job lease TTL then deleted the payload with no Kafka
	// redelivery path (permanent batch hole, lag already 0).
	var out job.Outcome
	for attempt := 1; ; attempt++ {
		var err error
		out, err = e.processWithSem(ctx, rec.Value, src)
		if err == nil {
			break
		}
		log.Printf("[kbatch-superfetch] process error group=%s job_id=%s attempt=%d: %v — retrying (renew kept)",
			group, jobID, attempt, err)
		if !sleepOrDone(ctx, retryBackoff(attempt)) {
			log.Printf("[kbatch-superfetch] process aborted group=%s job_id=%s — leaving in workset for reclaim",
				group, jobID)
			return
		}
	}

	// Apply BEFORE the fence check (Ruby SuperFetch parity). If the workset
	// entry expired or was reclaimed while #perform ran, skipping Apply would
	// permanently lose the job: Kafka already acked at Claim, and reclaim has
	// nothing left to re-produce. Event emit parks on DLT when events_topic
	// is down; Complete runs only after Apply returns nil.
	var lastApplyErr error
	for attempt := 1; ; attempt++ {
		if err := e.Apply(ctx, out); err == nil {
			break
		} else {
			lastApplyErr = err
			log.Printf("[kbatch-superfetch] apply error group=%s job_id=%s attempt=%d: %v — retrying (renew kept)",
				group, jobID, attempt, err)
		}
		if !sleepOrDone(ctx, retryBackoff(attempt)) {
			batchID := ""
			if out.Event != nil {
				batchID = out.Event.BatchID
			}
			instrument.JobApplyAborted(jobID, batchID, group, lastApplyErr)
			log.Printf("[kbatch-superfetch] apply aborted group=%s job_id=%s — leaving in workset for reclaim (Complete skipped)",
				group, jobID)
			return
		}
	}
	owned, err := e.Work.StillOwned(ctx, jobID, e.ConsumerID, fence)
	if err != nil || !owned {
		log.Printf("[kbatch-superfetch] lost fence group=%s job_id=%s owned=%v err=%v — apply already done, skip complete",
			group, jobID, owned, err)
		return
	}
	for i := 0; i < 5; i++ {
		if err := e.Work.Complete(ctx, jobID, e.ConsumerID, fence); err != nil {
			log.Printf("[kbatch-superfetch] complete error group=%s job_id=%s attempt=%d: %v",
				group, jobID, i+1, err)
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		return
	}
}

func retryBackoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 200 * time.Millisecond
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (e *SuperFetchExecutor) processWithSem(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
	select {
	case <-ctx.Done():
		return job.Outcome{}, ctx.Err()
	case e.Sem <- struct{}{}:
	}
	defer func() { <-e.Sem }()
	return e.Process(ctx, raw, src)
}

func (e *SuperFetchExecutor) startRenew(ctx context.Context, jobID, fence string) func() {
	stop := make(chan struct{})
	// Job-lease renew; member heartbeat is owned by StartHeartbeatLoop (every 20s).
	interval := e.LeaseTTL / 3
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				ok, err := e.Work.Renew(ctx, jobID, e.ConsumerID, fence, e.LeaseTTL)
				if err != nil {
					// Transient Redis errors must not stop renew — lease expiry
					// after Kafka ack would drop the job with no reclaim path.
					log.Printf("[kbatch-superfetch] renew error job_id=%s: %v — will retry", jobID, err)
					continue
				}
				if !ok {
					log.Printf("[kbatch-superfetch] renew lost fence job_id=%s — stop renew", jobID)
					return
				}
			}
		}
	}()
	return func() { close(stop) }
}

func (e *SuperFetchExecutor) processMissingJobID(ctx context.Context, cl *kgo.Client, rec *kgo.Record, group string) {
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.Process(ctx, rec.Value, src)
	if err != nil {
		log.Printf("[kbatch-superfetch] missing job_id process error group=%s: %v", group, err)
		return
	}
	if err := e.Apply(ctx, out); err != nil {
		log.Printf("[kbatch-superfetch] missing job_id apply error group=%s: %v", group, err)
		return
	}
	cl.MarkCommitRecords(rec)
}

// rewindUndispatched resets the consume position to the lowest offset per
// partition among the given (un-dispatched) records, so franz-go re-fetches them
// instead of skipping them. Called on an aborted dispatch (rebalance/stall)
// BEFORE AllowRebalance, on the poll goroutine — the only safe place to move
// offsets. Records already dispatched (claimed+marked) or acked (dedup/lost) are
// not included, so this never rewinds over work that was actually handled.
func rewindUndispatched(cl *kgo.Client, recs []*kgo.Record) {
	if cl == nil {
		return
	}
	offsets := undispatchedRewindOffsets(recs)
	if len(offsets) == 0 {
		return
	}
	cl.SetOffsets(offsets)
}

// undispatchedRewindOffsets returns the lowest offset per partition among recs,
// as the SetOffsets map used to rewind the consume position so un-dispatched
// records are redelivered. Epoch -1 means "no epoch" (consume from the offset).
func undispatchedRewindOffsets(recs []*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	if len(recs) == 0 {
		return nil
	}
	min := map[string]map[int32]int64{}
	for _, r := range recs {
		if r == nil {
			continue
		}
		if min[r.Topic] == nil {
			min[r.Topic] = map[int32]int64{}
		}
		if o, ok := min[r.Topic][r.Partition]; !ok || r.Offset < o {
			min[r.Topic][r.Partition] = r.Offset
		}
	}
	if len(min) == 0 {
		return nil
	}
	offsets := make(map[string]map[int32]kgo.EpochOffset, len(min))
	for t, ps := range min {
		offsets[t] = make(map[int32]kgo.EpochOffset, len(ps))
		for p, o := range ps {
			offsets[t][p] = kgo.EpochOffset{Epoch: -1, Offset: o}
		}
	}
	return offsets
}

func extractJobID(raw []byte) string {
	var m struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	return m.JobID
}
