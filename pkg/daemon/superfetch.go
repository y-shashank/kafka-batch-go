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

	// One shared renew goroutine per executor pipelines all in-flight lease
	// renewals (previously one goroutine + ticker + channel per job).
	renewMu   sync.Mutex
	renewSet  map[string]string // job_id → fence
	renewOnce sync.Once
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

// dispatchClient is the subset of *kgo.Client the dispatch loop needs (mark
// offsets + rewind the consume position). An interface — mirroring watermark's
// recordMarker — so tests can assert exact marks/rewinds without a live broker.
type dispatchClient interface {
	MarkCommitRecords(rs ...*kgo.Record)
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
}

// DispatchClaimsAndAcks claims each record, marks the Kafka offset, and starts
// perform in the background. Blocks only on ClaimWindow (not perform Sem) so
// rebalance is not held for the full #perform duration when window > Sem.
//
// ctx may be the poll-scoped procCtx (canceled when this function returns).
// #perform uses BindLife's context so it is not canceled by endProc.
// maxClaimPipeline bounds how many claims ride one Redis pipeline per flush.
const maxClaimPipeline = 256

type dispatchKind int

const (
	dispatchClaim dispatchKind = iota
	dispatchDuplicate
	dispatchMalformed
)

// dispatchItem is one record pending flush. Items in a chunk are contiguous
// with the poll batch and have NO external effects (no marks, no Redis claims,
// no DLT produce) until flushDispatchChunk processes them — in record order,
// because franz-go marks are per-partition high-water, so marking record N+1
// before N is handled would commit past N.
type dispatchItem struct {
	rec   *kgo.Record
	jobID string
	kind  dispatchKind
	slot  bool
}

// DispatchClaimsAndAcks pipelines all claims of a poll batch through one Redis
// round trip (previously one blocking EVAL per record on the poll goroutine —
// the per-pod throughput ceiling), then marks/dispatches strictly in record
// order. Loss semantics are identical to the sequential version: any item whose
// outcome is unknown aborts the batch at that record, releases the untouched
// tail's resources, and rewinds so redelivery retries.
func (e *SuperFetchExecutor) DispatchClaimsAndAcks(ctx context.Context, cl dispatchClient, recs []*kgo.Record, group string) {
	if e == nil || e.Work == nil {
		return
	}
	if !e.accepting.Load() {
		return
	}
	life := e.life()
	e.StartHeartbeatLoop(life)

	var chunk []dispatchItem
	chunkStart := 0 // recs index of chunk[0]

	// releaseChunkClaims returns resources for chunk items in [from, len):
	// they either never ran or their Redis claim will be resumed on redelivery.
	releaseChunkClaims := func(from int) {
		for _, it := range chunk[from:] {
			if it.kind != dispatchClaim {
				continue
			}
			e.inFlight.Delete(it.jobID)
			if it.slot {
				<-e.ClaimWindow
			}
		}
	}

	flush := func() bool {
		if len(chunk) == 0 {
			return true
		}
		stop, ok := e.flushDispatchChunk(ctx, cl, life, chunk, group)
		if !ok {
			// chunk[stop] failed (resources already released by flush); items
			// after it are unprocessed — release theirs and rewind from the
			// failed record so redelivery retries the whole tail.
			releaseChunkClaims(stop + 1)
			rewindUndispatched(cl, recs[chunkStart+stop:])
			return false
		}
		chunkStart += len(chunk)
		chunk = chunk[:0]
		return true
	}

	abortPending := func() {
		// Nothing in the pending chunk has external effects yet: release its
		// claim resources and rewind everything from the chunk start.
		releaseChunkClaims(0)
		rewindUndispatched(cl, recs[chunkStart:])
	}

	for i, rec := range recs {
		if !e.accepting.Load() {
			abortPending()
			return
		}
		jobID := extractJobID(rec.Value)
		it := dispatchItem{rec: rec, jobID: jobID}
		switch {
		case jobID == "":
			it.kind = dispatchMalformed
		default:
			if _, loaded := e.inFlight.LoadOrStore(jobID, struct{}{}); loaded {
				it.kind = dispatchDuplicate
			} else {
				it.kind = dispatchClaim
				// Acquire the ClaimWindow slot; if the window is full, flush the
				// pending chunk first so held slots make progress, then block.
				select {
				case e.ClaimWindow <- struct{}{}:
					it.slot = true
				default:
					// Window full: flush the pending chunk so held slots make
					// progress, then block for a slot. On flush failure the
					// rewind already covers this record (it is in the tail).
					if !flush() {
						e.inFlight.Delete(jobID)
						return
					}
					select {
					case <-ctx.Done():
						// Rebalance/abort: the poll cursor already passed this
						// batch; rewind so the un-dispatched tail redelivers
						// (for kept partitions) instead of being committed past.
						e.inFlight.Delete(jobID)
						rewindUndispatched(cl, recs[i:])
						return
					case e.ClaimWindow <- struct{}{}:
						it.slot = true
					}
				}
			}
		}
		chunk = append(chunk, it)
		if len(chunk) >= maxClaimPipeline {
			if !flush() {
				return
			}
		}
	}
	_ = flush()
}

// flushDispatchChunk applies a chunk's effects in record order. Returns
// (stopIdx, false) when chunk[stopIdx]'s outcome is unknown — its own resources
// are released; the caller rewinds from its record. Items before stopIdx are
// fully processed.
func (e *SuperFetchExecutor) flushDispatchChunk(ctx context.Context, cl dispatchClient, life context.Context, chunk []dispatchItem, group string) (int, bool) {
	var params []workset.ClaimParams
	for _, it := range chunk {
		if it.kind != dispatchClaim {
			continue
		}
		params = append(params, workset.ClaimParams{
			JobID: it.jobID, Payload: it.rec.Value, Topic: it.rec.Topic,
			Partition: it.rec.Partition, Offset: it.rec.Offset,
			ConsumerID: e.ConsumerID, LeaseTTL: e.LeaseTTL,
			HeartbeatTTL: e.HeartbeatTTL, StealGrace: e.OrphanGrace,
		})
	}
	var results []workset.ClaimResult
	var errs []error
	if len(params) > 0 {
		results, errs = e.Work.ClaimMany(life, params)
	}

	ri := 0
	for idx, it := range chunk {
		switch it.kind {
		case dispatchMalformed:
			// Malformed — process synchronously for DLT then ack (no Redis claim).
			if !e.processMissingJobID(ctx, cl, it.rec, group) {
				return idx, false
			}
		case dispatchDuplicate:
			// Already claimed/performing in this process (kafka redelivery).
			cl.MarkCommitRecords(it.rec)
		case dispatchClaim:
			res, err := results[ri], errs[ri]
			ri++
			if err != nil {
				// Unknown outcome (transient Redis failure). The record is not
				// marked; committing past it via later marks would drop the job
				// permanently — abort here so the caller rewinds. If the claim
				// DID land in Redis, redelivery resumes it by fence.
				log.Printf("[kbatch-superfetch] claim error group=%s job_id=%s: %v — rewinding undispatched tail",
					group, it.jobID, err)
				e.inFlight.Delete(it.jobID)
				if it.slot {
					<-e.ClaimWindow
				}
				return idx, false
			}
			if !res.Won {
				log.Printf("[kbatch-superfetch] claim lost group=%s job_id=%s — acking duplicate",
					group, it.jobID)
				cl.MarkCommitRecords(it.rec)
				e.inFlight.Delete(it.jobID)
				if it.slot {
					<-e.ClaimWindow
				}
				continue
			}
			// Durability: Redis owns the job before Kafka forgets it.
			cl.MarkCommitRecords(it.rec)
			// Renew from claim time so lease cannot expire while waiting for Sem.
			stopRenew := e.registerRenew(life, it.jobID, res.Fence)
			go e.perform(life, it.rec, it.jobID, res.Fence, group, stopRenew)
		}
	}
	return 0, true
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
	// Complete is fence-guarded in Lua (a lost fence is an atomic no-op), so the
	// old StillOwned pre-check was a redundant GET+unmarshal per job — and on a
	// transient error it skipped Complete entirely, leaving the entry to rot
	// until lease expiry.
	for i := 0; i < 5; i++ {
		if err := e.Work.Complete(ctx, jobID, e.ConsumerID, fence); err != nil {
			log.Printf("[kbatch-superfetch] complete error group=%s job_id=%s attempt=%d: %v",
				group, jobID, i+1, err)
			if !sleepOrDone(ctx, time.Duration(i+1)*50*time.Millisecond) {
				return
			}
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

// registerRenew adds a job lease to the shared renew loop and returns a func
// that removes it (call when the job's terminal outcome is applied). Renew
// cadence and failure semantics match the old per-job goroutine: transient
// Redis errors keep the entry (retry next tick); a lost fence drops it.
func (e *SuperFetchExecutor) registerRenew(ctx context.Context, jobID, fence string) func() {
	e.renewMu.Lock()
	if e.renewSet == nil {
		e.renewSet = make(map[string]string)
	}
	e.renewSet[jobID] = fence
	e.renewMu.Unlock()
	e.startRenewLoop(ctx)
	return func() {
		e.renewMu.Lock()
		if e.renewSet[jobID] == fence {
			delete(e.renewSet, jobID)
		}
		e.renewMu.Unlock()
	}
}

func (e *SuperFetchExecutor) startRenewLoop(ctx context.Context) {
	e.renewOnce.Do(func() {
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
				case <-ctx.Done():
					return
				case <-t.C:
					e.renewMu.Lock()
					entries := make([]workset.RenewEntry, 0, len(e.renewSet))
					for id, f := range e.renewSet {
						entries = append(entries, workset.RenewEntry{JobID: id, Fence: f})
					}
					e.renewMu.Unlock()
					if len(entries) == 0 {
						continue
					}
					ok, errs := e.Work.RenewMany(ctx, e.ConsumerID, entries, e.LeaseTTL)
					for i, ent := range entries {
						if errs[i] != nil {
							// Transient Redis errors must not stop renew — lease
							// expiry after Kafka ack would drop the job with no
							// reclaim path. Keep the entry and retry next tick.
							log.Printf("[kbatch-superfetch] renew error job_id=%s: %v — will retry", ent.JobID, errs[i])
							continue
						}
						if !ok[i] {
							log.Printf("[kbatch-superfetch] renew lost fence job_id=%s — stop renew", ent.JobID)
							e.renewMu.Lock()
							if e.renewSet[ent.JobID] == ent.Fence {
								delete(e.renewSet, ent.JobID)
							}
							e.renewMu.Unlock()
						}
					}
				}
			}
		}()
	})
}

// processMissingJobID routes a malformed record (no job_id) to the DLT and acks
// it. Returns false when routing failed and the record was NOT acked — the
// caller must rewind so the record is redelivered rather than committed past.
func (e *SuperFetchExecutor) processMissingJobID(ctx context.Context, cl dispatchClient, rec *kgo.Record, group string) bool {
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.Process(ctx, rec.Value, src)
	if err != nil {
		log.Printf("[kbatch-superfetch] missing job_id process error group=%s: %v", group, err)
		return false
	}
	if err := e.Apply(ctx, out); err != nil {
		log.Printf("[kbatch-superfetch] missing job_id apply error group=%s: %v", group, err)
		return false
	}
	cl.MarkCommitRecords(rec)
	return true
}

// rewindUndispatched resets the consume position to the lowest offset per
// partition among the given (un-dispatched) records, so franz-go re-fetches them
// instead of skipping them. Called on an aborted dispatch (rebalance/stall)
// BEFORE AllowRebalance, on the poll goroutine — the only safe place to move
// offsets. Records already dispatched (claimed+marked) or acked (dedup/lost) are
// not included, so this never rewinds over work that was actually handled.
func rewindUndispatched(cl dispatchClient, recs []*kgo.Record) {
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
