package daemon

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// WatermarkExecutor is the Redis-free alternative to SuperFetchExecutor. Instead
// of writing per-job ownership to a Redis working set and acking the Kafka offset
// ahead of #perform, it runs jobs concurrently out of order and commits only the
// contiguous completed-offset prefix per partition (the "watermark"). On crash or
// rebalance, everything after the last committed watermark is redelivered and
// re-run — so handlers MUST be idempotent and per-topic runtimes should be similar
// (a slow job holds the watermark; every faster job that finished behind it
// re-runs on crash). See config.ExecutionModeWatermark and the README.
//
// Two bounds, mirroring SuperFetch:
//   - Window: max jobs dispatched-but-not-yet-committed per member. A window slot
//     is taken at dispatch and released only when the offset is committed (or the
//     job failed infra-side). This bounds the pending-completion map and the number
//     of jobs re-run on crash.
//   - Sem: max concurrent #perform.
//
// MarkCommitRecords is only ever called from the poll-loop goroutine (via
// DispatchAndCommit / FlushMarks), never from a #perform goroutine, so the loop's
// franz-go client is never touched after it is closed on shutdown.
type WatermarkExecutor struct {
	ConsumerID string
	Window     chan struct{}
	Sem        chan struct{}
	Process    func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error)
	Apply      func(ctx context.Context, out job.Outcome) error

	lifeMu  sync.Mutex
	lifeCtx context.Context

	accepting atomic.Bool
	inFlight  int64 // dispatched-not-yet-finalized (committed or failed); for drain

	mu    sync.Mutex
	parts map[partitionKey]*partitionCommit
}

// recordMarker is the subset of *kgo.Client the watermark executor needs to
// commit offsets. An interface (not the concrete client) so tests can assert the
// exact records marked without a live broker.
type recordMarker interface {
	MarkCommitRecords(rs ...*kgo.Record)
}

// partitionCommit tracks the contiguous-prefix commit state for one partition.
type partitionCommit struct {
	expected int64                 // next offset to commit (the watermark head)
	inited   bool                  // expected has been seeded from the first record
	done     map[int64]*kgo.Record // completed offsets >= expected awaiting the prefix
}

// NewWatermarkExecutor builds an executor sized from the SuperFetch knobs so the
// same concurrency/window tuning applies to both modes.
func NewWatermarkExecutor(cfg config.Daemon, consumerID string,
	process func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error),
	apply func(ctx context.Context, out job.Outcome) error,
) *WatermarkExecutor {
	n := cfg.SuperFetchWorkers()
	win := cfg.SuperFetchClaimWindowSize()
	if win < n {
		win = n
	}
	e := &WatermarkExecutor{
		ConsumerID: consumerID,
		Window:     make(chan struct{}, win),
		Sem:        make(chan struct{}, n),
		Process:    process,
		Apply:      apply,
		parts:      make(map[partitionKey]*partitionCommit),
	}
	e.accepting.Store(true)
	return e
}

// BindLife pins the member-lifetime context used by #perform so a job outlives the
// poll-scoped procCtx (matching SuperFetch). Watermark keeps no Redis heartbeat.
func (e *WatermarkExecutor) BindLife(ctx context.Context) {
	if e == nil || ctx == nil {
		return
	}
	e.lifeMu.Lock()
	if e.lifeCtx == nil {
		e.lifeCtx = ctx
	}
	e.lifeMu.Unlock()
}

func (e *WatermarkExecutor) life() context.Context {
	e.lifeMu.Lock()
	defer e.lifeMu.Unlock()
	if e.lifeCtx != nil {
		return e.lifeCtx
	}
	return context.Background()
}

// StopAccepting refuses new dispatches (graceful shutdown step 1).
func (e *WatermarkExecutor) StopAccepting() {
	if e == nil {
		return
	}
	e.accepting.Store(false)
}

// InFlightCount returns jobs dispatched but not yet committed or failed.
func (e *WatermarkExecutor) InFlightCount() int {
	if e == nil {
		return 0
	}
	return int(atomic.LoadInt64(&e.inFlight))
}

// WaitInFlight blocks until in-flight is empty or timeout elapses; returns the
// remaining count (0 = drained cleanly).
func (e *WatermarkExecutor) WaitInFlight(timeout time.Duration) int {
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

// DispatchAndCommit launches each record's #perform on the pool (bounded by
// Window, then Sem inside perform) and then flushes any newly committable prefix.
// It blocks only on Window (backpressure) and returns immediately on ctx cancel
// (rebalance) so the franz-go BlockRebalanceOnPoll gate is not held for a full
// #perform. ctx is the poll-scoped procCtx; #perform uses the bound life context.
func (e *WatermarkExecutor) DispatchAndCommit(ctx context.Context, cl recordMarker, recs []*kgo.Record, group string) {
	if e == nil {
		return
	}
	if !e.accepting.Load() {
		return
	}
	life := e.life()
	for _, rec := range recs {
		if !e.accepting.Load() {
			break
		}
		select {
		case <-ctx.Done():
			// Rebalance/abort: stop dispatching. Undispatched records are never
			// marked → redelivered → re-run (idempotent).
			e.FlushMarks(cl)
			return
		case e.Window <- struct{}{}:
		}
		e.register(rec)
		atomic.AddInt64(&e.inFlight, 1)
		go e.perform(life, rec, group)
	}
	e.FlushMarks(cl)
}

// register seeds the partition tracker for a newly dispatched record. If the
// record's offset is below the current watermark (redelivery after a rebalance),
// the tracker resets to it so the prefix re-forms from the redelivered position.
func (e *WatermarkExecutor) register(rec *kgo.Record) {
	key := partitionKey{topic: rec.Topic, partition: rec.Partition}
	e.mu.Lock()
	defer e.mu.Unlock()
	pc := e.parts[key]
	if pc == nil {
		pc = &partitionCommit{done: make(map[int64]*kgo.Record)}
		e.parts[key] = pc
	}
	if !pc.inited || rec.Offset < pc.expected {
		pc.expected = rec.Offset
		pc.inited = true
		// Drop any stale completions at/above the reset point that predate it.
		for off := range pc.done {
			if off < rec.Offset {
				delete(pc.done, off)
			}
		}
	}
}

func (e *WatermarkExecutor) perform(ctx context.Context, rec *kgo.Record, group string) {
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.processWithSem(ctx, rec.Value, src)
	if err != nil {
		e.fail(rec, group, "process", err)
		return
	}
	if err := e.Apply(ctx, out); err != nil {
		e.fail(rec, group, "apply", err)
		return
	}
	e.noteDone(rec)
}

func (e *WatermarkExecutor) processWithSem(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
	select {
	case <-ctx.Done():
		return job.Outcome{}, ctx.Err()
	case e.Sem <- struct{}{}:
	}
	defer func() { <-e.Sem }()
	return e.Process(ctx, raw, src)
}

// noteDone records a successful completion. The offset becomes committable once
// it is the head of the contiguous prefix; its Window slot is released at commit
// time in FlushMarks (holding it bounds the pending-completion map).
func (e *WatermarkExecutor) noteDone(rec *kgo.Record) {
	key := partitionKey{topic: rec.Topic, partition: rec.Partition}
	e.mu.Lock()
	pc := e.parts[key]
	if pc != nil && (!pc.inited || rec.Offset >= pc.expected) {
		pc.done[rec.Offset] = rec
	}
	e.mu.Unlock()
	atomic.AddInt64(&e.inFlight, -1)
}

// fail marks an infra-side failure (Process/Apply error — never a business retry,
// which is carried in the Outcome). The offset is not committed and blocks its
// partition's watermark until redelivery on the next rebalance/restart. Its Window
// slot IS released so the failed record alone does not permanently pin a slot; the
// (window-1) completed-but-blocked records behind it still hold theirs, so the
// member backpressures rather than growing the pending map without bound.
func (e *WatermarkExecutor) fail(rec *kgo.Record, group, stage string, err error) {
	log.Printf("[kbatch-watermark] %s error group=%s topic=%s partition=%d offset=%d: %v — not committing (redelivers on restart)",
		stage, group, rec.Topic, rec.Partition, rec.Offset, err)
	atomic.AddInt64(&e.inFlight, -1)
	<-e.Window
}

// FlushMarks advances the contiguous committed prefix for every partition and
// marks those records for autocommit. Called from the poll-loop goroutine only:
// at the end of DispatchAndCommit and on every poll via the loop's onPoll hook
// (so completions on an idle topic still commit).
func (e *WatermarkExecutor) FlushMarks(cl recordMarker) {
	if e == nil || cl == nil {
		return
	}
	var toMark []*kgo.Record
	e.mu.Lock()
	for _, pc := range e.parts {
		if !pc.inited {
			continue
		}
		for {
			rec, ok := pc.done[pc.expected]
			if !ok {
				break
			}
			delete(pc.done, pc.expected)
			toMark = append(toMark, rec)
			pc.expected++
		}
	}
	e.mu.Unlock()
	if len(toMark) == 0 {
		return
	}
	cl.MarkCommitRecords(toMark...)
	// Release one Window slot per committed offset.
	for range toMark {
		<-e.Window
	}
}
