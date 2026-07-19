package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var partitionDeferPauseMu sync.Mutex

// deferPartitionPause pauses fetching one partition until wait elapses, then resumes.
// Prefer deferClientPartitionPause for *consumerClient — that path tracks the pause for
// pollWaitCtx and SetOffsets back to the uncommitted record on resume.
func deferPartitionPause(cl fetchPauser, rec *kgo.Record, wait time.Duration) {
	if cl == nil || rec == nil || wait <= 0 {
		return
	}
	topic := rec.Topic
	partition := rec.Partition
	parts := map[string][]int32{topic: {partition}}

	partitionDeferPauseMu.Lock()
	cl.PauseFetchPartitions(parts)
	partitionDeferPauseMu.Unlock()

	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		<-timer.C
		partitionDeferPauseMu.Lock()
		cl.ResumeFetchPartitions(parts)
		partitionDeferPauseMu.Unlock()
	}()
}

// deferClientPartitionPause pauses one partition until wait elapses, then seeks back to
// the uncommitted offset and resumes. Tracking lives in deferredPaused (not partPaused)
// so syncConsumptionFetchPause cannot clear the pause via the consumption killswitch.
// Timers are bound to the client's defer lifecycle and no-op after invalidateDeferredPauses.
func deferClientPartitionPause(cl *consumerClient, rec *kgo.Record, wait time.Duration) {
	if cl == nil || rec == nil || wait <= 0 {
		return
	}
	topic := rec.Topic
	partition := rec.Partition
	cl.pauseDeferredPartition(topic, partition, rec.Offset)

	life, gen := cl.deferLifecycle()
	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-life.Done():
			return
		}
		if !cl.deferGenerationLive(gen) {
			return
		}
		cl.clearDeferredPartitionPause(topic, partition)
	}()
}

func (c *consumerClient) initDeferLifecycle() {
	if c == nil {
		return
	}
	c.deferMu.Lock()
	defer c.deferMu.Unlock()
	if c.deferStop != nil {
		c.deferStop()
	}
	c.deferGen++
	c.deferLife, c.deferStop = context.WithCancel(context.Background())
}

func (c *consumerClient) deferLifecycle() (context.Context, uint64) {
	if c == nil {
		return context.Background(), 0
	}
	c.deferMu.Lock()
	defer c.deferMu.Unlock()
	if c.deferLife == nil {
		c.deferGen++
		c.deferLife, c.deferStop = context.WithCancel(context.Background())
	}
	return c.deferLife, c.deferGen
}

func (c *consumerClient) deferGenerationLive(gen uint64) bool {
	if c == nil {
		return false
	}
	c.deferMu.Lock()
	defer c.deferMu.Unlock()
	return c.deferGen == gen && c.deferLife != nil && c.deferLife.Err() == nil
}

// invalidateDeferredPauses cancels outstanding defer timers and drops deferred pause
// state. Called before CloseAllowingRebalance so timers cannot SetOffsets on a dead client.
func (c *consumerClient) invalidateDeferredPauses() {
	if c == nil {
		return
	}
	c.deferMu.Lock()
	if c.deferStop != nil {
		c.deferStop()
		c.deferStop = nil
		c.deferLife = nil
	}
	c.deferGen++
	c.deferMu.Unlock()

	c.pauseMu.Lock()
	c.deferredPaused = map[string]map[int32]int64{}
	c.pauseMu.Unlock()
}

func (c *consumerClient) pauseDeferredPartition(topic string, partition int32, offset int64) {
	if c == nil {
		return
	}
	p := c.pauser()
	c.pauseMu.Lock()
	if c.deferredPaused == nil {
		c.deferredPaused = map[string]map[int32]int64{}
	}
	if c.deferredPaused[topic] == nil {
		c.deferredPaused[topic] = map[int32]int64{}
	}
	rewind := false
	if existing, ok := c.deferredPaused[topic][partition]; !ok || offset < existing {
		c.deferredPaused[topic][partition] = offset
		rewind = true
	}
	c.pauseMu.Unlock()
	if p != nil {
		partitionDeferPauseMu.Lock()
		p.PauseFetchPartitions(map[string][]int32{topic: {partition}})
		partitionDeferPauseMu.Unlock()
	}
	// Rewind the consume cursor to the polled-but-unmarked record NOW, on the
	// poll goroutine (this is always called from the poll/process callback,
	// never a timer). Two reasons this must be synchronous, not deferred to the
	// resume timer:
	//  1. franz-go forbids SetOffsets concurrent with PollFetches; the old timer
	//     path called it from a side goroutine (a contract violation).
	//  2. PollRecords already advanced the consume cursor past these records.
	//     Leaving the rewind until the timer fires means that for the whole
	//     yield window the cursor sits ahead of an un-marked record — if a
	//     rebalance revokes the partition (or a later record is marked) in that
	//     window, franz-go commits past the deferred record and it is silently
	//     dropped (never redelivered, no DLT). Resetting here keeps the commit
	//     floor at the deferred offset so at worst the record is reprocessed.
	if rewind && c.Client != nil {
		c.SetOffsets(map[string]map[int32]kgo.EpochOffset{
			topic: {partition: {Offset: offset}},
		})
	}
}

func (c *consumerClient) clearDeferredPartitionPause(topic string, partition int32) {
	if c == nil {
		return
	}
	c.pauseMu.Lock()
	ok := false
	if c.deferredPaused != nil {
		if parts, exists := c.deferredPaused[topic]; exists {
			if _, has := parts[partition]; has {
				ok = true
				delete(parts, partition)
				if len(parts) == 0 {
					delete(c.deferredPaused, topic)
				}
			}
		}
	}
	c.pauseMu.Unlock()
	if !ok {
		return
	}
	// Resume only. The consume cursor was already rewound to the deferred offset
	// synchronously in pauseDeferredPartition (on the poll goroutine), so franz-go
	// re-fetches the deferred record on resume. We must NOT call SetOffsets here —
	// this runs on a timer goroutine and SetOffsets concurrent with PollFetches is
	// unsafe (it was the source of silently dropped low-priority records when a
	// rebalance raced the timer).
	if p := c.pauser(); p != nil {
		partitionDeferPauseMu.Lock()
		p.ResumeFetchPartitions(map[string][]int32{topic: {partition}})
		partitionDeferPauseMu.Unlock()
	}
}

// dropDeferredForRevoked forgets deferred-pause state for partitions revoked in a
// rebalance. Without this, a stale min-offset entry left behind by a revoke would
// make pauseDeferredPartition treat a later (higher-offset) yield on the same
// partition as "not a new minimum" and skip the synchronous rewind — reopening the
// silent-drop hole. Franz-go clears its own fetch-pause for revoked partitions, so
// we only need to clear our bookkeeping.
func (c *consumerClient) dropDeferredForRevoked(revoked map[string][]int32) {
	if c == nil || len(revoked) == 0 {
		return
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.deferredPaused == nil {
		return
	}
	for topic, parts := range revoked {
		pp, ok := c.deferredPaused[topic]
		if !ok {
			continue
		}
		for _, p := range parts {
			delete(pp, p)
		}
		if len(pp) == 0 {
			delete(c.deferredPaused, topic)
		}
	}
}

func (c *consumerClient) pauseEnginePartition(topic string, partition int32) {
	if c == nil {
		return
	}
	p := c.pauser()
	c.pauseMu.Lock()
	if c.enginePaused == nil {
		c.enginePaused = map[string]map[int32]struct{}{}
	}
	if c.enginePaused[topic] == nil {
		c.enginePaused[topic] = map[int32]struct{}{}
	}
	_, already := c.enginePaused[topic][partition]
	if !already {
		c.enginePaused[topic][partition] = struct{}{}
	}
	c.pauseMu.Unlock()
	if already || p == nil {
		return
	}
	partitionDeferPauseMu.Lock()
	p.PauseFetchPartitions(map[string][]int32{topic: {partition}})
	partitionDeferPauseMu.Unlock()
}

func (c *consumerClient) resumeEnginePartition(topic string, partition int32) {
	if c == nil {
		return
	}
	c.pauseMu.Lock()
	if c.enginePaused != nil {
		if parts, ok := c.enginePaused[topic]; ok {
			delete(parts, partition)
			if len(parts) == 0 {
				delete(c.enginePaused, topic)
			}
		}
	}
	c.pauseMu.Unlock()
	if p := c.pauser(); p != nil {
		partitionDeferPauseMu.Lock()
		p.ResumeFetchPartitions(map[string][]int32{topic: {partition}})
		partitionDeferPauseMu.Unlock()
	}
}
