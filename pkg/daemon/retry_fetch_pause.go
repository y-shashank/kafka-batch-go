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
	if existing, ok := c.deferredPaused[topic][partition]; !ok || offset < existing {
		c.deferredPaused[topic][partition] = offset
	}
	c.pauseMu.Unlock()
	if p != nil {
		partitionDeferPauseMu.Lock()
		p.PauseFetchPartitions(map[string][]int32{topic: {partition}})
		partitionDeferPauseMu.Unlock()
	}
}

func (c *consumerClient) clearDeferredPartitionPause(topic string, partition int32) {
	if c == nil {
		return
	}
	c.pauseMu.Lock()
	offset, ok := int64(0), false
	if c.deferredPaused != nil {
		if parts, exists := c.deferredPaused[topic]; exists {
			offset, ok = parts[partition]
			if ok {
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
	// Rewind past the Polled-but-unmarked record so Resume redelivers it. Without
	// SetOffsets, franz-go resumes fetching at the next offset and the message stalls
	// until a revoke/restart.
	if c.Client != nil {
		c.SetOffsets(map[string]map[int32]kgo.EpochOffset{
			topic: {partition: {Offset: offset}},
		})
	}
	if p := c.pauser(); p != nil {
		partitionDeferPauseMu.Lock()
		p.ResumeFetchPartitions(map[string][]int32{topic: {partition}})
		partitionDeferPauseMu.Unlock()
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
