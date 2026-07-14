package daemon

import (
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
func deferClientPartitionPause(cl *consumerClient, rec *kgo.Record, wait time.Duration) {
	if cl == nil || rec == nil || wait <= 0 {
		return
	}
	topic := rec.Topic
	partition := rec.Partition
	cl.pauseDeferredPartition(topic, partition, rec.Offset)

	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		<-timer.C
		cl.clearDeferredPartitionPause(topic, partition)
	}()
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
