package daemon

import (
	"context"
	"time"
)

// syncConsumptionFetchPause mirrors Redis/MySQL consumption killswitch state onto
// franz-go PauseFetchTopics/ResumeFetchTopics so paused topics are not fetched
// and offsets are not advanced (Ruby ConsumptionGate parity at topic level).
func (c *consumerClient) syncConsumptionFetchPause(ctx context.Context, pauseCtl pauseChecker, group string) {
	if c == nil || pauseCtl == nil {
		return
	}
	p := c.pauser()
	if p == nil {
		return
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()

	for _, topic := range c.topics {
		wantPause := pauseCtl.Paused(ctx, group, topic, 0)
		hadPause := c.topicPaused[topic]
		switch {
		case wantPause && !hadPause:
			p.PauseFetchTopics(topic)
			c.topicPaused[topic] = true
		case !wantPause && hadPause:
			p.ResumeFetchTopics(topic)
			c.topicPaused[topic] = false
		}
	}

	// Only sync killswitch partition pauses. Deferred retry/yield pauses live in
	// deferredPaused and are resumed by their own timers — clearing them here would
	// resume a not-yet-due retry record without SetOffsets and strand it.
	for topic, parts := range c.partPaused {
		still := make([]int32, 0, len(parts))
		for _, part := range parts {
			if pauseCtl.Paused(ctx, group, topic, part) {
				still = append(still, part)
			} else {
				p.ResumeFetchPartitions(map[string][]int32{topic: {part}})
			}
		}
		if len(still) == 0 {
			delete(c.partPaused, topic)
		} else {
			c.partPaused[topic] = still
		}
	}
}

// anyTopicPaused reports whether this client currently has any consume topic
// marked paused via PauseFetchTopics / PauseFetchPartitions (killswitch or deferred).
// When every fetchable partition is paused, PollRecords can block indefinitely — so
// the poll loop must use a bounded wait (see pollWaitCtx) or it will never re-enter
// syncConsumptionFetchPause / notice an async deferred resume.
func (c *consumerClient) anyTopicPaused() bool {
	if c == nil {
		return false
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	for _, paused := range c.topicPaused {
		if paused {
			return true
		}
	}
	return len(c.partPaused) > 0 || len(c.deferredPaused) > 0 || len(c.enginePaused) > 0
}

// pollWaitCtx bounds PollRecords while topics/partitions are fetch-paused so the
// loop can re-sync the killswitch. When nothing is paused, returns parent as-is.
func (c *consumerClient) pollWaitCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if !c.anyTopicPaused() {
		return parent, func() {}
	}
	return context.WithTimeout(parent, 500*time.Millisecond)
}

func (c *consumerClient) pauseConsumptionPartition(topic string, partition int32) {
	p := c.pauser()
	if p == nil {
		return
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	for _, part := range c.partPaused[topic] {
		if part == partition {
			return
		}
	}
	c.partPaused[topic] = append(c.partPaused[topic], partition)
	p.PauseFetchPartitions(map[string][]int32{topic: {partition}})
}
