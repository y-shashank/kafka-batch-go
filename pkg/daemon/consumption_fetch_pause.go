package daemon

import (
	"context"
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

func (c *consumerClient) pauseConsumptionPartition(topic string, partition int32) {
	p := c.pauser()
	if p == nil {
		return
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	for _, p := range c.partPaused[topic] {
		if p == partition {
			return
		}
	}
	c.partPaused[topic] = append(c.partPaused[topic], partition)
	p.PauseFetchPartitions(map[string][]int32{topic: {partition}})
}
