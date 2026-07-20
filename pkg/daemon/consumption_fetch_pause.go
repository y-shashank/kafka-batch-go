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

	if c.killswitchWant == nil {
		c.killswitchWant = map[string]bool{}
	}
	for _, topic := range c.topics {
		c.killswitchWant[topic] = pauseCtl.Paused(ctx, group, topic, 0)
		c.applyTopicPauseLocked(topic)
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

// pausedPollWait bounds the poll while a partition/topic is fetch-paused so the
// loop re-syncs the killswitch and notices async deferred resumes promptly.
const pausedPollWait = 500 * time.Millisecond

// idlePollWait bounds the poll when nothing is fetch-paused. franz-go's
// PollFetches/PollRecords blocks until a record is available (an empty broker
// fetch response does NOT return the poll) — so an idle-but-assigned consumer
// would sit inside a single poll call indefinitely, never reaching the loop's
// touch() calls, and the stall watchdog (defaultConsumerStallTimeout) would
// force-close the client and trigger a needless reconnect + group rebalance.
// That rebalance thrash was dropping/​delaying control events (e.g. a batch's
// terminal success/failed event landing while the events consumer was mid-
// reconnect), leaving batches stuck below total. Bounding the idle poll keeps
// the loop cycling well under the stall timeout so the watchdog only fires on a
// genuinely wedged poll. A record still returns the poll immediately, so this
// does not add latency to live traffic.
const idlePollWait = 5 * time.Second

// pollWaitCtx bounds PollRecords/PollFetches so the loop keeps making progress:
// a short bound while paused (re-sync the killswitch / notice deferred resumes)
// and a longer bound while idle (keep touching the stall watchdog). It never
// returns an unbounded context — an unbounded idle poll trips a false stall.
func (c *consumerClient) pollWaitCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if !c.anyTopicPaused() {
		return context.WithTimeout(parent, idlePollWait)
	}
	return context.WithTimeout(parent, pausedPollWait)
}

// applyTopicPauseLocked reconciles the franz-go fetch-pause for a topic to the OR
// of its two independent pause intents (killswitch + priority). Caller holds
// pauseMu. topicPaused tracks the currently-applied franz-go state.
func (c *consumerClient) applyTopicPauseLocked(topic string) {
	p := c.pauser()
	if p == nil {
		return
	}
	want := c.killswitchWant[topic] || c.priorityWant[topic]
	if want == c.topicPaused[topic] {
		return
	}
	if want {
		p.PauseFetchTopics(topic)
	} else {
		p.ResumeFetchTopics(topic)
	}
	c.topicPaused[topic] = want
}

// setPriorityTopicPause records the priority-yield pause intent for a topic and
// reconciles the franz-go fetch-pause. Returns true when the applied state
// changed. Safe to call from the poll goroutine's pre-poll hook. Proactively
// pausing (instead of polling then rewinding) is what makes priority yielding
// stall/rebalance-safe: a never-fetched record can't have its offset committed
// past it, so it is always redelivered.
func (c *consumerClient) setPriorityTopicPause(topic string, want bool) bool {
	if c == nil {
		return false
	}
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.priorityWant == nil {
		c.priorityWant = map[string]bool{}
	}
	before := c.topicPaused[topic]
	c.priorityWant[topic] = want
	c.applyTopicPauseLocked(topic)
	return c.topicPaused[topic] != before
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
