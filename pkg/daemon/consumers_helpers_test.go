package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
)

type lagGateReader struct{ hasLag bool }

func (l lagGateReader) GroupHasLag(context.Context, string, []string) (bool, error) {
	return l.hasLag, nil
}

func TestFilterPriorityRecords(t *testing.T) {
	fp := &recordingFetchPauser{}
	cc := &consumerClient{
		partPaused:  map[string][]int32{},
		topicPaused: map[string]bool{},
		pauseOps:    fp,
	}
	pause := &mockPauseChecker{paused: map[string]bool{
		"g\x1fp1\x1f2": true,
	}}
	specByTopic := map[string]priority.TopicSpec{
		"p0": {Topic: "p0"},
		"p1": {Topic: "p1"},
	}
	recs := []*kgo.Record{
		{Topic: "unknown", Partition: 0},
		{Topic: "p1", Partition: 2},
		{Topic: "p0", Partition: 0},
	}
	pc := priority.Config{ConsumerGroup: "g"}
	got := filterPriorityRecords(context.Background(), cc, pc, pause, nil, specByTopic, recs)
	if len(got) != 1 || got[0].Topic != "p0" {
		t.Fatalf("got=%v", got)
	}
	if len(fp.partPaused["p1"]) != 1 || fp.partPaused["p1"][0] != 2 {
		t.Fatalf("partPaused=%v", fp.partPaused)
	}
}

func TestPriorityPrePollHook(t *testing.T) {
	fp := &recordingFetchPauser{}
	cc := &consumerClient{
		topics:      []string{"p1"},
		topicPaused: map[string]bool{},
		partPaused:  map[string][]int32{},
		pauseOps:    fp,
	}
	gate := priority.NewGate(lagGateReader{hasLag: true}, time.Nanosecond)
	specByTopic := map[string]priority.TopicSpec{
		"p1": {
			Topic: "p1", Rank: 1, Mode: priority.ModeStrict,
			HigherTopics: []string{"p0"}, ConsumerGroup: "g",
		},
		"p0": {Topic: "p0", Rank: 0},
	}
	ticks := map[string]int{}
	hook := priorityPrePollHook(cc, gate, specByTopic, ticks, 10*time.Millisecond)
	hook(context.Background())
	if !cc.topicPaused["p1"] {
		t.Fatal("expected priority pause")
	}

	// Weighted interleave lets through every N cycles.
	cc2 := &consumerClient{
		topics:      []string{"p1"},
		topicPaused: map[string]bool{},
		partPaused:  map[string][]int32{},
		pauseOps:    &recordingFetchPauser{},
	}
	specByTopic["p1"] = priority.TopicSpec{
		Topic: "p1", Rank: 1, Mode: priority.ModeWeighted,
		HigherTopics: []string{"p0"}, ConsumerGroup: "g", WeightedInterleave: 2,
	}
	ticks = map[string]int{}
	hook = priorityPrePollHook(cc2, gate, specByTopic, ticks, time.Millisecond)
	hook(context.Background()) // tick 1 → pause
	if !cc2.topicPaused["p1"] {
		t.Fatal("weighted cycle 1 should pause")
	}
	hook(context.Background()) // tick 2 → allow
	if cc2.topicPaused["p1"] {
		t.Fatal("weighted cycle 2 should resume")
	}
}

func TestRunSuperFetchConsumerGroupMembersGuards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunSuperFetchConsumerGroupMembers(ctx, nil, 0, nil, "g", nil, config.ConsumerFetchSettings{}, nil, nil, nil, nil)
	RunSuperFetchConsumerGroupMembers(ctx, nil, 1, nil, "g", []string{"jobs"}, config.ConsumerFetchSettings{},
		func(string) *SuperFetchExecutor { return nil }, nil, nil, nil)
}

func TestPauserNilConsumerClient(t *testing.T) {
	var cc *consumerClient
	if cc.pauser() != nil {
		t.Fatal("nil client pauser")
	}
	fp := &recordingFetchPauser{}
	cc = &consumerClient{pauseOps: fp}
	if cc.pauser() != fp {
		t.Fatal("pauseOps")
	}
}
