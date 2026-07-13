package daemon

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestPollAbortControllerCancelsProcessing(t *testing.T) {
	var abort pollAbortController
	parent := context.Background()
	procCtx, end := abort.begin(parent)
	defer end()

	abort.trigger()
	if procCtx.Err() == nil {
		t.Fatal("expected cancelled processing context")
	}
}

func TestHealthMemberKeySingleMember(t *testing.T) {
	if got := healthMemberKey("g", 1, 1); got != "g" {
		t.Fatalf("got %q", got)
	}
}

func TestHealthMemberKeyMultiMember(t *testing.T) {
	if got := healthMemberKey("g", 3, 10); got != "g#m3" {
		t.Fatalf("got %q", got)
	}
}

func TestMemberLabel(t *testing.T) {
	if got := memberLabel(2, 10); got != "2/10" {
		t.Fatalf("got %q", got)
	}
}

type mockPauseChecker struct {
	paused map[string]bool
}

func (m *mockPauseChecker) Paused(_ context.Context, group, topic string, partition int32) bool {
	if m.paused == nil {
		return false
	}
	if m.paused[group+"\x1f"+topic] {
		return true
	}
	return m.paused[group+"\x1f"+topic+"\x1f"+strconv.FormatInt(int64(partition), 10)]
}

func (m *mockPauseChecker) ActiveHigherTopics(_ context.Context, _ string, higher []string) []string {
	return higher
}

type recordingFetchPauser struct {
	topicPaused  []string
	topicResumed []string
	partPaused   map[string][]int32
}

func (r *recordingFetchPauser) PauseFetchTopics(topics ...string) []string {
	r.topicPaused = append(r.topicPaused, topics...)
	return topics
}

func (r *recordingFetchPauser) ResumeFetchTopics(topics ...string) {
	r.topicResumed = append(r.topicResumed, topics...)
}

func (r *recordingFetchPauser) PauseFetchPartitions(parts map[string][]int32) map[string][]int32 {
	if r.partPaused == nil {
		r.partPaused = map[string][]int32{}
	}
	for t, ps := range parts {
		r.partPaused[t] = append(r.partPaused[t], ps...)
	}
	return parts
}

func (r *recordingFetchPauser) ResumeFetchPartitions(map[string][]int32) {}

func TestSyncConsumptionFetchPauseTopic(t *testing.T) {
	pause := &mockPauseChecker{paused: map[string]bool{"g\x1fjobs": true}}
	fp := &recordingFetchPauser{}
	cc := &consumerClient{
		topics:      []string{"jobs"},
		topicPaused: map[string]bool{},
		partPaused:  map[string][]int32{},
		pauseOps:    fp,
	}

	cc.syncConsumptionFetchPause(context.Background(), pause, "g")
	if len(fp.topicPaused) != 1 || fp.topicPaused[0] != "jobs" {
		t.Fatalf("topicPaused=%v", fp.topicPaused)
	}

	pause.paused["g\x1fjobs"] = false
	cc.syncConsumptionFetchPause(context.Background(), pause, "g")
	if len(fp.topicResumed) != 1 || fp.topicResumed[0] != "jobs" {
		t.Fatalf("topicResumed=%v", fp.topicResumed)
	}
}

func TestPauseConsumptionPartitionTracks(t *testing.T) {
	fp := &recordingFetchPauser{}
	cc := &consumerClient{
		partPaused:  map[string][]int32{},
		topicPaused: map[string]bool{},
		pauseOps:    fp,
	}
	cc.pauseConsumptionPartition("jobs", 2)
	if len(fp.partPaused["jobs"]) != 1 || fp.partPaused["jobs"][0] != 2 {
		t.Fatalf("partPaused=%v", fp.partPaused)
	}
}

func TestDeferPartitionPauseNoPanicWithNil(t *testing.T) {
	deferPartitionPause(nil, nil, time.Millisecond)
}

func TestProcessingContextAbortIsContextErr(t *testing.T) {
	var abort pollAbortController
	procCtx, end := abort.begin(context.Background())
	abort.trigger()
	end()
	if !isContextErr(procCtx.Err()) {
		t.Fatalf("err=%v", procCtx.Err())
	}
	if !errors.Is(procCtx.Err(), context.Canceled) {
		t.Fatalf("err=%v", procCtx.Err())
	}
}

func TestConsumerHealthPerMemberKeys(t *testing.T) {
	h := NewConsumerHealthTracker(50*time.Millisecond, 20*time.Millisecond)
	h.Register("g#m1")
	h.Register("g#m2")
	h.RecordPoll("g#m1")
	ok, _ := h.Healthy(context.Background())
	if !ok {
		t.Fatal("m1 polled should not fail entire health during boot grace for m2")
	}
	time.Sleep(25 * time.Millisecond)
	ok, detail := h.Healthy(context.Background())
	if ok {
		t.Fatalf("m2 never polled should fail health: %s", detail)
	}
}
