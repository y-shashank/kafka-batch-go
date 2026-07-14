package daemon

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
	partResumed  map[string][]int32
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

func (r *recordingFetchPauser) ResumeFetchPartitions(parts map[string][]int32) {
	if r.partResumed == nil {
		r.partResumed = map[string][]int32{}
	}
	for t, ps := range parts {
		r.partResumed[t] = append(r.partResumed[t], ps...)
	}
}

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
	if !cc.anyTopicPaused() {
		t.Fatal("expected anyTopicPaused after pause")
	}

	pause.paused["g\x1fjobs"] = false
	cc.syncConsumptionFetchPause(context.Background(), pause, "g")
	if len(fp.topicResumed) != 1 || fp.topicResumed[0] != "jobs" {
		t.Fatalf("topicResumed=%v", fp.topicResumed)
	}
	if cc.anyTopicPaused() {
		t.Fatal("expected no paused topics after resume")
	}
}

func TestPollWaitCtxBoundsWhenPaused(t *testing.T) {
	cc := &consumerClient{topicPaused: map[string]bool{"jobs": true}}
	parent := context.Background()
	ctx, cancel := cc.pollWaitCtx(parent)
	defer cancel()
	if ctx == parent {
		t.Fatal("expected bounded poll ctx while paused")
	}
	cc.topicPaused["jobs"] = false
	ctx2, cancel2 := cc.pollWaitCtx(parent)
	defer cancel2()
	if ctx2 != parent {
		t.Fatal("expected parent poll ctx when nothing paused")
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

func TestDeferredPauseNotClearedByConsumptionSync(t *testing.T) {
	pause := &mockPauseChecker{paused: map[string]bool{}}
	fp := &recordingFetchPauser{partResumed: map[string][]int32{}}
	cc := &consumerClient{
		topics:         []string{"retry.short"},
		topicPaused:    map[string]bool{},
		partPaused:     map[string][]int32{},
		deferredPaused: map[string]map[int32]int64{},
		pauseOps:       fp,
	}
	cc.initDeferLifecycle()
	cc.pauseDeferredPartition("retry.short", 0, 7)
	if !cc.anyTopicPaused() {
		t.Fatal("expected deferred pause to count as paused")
	}
	cc.syncConsumptionFetchPause(context.Background(), pause, "g")
	if _, ok := cc.deferredPaused["retry.short"][0]; !ok {
		t.Fatal("syncConsumptionFetchPause cleared deferred pause")
	}
	if len(fp.partResumed["retry.short"]) != 0 {
		t.Fatalf("unexpected resume of deferred partition: %v", fp.partResumed)
	}
	cc.clearDeferredPartitionPause("retry.short", 0)
	if cc.anyTopicPaused() {
		t.Fatal("expected deferred pause cleared")
	}
	if len(fp.partResumed["retry.short"]) != 1 || fp.partResumed["retry.short"][0] != 0 {
		t.Fatalf("partResumed=%v", fp.partResumed)
	}
}

func TestEnginePauseCountsForPollWait(t *testing.T) {
	fp := &recordingFetchPauser{}
	cc := &consumerClient{
		topicPaused:  map[string]bool{},
		enginePaused: map[string]map[int32]struct{}{},
		pauseOps:     fp,
	}
	cc.pauseEnginePartition("events", 0)
	if !cc.anyTopicPaused() {
		t.Fatal("expected engine pause to bound pollWaitCtx")
	}
	cc.resumeEnginePartition("events", 0)
	if cc.anyTopicPaused() {
		t.Fatal("expected engine pause cleared")
	}
}

func TestInvalidateDeferredPausesCancelsTimers(t *testing.T) {
	fp := &recordingFetchPauser{partResumed: map[string][]int32{}}
	cc := &consumerClient{
		deferredPaused: map[string]map[int32]int64{},
		pauseOps:       fp,
	}
	cc.initDeferLifecycle()
	rec := &kgo.Record{Topic: "retry.short", Partition: 0, Offset: 3}
	deferClientPartitionPause(cc, rec, 50*time.Millisecond)
	cc.invalidateDeferredPauses()
	time.Sleep(80 * time.Millisecond)
	if len(fp.partResumed["retry.short"]) != 0 {
		t.Fatalf("timer resumed after invalidate: %v", fp.partResumed)
	}
	if len(cc.deferredPaused) != 0 {
		t.Fatalf("deferredPaused=%v", cc.deferredPaused)
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
