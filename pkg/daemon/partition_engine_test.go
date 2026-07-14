package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeMarker struct {
	mu       sync.Mutex
	marked   []*kgo.Record
	markedCh chan struct{}
}

func (m *fakeMarker) MarkCommitRecords(recs ...*kgo.Record) {
	m.mu.Lock()
	m.marked = append(m.marked, recs...)
	m.mu.Unlock()
	if m.markedCh != nil {
		m.markedCh <- struct{}{}
	}
}

func (m *fakeMarker) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.marked)
}

type fakePauser struct {
	mu      sync.Mutex
	paused  [][]int32
	resumed [][]int32
}

func (p *fakePauser) PauseFetchPartitions(m map[string][]int32) map[string][]int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, parts := range m {
		p.paused = append(p.paused, append([]int32(nil), parts...))
	}
	return nil
}

func (p *fakePauser) ResumeFetchPartitions(m map[string][]int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, parts := range m {
		p.resumed = append(p.resumed, append([]int32(nil), parts...))
	}
}

func (p *fakePauser) PauseFetchTopics(...string) []string { return nil }
func (p *fakePauser) ResumeFetchTopics(...string)         {}

func (p *fakePauser) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.paused), len(p.resumed)
}

type fakeCommitter struct {
	mu    sync.Mutex
	calls int
}

func (c *fakeCommitter) CommitMarkedOffsets(context.Context) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return nil
}

func (c *fakeCommitter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newTestEngine(handle PartitionBatchHandler) (*partitionEngine, *fakeMarker, *fakePauser, *fakeCommitter) {
	m := &fakeMarker{markedCh: make(chan struct{}, 8)}
	p := &fakePauser{}
	c := &fakeCommitter{}
	e := &partitionEngine{
		cfg:       partitionConsumerConfig{group: "g", handle: handle, chanBuffer: 1},
		workers:   map[partitionKey]*partitionWorker{},
		markOps:   m,
		pauseOps:  p,
		commitOps: c,
	}
	e.setProcCtx(context.Background())
	return e, m, p, c
}

func ftp(topic string, partition int32, offsets ...int64) kgo.FetchTopicPartition {
	f := kgo.FetchTopicPartition{Topic: topic}
	f.Partition = partition
	for _, off := range offsets {
		f.Records = append(f.Records, &kgo.Record{Topic: topic, Partition: partition, Offset: off})
	}
	return f
}

// A successfully handled partition batch is marked committed in full.
func TestPartitionWorkerMarksOnSuccess(t *testing.T) {
	e, m, _, _ := newTestEngine(func(context.Context, []*kgo.Record) error { return nil })
	e.assigned(context.Background(), nil, map[string][]int32{"t": {0}})
	defer e.drainAllWorkers()

	w := e.workerFor("t", 0)
	if w == nil {
		t.Fatal("worker not assigned")
	}
	w.recs <- ftp("t", 0, 10, 11, 12)

	select {
	case <-m.markedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("records never marked")
	}
	if got := m.count(); got != 3 {
		t.Fatalf("marked=%d want 3", got)
	}
}

// Regression: the poll loop must not cancel procCtx as soon as route returns.
// Workers process asynchronously; a cancelled context made event handling fail
// and left batches stuck (e2e BatchCompletion / EventsTopicJobSuccess).
func TestPartitionWorkerSeesAliveProcCtx(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	e, m, _, _ := newTestEngine(func(ctx context.Context, _ []*kgo.Record) error {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
	e.assigned(context.Background(), nil, map[string][]int32{"t": {0}})
	defer e.drainAllWorkers()

	// Simulate a long-lived processing context (what poll now keeps across route).
	procCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.setProcCtx(procCtx)

	w := e.workerFor("t", 0)
	w.recs <- ftp("t", 0, 1)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	// Cancelling only after the worker has the live ctx (rebalance abort) should
	// still be possible; here we leave it alive through completion.
	close(release)

	select {
	case <-m.markedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("records never marked with surviving procCtx")
	}
}

// A handler error must leave the batch unmarked so it redelivers (at-least-once).
func TestPartitionWorkerNoMarkOnError(t *testing.T) {
	e, m, _, _ := newTestEngine(func(context.Context, []*kgo.Record) error { return errors.New("boom") })
	e.assigned(context.Background(), nil, map[string][]int32{"t": {0}})
	defer e.drainAllWorkers()

	w := e.workerFor("t", 0)
	w.recs <- ftp("t", 0, 10)

	// Give the worker time to run the handler and (not) mark.
	time.Sleep(200 * time.Millisecond)
	if got := m.count(); got != 0 {
		t.Fatalf("marked=%d want 0 on handler error", got)
	}
}

// When a worker's channel is full, deliver pauses that partition and resumes it once
// the worker accepts the batch — per-partition backpressure with no cross-partition block.
func TestDeliverBackpressurePausesAndResumes(t *testing.T) {
	e, _, p, _ := newTestEngine(func(context.Context, []*kgo.Record) error { return nil })
	// A worker we never start, so its buffered channel (cap 1) fills up.
	w := &partitionWorker{topic: "t", partition: 0, recs: make(chan kgo.FetchTopicPartition, 1), quit: make(chan struct{}), done: make(chan struct{})}

	e.deliver(context.Background(), w, ftp("t", 0, 1)) // fills the buffer, no pause
	if paused, _ := p.counts(); paused != 0 {
		t.Fatalf("unexpected pause before buffer full: %d", paused)
	}

	e.deliver(context.Background(), w, ftp("t", 0, 2)) // buffer full → pause + async handoff
	if paused, _ := p.counts(); paused != 1 {
		t.Fatalf("expected 1 pause on full buffer, got %d", paused)
	}

	<-w.recs // drain one, freeing a slot so the async handoff completes
	deadline := time.After(2 * time.Second)
	for {
		if _, resumed := p.counts(); resumed == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("partition never resumed after worker drained")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Revoking partitions stops their workers and commits marked offsets before releasing —
// the critical franz-go gotcha when supplying a custom OnPartitionsRevoked.
func TestRevokedStopsWorkersAndCommits(t *testing.T) {
	e, _, _, c := newTestEngine(func(context.Context, []*kgo.Record) error { return nil })
	e.assigned(context.Background(), nil, map[string][]int32{"t": {0, 1}})
	if e.workerFor("t", 0) == nil || e.workerFor("t", 1) == nil {
		t.Fatal("workers not assigned")
	}

	e.revoked(context.Background(), nil, map[string][]int32{"t": {0, 1}})

	if e.workerFor("t", 0) != nil || e.workerFor("t", 1) != nil {
		t.Fatal("workers not removed on revoke")
	}
	if c.count() != 1 {
		t.Fatalf("CommitMarkedOffsets calls=%d want 1", c.count())
	}
}
