package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/schedule"
)

type stubScheduleIndex struct {
	failUntil int
	calls     int
	err       error
}

func (s *stubScheduleIndex) scheduleOne(ctx context.Context, e schedule.ScheduleEntry) error {
	s.calls++
	if s.calls <= s.failUntil {
		return s.err
	}
	return nil
}

func (s *stubScheduleIndex) scheduleMany(ctx context.Context, entries []schedule.ScheduleEntry) error {
	s.calls++
	if s.calls <= s.failUntil {
		return s.err
	}
	return nil
}

func TestWriteScheduleIndexRetryThenOK(t *testing.T) {
	stub := &stubScheduleIndex{failUntil: 2, err: errors.New("transient")}
	c := &Client{
		cfg: Config{
			ScheduleIndexWriteRetries: 3,
			ScheduleIndexWriteBackoff: time.Millisecond,
		},
		sched: stub,
	}
	err := c.writeScheduleIndex(context.Background(), []schedule.ScheduleEntry{
		{JobID: "j1", RunAt: time.Now()},
	}, "b1", "j1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if stub.calls != 3 {
		t.Fatalf("calls=%d", stub.calls)
	}
}

func TestWriteScheduleIndexExhaustsRetries(t *testing.T) {
	stub := &stubScheduleIndex{failUntil: 10, err: errors.New("hard fail")}
	c := &Client{
		cfg: Config{
			ScheduleIndexWriteRetries: 2,
			ScheduleIndexWriteBackoff: 0,
		},
		sched: stub,
	}
	err := c.writeScheduleIndex(context.Background(), []schedule.ScheduleEntry{
		{JobID: "j1"}, {JobID: "j2"},
	}, "b1", "j1", 2)
	pe, ok := err.(*PartialProduceError)
	if !ok {
		t.Fatalf("err=%v", err)
	}
	if pe.ProducedCount != 0 {
		t.Fatalf("produced=%d", pe.ProducedCount)
	}
	if stub.calls != 2 {
		t.Fatalf("calls=%d", stub.calls)
	}
}

func TestWriteScheduleIndexDefaultRetries(t *testing.T) {
	stub := &stubScheduleIndex{failUntil: 0}
	c := &Client{cfg: Config{ScheduleIndexWriteRetries: 0}, sched: stub}
	if err := c.writeScheduleIndex(context.Background(), []schedule.ScheduleEntry{{JobID: "j"}}, "", "j", 1); err != nil {
		t.Fatal(err)
	}
}

func TestProduceInChunksEmpty(t *testing.T) {
	c := &Client{cfg: DefaultConfig()}
	n, err := c.produceInChunks(context.Background(), nil)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestScheduleMessagesEmpty(t *testing.T) {
	c := &Client{cfg: DefaultConfig()}
	if err := c.scheduleMessages(context.Background(), nil, time.Now(), "b"); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSize(t *testing.T) {
	c := &Client{cfg: Config{ProduceChunkSize: 0}}
	if got := c.chunkSize(); got != 500 {
		t.Fatalf("default chunk=%d", got)
	}
	c.cfg.ProduceChunkSize = 12
	if got := c.chunkSize(); got != 12 {
		t.Fatalf("chunk=%d", got)
	}
}
