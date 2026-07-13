package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type retryCommitClient struct {
	committed []*kgo.Record
}

func (c *retryCommitClient) MarkCommitRecords(recs ...*kgo.Record) {
	c.committed = append(c.committed, recs...)
}

func TestProcessOneRetryRecordPausesWithoutCommit(t *testing.T) {
	cl := &retryCommitClient{}
	rec := &kgo.Record{Topic: "retry.short", Partition: 2, Offset: 10}
	handle := func(*kgo.Record) error {
		return &retryPausedError{duration: time.Millisecond}
	}
	// nil pause client skips PauseFetchPartitions; commit behavior is what we assert.
	processOneRetryRecord(context.Background(), cl, nil, handle, rec, "kafka-batch-retry")
	if len(cl.committed) != 0 {
		t.Fatalf("committed=%v", cl.committed)
	}
}

func TestProcessOneRetryRecordCommitsOnDispatch(t *testing.T) {
	cl := &retryCommitClient{}
	rec := &kgo.Record{Topic: "retry.short", Partition: 2, Offset: 10}
	handle := func(*kgo.Record) error { return nil }
	processOneRetryRecord(context.Background(), cl, nil, handle, rec, "kafka-batch-retry")
	if len(cl.committed) != 1 || cl.committed[0].Offset != 10 {
		t.Fatalf("committed=%v", cl.committed)
	}
}

func TestProcessOneRetryRecordDoesNotCommitOnHandlerError(t *testing.T) {
	cl := &retryCommitClient{}
	rec := &kgo.Record{Topic: "retry.short", Partition: 2, Offset: 10}
	handle := func(*kgo.Record) error { return errors.New("produce failed") }
	processOneRetryRecord(context.Background(), cl, nil, handle, rec, "kafka-batch-retry")
	if len(cl.committed) != 0 {
		t.Fatalf("committed=%v", cl.committed)
	}
}
