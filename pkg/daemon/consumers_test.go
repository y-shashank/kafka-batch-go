package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestIsContextErr(t *testing.T) {
	if !isContextErr(context.Canceled) {
		t.Fatal("expected context.Canceled")
	}
	if !isContextErr(context.DeadlineExceeded) {
		t.Fatal("expected context.DeadlineExceeded")
	}
	if isContextErr(errors.New("kafka down")) {
		t.Fatal("unexpected")
	}
}

func TestSafeHandleRecoversPanic(t *testing.T) {
	rec := &kgo.Record{Topic: "t", Partition: 0, Offset: 1}
	err := safeHandle(func(*kgo.Record) error {
		panic("boom")
	}, rec)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSafeHandlePassesThroughError(t *testing.T) {
	rec := &kgo.Record{Topic: "t"}
	want := errors.New("fail")
	err := safeHandle(func(*kgo.Record) error { return want }, rec)
	if !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}

func TestSafeBatchHandleRecoversPanic(t *testing.T) {
	rec := &kgo.Record{Topic: "t", Partition: 0, Offset: 1}
	err := safeBatchHandle(context.Background(), func(context.Context, []*kgo.Record) error {
		panic("boom")
	}, []*kgo.Record{rec})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestConsumerHealthHealthy(t *testing.T) {
	h := NewConsumerHealthTracker(50*time.Millisecond, 20*time.Millisecond)
	h.Register("g1")
	ok, _ := h.Healthy(context.Background())
	if !ok {
		t.Fatal("boot grace should pass")
	}
	h.RecordPoll("g1")
	ok, _ = h.Healthy(context.Background())
	if !ok {
		t.Fatal("fresh poll should pass")
	}
	time.Sleep(60 * time.Millisecond)
	ok, detail := h.Healthy(context.Background())
	if ok {
		t.Fatalf("expected stale, got ok detail=%q", detail)
	}
}

func TestConsumerHealthNeverPolled(t *testing.T) {
	h := NewConsumerHealthTracker(50*time.Millisecond, 10*time.Millisecond)
	h.Register("g1")
	time.Sleep(15 * time.Millisecond)
	ok, _ := h.Healthy(context.Background())
	if ok {
		t.Fatal("expected unhealthy when never polled past boot grace")
	}
}
