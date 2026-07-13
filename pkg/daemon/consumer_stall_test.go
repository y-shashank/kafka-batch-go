package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStallWatchdogCancelsOnInactivity(t *testing.T) {
	ctx, touch, stop := startStallWatchdog(context.Background(), 50*time.Millisecond)
	defer stop()

	touch()
	time.Sleep(120 * time.Millisecond)

	if err := consumerLoopDoneErr(ctx); !errors.Is(err, errConsumerStalled) {
		t.Fatalf("err=%v want %v", err, errConsumerStalled)
	}
}

func TestStallWatchdogStaysAliveWithActivity(t *testing.T) {
	ctx, touch, stop := startStallWatchdog(context.Background(), 200*time.Millisecond)
	defer stop()

	for range 5 {
		touch()
		time.Sleep(40 * time.Millisecond)
	}
	if err := consumerLoopDoneErr(ctx); err != nil {
		t.Fatalf("unexpected err=%v", err)
	}
}
