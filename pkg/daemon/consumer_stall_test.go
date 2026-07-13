package daemon

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStallWatchdogCancelsOnInactivity(t *testing.T) {
	ctx, touch, stop := attachConsumerStallGuardFor(context.Background(), nil, "test", 50*time.Millisecond)
	defer stop()

	touch()
	time.Sleep(120 * time.Millisecond)

	if !errors.Is(context.Cause(ctx), errConsumerStalled) {
		t.Fatalf("cause=%v want %v", context.Cause(ctx), errConsumerStalled)
	}
}

func TestStallWatchdogStaysAliveWithActivity(t *testing.T) {
	ctx, touch, stop := attachConsumerStallGuardFor(context.Background(), nil, "test", 200*time.Millisecond)
	defer stop()

	for range 5 {
		touch()
		time.Sleep(40 * time.Millisecond)
	}
	if err := consumerLoopDoneErr(ctx); err != nil {
		t.Fatalf("unexpected err=%v", err)
	}
}
