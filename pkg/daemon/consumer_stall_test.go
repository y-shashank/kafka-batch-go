package daemon

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
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

func TestRunWithStallHeartbeatReturnsError(t *testing.T) {
	want := errors.New("boom")
	err := runWithStallHeartbeat(func() {}, 90*time.Second, func() error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want %v", err, want)
	}
}

func TestRunWithStallHeartbeatTouchesDuringWork(t *testing.T) {
	prev := consumerStallTimeoutSetting
	consumerStallTimeoutSetting = 300 * time.Millisecond
	t.Cleanup(func() { consumerStallTimeoutSetting = prev })

	var touches int
	touch := func() { touches++ }
	err := runWithStallHeartbeat(touch, 0, func() error {
		time.Sleep(220 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err=%v", err)
	}
	if touches < 2 {
		t.Fatalf("touches=%d want >= 2", touches)
	}
}

func TestRunWithStallHeartbeatRunsFnOnCallingGoroutine(t *testing.T) {
	var callerID int64
	var fnID int64
	callerID = int64(currentGoroutineID())
	err := runWithStallHeartbeat(nil, 90*time.Second, func() error {
		fnID = int64(currentGoroutineID())
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err=%v", err)
	}
	if callerID != fnID {
		t.Fatalf("fn ran on goroutine %d, caller was %d", fnID, callerID)
	}
}

func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.ParseUint(idField, 10, 64)
	return id
}
