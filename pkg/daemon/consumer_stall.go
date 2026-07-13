package daemon

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// errConsumerStalled is returned when a consumer loop makes no progress for too long.
var errConsumerStalled = errors.New("consumer stalled")

// consumerStallTimeout is how long a retry/events consumer may go without loop
// progress before the watchdog cancels the client and supervised restart runs.
const consumerStallTimeout = 90 * time.Second

// startStallWatchdog returns a child context and touch callback. Call touch() on
// every consumer poll iteration. If touch is not called within stall, the child
// context is cancelled with errConsumerStalled so the loop can reconnect.
func startStallWatchdog(parent context.Context, stall time.Duration) (context.Context, func(), func()) {
	ctx, cancel := context.WithCancelCause(parent)
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	touch := func() {
		last.Store(time.Now().UnixNano())
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := stall / 6
		if tick < 100*time.Millisecond {
			tick = 100 * time.Millisecond
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(time.Unix(0, last.Load())) > stall {
					cancel(errConsumerStalled)
					return
				}
			}
		}
	}()
	stop := func() {
		cancel(context.Canceled)
		<-done
	}
	return ctx, touch, stop
}

func consumerLoopDoneErr(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	if errors.Is(context.Cause(ctx), errConsumerStalled) {
		return errConsumerStalled
	}
	if isContextErr(ctx.Err()) {
		return nil
	}
	return ctx.Err()
}
