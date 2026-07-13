package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// errConsumerStalled is returned when a consumer loop makes no progress for too long.
var errConsumerStalled = errors.New("consumer stalled")

// consumerStallTimeout is how long a consumer may go without loop progress before
// the watchdog force-closes the franz-go client and the supervised loop reconnects.
const consumerStallTimeout = 90 * time.Second

type rebalanceCloser interface {
	AllowRebalance()
	CloseAllowingRebalance()
}

// attachConsumerStallGuard returns a child context and touch callback. Call touch()
// after each poll cycle. On stall it cancels the context and calls
// CloseAllowingRebalance from a side goroutine — required because franz-go's
// BlockRebalanceOnPoll can deadlock on ctx cancel or Close() alone.
func attachConsumerStallGuard(parent context.Context, cl rebalanceCloser, label string) (context.Context, func(), func()) {
	return attachConsumerStallGuardFor(parent, cl, label, consumerStallTimeout)
}

func attachConsumerStallGuardFor(parent context.Context, cl rebalanceCloser, label string, stall time.Duration) (context.Context, func(), func()) {
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
				if time.Since(time.Unix(0, last.Load())) <= stall {
					continue
				}
				log.Printf("[kbatch-daemon] %s stalled — forcing reconnect", label)
				cancel(errConsumerStalled)
				if cl != nil {
					cl.AllowRebalance()
					cl.CloseAllowingRebalance()
				}
				return
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

func stalledRestartErr(group string) error {
	return fmt.Errorf("consumer group=%s stalled — restarting client", group)
}

func closeGroupConsumer(cl rebalanceCloser) {
	if cl == nil {
		return
	}
	cl.CloseAllowingRebalance()
}

func checkFetchErrs(ctx context.Context, cl rebalanceCloser, fetches kgo.Fetches, group string) error {
	for _, e := range fetches.Errors() {
		if e.Err == nil {
			continue
		}
		if isContextErr(e.Err) {
			releasePollGate(cl)
			if errors.Is(context.Cause(ctx), errConsumerStalled) {
				return stalledRestartErr(group)
			}
			return nil
		}
		return fmt.Errorf("poll group=%s topic=%s: %w", group, e.Topic, e.Err)
	}
	return nil
}

func releasePollGate(cl rebalanceCloser) {
	if cl != nil {
		cl.AllowRebalance()
	}
}
