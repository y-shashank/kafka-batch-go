package daemon

import (
	"context"
	"sync"
)

// pollAbortController cancels in-flight poll processing when franz-go signals
// that a rebalance is blocked (OnPartitionsCallbackBlocked).
type pollAbortController struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (a *pollAbortController) begin(parent context.Context) (context.Context, func()) {
	procCtx, cancel := context.WithCancel(parent)
	a.mu.Lock()
	a.cancel = cancel
	a.mu.Unlock()
	return procCtx, func() {
		cancel()
		a.mu.Lock()
		a.cancel = nil
		a.mu.Unlock()
	}
}

func (a *pollAbortController) trigger() {
	a.mu.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()
}
