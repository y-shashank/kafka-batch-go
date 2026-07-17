package instrument

import (
	"log"
	"sync"
)

// Handler receives lifecycle events from the Go control plane and worker.
// Event names mirror Ruby ActiveSupport::Notifications (without .kafka_batch suffix).
// Consumer-side: job.processed, job.retried, job.failed, job.cancelled, job.expired,
// job.emit_retried, job.uniq_skipped, batch.completed, callback.invoked, callback.failed,
// dlt.published, scheduled.dispatched, consumer.priority_yielded
// Produce-side (Go client, planned): batch.created, batch.sealed, scheduled.enqueued,
// scheduled.enqueued_bulk, scheduled.index_failed
// Reconciler (planned): reconciler.ran
// SuperFetch / workset: workset.reclaimed (orphan-reclaim sweep), super_fetch.drained
// (graceful shutdown drain) — Ruby parity: KafkaBatch::Instrumentation.
//
// Multiple independent subscribers can coexist (e.g. metrics.Install and a
// perf-writer both listening at once) via AddHandler. SetHandler remains for
// tests / single-sink installs but REPLACES every handler registered via
// either SetHandler or AddHandler — do not mix the two in the same process
// unless that replace-all semantics is what you want.
type handlerFunc = func(event string, payload map[string]interface{}, durationMs float64)

var (
	mu       sync.RWMutex
	handlers = map[int]handlerFunc{}
	nextID   int
)

// SetHandler installs a process-wide instrumentation callback, replacing any
// handlers previously registered via SetHandler or AddHandler. Pass nil to
// clear all handlers. Prefer AddHandler when multiple independent
// subscribers need to coexist.
func SetHandler(fn handlerFunc) {
	mu.Lock()
	defer mu.Unlock()
	handlers = map[int]handlerFunc{}
	if fn != nil {
		nextID++
		handlers[nextID] = fn
	}
}

// AddHandler registers an additional instrumentation callback without
// disturbing handlers registered by other callers. Returns a func that
// unregisters just this handler; safe to call more than once.
func AddHandler(fn handlerFunc) (remove func()) {
	if fn == nil {
		return func() {}
	}
	mu.Lock()
	nextID++
	id := nextID
	handlers[id] = fn
	mu.Unlock()

	var removed bool
	var removeMu sync.Mutex
	return func() {
		removeMu.Lock()
		defer removeMu.Unlock()
		if removed {
			return
		}
		removed = true
		mu.Lock()
		delete(handlers, id)
		mu.Unlock()
	}
}

// Emit publishes one instrumentation event. Always logs at debug; every
// registered handler receives a copy. A panicking handler is recovered and
// logged so it can never take down the caller or block other handlers.
func Emit(event string, payload map[string]interface{}, durationMs float64) {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	log.Printf("[kbatch-instrument] event=%s duration_ms=%.2f payload=%v", event, durationMs, payload)

	mu.RLock()
	fns := make([]handlerFunc, 0, len(handlers))
	for _, fn := range handlers {
		fns = append(fns, fn)
	}
	mu.RUnlock()

	for _, fn := range fns {
		invoke(fn, event, payload, durationMs)
	}
}

func invoke(fn handlerFunc, event string, payload map[string]interface{}, durationMs float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch-instrument] handler panic event=%s: %v", event, r)
		}
	}()
	fn(event, payload, durationMs)
}
