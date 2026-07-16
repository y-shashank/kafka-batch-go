// Package cancellation provides a process-local cache of cancelled batch IDs
// (Ruby KafkaBatch::CancellationCache parity).
//
// Job / schedule paths consult the cache instead of Redis ZSCORE per message.
// The full cancelled index is refreshed at most once per TTL window.
package cancellation

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// FetchIDs loads active cancelled batch IDs from the store.
type FetchIDs func(ctx context.Context) ([]string, error)

// Cache is a TTL snapshot of cancelled batch IDs.
type Cache struct {
	ttl   time.Duration
	fetch FetchIDs
	now   func() time.Time

	mu        sync.Mutex
	ids       map[string]struct{}
	fetchedAt time.Time
	loaded    bool
}

// processCache is set by daemon/worker so Client.CancelBatch can Add optimistically.
var processCache atomic.Pointer[Cache]

// SetProcessCache registers the cache for this process (nil clears).
func SetProcessCache(c *Cache) {
	processCache.Store(c)
}

// AddToProcess optimistically inserts a batch id into the process cache, if set.
func AddToProcess(batchID string) {
	if c := processCache.Load(); c != nil {
		c.Add(batchID)
	}
}

// New builds a cache. ttl <= 0 refreshes on every Cancelled call (tests).
func New(ttl time.Duration, fetch FetchIDs) *Cache {
	return &Cache{
		ttl:   ttl,
		fetch: fetch,
		now:   time.Now,
		ids:   map[string]struct{}{},
	}
}

// Cancelled reports whether batchID is known-cancelled as of the last refresh.
// Signature matches schedule.BatchCancelled for drop-in wiring.
func (c *Cache) Cancelled(ctx context.Context, batchID string) (bool, error) {
	if c == nil || batchID == "" {
		return false, nil
	}
	ids, err := c.currentIDs(ctx)
	if err != nil {
		return false, err
	}
	_, ok := ids[batchID]
	return ok, nil
}

// Add inserts a batch id immediately (e.g. after CancelBatch in this process).
// Preserves the refresh timestamp so a fresh snapshot stays fresh (Ruby #add).
func (c *Cache) Add(batchID string) {
	if c == nil || batchID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make(map[string]struct{}, len(c.ids)+1)
	for id := range c.ids {
		next[id] = struct{}{}
	}
	next[batchID] = struct{}{}
	c.ids = next
	c.loaded = true
}

// Reset drops the snapshot (tests).
func (c *Cache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids = map[string]struct{}{}
	c.fetchedAt = time.Time{}
	c.loaded = false
}

func (c *Cache) currentIDs(ctx context.Context) (map[string]struct{}, error) {
	c.mu.Lock()
	if c.loaded && c.freshLocked() {
		ids := c.ids
		c.mu.Unlock()
		return ids, nil
	}
	c.mu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded && c.freshLocked() {
		return c.ids, nil
	}

	ids, err := c.fetch(ctx)
	if err != nil {
		log.Printf("[kbatch-cancellation] refresh failed: %v – keeping previous set", err)
		if c.loaded {
			return c.ids, nil
		}
		return map[string]struct{}{}, nil
	}
	next := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			next[id] = struct{}{}
		}
	}
	c.ids = next
	c.fetchedAt = c.now()
	c.loaded = true
	return c.ids, nil
}

func (c *Cache) freshLocked() bool {
	if c.ttl <= 0 {
		return false
	}
	return !c.fetchedAt.IsZero() && c.now().Sub(c.fetchedAt) < c.ttl
}
