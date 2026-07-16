package cancellation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheRefreshesOncePerTTL(t *testing.T) {
	var fetches atomic.Int32
	c := New(time.Minute, func(ctx context.Context) ([]string, error) {
		fetches.Add(1)
		return []string{"b1"}, nil
	})
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }

	for i := 0; i < 5; i++ {
		ok, err := c.Cancelled(context.Background(), "b1")
		if err != nil || !ok {
			t.Fatalf("cancelled b1: ok=%v err=%v", ok, err)
		}
	}
	if fetches.Load() != 1 {
		t.Fatalf("fetches=%d want 1", fetches.Load())
	}

	ok, err := c.Cancelled(context.Background(), "b2")
	if err != nil || ok {
		t.Fatalf("b2 should be false: ok=%v err=%v", ok, err)
	}
	if fetches.Load() != 1 {
		t.Fatalf("fetches=%d want still 1", fetches.Load())
	}

	now = now.Add(2 * time.Minute)
	_, _ = c.Cancelled(context.Background(), "b1")
	if fetches.Load() != 2 {
		t.Fatalf("fetches=%d want 2 after TTL", fetches.Load())
	}
}

func TestCacheAddPreservesFreshSnapshot(t *testing.T) {
	var fetches atomic.Int32
	c := New(time.Minute, func(ctx context.Context) ([]string, error) {
		fetches.Add(1)
		return []string{"b1"}, nil
	})
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }

	_, _ = c.Cancelled(context.Background(), "b1")
	c.Add("b-new")
	ok, err := c.Cancelled(context.Background(), "b-new")
	if err != nil || !ok {
		t.Fatalf("add should be visible: ok=%v err=%v", ok, err)
	}
	if fetches.Load() != 1 {
		t.Fatalf("Add must not force refresh: fetches=%d", fetches.Load())
	}
}

func TestCacheRefreshFailureKeepsPrevious(t *testing.T) {
	var fetches atomic.Int32
	c := New(0, func(ctx context.Context) ([]string, error) {
		n := fetches.Add(1)
		if n == 1 {
			return []string{"b1"}, nil
		}
		return nil, context.DeadlineExceeded
	})
	ok, err := c.Cancelled(context.Background(), "b1")
	if err != nil || !ok {
		t.Fatalf("first: ok=%v err=%v", ok, err)
	}
	ok, err = c.Cancelled(context.Background(), "b1")
	if err != nil || !ok {
		t.Fatalf("after failed refresh should keep b1: ok=%v err=%v", ok, err)
	}
}
