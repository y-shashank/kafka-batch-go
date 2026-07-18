package cron

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func getenv(k string) string { return os.Getenv(k) }

func testCtx() context.Context { return context.Background() }

// setLastFire back-dates last_fire_at directly (tests only).
func setLastFire(t *testing.T, s *Store, id int64, when time.Time) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE kafka_batch_recurring_schedules SET last_fire_at = ? WHERE id = ?`, when.UTC(), id); err != nil {
		t.Fatalf("setLastFire: %v", err)
	}
}

// eventCapture records emitted instrumentation events for assertions.
type eventCapture struct {
	mu      sync.Mutex
	events  []capturedEvent
	removed func()
}

type capturedEvent struct {
	name    string
	payload map[string]interface{}
}

func captureEvents() *eventCapture {
	c := &eventCapture{}
	c.removed = instrument.AddHandler(func(name string, payload map[string]interface{}, _ float64) {
		c.mu.Lock()
		c.events = append(c.events, capturedEvent{name: name, payload: payload})
		c.mu.Unlock()
	})
	return c
}

func (c *eventCapture) stop() {
	if c.removed != nil {
		c.removed()
	}
}

// count returns how many captured events match name and, when key != "", have
// payload[key] == val.
func (c *eventCapture) count(name, key, val string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.events {
		if e.name != name {
			continue
		}
		if key == "" {
			n++
			continue
		}
		if s, _ := e.payload[key].(string); s == val {
			n++
		}
	}
	return n
}

func (c *eventCapture) names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.events))
	for _, e := range c.events {
		out = append(out, e.name)
	}
	return out
}
