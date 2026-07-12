package fairness

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Regression for Finding #2: Forwarder.handleExpired releases the lease
// (Complete) and removes the forwarding entry (ConfirmForward) BEFORE the
// completion event is durably emitted via OnExpired, and it swallows the
// OnExpired error (forwarder.go:59-76).
//
// The job payload was already LPOP'd from Redis during Checkout. So once
// ConfirmForward deletes the forwarding entry, ListStaleForwards can no longer
// recover it. If OnExpired's produce fails transiently (Kafka blip), the job is
// gone from Redis, no completion event was written, and the batch is stuck.
//
// This mirrors the happy-path discipline in forwardJob (produce, THEN confirm):
// the forwarding entry must survive an OnExpired failure so stale-forward
// recovery can retry the drop.
//
// EXPECTED TODAY: FAILS (forwarding hash is empty — entry already removed).
func TestForwarderExpiredDropFailureDoesNotStrandJob(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	sched := NewScheduler(rdb, Settings{
		Lane:              LaneTime,
		GlobalConcurrency: 10,
		ReadyWindow:       100,
		LeaseTTL:          60,
		DefaultWeight:     1,
	})

	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": "j-exp", "tenant_id": "acme", "batch_id": "b-stuck",
		"batch_seq": 1, "valid_till": "2000-01-01T00:00:00Z",
	})
	if _, err := sched.Enqueue(ctx, "acme", payload); err != nil {
		t.Fatal(err)
	}

	dropAttempts := 0
	fwd := &Forwarder{
		Lane:       LaneTime,
		Scheduler:  sched,
		ReadyTopic: "ready.time",
		Producer:   &memProducer{},
		// After valid_till, so the checked-out job is treated as expired.
		Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
		// Simulate the completion-event/DLT produce failing transiently.
		OnExpired: func(context.Context, *CheckoutResult, []byte) error {
			dropAttempts++
			return errors.New("kafka produce failed")
		},
	}

	if !fwd.ForwardOnce(ctx) {
		t.Fatal("expected expired job to be handled")
	}
	if dropAttempts != 1 {
		t.Fatalf("expected OnExpired to be attempted once, got %d", dropAttempts)
	}

	// The failed drop must leave the job recoverable. The forwarding entry is the
	// only durable record of the checked-out payload (it was already LPOP'd from
	// the ready list), so it must NOT be confirmed/removed until the drop
	// succeeds — otherwise the job is lost and its batch never advances.
	n, err := rdb.HLen(ctx, forwardingKey(LaneTime)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatalf("BUG (Finding #2): OnExpired failed but the forwarding entry was " +
			"already removed — the expired job is lost and batch \"b-stuck\" is stuck")
	}
}
