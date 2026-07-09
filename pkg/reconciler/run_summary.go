package reconciler

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

const (
	keyLast = "kafka_batch:reconciler:last"
	keySkip = "kafka_batch:reconciler:last_skip"
)

// SaveLast persists the latest reconciler sweep summary.
func SaveLast(ctx context.Context, st *store.RedisStore, summary Summary) {
	rdb := st.RawClient()
	if rdb == nil {
		return
	}
	details, _ := json.Marshal(summary.Details)
	fields := map[string]interface{}{
		"ran_at":          summary.RanAt,
		"triggered_by":    summary.TriggeredBy,
		"duration":        summary.Duration,
		"found_stale":     summary.FoundStale,
		"processed_stale": summary.ProcessedStale,
		"found_lost":      summary.FoundLost,
		"processed_lost":  summary.ProcessedLost,
		"capped_stale":    boolStr(summary.CappedStale),
		"capped_lost":     boolStr(summary.CappedLost),
		"recovered_stale": summary.RecoveredStale,
		"refired_lost":    summary.RefiredLost,
		"skipped_stale":   summary.SkippedStale,
		"produce_failed":  summary.ProduceFailed,
		"details":         string(details),
	}
	if err := rdb.HSet(ctx, keyLast, fields).Err(); err != nil {
		log.Printf("[kbatch-reconciler] save_last failed: %v", err)
	}
}

// SaveSkip records a lock-skipped sweep.
func SaveSkip(ctx context.Context, st *store.RedisStore) {
	rdb := st.RawClient()
	if rdb == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := rdb.HSet(ctx, keySkip, "at", now, "reason", "lock_held").Err(); err != nil {
		log.Printf("[kbatch-reconciler] save_skip failed: %v", err)
	}
}

func boolStr(v bool) string {
	if v {
		return "1"
	}
	return "0"
}
