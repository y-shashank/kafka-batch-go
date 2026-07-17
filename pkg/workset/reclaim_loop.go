package workset

import (
	"context"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// RunReclaimScheduler periodically reclaims orphans until ctx is cancelled.
func RunReclaimScheduler(ctx context.Context, store *Store, prod Producer, every time.Duration, limit int, grace time.Duration, onTick func()) {
	if store == nil || prod == nil {
		return
	}
	if every <= 0 {
		every = 30 * time.Second
	}
	if limit < 1 {
		limit = 100
	}
	grace = resolveGrace(grace)
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		log.Printf("[kbatch-workset] reclaim scheduler started every=%s limit=%d grace=%s", every, limit, grace)
		for {
			select {
			case <-ctx.Done():
				log.Printf("[kbatch-workset] reclaim scheduler stopped")
				return
			case <-ticker.C:
				if onTick != nil {
					onTick()
				}
				started := time.Now()
				res, err := store.ReclaimOrphans(ctx, prod, limit, 30*time.Second, grace)
				if err != nil {
					log.Printf("[kbatch-workset] reclaim sweep error: %v", err)
					continue
				}
				if res.Reclaimed > 0 || res.Failed > 0 {
					log.Printf("[kbatch-workset] reclaim sweep checked=%d reclaimed=%d failed=%d skipped=%d",
						res.Checked, res.Reclaimed, res.Failed, res.Skipped)
				}
				instrument.WorksetReclaimed(res.Checked, res.Reclaimed, res.Failed, res.Skipped, time.Since(started))
			}
		}
	}()
}
