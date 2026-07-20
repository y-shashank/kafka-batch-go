package alerts

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

// RunScheduler starts the control-plane health-alerts evaluator loop.
// kbatch daemon is always control plane — always start the loop; each tick
// reloads Redis settings and no-ops when effective enabled is false.
// Shares NX lock + open/notify dedupe keys with Ruby (no double Slack).
// UI Send-test remains Ruby-only.
func RunScheduler(ctx context.Context, cfg config.Daemon, rdb *redis.Client, onTick func()) {
	removeHooks := InstallInstrumentHooks(rdb)
	go func() {
		defer removeHooks()

		interval := time.Duration(positiveOr(cfg.AlertsIntervalSec, 60)) * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		log.Printf("[kbatch-alerts] evaluator started interval=%s (Redis settings win; notify once per open/resolve)", interval)

		tick := func() {
			if onTick != nil {
				onTick()
			}
			eff := loadEffective(ctx, rdb, cfg)
			if eff.Interval > 0 {
				next := time.Duration(eff.Interval) * time.Second
				if next != interval {
					ticker.Reset(next)
					interval = next
				}
			}
			sum := EvaluateOnce(ctx, rdb, eff)
			if ok, _ := sum["ok"].(bool); !ok {
				if reason, _ := sum["reason"].(string); reason != "" && reason != "disabled" && reason != "lock" {
					log.Printf("[kbatch-alerts] tick: %v", sum["reason"])
				}
				return
			}
			fired, _ := sum["fired"].(int)
			resolved, _ := sum["resolved"].(int)
			if fired > 0 || resolved > 0 {
				log.Printf("[kbatch-alerts] tick findings=%v open=%v fired=%v resolved=%v",
					sum["findings"], sum["open"], fired, resolved)
			}
		}

		tick()
		for {
			select {
			case <-ctx.Done():
				log.Printf("[kbatch-alerts] evaluator stopped")
				return
			case <-ticker.C:
				tick()
			}
		}
	}()
}
