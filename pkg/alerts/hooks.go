package alerts

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// InstallInstrumentHooks records DLT publishes and cron.stale into the shared
// Redis alert state (same keys Ruby Alerts::State uses).
func InstallInstrumentHooks(rdb *redis.Client) (remove func()) {
	st := NewState(rdb)
	return instrument.AddHandler(func(event string, payload map[string]interface{}, _ float64) {
		ctx := context.Background()
		switch event {
		case "dlt.published":
			st.IncrDLT(ctx)
		case "cron.stale":
			schedule, _ := payload["schedule"].(string)
			jobType, _ := payload["job_type"].(string)
			stale := intFrom(payload["stale_seconds"])
			if schedule != "" {
				st.MarkCronStale(ctx, schedule, jobType, stale)
			}
		}
	})
}

func intFrom(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	default:
		return 0
	}
}
