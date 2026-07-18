package daemon

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/cron"
)

// recurringEnqueuer adapts client.Client to cron.Enqueuer. A uniq-duplicate
// skip is treated as a successful dispatch (the job already exists).
type recurringEnqueuer struct{ c *client.Client }

func (e recurringEnqueuer) Enqueue(ctx context.Context, jobType string, payload map[string]interface{}, opts cron.EnqueueOpts) (string, error) {
	id, err := e.c.EnqueueJob(ctx, jobType, payload, client.PushOptions{JobID: opts.JobID, TenantID: opts.TenantID})
	if errors.Is(err, client.ErrJobSkipped) {
		return opts.JobID, nil
	}
	return id, err
}

// StartRecurringScheduler wires and launches the cron ticker. It returns a
// cleanup func to release the produce client and schedule store on shutdown.
func StartRecurringScheduler(ctx context.Context, cfg config.Daemon, rdb *redis.Client, loopHealth *LoopHealth) (func(), error) {
	dsn := strings.TrimSpace(cfg.RecurringMySQLDSN)
	if dsn == "" {
		dsn = strings.TrimSpace(cfg.ScheduleMySQLDSN)
	}
	if dsn == "" {
		return nil, fmt.Errorf("recurring scheduler requires recurring_mysql_dsn (or schedule_mysql_dsn)")
	}

	store, err := cron.NewStore(dsn)
	if err != nil {
		return nil, fmt.Errorf("recurring store: %w", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}

	cl, err := client.New(client.ConfigFromDaemon(cfg))
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("recurring enqueue client: %w", err)
	}

	ticker := &cron.Ticker{
		Store:          store,
		Lock:           cron.NewLock(rdb, cfg.RecurringLockTTL),
		Enqueuer:       recurringEnqueuer{c: cl},
		Window:         cfg.RecurringWindow,
		BatchSize:      cfg.RecurringBatchSize,
		MisfireGrace:   cfg.RecurringMisfireGrace,
		MaxBackfill:    cfg.RecurringMaxBackfill,
		RecoverEvery:   cfg.RecurringRecoverEvery,
		RecoverGrace:   cfg.RecurringRecoverGrace,
		PruneEvery:     cfg.RecurringPruneEvery,
		PruneRetention: cfg.RecurringPruneRetention,
		HeartbeatEvery: cfg.RecurringHeartbeatEvery,
		StaleFactor:    cfg.RecurringStaleFactor,
		RecordActivity: func() { loopHealth.RecordTick("recurring-scheduler") },
	}

	go runLoopSupervised(ctx, "recurring-scheduler", loopHealth, func(ctx context.Context) error {
		ticker.Run(ctx)
		return nil
	})
	log.Printf("kbatch recurring scheduler enabled window=%s store=mysql", cfg.RecurringWindow)

	return func() {
		cl.Close()
		_ = store.Close()
	}, nil
}
