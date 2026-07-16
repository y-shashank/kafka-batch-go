//go:build integration

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestE2E_RetryFailureStoreLifecycle_MySQL is the only failure-store
// lifecycle test: no per-job failure metadata is ever written to Redis
// (exhausted jobs land on the dead-letter topic, retrying jobs are listed
// live from the retry topics), so there is nothing to observe there. Only
// the MySQL-backed store (config.Store == "mysql") durably records failures.
func TestE2E_RetryFailureStoreLifecycle_MySQL(t *testing.T) {
	mysqlDSN := mysqlFailuresDSN()
	if mysqlDSN == "" {
		t.Skip("set KAFKA_BATCH_TEST_MYSQL_DSN for MySQL failure-store integration")
	}
	if err := prepareMySQLFailures(mysqlDSN); err != nil {
		t.Fatalf("prepare mysql failures: %v", err)
	}
	t.Cleanup(func() { _ = truncateMySQLFailures(mysqlDSN) })

	s := NewStack(t, baseHandlersStack, func(_ *Stack, cfg *daemonYAML) {
		cfg.Store = "mysql"
		cfg.StoreMySQLDSN = mysqlDSN
		cfg.RetryTiers = map[string]int{"short": 2, "medium": 2, "large": 2}
	})
	s.Start()
	defer s.Stop()

	c := s.NewClient()
	defer c.Close()
	ctx := context.Background()

	var jobID string
	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "retry mysql failure store"}, func(b *client.Batch) error {
		var err error
		jobID, err = b.PushJob(ctx, "integration.go_retry_once", map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	s.WaitMySQLFailureStatus(ctx, mysqlDSN, batch.ID(), jobID, "retrying", 20*time.Second)

	s.WaitBatch(ctx, batch.ID(), "success")
	if got := s.WaitMarker(45 * time.Second); got != jobID {
		t.Fatalf("marker = %q want %q", got, jobID)
	}
	s.WaitMySQLFailureCleared(ctx, mysqlDSN, batch.ID(), jobID, 10*time.Second)
}
