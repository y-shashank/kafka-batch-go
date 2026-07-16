//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_RetryFailureStore_GoExec asserts fail → retrying → success → failure cleared.
func TestMatrix_RetryFailureStore_GoExec(t *testing.T) {
	if testing.Short() {
		t.Skip("matrix failure-store scenario is slow")
	}
	runRetryFailureStoreScenario(t, Combo{
		Name: "go_client_go_control_go_exec", Client: ClientGo, Control: ControlGo,
		Exec: e2e.ExecMode{Go: true},
	}, false, "")
}

// TestMatrix_RetryFailureStore_RubyExec_GoControl covers cross-runtime retry with
// Ruby execution and Go control (retry consumer re-enqueues after Ruby failure).
func TestMatrix_RetryFailureStore_RubyExec_GoControl(t *testing.T) {
	if testing.Short() {
		t.Skip("matrix failure-store scenario is slow")
	}
	runRetryFailureStoreScenario(t, Combo{
		Name: "go_client_go_control_ruby_exec", Client: ClientGo, Control: ControlGo,
		Exec: e2e.ExecMode{Ruby: true},
	}, false, "")
}

// TestMatrix_RetryFailureStore_MySQL_GoExec requires KAFKA_BATCH_TEST_MYSQL_DSN.
func TestMatrix_RetryFailureStore_MySQL_GoExec(t *testing.T) {
	if testing.Short() {
		t.Skip("matrix failure-store scenario is slow")
	}
	mysqlDSN := e2e.MySQLFailuresDSN()
	if mysqlDSN == "" {
		t.Skip("set KAFKA_BATCH_TEST_MYSQL_DSN for MySQL failure-store matrix test")
	}
	runRetryFailureStoreScenario(t, Combo{
		Name: "go_client_go_control_go_exec_mysql", Client: ClientGo, Control: ControlGo,
		Exec: e2e.ExecMode{Go: true},
	}, true, mysqlDSN)
}

func runRetryFailureStoreScenario(t *testing.T, combo Combo, mysql bool, mysqlDSN string) {
	t.Helper()
	if mysql {
		if err := e2e.PrepareMySQLFailuresTable(mysqlDSN); err != nil {
			t.Fatalf("prepare mysql failures: %v", err)
		}
		t.Cleanup(func() { _ = e2e.TruncateMySQLFailuresTable(mysqlDSN) })
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, func(_ *e2e.Stack, cfg *e2e.DaemonYAML) {
		// Keep "retrying" visible long enough for WaitMySQLFailureStatus
		// (zero-delay can race); only observed in the mysql variant below.
		cfg.RetryTiers = map[string]int{"short": 2, "medium": 2, "large": 2}
		if mysql {
			cfg.Store = "mysql"
			cfg.StoreMySQLDSN = mysqlDSN
		}
	})
	s.StartWithOptions(e2e.StackStartOptions{
		Control: e2e.ControlMode(combo.Control),
		Exec:    combo.Exec,
	})
	defer s.Stop()

	c := NewClient(s, combo.Client)
	defer c.Close()
	ctx := context.Background()

	var jobID string
	handler := "integration.go_retry_once"
	markerPath := s.MarkerPath
	runtime := "go"
	if combo.Exec.Ruby {
		handler = "integration.ruby_retry_once"
		markerPath = s.RubyMarkerPath
		runtime = "ruby"
	}

	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix retry failure store"}, func(b MatrixBatch) error {
		var err error
		jobID, err = b.PushJob(ctx, handler, map[string]interface{}{"ping": 1}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID = batchJobID(batch, jobID, runtime)

	// No per-job failure metadata is ever written to Redis, so only the
	// MySQL variant has a failure-store lifecycle to observe here.
	if mysql {
		s.WaitMySQLFailureStatus(ctx, mysqlDSN, batch.ID(), jobID, "retrying", 20*time.Second)
	}

	s.WaitBatchTimeout(ctx, 90*time.Second, batch.ID(), "success")
	if combo.Exec.Ruby {
		s.DrainRubyExecution(60 * time.Second)
	}
	if m := s.WaitMarkerAt(markerPath, 45*time.Second); m != jobID {
		t.Fatalf("marker = %q want %q", m, jobID)
	}

	if mysql {
		s.WaitMySQLFailureCleared(ctx, mysqlDSN, batch.ID(), jobID, 10*time.Second)
	}
}
