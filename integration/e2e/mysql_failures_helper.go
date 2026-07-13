//go:build integration

package e2e

import (
	"context"
	"database/sql"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/y-shashank/kafka-batch-go/pkg/dsn"
)

func mysqlFailuresDSN() string {
	if v := os.Getenv("KAFKA_BATCH_TEST_MYSQL_DSN"); v != "" {
		return v
	}
	return os.Getenv("KAFKA_BATCH_TEST_SCHEDULE_MYSQL_DSN")
}

func prepareMySQLFailures(conn string) error {
	db, err := openMySQLFailures(conn)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS kafka_batch_failures (
  batch_id CHAR(36) NOT NULL,
  job_id CHAR(36) NOT NULL,
  worker_class VARCHAR(255) NOT NULL DEFAULT '',
  error_class VARCHAR(255) NOT NULL DEFAULT '',
  error_message TEXT,
  attempt INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'failed',
  failed_at DATETIME(6) NOT NULL,
  next_retry_at DATETIME(6) NULL,
  PRIMARY KEY (batch_id, job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	return err
}

func truncateMySQLFailures(conn string) error {
	db, err := openMySQLFailures(conn)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`TRUNCATE TABLE kafka_batch_failures`)
	return err
}

func openMySQLFailures(conn string) (*sql.DB, error) {
	normalized, err := dsn.Normalize(conn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", normalized)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func mysqlFailureStatus(ctx context.Context, conn, batchID, jobID string) (string, bool) {
	db, err := openMySQLFailures(conn)
	if err != nil {
		return "", false
	}
	defer db.Close()
	var status string
	err = db.QueryRowContext(ctx,
		`SELECT status FROM kafka_batch_failures WHERE batch_id = ? AND job_id = ?`,
		batchID, jobID,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		return "", false
	}
	return status, true
}

func (s *Stack) WaitMySQLFailureStatus(ctx context.Context, conn, batchID, jobID, want string, timeout time.Duration) {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, ok := mysqlFailureStatus(ctx, conn, batchID, jobID); ok && st == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.T.Fatalf("timeout waiting mysql failure status=%q batch=%s job=%s", want, batchID, jobID)
}

func (s *Stack) WaitMySQLFailureCleared(ctx context.Context, conn, batchID, jobID string, timeout time.Duration) {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := mysqlFailureStatus(ctx, conn, batchID, jobID); !ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.T.Fatalf("timeout waiting mysql failure clear batch=%s job=%s", batchID, jobID)
}
