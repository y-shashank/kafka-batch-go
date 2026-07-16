package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/y-shashank/kafka-batch-go/pkg/dsn"
)

// MySQLFailures records job failures to kafka_batch_failures (Ruby MysqlStore parity).
type MySQLFailures struct {
	db *sql.DB
}

func NewMySQLFailures(conn string) (*MySQLFailures, error) {
	dataSource, err := dsn.Normalize(conn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql failures ping: %w", err)
	}
	return &MySQLFailures{db: db}, nil
}

func (m *MySQLFailures) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *MySQLFailures) RecordFailure(ctx context.Context, e FailureEntry) error {
	if m == nil || m.db == nil || e.BatchID == "" || e.JobID == "" {
		return nil
	}
	status := e.Status
	if status == "" {
		status = "failed"
	}
	now := time.Now().UTC()
	_, err := m.db.ExecContext(ctx, `
INSERT INTO kafka_batch_failures
  (batch_id, job_id, worker_class, error_class, error_message, attempt, status, failed_at, next_retry_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  worker_class = VALUES(worker_class),
  error_class = VALUES(error_class),
  error_message = VALUES(error_message),
  attempt = VALUES(attempt),
  status = VALUES(status),
  failed_at = VALUES(failed_at),
  next_retry_at = VALUES(next_retry_at)`,
		e.BatchID, e.JobID, e.WorkerClass, e.ErrorClass, e.ErrorMessage,
		e.Attempt, status, now, nullIfEmpty(e.NextRetryAt),
	)
	return err
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// ClearFailure removes a per-batch failure row after a successful retry.
func (m *MySQLFailures) ClearFailure(ctx context.Context, batchID, jobID string) error {
	if m == nil || m.db == nil || batchID == "" || jobID == "" {
		return nil
	}
	_, err := m.db.ExecContext(ctx,
		`DELETE FROM kafka_batch_failures WHERE batch_id = ? AND job_id = ?`,
		batchID, jobID,
	)
	return err
}
