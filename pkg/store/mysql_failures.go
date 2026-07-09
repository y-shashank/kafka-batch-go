package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLFailures records job failures to kafka_batch_failures (Ruby MysqlStore parity).
type MySQLFailures struct {
	db *sql.DB
}

func NewMySQLFailures(dsn string) (*MySQLFailures, error) {
	db, err := sql.Open("mysql", dsn)
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

// CompositeFailures writes to Redis and optionally MySQL.
type CompositeFailures struct {
	Redis *RedisStore
	MySQL *MySQLFailures
}

func (c *CompositeFailures) RecordFailure(ctx context.Context, e FailureEntry) error {
	if c.Redis != nil {
		_ = c.Redis.RecordFailure(ctx, e)
	}
	if c.MySQL != nil {
		return c.MySQL.RecordFailure(ctx, e)
	}
	return nil
}
