package cron

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/y-shashank/kafka-batch-go/pkg/dsn"
)

// Store persists recurring schedule definitions and the fire-idempotency ledger.
type Store struct {
	db *sql.DB
}

// NewStore opens a MySQL connection for the recurring scheduler.
func NewStore(conn string) (*Store, error) {
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
		return nil, fmt.Errorf("cron store ping: %w", err)
	}
	return &Store{db: db}, nil
}

// NewStoreDB wraps an existing *sql.DB (tests).
func NewStoreDB(db *sql.DB) *Store { return &Store{db: db} }

// Close releases the connection.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// EnsureSchema creates the schedule and fire-ledger tables if absent. Safe to
// call on every startup; production may prefer the checked-in migration.
func (s *Store) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS kafka_batch_recurring_schedules (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name           VARCHAR(191) NOT NULL,
  cron_expr      VARCHAR(120) NOT NULL,
  timezone       VARCHAR(64)  NOT NULL DEFAULT 'UTC',
  job_type       VARCHAR(120) NOT NULL,
  args_json      JSON NULL,
  tenant_id      VARCHAR(120) NULL,
  enabled        TINYINT(1) NOT NULL DEFAULT 1,
  misfire_policy VARCHAR(16) NOT NULL DEFAULT 'fire_once',
  next_run_at    DATETIME NOT NULL,
  last_fire_at   DATETIME NULL,
  created_at     DATETIME NOT NULL,
  updated_at     DATETIME NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_name (name),
  KEY idx_due (enabled, next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE IF NOT EXISTS kafka_batch_recurring_fires (
  schedule_id   BIGINT UNSIGNED NOT NULL,
  fire_at       DATETIME NOT NULL,
  status        VARCHAR(16) NOT NULL DEFAULT 'pending',
  job_id        VARCHAR(191) NULL,
  created_at    DATETIME NOT NULL,
  dispatched_at DATETIME NULL,
  PRIMARY KEY (schedule_id, fire_at),
  KEY idx_pending (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("cron ensure schema: %w", err)
		}
	}
	return nil
}

// ---- CRUD ----------------------------------------------------------------

// Upsert inserts or updates a schedule by name and returns its id. next_run_at
// is (re)computed from the cron expression on every upsert so an edited cron
// takes effect immediately.
func (s *Store) Upsert(ctx context.Context, sc Schedule) (int64, error) {
	if sc.Name == "" || sc.CronExpr == "" || sc.JobType == "" {
		return 0, errors.New("cron upsert: name, cron_expr and job_type are required")
	}
	if sc.Misfire == "" {
		sc.Misfire = MisfireFireOnce
	}
	if !sc.Misfire.Valid() {
		return 0, fmt.Errorf("cron upsert: invalid misfire_policy %q", sc.Misfire)
	}
	if sc.Timezone == "" {
		sc.Timezone = "UTC"
	}
	expr, err := Parse(sc.CronExpr)
	if err != nil {
		return 0, err
	}
	loc, err := sc.Location()
	if err != nil {
		return 0, err
	}
	next, ok := expr.Next(time.Now(), loc)
	if !ok {
		return 0, fmt.Errorf("cron upsert: %q never fires", sc.CronExpr)
	}
	args := []byte("{}")
	if sc.Args != nil {
		if args, err = json.Marshal(sc.Args); err != nil {
			return 0, err
		}
	}
	var tenant interface{}
	if sc.TenantID != "" {
		tenant = sc.TenantID
	}
	now := time.Now().UTC()
	enabled := 0
	if sc.Enabled {
		enabled = 1
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO kafka_batch_recurring_schedules
  (name, cron_expr, timezone, job_type, args_json, tenant_id, enabled, misfire_policy, next_run_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  cron_expr = VALUES(cron_expr),
  timezone = VALUES(timezone),
  job_type = VALUES(job_type),
  args_json = VALUES(args_json),
  tenant_id = VALUES(tenant_id),
  enabled = VALUES(enabled),
  misfire_policy = VALUES(misfire_policy),
  next_run_at = VALUES(next_run_at),
  updated_at = VALUES(updated_at)`,
		sc.Name, sc.CronExpr, sc.Timezone, sc.JobType, args, tenant, enabled, string(sc.Misfire),
		next.UTC(), now, now,
	)
	if err != nil {
		return 0, err
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		return id, nil
	}
	var id int64
	err = s.db.QueryRowContext(ctx, `SELECT id FROM kafka_batch_recurring_schedules WHERE name = ?`, sc.Name).Scan(&id)
	return id, err
}

// SetEnabled toggles a schedule on or off by name.
func (s *Store) SetEnabled(ctx context.Context, name string, enabled bool) error {
	v := 0
	if enabled {
		v = 1
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE kafka_batch_recurring_schedules SET enabled = ?, updated_at = ? WHERE name = ?`,
		v, time.Now().UTC(), name)
	return err
}

// Delete removes a schedule (and, by cascade of app logic, leaves its ledger
// rows harmlessly orphaned — they are pruned by Prune).
func (s *Store) Delete(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kafka_batch_recurring_schedules WHERE name = ?`, name)
	return err
}

// List returns all schedules ordered by name.
func (s *Store) List(ctx context.Context) ([]Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, cron_expr, timezone, job_type, args_json, tenant_id, enabled, misfire_policy, next_run_at, last_fire_at
FROM kafka_batch_recurring_schedules ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// ---- Fire dispatch -------------------------------------------------------

// ClaimedFire is one instant that this node newly claimed and must enqueue.
type ClaimedFire struct {
	ScheduleID int64
	Name       string
	JobType    string
	TenantID   string
	Args       map[string]interface{}
	FireAt     time.Time // UTC
}

// ClaimAndAdvance is the heart of the scheduler. In a single transaction it:
//
//  1. selects due schedules (enabled AND next_run_at <= now) FOR UPDATE SKIP
//     LOCKED so concurrent tickers never contend on the same row;
//  2. asks `plan` (which owns the cron/misfire logic) for the fire instants and
//     the new next_run_at;
//  3. INSERTs each fire into the ledger with INSERT IGNORE — a duplicate
//     (schedule_id, fire_at) is silently skipped, giving exactly-once emission
//     regardless of leader flapping;
//  4. advances next_run_at / last_fire_at.
//
// It returns only the NEWLY-inserted fires (the ones this call must enqueue).
func (s *Store) ClaimAndAdvance(ctx context.Context, now time.Time, limit int, plan func(Schedule) (Plan, error)) ([]ClaimedFire, error) {
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
SELECT id, name, cron_expr, timezone, job_type, args_json, tenant_id, enabled, misfire_policy, next_run_at, last_fire_at
FROM kafka_batch_recurring_schedules
WHERE enabled = 1 AND next_run_at <= ?
ORDER BY next_run_at
LIMIT ?
FOR UPDATE SKIP LOCKED`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	var due []Schedule
	for rows.Next() {
		sc, scanErr := scanSchedule(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		due = append(due, sc)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	created := time.Now().UTC()
	var claimed []ClaimedFire
	for _, sc := range due {
		p, planErr := plan(sc)
		if planErr != nil {
			// A poison schedule (bad cron / bad tz) must not wedge the loop or
			// starve siblings. Disable it and let the caller alert.
			if _, derr := tx.ExecContext(ctx,
				`UPDATE kafka_batch_recurring_schedules SET enabled = 0, updated_at = ? WHERE id = ?`,
				created, sc.ID); derr != nil {
				return nil, derr
			}
			continue
		}
		var lastFire *time.Time
		for _, fireAt := range p.Fires {
			res, ferr := tx.ExecContext(ctx, `
INSERT IGNORE INTO kafka_batch_recurring_fires (schedule_id, fire_at, status, created_at)
VALUES (?, ?, 'pending', ?)`, sc.ID, fireAt.UTC(), created)
			if ferr != nil {
				return nil, ferr
			}
			f := fireAt.UTC()
			lastFire = &f
			if n, _ := res.RowsAffected(); n == 1 {
				claimed = append(claimed, ClaimedFire{
					ScheduleID: sc.ID, Name: sc.Name, JobType: sc.JobType,
					TenantID: sc.TenantID, Args: sc.Args, FireAt: f,
				})
			}
		}
		if lastFire != nil {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE kafka_batch_recurring_schedules SET next_run_at = ?, last_fire_at = ?, updated_at = ? WHERE id = ?`,
				p.NewNext.UTC(), *lastFire, created, sc.ID); uerr != nil {
				return nil, uerr
			}
		} else {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE kafka_batch_recurring_schedules SET next_run_at = ?, updated_at = ? WHERE id = ?`,
				p.NewNext.UTC(), created, sc.ID); uerr != nil {
				return nil, uerr
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

// MarkDispatched records that a claimed fire was successfully enqueued.
func (s *Store) MarkDispatched(ctx context.Context, scheduleID int64, fireAt time.Time, jobID string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE kafka_batch_recurring_fires
SET status = 'dispatched', job_id = ?, dispatched_at = ?
WHERE schedule_id = ? AND fire_at = ?`,
		jobID, time.Now().UTC(), scheduleID, fireAt.UTC())
	return err
}

// RecoverPending returns fires that were claimed (committed to the ledger) but
// never marked dispatched and are older than `olderThan` — i.e. the tick
// crashed between commit and enqueue. Re-enqueueing them with the same
// deterministic job id makes recovery idempotent end-to-end.
func (s *Store) RecoverPending(ctx context.Context, olderThan time.Time, limit int) ([]ClaimedFire, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT f.schedule_id, f.fire_at, s.name, s.job_type, s.tenant_id, s.args_json
FROM kafka_batch_recurring_fires f
JOIN kafka_batch_recurring_schedules s ON s.id = f.schedule_id
WHERE f.status = 'pending' AND f.created_at < ?
ORDER BY f.created_at
LIMIT ?`, olderThan.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedFire
	for rows.Next() {
		var cf ClaimedFire
		var argsRaw sql.NullString
		var tenant sql.NullString
		if err := rows.Scan(&cf.ScheduleID, &cf.FireAt, &cf.Name, &cf.JobType, &tenant, &argsRaw); err != nil {
			return nil, err
		}
		cf.TenantID = tenant.String
		cf.Args = decodeArgs(argsRaw)
		out = append(out, cf)
	}
	return out, rows.Err()
}

// Prune deletes dispatched ledger rows older than the retention horizon.
func (s *Store) Prune(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM kafka_batch_recurring_fires WHERE status = 'dispatched' AND dispatched_at < ?`,
		olderThan.UTC())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ---- scanning ------------------------------------------------------------

func scanSchedule(rows *sql.Rows) (Schedule, error) {
	var sc Schedule
	var argsRaw sql.NullString
	var tenant sql.NullString
	var last sql.NullTime
	var enabled int
	var misfire string
	if err := rows.Scan(&sc.ID, &sc.Name, &sc.CronExpr, &sc.Timezone, &sc.JobType,
		&argsRaw, &tenant, &enabled, &misfire, &sc.NextRunAt, &last); err != nil {
		return Schedule{}, err
	}
	sc.Enabled = enabled == 1
	sc.Misfire = MisfirePolicy(misfire)
	sc.TenantID = tenant.String
	sc.Args = decodeArgs(argsRaw)
	if last.Valid {
		t := last.Time.UTC()
		sc.LastFire = &t
	}
	sc.NextRunAt = sc.NextRunAt.UTC()
	return sc, nil
}

func decodeArgs(raw sql.NullString) map[string]interface{} {
	if !raw.Valid || raw.String == "" {
		return map[string]interface{}{}
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw.String), &m); err != nil || m == nil {
		return map[string]interface{}{}
	}
	return m
}
