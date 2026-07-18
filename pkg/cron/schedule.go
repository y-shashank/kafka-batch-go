package cron

import (
	"fmt"
	"time"
)

// MisfirePolicy decides what happens when the scheduler was down (or lagging)
// across one or more fire instants.
type MisfirePolicy string

const (
	// MisfireSkip drops instants missed by more than the grace window and only
	// resumes at the next future instant. Use for work where a stale run is
	// worthless (e.g. "recompute current metrics").
	MisfireSkip MisfirePolicy = "skip"
	// MisfireFireOnce fires the oldest missed instant exactly once, then fast-
	// forwards past the rest of the gap. Sane default.
	MisfireFireOnce MisfirePolicy = "fire_once"
	// MisfireBackfill fires every missed instant in the gap (capped per tick).
	// Use only for genuinely time-partitioned work.
	MisfireBackfill MisfirePolicy = "backfill"
)

// Valid reports whether p is a known policy.
func (p MisfirePolicy) Valid() bool {
	switch p {
	case MisfireSkip, MisfireFireOnce, MisfireBackfill:
		return true
	default:
		return false
	}
}

// Schedule is one recurring definition (a row of kafka_batch_recurring_schedules).
type Schedule struct {
	ID        int64
	Name      string
	CronExpr  string
	Timezone  string
	JobType   string
	Args      map[string]interface{}
	TenantID  string
	Enabled   bool
	Misfire   MisfirePolicy
	NextRunAt time.Time // UTC; the next (or an already-due) scheduled instant
	LastFire  *time.Time
}

// Location resolves the schedule's timezone, defaulting to UTC.
func (s Schedule) Location() (*time.Location, error) {
	if s.Timezone == "" || s.Timezone == "UTC" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: bad timezone %q: %w", s.Name, s.Timezone, err)
	}
	return loc, nil
}

// Plan is the result of evaluating a due schedule at a point in time.
type Plan struct {
	Fires   []time.Time // instants to enqueue (UTC), each becomes a ledger row
	NewNext time.Time   // next_run_at to persist (UTC), strictly after now
}

// PlanFires computes which instants to fire and the new next_run_at for a
// schedule whose NextRunAt is due (<= now), given the parsed expression and
// misfire policy. All returned times are UTC.
//
//   - grace: an instant within `grace` of now counts as "on time" and always
//     fires regardless of policy; older instants are "missed".
//   - maxBackfill: caps fires emitted in a single tick for MisfireBackfill so a
//     long outage cannot enqueue an unbounded burst; the remainder drains on
//     subsequent ticks (deduped by the fire ledger).
func PlanFires(s Schedule, expr Expr, loc *time.Location, now time.Time, grace time.Duration, maxBackfill int) Plan {
	next := s.NextRunAt
	now = now.UTC()
	if maxBackfill < 1 {
		maxBackfill = 1
	}

	switch s.Misfire {
	case MisfireBackfill:
		var fires []time.Time
		cur := next
		for !cur.After(now) {
			fires = append(fires, cur.UTC())
			nn, ok := expr.Next(cur, loc)
			if !ok {
				return Plan{Fires: fires, NewNext: cur.Add(time.Minute).UTC()}
			}
			cur = nn
			if len(fires) >= maxBackfill {
				break
			}
		}
		return Plan{Fires: fires, NewNext: cur.UTC()}

	case MisfireSkip:
		var fires []time.Time
		if now.Sub(next) <= grace { // on time — fire it
			fires = append(fires, next.UTC())
		}
		return Plan{Fires: fires, NewNext: advancePast(expr, loc, next, now)}

	default: // MisfireFireOnce (and any unknown value falls back to the safe default)
		return Plan{
			Fires:   []time.Time{next.UTC()},
			NewNext: advancePast(expr, loc, next, now),
		}
	}
}

// advancePast returns the first scheduled instant strictly after now, walking
// forward from `from`. Guards against a non-advancing expression.
func advancePast(expr Expr, loc *time.Location, from, now time.Time) time.Time {
	cur := from
	for i := 0; i < 1_000_000; i++ {
		nn, ok := expr.Next(cur, loc)
		if !ok {
			return cur.Add(time.Minute).UTC()
		}
		cur = nn
		if cur.After(now) {
			return cur.UTC()
		}
	}
	return now.Add(time.Minute).UTC()
}
