// Package cron implements a recurring ("whenever"-style) scheduler for
// kafka-batch. Unlike the delayed-job poller in pkg/schedule (one-shot
// perform_in/perform_at), this package fires a *job* on a repeating cron
// schedule. It never runs arbitrary code — a due schedule only enqueues a
// registered manifest handler through the normal client produce path, so
// recurring jobs inherit routing (plain/priority/fair), retries, DLQ and
// fairness for free.
//
// Correctness rests on an idempotency ledger keyed by (schedule_id, fire_at),
// not on the leader lock: a lock flap or a retried tick can never double-enqueue
// a fire instant. See Store and Ticker.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Expr is a parsed 5-field cron expression (minute hour dom month dow).
//
// Field ranges: minute 0-59, hour 0-23, day-of-month 1-31, month 1-12,
// day-of-week 0-6 (0 or 7 = Sunday). Supports '*', '?', lists (a,b),
// ranges (a-b), steps (*/n, a-b/n) and named months/weekdays (jan, mon…).
//
// When both day-of-month and day-of-week are restricted, a day matches when
// EITHER field matches (standard Vixie-cron semantics).
type Expr struct {
	minute  uint64 // bit i set ⇒ minute i allowed
	hour    uint64
	dom     uint64 // bits 1..31
	month   uint64 // bits 1..12
	dow     uint64 // bits 0..6 (Sunday=0)
	domStar bool
	dowStar bool
	raw     string
}

// Raw returns the original expression text.
func (e Expr) Raw() string { return e.raw }

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// Parse compiles a 5-field cron expression. A handful of common macros
// (@hourly, @daily, @midnight, @weekly, @monthly, @yearly/@annually) are
// accepted as shorthand.
func Parse(expr string) (Expr, error) {
	raw := strings.TrimSpace(expr)
	if m, ok := macros[strings.ToLower(raw)]; ok {
		out, err := parseFields(m)
		if err != nil {
			return Expr{}, err
		}
		out.raw = raw
		return out, nil
	}
	out, err := parseFields(raw)
	if err != nil {
		return Expr{}, err
	}
	out.raw = raw
	return out, nil
}

var macros = map[string]string{
	"@hourly":   "0 * * * *",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@weekly":   "0 0 * * 0",
	"@monthly":  "0 0 1 * *",
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
}

func parseFields(raw string) (Expr, error) {
	fields := strings.Fields(raw)
	if len(fields) != 5 {
		return Expr{}, fmt.Errorf("cron: expected 5 fields, got %d in %q", len(fields), raw)
	}
	var e Expr
	var err error
	if e.minute, _, err = parseField(fields[0], 0, 59, nil); err != nil {
		return Expr{}, fmt.Errorf("cron: minute: %w", err)
	}
	if e.hour, _, err = parseField(fields[1], 0, 23, nil); err != nil {
		return Expr{}, fmt.Errorf("cron: hour: %w", err)
	}
	if e.dom, e.domStar, err = parseField(fields[2], 1, 31, nil); err != nil {
		return Expr{}, fmt.Errorf("cron: day-of-month: %w", err)
	}
	if e.month, _, err = parseField(fields[3], 1, 12, monthNames); err != nil {
		return Expr{}, fmt.Errorf("cron: month: %w", err)
	}
	if e.dow, e.dowStar, err = parseField(fields[4], 0, 7, dowNames); err != nil {
		return Expr{}, fmt.Errorf("cron: day-of-week: %w", err)
	}
	// Normalize weekday 7 → 0 (both mean Sunday).
	if e.dow&(1<<7) != 0 {
		e.dow |= 1 << 0
		e.dow &^= 1 << 7
	}
	return e, nil
}

// parseField returns the allowed-value bitmask, whether the field was "*"
// (or "?"), and an error. names maps textual aliases to numbers.
func parseField(field string, min, max int, names map[string]int) (uint64, bool, error) {
	isStar := field == "*" || field == "?"
	var mask uint64
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, false, fmt.Errorf("empty term in %q", field)
		}
		lo, hi, step, err := parseTerm(part, min, max, names)
		if err != nil {
			return 0, false, err
		}
		for v := lo; v <= hi; v += step {
			mask |= 1 << uint(v)
		}
	}
	if mask == 0 {
		return 0, false, fmt.Errorf("no values matched in %q", field)
	}
	return mask, isStar, nil
}

func parseTerm(part string, min, max int, names map[string]int) (lo, hi, step int, err error) {
	step = 1
	if i := strings.IndexByte(part, '/'); i >= 0 {
		stepStr := part[i+1:]
		part = part[:i]
		step, err = strconv.Atoi(stepStr)
		if err != nil || step < 1 {
			return 0, 0, 0, fmt.Errorf("invalid step %q", stepStr)
		}
	}
	switch {
	case part == "*" || part == "?":
		return min, max, step, nil
	case strings.IndexByte(part, '-') >= 0:
		bounds := strings.SplitN(part, "-", 2)
		lo, err = atoiOrName(bounds[0], names)
		if err != nil {
			return 0, 0, 0, err
		}
		hi, err = atoiOrName(bounds[1], names)
		if err != nil {
			return 0, 0, 0, err
		}
	default:
		lo, err = atoiOrName(part, names)
		if err != nil {
			return 0, 0, 0, err
		}
		hi = lo
	}
	if lo < min || hi > max || lo > hi {
		return 0, 0, 0, fmt.Errorf("value %d-%d out of range [%d,%d]", lo, hi, min, max)
	}
	return lo, hi, step, nil
}

func atoiOrName(s string, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", s)
	}
	return v, nil
}

// Next returns the first instant strictly after `after`, evaluated in loc,
// that matches the expression. ok is false only if no match exists within a
// bounded horizon (e.g. an impossible date like Feb 30) — a guard against an
// unbounded scan. The returned time is in loc; store it as UTC.
//
// Working in wall-clock via time.Date(...) in loc makes DST transitions fall
// out naturally: a skipped spring-forward hour simply won't match, and the
// scan advances to the next valid instant.
func (e Expr) Next(after time.Time, loc *time.Location) (time.Time, bool) {
	if loc == nil {
		loc = time.UTC
	}
	t := after.In(loc).Truncate(time.Minute).Add(time.Minute)
	yearLimit := t.Year() + 5
	for {
		if t.Year() > yearLimit {
			return time.Time{}, false
		}
		if e.month&(1<<uint(t.Month())) == 0 {
			// Jump to 00:00 on the 1st of the next month.
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 1, 0)
			continue
		}
		if !e.dayMatches(t) {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc).AddDate(0, 0, 1)
			continue
		}
		if e.hour&(1<<uint(t.Hour())) == 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc).Add(time.Hour)
			continue
		}
		if e.minute&(1<<uint(t.Minute())) == 0 {
			t = t.Add(time.Minute)
			continue
		}
		return t, true
	}
}

func (e Expr) dayMatches(t time.Time) bool {
	if e.domStar && e.dowStar {
		return true
	}
	domMatch := e.dom&(1<<uint(t.Day())) != 0
	dowMatch := e.dow&(1<<uint(int(t.Weekday()))) != 0 // time.Sunday == 0
	if e.domStar {
		return dowMatch
	}
	if e.dowStar {
		return domMatch
	}
	return domMatch || dowMatch
}
