package alerts

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	lockKey          = "kafka_batch:alerts:lock"
	openKey          = "kafka_batch:alerts:open"
	breachKey        = "kafka_batch:alerts:breach"
	healthyKey       = "kafka_batch:alerts:healthy"
	lastKey          = "kafka_batch:alerts:last"
	baselineKey      = "kafka_batch:alerts:baseline"
	dltCounterPrefix = "kafka_batch:alerts:dlt:min:"
	cronStaleKey     = "kafka_batch:alerts:cron_stale"
)

type State struct {
	rdb *redis.Client
}

func NewState(rdb *redis.Client) *State { return &State{rdb: rdb} }

func (s *State) TryLock(ctx context.Context, ttlSec int) bool {
	if ttlSec < 2 {
		ttlSec = 2
	}
	ok, err := s.rdb.SetNX(ctx, lockKey, "1", time.Duration(ttlSec)*time.Second).Result()
	return err == nil && ok
}

func (s *State) OpenAlerts(ctx context.Context) []map[string]interface{} {
	raw, err := s.rdb.HGetAll(ctx, openKey).Result()
	if err != nil {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, jsonStr := range raw {
		var m map[string]interface{}
		if json.Unmarshal([]byte(jsonStr), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

func (s *State) GetOpen(ctx context.Context, fp string) map[string]interface{} {
	raw, err := s.rdb.HGet(ctx, openKey, fp).Result()
	if err != nil || raw == "" {
		return nil
	}
	var m map[string]interface{}
	if json.Unmarshal([]byte(raw), &m) != nil {
		return nil
	}
	return m
}

func (s *State) SetOpen(ctx context.Context, fp string, incident map[string]interface{}) {
	b, _ := json.Marshal(incident)
	_ = s.rdb.HSet(ctx, openKey, fp, string(b)).Err()
}

// ClaimOpen atomically opens an incident (HSETNX). Only the first writer
// (Ruby or Go) returns true — prevents duplicate Slack/webhook fires.
func (s *State) ClaimOpen(ctx context.Context, fp string, incident map[string]interface{}) bool {
	b, _ := json.Marshal(incident)
	ok, err := s.rdb.HSetNX(ctx, openKey, fp, string(b)).Result()
	return err == nil && ok
}

// TouchOpen refreshes summary on an open incident without re-notifying.
func (s *State) TouchOpen(ctx context.Context, fp, summary string) {
	open := s.GetOpen(ctx, fp)
	if open == nil {
		return
	}
	open["summary"] = summary
	s.SetOpen(ctx, fp, open)
}

// ClearOpen removes an open incident; returns true if one was removed.
func (s *State) ClearOpen(ctx context.Context, fp string) bool {
	n, err := s.rdb.HDel(ctx, openKey, fp).Result()
	return err == nil && n > 0
}

const notifyDedupePrefix = "kafka_batch:alerts:notify_dedupe:"

// ClaimNotify is a short NX gate so concurrent evaluators do not double-deliver.
func (s *State) ClaimNotify(ctx context.Context, fingerprint, event string, ttlSec int) bool {
	if ttlSec < 60 {
		ttlSec = 60
	}
	key := notifyDedupePrefix + event + ":" + fingerprint
	ok, err := s.rdb.SetNX(ctx, key, "1", time.Duration(ttlSec)*time.Second).Result()
	return err == nil && ok
}

func (s *State) BreachCount(ctx context.Context, fp string) int {
	v, _ := s.rdb.HGet(ctx, breachKey, fp).Int()
	return v
}

func (s *State) IncrBreach(ctx context.Context, fp string) {
	pipe := s.rdb.Pipeline()
	pipe.HIncrBy(ctx, breachKey, fp, 1)
	pipe.HSet(ctx, healthyKey, fp, "0")
	_, _ = pipe.Exec(ctx)
}

func (s *State) ResetBreach(ctx context.Context, fp string) {
	_ = s.rdb.HSet(ctx, breachKey, fp, "0").Err()
}

func (s *State) HealthyCount(ctx context.Context, fp string) int {
	v, _ := s.rdb.HGet(ctx, healthyKey, fp).Int()
	return v
}

func (s *State) IncrHealthy(ctx context.Context, fp string) {
	pipe := s.rdb.Pipeline()
	pipe.HIncrBy(ctx, healthyKey, fp, 1)
	pipe.HSet(ctx, breachKey, fp, "0")
	_, _ = pipe.Exec(ctx)
}

func (s *State) ResetHealthy(ctx context.Context, fp string) {
	_ = s.rdb.HSet(ctx, healthyKey, fp, "0").Err()
}

func (s *State) SaveLast(ctx context.Context, summary map[string]interface{}) {
	b, _ := json.Marshal(summary)
	_ = s.rdb.Set(ctx, lastKey, string(b), 0).Err()
}

func (s *State) LoadBaseline(ctx context.Context) map[string]map[string]interface{} {
	raw, err := s.rdb.Get(ctx, baselineKey).Result()
	if err != nil || raw == "" {
		return map[string]map[string]interface{}{}
	}
	var m map[string]map[string]interface{}
	if json.Unmarshal([]byte(raw), &m) != nil {
		return map[string]map[string]interface{}{}
	}
	return m
}

func (s *State) SaveBaseline(ctx context.Context, baseline map[string]map[string]interface{}) {
	b, _ := json.Marshal(baseline)
	_ = s.rdb.Set(ctx, baselineKey, string(b), 24*time.Hour).Err()
}

func (s *State) IncrDLT(ctx context.Context) {
	bucket := (time.Now().Unix() / 60) * 60
	key := dltCounterPrefix + strconv.FormatInt(bucket, 10)
	pipe := s.rdb.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, time.Hour)
	_, _ = pipe.Exec(ctx)
}

func (s *State) DLTCountLastMinute(ctx context.Context) int {
	bucket := (time.Now().Unix() / 60) * 60
	v, _ := s.rdb.Get(ctx, dltCounterPrefix+strconv.FormatInt(bucket, 10)).Int()
	return v
}

func (s *State) MarkCronStale(ctx context.Context, schedule, jobType string, staleSeconds int) {
	entry := map[string]interface{}{
		"schedule":      schedule,
		"job_type":      jobType,
		"stale_seconds": staleSeconds,
		"at":            nowISO(),
	}
	b, _ := json.Marshal(entry)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, cronStaleKey, schedule, string(b))
	pipe.Expire(ctx, cronStaleKey, time.Hour)
	_, _ = pipe.Exec(ctx)
}

func (s *State) CronStaleEntries(ctx context.Context) []map[string]interface{} {
	raw, err := s.rdb.HGetAll(ctx, cronStaleKey).Result()
	if err != nil {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, jsonStr := range raw {
		var m map[string]interface{}
		if json.Unmarshal([]byte(jsonStr), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}
