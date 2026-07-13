package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// CompletionEvent is input to RecordCompletionsBatch.
type CompletionEvent struct {
	BatchID  string
	JobID    string
	Status   string // success | failed
	BatchSeq int64
}

// Batch is a batch ledger record.
type Batch struct {
	ID              string
	TotalJobs       int64
	CompletedCount  int64
	FailedCount     int64
	Status          string
	OnSuccess       string
	OnComplete      string
	Meta            string
	CallbackArgs    string
	Description     string
	TenantID        string
	LockedAt            string
	FinishedAt          string
	ReconcilerRefiredAt string
	CallbackClaimed     bool
}

// FinishedBatch is returned when a batch just completed.
type FinishedBatch struct {
	Batch   *Batch
	Outcome string
}

// CompletionsResult aggregates record_completions_batch output.
type CompletionsResult struct {
	Finished []FinishedBatch
	Replays  []string
}

// RedisStore implements the batch ledger against Redis (Ruby-compatible).
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, ttl: ttl}
}

// RawClient exposes the underlying Redis client (reconciler summaries, tests).
func (s *RedisStore) RawClient() *redis.Client {
	if s == nil {
		return nil
	}
	return s.client
}

func (s *RedisStore) completionKeys(batchID string) []string {
	return []string{batchKey(batchID), bitmapKey(batchID), runningIndex, doneIndex, countsKey}
}

func (s *RedisStore) RecordCompletionsBatch(ctx context.Context, events []CompletionEvent) (CompletionsResult, error) {
	out := CompletionsResult{}
	if len(events) == 0 {
		return out, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	nowFloat := fmt.Sprintf("%f", float64(time.Now().UnixNano())/1e9)
	ttlSec := strconv.Itoa(int(s.ttl.Seconds()))

	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(events))
	for i, e := range events {
		field := "failed_count"
		if e.Status == "success" {
			field = "completed_count"
		}
		cmds[i] = pipe.Eval(ctx, batchDoneJobLua, s.completionKeys(e.BatchID),
			e.BatchSeq, field, ttlSec, now, nowFloat)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out, err
	}

	finalized := []int{}
	for i, cmd := range cmds {
		res, err := cmd.Slice()
		if err != nil {
			return out, err
		}
		code, _ := res[0].(int64)
		payload, _ := res[1].(string)
		switch code {
		case 1:
			finalized = append(finalized, i)
		case 0:
			if payload == "duplicate" {
				out.Replays = append(out.Replays, events[i].BatchID)
			}
		}
	}

	for _, idx := range finalized {
		res, _ := cmds[idx].Slice()
		outcome, _ := res[1].(string)
		batch, err := s.FindBatch(ctx, events[idx].BatchID)
		if err != nil {
			return out, err
		}
		out.Finished = append(out.Finished, FinishedBatch{Batch: batch, Outcome: outcome})
	}
	return out, nil
}

func (s *RedisStore) FindBatch(ctx context.Context, id string) (*Batch, error) {
	h, err := s.client.HGetAll(ctx, batchKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, nil
	}
	b := hashToBatch(h)
	return b, nil
}

// CompletionRecorded reports whether the batch dedup bitmap already has the bit
// for this job's sequence set — i.e. the job's completion was already counted.
// Used to decide whether a fair-slot skip is a genuine duplicate (safe to drop)
// or an orphaned slot whose holder died before counting (must be re-run).
func (s *RedisStore) CompletionRecorded(ctx context.Context, batchID string, seq int64) (bool, error) {
	if s == nil || s.client == nil || seq < 1 {
		return false, nil
	}
	bit, err := s.client.GetBit(ctx, bitmapKey(batchID), seq-1).Result()
	if err != nil {
		return false, err
	}
	return bit == 1, nil
}

func (s *RedisStore) ClaimCallback(ctx context.Context, batchID, nodeID string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.client.Eval(ctx, claimCallbackLua,
		[]string{batchKey(batchID), doneIndex},
		now, nodeID, batchID,
	).Int()
	return res == 1, err
}

func (s *RedisStore) CallbackDispatched(ctx context.Context, batchID string) (bool, error) {
	v, err := s.client.HGet(ctx, batchKey(batchID), "callback_dispatched_at").Result()
	if err == redis.Nil {
		return false, nil
	}
	return v != "", err
}

// FindReplayCallbackBatches loads callback-eligible batches for replay IDs in one pipelined round trip.
func (s *RedisStore) FindReplayCallbackBatches(ctx context.Context, ids []string) ([]*Batch, error) {
	if s == nil || s.client == nil || len(ids) == 0 {
		return nil, nil
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, batchKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	out := make([]*Batch, 0, len(ids))
	for _, cmd := range cmds {
		h, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if len(h) == 0 || h["callback_dispatched_at"] != "" {
			continue
		}
		b := hashToBatch(h)
		if b == nil {
			continue
		}
		if b.Status != "success" && b.Status != "complete" {
			continue
		}
		out = append(out, b)
	}
	return out, nil
}

// MarkReconcilerRefired records that the reconciler re-produced a callback for this batch.
func (s *RedisStore) MarkReconcilerRefired(ctx context.Context, batchID string) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis store not configured")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return s.client.HSet(ctx, batchKey(batchID), "reconciler_refired_at", now).Err()
}

func (s *RedisStore) BatchCancelled(ctx context.Context, batchID string) (bool, error) {
	score, err := s.client.ZScore(ctx, cancelledIndex, batchID).Result()
	if err == redis.Nil {
		return false, nil
	}
	return score > 0, err
}
