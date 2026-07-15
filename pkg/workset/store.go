// Package workset implements Sidekiq SuperFetch-style in-flight job ownership.
//
// Kafka delivers jobs and acks offsets after a successful Redis claim. Redis
// tracks who owns each running job so a reconciler can re-produce payloads when
// a consumer dies (heartbeat missing).
package workset

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	jobKeyPrefix       = "kafka_batch:work:job:"
	byConsumerPrefix   = "kafka_batch:work:by_consumer:"
	indexKey           = "kafka_batch:work:index"
	reclaimingPrefix   = "kafka_batch:work:reclaiming:"
	liveConsumerPrefix = "kafka_batch:live:consumer:"
)

// Entry is one in-flight job owned by a consumer.
type Entry struct {
	JobID      string `json:"job_id"`
	Payload    []byte `json:"payload"` // raw Kafka value
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
	ConsumerID string `json:"consumer_id"`
	Fence      string `json:"fence"`
	ClaimedAt  string `json:"claimed_at"`
	Runtime    string `json:"runtime"`
}

// ClaimParams is the write-ahead ownership request before Kafka ack.
type ClaimParams struct {
	JobID      string
	Payload    []byte
	Topic      string
	Partition  int32
	Offset     int64
	ConsumerID string
	LeaseTTL   time.Duration
}

// ClaimResult is the outcome of Claim.
type ClaimResult struct {
	Won   bool
	Fence string
	Entry *Entry
}

// Store is the Redis working-set ledger.
type Store struct {
	client *redis.Client
}

func NewStore(client *redis.Client) *Store {
	return &Store{client: client}
}

func jobKey(jobID string) string       { return jobKeyPrefix + jobID }
func byConsumerKey(id string) string   { return byConsumerPrefix + id }
func reclaimingKey(jobID string) string { return reclaimingPrefix + jobID }
func liveKey(consumerID string) string { return liveConsumerPrefix + consumerID }

// Claim atomically takes ownership of jobID (or steals from a dead consumer).
// On Won=true the caller must Kafka-ack then perform; call Complete when done.
func (s *Store) Claim(ctx context.Context, p ClaimParams) (ClaimResult, error) {
	if s == nil || s.client == nil {
		return ClaimResult{}, fmt.Errorf("workset: nil store")
	}
	if p.JobID == "" {
		return ClaimResult{}, fmt.Errorf("workset: empty job_id")
	}
	ttl := p.LeaseTTL
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	fence := uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	entry := &Entry{
		JobID:      p.JobID,
		Payload:    append([]byte(nil), p.Payload...),
		Topic:      p.Topic,
		Partition:  p.Partition,
		Offset:     p.Offset,
		ConsumerID: p.ConsumerID,
		Fence:      fence,
		ClaimedAt:  now,
		Runtime:    "go",
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return ClaimResult{}, err
	}
	res, err := s.client.Eval(ctx, claimLua,
		[]string{jobKey(p.JobID), byConsumerKey(p.ConsumerID), indexKey, liveConsumerPrefix},
		p.JobID, p.ConsumerID, fence, string(raw), int(ttl.Seconds()),
	).Int()
	if err != nil {
		return ClaimResult{}, err
	}
	switch res {
	case 1:
		return ClaimResult{Won: true, Fence: fence, Entry: entry}, nil
	case 2:
		// Resume prior claim by this consumer (crash between claim and kafka ack).
		existing, err := s.getEntry(ctx, p.JobID)
		if err != nil || existing == nil {
			return ClaimResult{Won: false}, err
		}
		return ClaimResult{Won: true, Fence: existing.Fence, Entry: existing}, nil
	default:
		return ClaimResult{Won: false}, nil
	}
}

func (s *Store) getEntry(ctx context.Context, jobID string) (*Entry, error) {
	raw, err := s.client.Get(ctx, jobKey(jobID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// Renew extends the TTL on an owned job (call periodically during long performs).
func (s *Store) Renew(ctx context.Context, jobID, consumerID, fence string, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil || jobID == "" {
		return false, nil
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	n, err := s.client.Eval(ctx, renewLua,
		[]string{jobKey(jobID)},
		consumerID, fence, int(ttl.Seconds()),
	).Int()
	return n == 1, err
}

// StillOwned reports whether this consumer still holds the fence (pre-completion check).
func (s *Store) StillOwned(ctx context.Context, jobID, consumerID, fence string) (bool, error) {
	if s == nil || s.client == nil || jobID == "" {
		return false, nil
	}
	raw, err := s.client.Get(ctx, jobKey(jobID)).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil {
		return false, err
	}
	return e.ConsumerID == consumerID && e.Fence == fence, nil
}

// Complete removes ownership after a successful terminal outcome (event/retry/DLT applied).
func (s *Store) Complete(ctx context.Context, jobID, consumerID, fence string) error {
	if s == nil || s.client == nil || jobID == "" {
		return nil
	}
	_, err := s.client.Eval(ctx, completeLua,
		[]string{jobKey(jobID), byConsumerKey(consumerID), indexKey},
		jobID, consumerID, fence,
	).Result()
	return err
}

// ListOrphans returns working-set entries whose consumer heartbeat is missing.
// grace is how long after claimed_at we wait before considering a never-hearted consumer dead;
// heartbeat absence is the primary signal (EXISTS live:consumer).
func (s *Store) ListOrphans(ctx context.Context, limit int) ([]Entry, error) {
	if s == nil || s.client == nil {
		return nil, nil
	}
	if limit < 1 {
		limit = 100
	}
	ids, err := s.client.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0)
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		raw, err := s.client.Get(ctx, jobKey(id)).Bytes()
		if err == redis.Nil {
			_ = s.client.SRem(ctx, indexKey, id).Err()
			continue
		}
		if err != nil {
			return out, err
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			continue
		}
		alive, err := s.client.Exists(ctx, liveKey(e.ConsumerID)).Result()
		if err != nil {
			return out, err
		}
		if alive == 0 {
			out = append(out, e)
		}
	}
	return out, nil
}

// BeginReclaim takes a short NX lock so two reconcilers do not double-push.
func (s *Store) BeginReclaim(ctx context.Context, jobID string, lockTTL time.Duration) (bool, error) {
	if s == nil || s.client == nil || jobID == "" {
		return false, nil
	}
	if lockTTL <= 0 {
		lockTTL = 30 * time.Second
	}
	ok, err := s.client.SetNX(ctx, reclaimingKey(jobID), "1", lockTTL).Result()
	return ok, err
}

// FinishReclaim removes the working-set entry after a successful re-produce.
func (s *Store) FinishReclaim(ctx context.Context, e Entry) error {
	if s == nil || s.client == nil {
		return nil
	}
	_, err := s.client.Eval(ctx, finishReclaimLua,
		[]string{jobKey(e.JobID), byConsumerKey(e.ConsumerID), indexKey, reclaimingKey(e.JobID)},
		e.JobID, e.Fence,
	).Result()
	return err
}

// AbortReclaim drops the reclaim lock without removing the job (produce failed).
func (s *Store) AbortReclaim(ctx context.Context, jobID string) error {
	if s == nil || s.client == nil || jobID == "" {
		return nil
	}
	return s.client.Del(ctx, reclaimingKey(jobID)).Err()
}

// TouchConsumer refreshes the live:consumer heartbeat for a SuperFetch member ID.
// Must use the same consumerID stored on working-set entries so reclaim does not
// false-positive while the member is alive.
func (s *Store) TouchConsumer(ctx context.Context, consumerID string, ttl time.Duration) error {
	if s == nil || s.client == nil || consumerID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return s.client.Set(ctx, liveKey(consumerID), "1", ttl).Err()
}
