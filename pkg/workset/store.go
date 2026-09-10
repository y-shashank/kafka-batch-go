// Package workset implements Sidekiq SuperFetch-style in-flight job ownership.
//
// Kafka delivers jobs and acks offsets after a successful Redis claim. Redis
// tracks who owns each running job so a reconciler can re-produce payloads when
// a consumer dies (heartbeat missing for longer than the orphan grace).
package workset

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
	"github.com/y-shashank/kafka-batch-go/pkg/liveness"
)

const (
	jobKeyPrefix       = "kafka_batch:work:job:"
	byConsumerPrefix   = "kafka_batch:work:by_consumer:"
	indexKey           = "kafka_batch:work:index" // ZSET score=claimed_at_unix
	reclaimingPrefix   = "kafka_batch:work:reclaiming:"
	producedPrefix     = "kafka_batch:work:produced:"
	liveConsumerPrefix = "kafka_batch:live:consumer:"

	// DefaultOrphanGrace is how long after claim we wait before treating a
	// missing heartbeat as death (≈2× default 20s heartbeat interval).
	DefaultOrphanGrace = 40 * time.Second
	defaultLeaseTTL    = 2 * time.Minute
	defaultHeartbeatTTL = 180 * time.Second
	producedMarkerTTL  = time.Hour
	// reclaimSafetyBuffer covers the reclaim-loop interval (default 30s) plus a
	// margin, used by the lease-TTL floor below.
	reclaimSafetyBuffer = 60 * time.Second
)

// floorLeaseTTL enforces the invariant that a claim's payload lease must outlive
// the window in which reclaim can act on a dead consumer: lease >
// heartbeat_ttl + orphan_grace + reclaim_interval. Reclaim only re-produces a
// crashed consumer's in-flight jobs once its live:consumer heartbeat has expired
// (up to hbTTL after death); if the payload lease expires first, ListOrphans
// finds an index entry with no payload and can only prune it — the job is lost
// (its Kafka offset was already committed at claim). The shipped daemon raises
// the lease before claiming (pkg/daemon/superfetch.go), but enforcing the floor
// here too means a direct pkg/workset.Store caller cannot bypass the invariant.
func floorLeaseTTL(lease, hbTTL, grace time.Duration) time.Duration {
	if min := hbTTL + grace + reclaimSafetyBuffer; lease < min {
		return min
	}
	return lease
}

// Entry is one in-flight job owned by a consumer.
type Entry struct {
	JobID         string `json:"job_id"`
	Payload       []byte `json:"payload"` // Kafka value (raw, or gzip when Encoding=gzip)
	Encoding      string `json:"encoding,omitempty"` // "" legacy raw; "gzip" compressed body
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Offset        int64  `json:"offset"`
	ConsumerID    string `json:"consumer_id"`
	Fence         string `json:"fence"`
	ClaimedAt     string `json:"claimed_at"`
	ClaimedAtUnix int64  `json:"claimed_at_unix"`
	Runtime       string `json:"runtime"`
}

// ClaimParams is the write-ahead ownership request before Kafka ack.
type ClaimParams struct {
	JobID        string
	Payload      []byte
	Topic        string
	Partition    int32
	Offset       int64
	ConsumerID   string
	LeaseTTL     time.Duration
	HeartbeatTTL time.Duration
	// StealGrace is the minimum age before stealing from a dead owner.
	// 0 uses DefaultOrphanGrace; negative disables the age gate (tests only).
	StealGrace time.Duration
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
	// dltTopic, when set, is where reclaim parks deterministically un-reclaimable
	// payloads (decode/encode/missing-topic) instead of retrying them until the
	// lease TTL expires and the job is silently lost. Empty ⇒ prior behavior.
	dltTopic string
}

func NewStore(client *redis.Client) *Store {
	return &Store{client: client}
}

// SetDLTTopic sets the dead-letter topic used for un-reclaimable orphan payloads.
func (s *Store) SetDLTTopic(topic string) {
	if s != nil {
		s.dltTopic = topic
	}
}

func jobKey(jobID string) string         { return jobKeyPrefix + jobID }
func byConsumerKey(id string) string     { return byConsumerPrefix + id }
func reclaimingKey(jobID string) string  { return reclaimingPrefix + jobID }
func producedKey(jobID string) string    { return producedPrefix + jobID }
func liveKey(consumerID string) string   { return liveConsumerPrefix + consumerID }

func resolveGrace(d time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	if d == 0 {
		return DefaultOrphanGrace
	}
	return d
}

// Claim atomically takes ownership of jobID (or steals from a dead consumer past grace).
// On Won=true the caller must Kafka-ack then perform; call Complete when done.
// Claim also SETs live:consumer:{id} so reclaim cannot race the first heartbeat.
func (s *Store) Claim(ctx context.Context, p ClaimParams) (ClaimResult, error) {
	if s == nil || s.client == nil {
		return ClaimResult{}, fmt.Errorf("workset: nil store")
	}
	if p.JobID == "" {
		return ClaimResult{}, fmt.Errorf("workset: empty job_id")
	}
	ttl := p.LeaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	hbTTL := p.HeartbeatTTL
	if hbTTL <= 0 {
		hbTTL = defaultHeartbeatTTL
	}
	grace := resolveGrace(p.StealGrace)
	ttl = floorLeaseTTL(ttl, hbTTL, grace)
	fence := uuid.NewString()
	now := time.Now().UTC()
	entry := &Entry{
		JobID:         p.JobID,
		Payload:       append([]byte(nil), p.Payload...),
		Topic:         p.Topic,
		Partition:     p.Partition,
		Offset:        p.Offset,
		ConsumerID:    p.ConsumerID,
		Fence:         fence,
		ClaimedAt:     now.Format(time.RFC3339Nano),
		ClaimedAtUnix: now.Unix(),
		Runtime:       "go",
	}
	raw, err := marshalEntryJSON(entry)
	if err != nil {
		return ClaimResult{}, err
	}
	res, err := claimScript.Run(ctx, s.client,
		[]string{jobKey(p.JobID), byConsumerKey(p.ConsumerID), indexKey, liveConsumerPrefix},
		p.JobID, p.ConsumerID, fence, string(raw),
		int(ttl.Seconds()), now.Unix(), int(grace.Seconds()), int(hbTTL.Seconds()),
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

// ClaimMany pipelines one claim per params entry and returns results and
// per-entry errors, both indexed 1:1 with params. A per-entry error means that
// claim's outcome is UNKNOWN (transport/script failure) — the caller must not
// ack that record; results for other entries are still valid. Entries that
// resumed a prior claim (crash between claim and Kafka ack) are resolved to
// their existing fence exactly like Claim.
func (s *Store) ClaimMany(ctx context.Context, params []ClaimParams) ([]ClaimResult, []error) {
	results := make([]ClaimResult, len(params))
	errs := make([]error, len(params))
	if s == nil || s.client == nil {
		for i := range errs {
			errs[i] = fmt.Errorf("workset: nil store")
		}
		return results, errs
	}
	if len(params) == 0 {
		return results, errs
	}

	now := time.Now().UTC()
	entries := make([]*Entry, len(params))
	type callArgs struct {
		keys []string
		argv []interface{}
	}
	calls := make([]callArgs, len(params))
	for i, p := range params {
		if p.JobID == "" {
			errs[i] = fmt.Errorf("workset: empty job_id")
			continue
		}
		ttl := p.LeaseTTL
		if ttl <= 0 {
			ttl = defaultLeaseTTL
		}
		hbTTL := p.HeartbeatTTL
		if hbTTL <= 0 {
			hbTTL = defaultHeartbeatTTL
		}
		grace := resolveGrace(p.StealGrace)
		ttl = floorLeaseTTL(ttl, hbTTL, grace)
		fence := uuid.NewString()
		entry := &Entry{
			JobID:         p.JobID,
			Payload:       append([]byte(nil), p.Payload...),
			Topic:         p.Topic,
			Partition:     p.Partition,
			Offset:        p.Offset,
			ConsumerID:    p.ConsumerID,
			Fence:         fence,
			ClaimedAt:     now.Format(time.RFC3339Nano),
			ClaimedAtUnix: now.Unix(),
			Runtime:       "go",
		}
		raw, err := marshalEntryJSON(entry)
		if err != nil {
			errs[i] = err
			continue
		}
		entries[i] = entry
		calls[i] = callArgs{
			keys: []string{jobKey(p.JobID), byConsumerKey(p.ConsumerID), indexKey, liveConsumerPrefix},
			argv: []interface{}{p.JobID, p.ConsumerID, entry.Fence, string(raw),
				int(ttl.Seconds()), now.Unix(), int(grace.Seconds()), int(hbTTL.Seconds())},
		}
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(params))
	for i := range params {
		if errs[i] != nil || entries[i] == nil {
			continue
		}
		cmds[i] = claimScript.EvalSha(ctx, pipe, calls[i].keys, calls[i].argv...)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		for i := range params {
			if errs[i] == nil && cmds[i] != nil && cmds[i].Err() != nil {
				errs[i] = cmds[i].Err()
			}
		}
	}
	// NOSCRIPT (cold script cache): retry only the failed commands with EVAL,
	// which caches the script server-side. Never re-run commands that
	// succeeded — the claim script has side effects.
	var retryIdx []int
	for i, cmd := range cmds {
		if cmd != nil && cmd.Err() != nil && redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
			retryIdx = append(retryIdx, i)
		}
	}
	if len(retryIdx) > 0 {
		rpipe := s.client.Pipeline()
		rcmds := make([]*redis.Cmd, len(retryIdx))
		for j, i := range retryIdx {
			rcmds[j] = claimScript.Eval(ctx, rpipe, calls[i].keys, calls[i].argv...)
		}
		if _, err := rpipe.Exec(ctx); err != nil && err != redis.Nil {
			// Individual cmd errors below still decide per-entry outcomes.
			_ = err
		}
		for j, i := range retryIdx {
			cmds[i] = rcmds[j]
		}
	}

	// Resolve results; resumed claims (code 2) need the existing entry's fence.
	var resumeIdx []int
	for i := range params {
		if errs[i] != nil || cmds[i] == nil {
			continue
		}
		res, err := cmds[i].Int()
		if err != nil {
			errs[i] = err
			continue
		}
		switch res {
		case 1:
			results[i] = ClaimResult{Won: true, Fence: entries[i].Fence, Entry: entries[i]}
		case 2:
			resumeIdx = append(resumeIdx, i)
		default:
			results[i] = ClaimResult{Won: false}
		}
	}
	if len(resumeIdx) > 0 {
		gpipe := s.client.Pipeline()
		gcmds := make([]*redis.StringCmd, len(resumeIdx))
		for j, i := range resumeIdx {
			gcmds[j] = gpipe.Get(ctx, jobKey(params[i].JobID))
		}
		if _, err := gpipe.Exec(ctx); err != nil && err != redis.Nil {
			_ = err
		}
		for j, i := range resumeIdx {
			raw, err := gcmds[j].Bytes()
			if err == redis.Nil {
				results[i] = ClaimResult{Won: false}
				continue
			}
			if err != nil {
				errs[i] = err
				continue
			}
			var e Entry
			if uerr := json.Unmarshal(raw, &e); uerr != nil {
				errs[i] = uerr
				continue
			}
			results[i] = ClaimResult{Won: true, Fence: e.Fence, Entry: &e}
		}
	}
	return results, errs
}

// RenewEntry is one (jobID, fence) lease to extend.
type RenewEntry struct {
	JobID string
	Fence string
}

// RenewMany pipelines lease renewals. ok[i] is false when the fence no longer
// matches (entry stolen/completed); errs[i] carries transient failures (the
// caller should keep renewing those).
func (s *Store) RenewMany(ctx context.Context, consumerID string, entries []RenewEntry, ttl time.Duration) ([]bool, []error) {
	ok := make([]bool, len(entries))
	errs := make([]error, len(entries))
	if s == nil || s.client == nil || len(entries) == 0 {
		return ok, errs
	}
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(entries))
	for i, e := range entries {
		cmds[i] = renewScript.EvalSha(ctx, pipe, []string{jobKey(e.JobID)}, consumerID, e.Fence, int(ttl.Seconds()))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil && !redis.HasErrorPrefix(err, "NOSCRIPT") {
		_ = err
	}
	var retryIdx []int
	for i, cmd := range cmds {
		if cmd.Err() != nil && redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
			retryIdx = append(retryIdx, i)
		}
	}
	if len(retryIdx) > 0 {
		rpipe := s.client.Pipeline()
		rcmds := make([]*redis.Cmd, len(retryIdx))
		for j, i := range retryIdx {
			e := entries[i]
			rcmds[j] = renewScript.Eval(ctx, rpipe, []string{jobKey(e.JobID)}, consumerID, e.Fence, int(ttl.Seconds()))
		}
		if _, err := rpipe.Exec(ctx); err != nil && err != redis.Nil {
			_ = err
		}
		for j, i := range retryIdx {
			cmds[i] = rcmds[j]
		}
	}
	for i, cmd := range cmds {
		n, err := cmd.Int()
		if err != nil {
			errs[i] = err
			continue
		}
		ok[i] = n == 1
	}
	return ok, errs
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
		ttl = defaultLeaseTTL
	}
	n, err := renewScript.Run(ctx, s.client,
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
	_, err := completeScript.Run(ctx, s.client,
		[]string{jobKey(jobID), byConsumerKey(consumerID), indexKey},
		jobID, consumerID, fence,
	).Result()
	return err
}

// ListOrphans returns working-set entries older than grace whose consumer
// heartbeat is missing. Uses ZRANGEBYSCORE on the claimed_at index (not SMEMBERS).
func (s *Store) ListOrphans(ctx context.Context, limit int, grace time.Duration) ([]Entry, error) {
	if s == nil || s.client == nil {
		return nil, nil
	}
	if limit < 1 {
		limit = 100
	}
	grace = resolveGrace(grace)
	maxScore := time.Now().Unix() - int64(grace.Seconds())
	// Fetch a bit more than limit so EXISTS filtering still fills the batch.
	ids, err := s.client.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", maxScore),
		Offset: 0,
		Count:  int64(limit * 3),
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// Pass 1: pipeline GETs for all aged candidates.
	gpipe := s.client.Pipeline()
	getCmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		getCmds[i] = gpipe.Get(ctx, jobKey(id))
	}
	_, _ = gpipe.Exec(ctx)

	type candidate struct {
		entry Entry
	}
	candidates := make([]candidate, 0, len(ids))
	missing := make([]string, 0)
	for i, id := range ids {
		raw, err := getCmds[i].Bytes()
		if err == redis.Nil || (err == nil && len(raw) == 0) {
			missing = append(missing, id)
			continue
		}
		if err != nil {
			return nil, err
		}
		var e Entry
		if err := json.Unmarshal(raw, &e); err != nil {
			// Poisoned entry: the value exists but cannot decode. Skipping
			// WITHOUT pruning left it at the front of the aged window forever —
			// ≥ 3×limit of these starve the sweep and real orphans behind them
			// are never listed. Prune it like the missing-payload case (loudly:
			// its job cannot be reclaimed).
			log.Printf("[kbatch-workset] list_orphans: job %s payload does not decode (%v) — pruning from index; job NOT reclaimable", id, err)
			instrument.WorksetPayloadMissing(1)
			missing = append(missing, id)
			continue
		}
		candidates = append(candidates, candidate{entry: e})
	}
	if len(missing) > 0 {
		log.Printf("[kbatch-workset] list_orphans: %d aged index entries missing job payloads (lease TTL expiry?) — cannot reclaim without payload; JOBS LOST (offset already committed). Ensure super_fetch_lease_ttl > liveness_ttl + orphan_grace + reclaim_interval",
			len(missing))
		instrument.WorksetPayloadMissing(len(missing))
		rpipe := s.client.Pipeline()
		for _, id := range missing {
			rpipe.ZRem(ctx, indexKey, id)
		}
		_, _ = rpipe.Exec(ctx)
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Pass 2: pipeline EXISTS for distinct consumer IDs (many orphans share a dead pod).
	aliveByConsumer := make(map[string]bool, len(candidates))
	unique := make([]string, 0, len(candidates))
	for _, c := range candidates {
		cid := c.entry.ConsumerID
		if _, seen := aliveByConsumer[cid]; seen {
			continue
		}
		aliveByConsumer[cid] = false
		unique = append(unique, cid)
	}
	epipe := s.client.Pipeline()
	existCmds := make([]*redis.IntCmd, len(unique))
	for i, cid := range unique {
		existCmds[i] = epipe.Exists(ctx, liveKey(cid))
	}
	if _, err := epipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for i, cid := range unique {
		n, err := existCmds[i].Result()
		if err != nil {
			return nil, err
		}
		aliveByConsumer[cid] = n > 0
	}

	out := make([]Entry, 0, limit)
	for _, c := range candidates {
		if len(out) >= limit {
			break
		}
		if !aliveByConsumer[c.entry.ConsumerID] {
			out = append(out, c.entry)
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

// MarkProduced records that a reclaim produce already succeeded so a later
// FinishReclaim failure cannot cause a second Kafka produce.
func (s *Store) MarkProduced(ctx context.Context, jobID, fence string, ttl time.Duration) error {
	if s == nil || s.client == nil || jobID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = producedMarkerTTL
	}
	return s.client.Set(ctx, producedKey(jobID), fence, ttl).Err()
}

// ProducedFence returns the fence stored by MarkProduced, or "" if none.
func (s *Store) ProducedFence(ctx context.Context, jobID string) (string, error) {
	if s == nil || s.client == nil || jobID == "" {
		return "", nil
	}
	v, err := s.client.Get(ctx, producedKey(jobID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// FinishReclaim removes the working-set entry after a successful re-produce.
// Returns 1 on success, 0 if the fence no longer matches (entry stolen/changed).
func (s *Store) FinishReclaim(ctx context.Context, e Entry) (int, error) {
	if s == nil || s.client == nil {
		return 1, nil
	}
	n, err := finishReclaimScript.Run(ctx, s.client,
		[]string{jobKey(e.JobID), byConsumerKey(e.ConsumerID), indexKey, reclaimingKey(e.JobID), producedKey(e.JobID)},
		e.JobID, e.Fence,
	).Int()
	return n, err
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
// false-positive while the member is alive. Writes JSON (rss/cpu) so the Ruby
// /live dashboard can show SuperFetch members the same way as poll consumers.
func (s *Store) TouchConsumer(ctx context.Context, consumerID string, ttl time.Duration) error {
	if s == nil || s.client == nil || consumerID == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultHeartbeatTTL
	}
	payload := liveness.ConsumerHeartbeatJSON(consumerID, "", liveness.DefaultProcessSampler())
	return s.client.Set(ctx, liveKey(consumerID), payload, ttl).Err()
}

// DeleteConsumer removes live:consumer:{id} so reclaim can proceed immediately
// after a graceful drain that still has workset leftovers (skip waiting for TTL).
func (s *Store) DeleteConsumer(ctx context.Context, consumerID string) error {
	if s == nil || s.client == nil || consumerID == "" {
		return nil
	}
	return s.client.Del(ctx, liveKey(consumerID)).Err()
}
