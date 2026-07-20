package fairness

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// reclaimClaimTTL bounds how long a reclaim-in-progress marker lives. It only needs to
// outlast one produce+confirm round trip; sized generously since a false "already
// claimed" skip just means this tick's reclaim retries on the next tick.
const reclaimClaimTTL = 30 * time.Second

// CheckoutResult is returned by Scheduler.Checkout.
type CheckoutResult struct {
	TenantID string
	Payload  []byte
	SlotID   string
}

// StaleForward is an orphaned forwarding-buffer entry.
type StaleForward struct {
	SlotID   string
	TenantID string
	Payload  []byte
}

// Stats is a scheduler snapshot for dashboards/tests.
type Stats struct {
	ActiveTenants   int64
	InflightTotal   int64
	ForwardingDepth int64
	Budget          int
	Window          int
}

type activeView struct {
	count     int
	sumWeight float64
}

// Scheduler is the Redis WFQ scheduler for one fairness lane.
type Scheduler struct {
	Lane     Lane
	Client   *redis.Client
	Settings Settings

	activeMu     sync.Mutex
	activeView   activeView
	activeViewAt time.Time

	weightMu       sync.Mutex
	weightsCache   map[string]float64
	weightsCacheAt time.Time
}

func NewScheduler(client *redis.Client, settings Settings) *Scheduler {
	if settings.ReadyWindow <= 0 {
		settings.ReadyWindow = 500
	}
	if settings.DefaultWeight <= 0 {
		settings.DefaultWeight = 1.0
	}
	return &Scheduler{Lane: settings.Lane, Client: client, Settings: settings}
}

func (s *Scheduler) Enqueue(ctx context.Context, tenantID string, payload []byte) (bool, error) {
	if err := ValidateLane(s.Lane); err != nil {
		return false, err
	}
	res, err := s.Client.Eval(ctx, EnqueueLua,
		[]string{ringKey(s.Lane), vtimeKey(s.Lane)},
		tenantID, string(payload), s.Settings.ReadyWindow, readyPrefix(s.Lane),
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (s *Scheduler) Checkout(ctx context.Context) (*CheckoutResult, error) {
	if err := ValidateLane(s.Lane); err != nil {
		return nil, err
	}
	view := s.activeViewCached()
	slotID := uuid.NewString()
	weighted := s.Settings.weightedFlag()

	var res []interface{}
	var err error
	if s.Lane == LaneTime {
		res, err = s.Client.Eval(ctx, CheckoutLuaTime,
			[]string{ringKey(s.Lane), leasesKey(s.Lane), weightKey(s.Lane), forwardingKey(s.Lane), forwardingMetaKey(s.Lane)},
			s.Settings.GlobalConcurrency, s.Settings.MaxInflightPerTenant, readyPrefix(s.Lane),
			s.Settings.fetchN(), s.Settings.DefaultWeight, weighted,
			view.count, view.sumWeight,
			"0", s.Settings.EffectiveLeaseTTL(), slotID, leasePrefix(s.Lane),
		).Slice()
	} else {
		res, err = s.Client.Eval(ctx, CheckoutLuaCount,
			[]string{ringKey(s.Lane), vtimeKey(s.Lane), leasesKey(s.Lane), weightKey(s.Lane), forwardingKey(s.Lane), forwardingMetaKey(s.Lane)},
			s.Settings.GlobalConcurrency, s.Settings.MaxInflightPerTenant, readyPrefix(s.Lane),
			s.Settings.DefaultWeight, s.Settings.fetchN(), weighted,
			view.count, view.sumWeight,
			"0", s.Settings.EffectiveLeaseTTL(), slotID, leasePrefix(s.Lane),
		).Slice()
	}
	if err != nil {
		return nil, err
	}
	if len(res) < 1 {
		return nil, nil
	}
	code, _ := res[0].(int64)
	if code != 1 {
		return nil, nil
	}
	tenant, _ := res[1].(string)
	payload, _ := res[2].(string)
	return &CheckoutResult{TenantID: tenant, Payload: []byte(payload), SlotID: slotID}, nil
}

func (s *Scheduler) ConfirmForward(ctx context.Context, slotID string) (bool, error) {
	if slotID == "" {
		return false, nil
	}
	n, err := s.Client.Eval(ctx, ConfirmForwardLua,
		[]string{forwardingKey(s.Lane), forwardingMetaKey(s.Lane)}, slotID,
	).Int()
	return n == 1, err
}

func (s *Scheduler) AbortForward(ctx context.Context, slotID, tenantID string) (bool, error) {
	if slotID == "" || tenantID == "" {
		return false, nil
	}
	var n int
	var err error
	if s.Lane == LaneTime {
		n, err = s.Client.Eval(ctx, AbortForwardLuaTime,
			[]string{forwardingKey(s.Lane), forwardingMetaKey(s.Lane), leasesKey(s.Lane), TenantLeaseKey(s.Lane, tenantID)},
			slotID, tenantID, readyPrefix(s.Lane),
		).Int()
	} else {
		n, err = s.Client.Eval(ctx, AbortForwardLuaCount,
			[]string{forwardingKey(s.Lane), forwardingMetaKey(s.Lane), leasesKey(s.Lane), TenantLeaseKey(s.Lane, tenantID), ringKey(s.Lane), vtimeKey(s.Lane), weightKey(s.Lane)},
			slotID, tenantID, readyPrefix(s.Lane), s.Settings.DefaultWeight,
		).Int()
	}
	return n == 1, err
}

func (s *Scheduler) Complete(ctx context.Context, tenantID, slotID string, durationSec float64) error {
	if slotID == "" && s.Lane == LaneThroughput {
		return nil
	}
	if s.Lane == LaneThroughput {
		if slotID == "" {
			return nil
		}
		_, err := s.Client.Eval(ctx, CompleteLuaCountLease,
			[]string{leasesKey(s.Lane), TenantLeaseKey(s.Lane, tenantID)},
			slotID,
		).Result()
		return err
	}
	w := s.WeightFor(ctx, tenantID)
	inc := durationSec
	if w > 0 {
		inc = durationSec / w
	}
	if slotID == "" {
		_, err := s.Client.Eval(ctx, CompleteLuaTimeLegacy,
			[]string{vtimeKey(s.Lane), ringKey(s.Lane)},
			tenantID, inc, readyPrefix(s.Lane),
		).Result()
		return err
	}
	_, err := s.Client.Eval(ctx, CompleteLuaTimeLease,
		[]string{leasesKey(s.Lane), TenantLeaseKey(s.Lane, tenantID), vtimeKey(s.Lane), ringKey(s.Lane)},
		tenantID, inc, readyPrefix(s.Lane), slotID,
	).Result()
	return err
}

func (s *Scheduler) RenewLease(ctx context.Context, tenantID, slotID string) error {
	if slotID == "" {
		return nil
	}
	expiry := float64(time.Now().UnixNano())/1e9 + s.Settings.EffectiveLeaseTTL()
	pipe := s.Client.Pipeline()
	pipe.ZAddArgs(ctx, leasesKey(s.Lane), redis.ZAddArgs{
		XX: true, Members: []redis.Z{{Score: expiry, Member: slotID}},
	})
	pipe.ZAddArgs(ctx, TenantLeaseKey(s.Lane, tenantID), redis.ZAddArgs{
		XX: true, Members: []redis.Z{{Score: expiry, Member: slotID}},
	})
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Scheduler) rearmLease(ctx context.Context, tenantID, slotID string) error {
	expiry := float64(time.Now().UnixNano())/1e9 + s.Settings.EffectiveLeaseTTL()
	_, err := s.Client.Eval(ctx, RearmLeaseLua,
		[]string{leasesKey(s.Lane), TenantLeaseKey(s.Lane, tenantID)},
		slotID, expiry,
	).Result()
	return err
}

// SlotLeaseActive reports whether the slot still holds a live lease — i.e. its
// holder is presumed alive and renewing. Returns the lease expiry (unix seconds)
// when present. Used to distinguish an orphaned slot (holder died, lease gone or
// expired → safe to re-run) from a slot whose holder is still working (defer and
// re-check after expiry rather than double-run).
func (s *Scheduler) SlotLeaseActive(ctx context.Context, slotID string) (active bool, expiry float64, err error) {
	if slotID == "" {
		return false, 0, nil
	}
	score, err := s.Client.ZScore(ctx, leasesKey(s.Lane), slotID).Result()
	if err == redis.Nil {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	now := float64(time.Now().UnixNano()) / 1e9
	return score > now, score, nil
}

func (s *Scheduler) ClaimSlotExecution(ctx context.Context, slotID string) (bool, error) {
	if slotID == "" {
		return true, nil
	}
	ok, err := s.Client.SetNX(ctx, SlotDedupKey(s.Lane, slotID), "1", time.Duration(s.Settings.slotDedupTTL())*time.Second).Result()
	if err != nil {
		return true, err
	}
	return ok, nil
}

// ClearSlotExecution removes the fair slot dedup key so a SuperFetch reclaim can re-run.
func (s *Scheduler) ClearSlotExecution(ctx context.Context, slotID string) error {
	if s == nil || s.Client == nil || slotID == "" {
		return nil
	}
	return s.Client.Del(ctx, SlotDedupKey(s.Lane, slotID)).Err()
}

func (s *Scheduler) ListStaleForwards(ctx context.Context) ([]StaleForward, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	grace := s.Settings.ForwardingRecoveryGrace
	entries, err := s.Client.HGetAll(ctx, forwardingKey(s.Lane)).Result()
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	out := make([]StaleForward, 0)
	for slotID, payload := range entries {
		exp, err := s.Client.ZScore(ctx, leasesKey(s.Lane), slotID).Result()
		if err != nil && err != redis.Nil {
			return out, err
		}
		if err == nil {
			// Lease record still present: only reclaim once it's actually expired *and*
			// past the grace window (gives an in-flight renewal a chance to land before
			// we steal the slot).
			if exp > now || (now-exp) < grace {
				continue
			}
		}
		// err == redis.Nil here means the lease record is gone entirely. Checkout creates
		// the forwarding-buffer entry and its lease atomically in one EVAL, and every
		// legitimate teardown path (ConfirmForward, AbortForward, Complete) clears the
		// forwarding-buffer entry no later than it clears the lease. So a forwarding entry
		// that still exists with NO lease at all can only mean the producing process
		// crashed between checkout and confirm/abort, and ReclaimExpiredLeases has since
		// swept the (unconditional, no-grace) lease record. That is *more* stale than an
		// expired-but-present lease, not less: skipping it (the previous behavior) silently
		// orphaned the payload forever, since ReclaimExpiredLeases runs on the same cadence
		// and always purges the lease before this grace window would have elapsed.
		tenant, _ := s.Client.HGet(ctx, forwardingMetaKey(s.Lane), slotID).Result()
		if tenant == "" {
			tenant = tenantFromPayload([]byte(payload))
		}
		if tenant == "" {
			continue
		}
		out = append(out, StaleForward{SlotID: slotID, TenantID: tenant, Payload: []byte(payload)})
	}
	return out, nil
}

func (s *Scheduler) ReclaimStaleForward(ctx context.Context, entry StaleForward, produce func(payload []byte, key string) error) error {
	// Claim the reclaim atomically before producing. Without this, two forwarder replicas
	// (a normal horizontally-scaled deployment) can both list the same stale entry and
	// both call produce() before either wins the ConfirmForward race, duplicating the job
	// onto the ready topic.
	won, err := s.Client.SetNX(ctx, reclaimClaimKey(s.Lane, entry.SlotID), "1", reclaimClaimTTL).Result()
	if err != nil {
		return err
	}
	if !won {
		return nil
	}
	marked, key, err := markSlot(entry.Payload, entry.TenantID, entry.SlotID, s.Lane)
	if err != nil {
		return err
	}
	if err := produce(marked, key); err != nil {
		_, _ = s.AbortForward(ctx, entry.SlotID, entry.TenantID)
		return err
	}
	if _, err := s.ConfirmForward(ctx, entry.SlotID); err != nil {
		return err
	}
	return s.rearmLease(ctx, entry.TenantID, entry.SlotID)
}

func (s *Scheduler) ReclaimExpiredLeases(ctx context.Context) (int64, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	ok, err := s.Client.SetNX(ctx, reclaimLockKey(s.Lane), "1", 25*time.Second).Result()
	if err != nil || !ok {
		return 0, err
	}
	n, err := s.Client.ZRemRangeByScore(ctx, leasesKey(s.Lane), "-inf", fmt.Sprintf("%f", now)).Result()
	if err != nil {
		return 0, err
	}
	iter := s.Client.Scan(ctx, 0, leasePrefix(s.Lane)+"*", 500).Iterator()
	for iter.Next(ctx) {
		_ = s.Client.ZRemRangeByScore(ctx, iter.Val(), "-inf", fmt.Sprintf("%f", now)).Err()
	}
	return n, iter.Err()
}

func (s *Scheduler) ReadyDepth(ctx context.Context, tenantID string) (int64, error) {
	return s.Client.LLen(ctx, ReadyKey(s.Lane, tenantID)).Result()
}

func (s *Scheduler) RingSize(ctx context.Context) (int64, error) {
	return s.Client.ZCard(ctx, ringKey(s.Lane)).Result()
}

func (s *Scheduler) Stats(ctx context.Context) (Stats, error) {
	now := float64(time.Now().UnixNano()) / 1e9
	pipe := s.Client.Pipeline()
	active := pipe.ZCard(ctx, ringKey(s.Lane))
	live := pipe.ZCount(ctx, leasesKey(s.Lane), fmt.Sprintf("(%f", now), "+inf")
	fwd := pipe.HLen(ctx, forwardingKey(s.Lane))
	if _, err := pipe.Exec(ctx); err != nil {
		return Stats{}, err
	}
	a, _ := active.Result()
	l, _ := live.Result()
	f, _ := fwd.Result()
	return Stats{
		ActiveTenants: a, InflightTotal: l, ForwardingDepth: f,
		Budget: s.Settings.GlobalConcurrency, Window: s.Settings.ReadyWindow,
	}, nil
}

// ResetVtimeIfQuiescent clears the per-tenant virtual-time ledger (preserving
// weights) iff the lane is fully quiescent on the Redis side: empty ring, no live
// leases, and an empty forwarding buffer. The check and delete run atomically in
// one script, so a tenant enqueuing mid-check cannot have its freshly-seeded vtime
// wiped. Returns true when the reset was applied.
//
// This gives "fresh fairness per active period" — once a lane drains completely,
// the next burst of work starts every tenant even, so a busy period does not carry
// virtual-time debt/credit into the next one. It also bounds unbounded vtime growth
// over long uptimes. The caller is responsible for gating this on a debounce and
// (optionally) zero ingest lag so it never fires during a transient lull.
func (s *Scheduler) ResetVtimeIfQuiescent(ctx context.Context) (bool, error) {
	if err := ValidateLane(s.Lane); err != nil {
		return false, err
	}
	now := float64(time.Now().UnixNano()) / 1e9
	n, err := s.Client.Eval(ctx, ResetVtimeIfQuiescentLua,
		[]string{ringKey(s.Lane), vtimeKey(s.Lane), leasesKey(s.Lane), forwardingKey(s.Lane)},
		fmt.Sprintf("%f", now),
	).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// IngestPending reports whether the lane's ingest topic still has undispatched
// backlog (any partition with consumer-group lag > 0). When no ingest-lag counter
// is configured it returns false, so callers fall back to Redis-only quiescence.
func (s *Scheduler) IngestPending(ctx context.Context) (bool, error) {
	if s.Settings.IngestLag == nil {
		return false, nil
	}
	n, err := s.Settings.IngestLag.IngestActiveCount(ctx, s.Settings.DispatchConsumerGroup, s.Settings.IngestTopic)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Scheduler) Vtime(ctx context.Context, tenantID string) (float64, error) {
	v, err := s.Client.HGet(ctx, vtimeKey(s.Lane), tenantID).Float64()
	if err == redis.Nil {
		return 0, nil
	}
	return v, err
}

func (s *Scheduler) SetWeight(ctx context.Context, tenantID string, weight float64) error {
	s.bustWeightCache()
	return s.Client.HSet(ctx, weightKey(s.Lane), tenantID, weight).Err()
}

func (s *Scheduler) WeightFor(ctx context.Context, tenantID string) float64 {
	s.weightMu.Lock()
	defer s.weightMu.Unlock()
	if s.weightsCache != nil && time.Since(s.weightsCacheAt) < s.Settings.WeightCacheTTL {
		if w, ok := s.weightsCache[tenantID]; ok {
			return w
		}
	}
	all, err := s.Client.HGetAll(ctx, weightKey(s.Lane)).Result()
	if err != nil {
		return s.Settings.DefaultWeight
	}
	cache := make(map[string]float64, len(all)+1)
	for k, v := range all {
		var f float64
		fmt.Sscanf(v, "%f", &f)
		if f > 0 {
			cache[k] = f
		}
	}
	s.weightsCache = cache
	s.weightsCacheAt = time.Now()
	if w, ok := cache[tenantID]; ok {
		return w
	}
	return s.Settings.DefaultWeight
}

func (s *Scheduler) Reset(ctx context.Context) error {
	s.bustWeightCache()
	iter := s.Client.Scan(ctx, 0, ns(s.Lane)+"*", 200).Iterator()
	keys := make([]string, 0)
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return s.Client.Del(ctx, keys...).Err()
	}
	return nil
}

func (s *Scheduler) activeViewCached() activeView {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if time.Since(s.activeViewAt) < s.Settings.ActiveCountTTL && s.activeView.count > 0 {
		return s.activeView
	}
	s.activeView = s.computeActiveView(context.Background())
	s.activeViewAt = time.Now()
	return s.activeView
}

func (s *Scheduler) computeActiveView(ctx context.Context) activeView {
	ringView := s.computeRingLeaseView(ctx)
	if s.Settings.ActiveCountSource == "ingest_lag" && s.Settings.IngestLag != nil {
		n, err := s.Settings.IngestLag.IngestActiveCount(ctx, s.Settings.DispatchConsumerGroup, s.Settings.IngestTopic)
		if err == nil {
			sum := ringView.sumWeight
			// Weighted checkout requires shint > 0 to avoid full-ring ZRANGE in Lua.
			if s.Settings.weightedFlag() == 1 && sum <= 0 && n > 0 {
				dw := s.Settings.DefaultWeight
				if dw <= 0 {
					dw = 1
				}
				sum = dw * float64(n)
			}
			return activeView{count: n, sumWeight: sum}
		}
	}
	return ringView
}

func (s *Scheduler) computeRingLeaseView(ctx context.Context) activeView {
	members, err := s.Client.ZRange(ctx, ringKey(s.Lane), 0, -1).Result()
	if err != nil {
		return activeView{}
	}
	ids := append([]string{}, members...)
	// include tenants with live leases
	iter := s.Client.Scan(ctx, 0, leasePrefix(s.Lane)+"*", 200).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		n, err := s.Client.ZCard(ctx, k).Result()
		if err == nil && n > 0 {
			ids = append(ids, k[len(leasePrefix(s.Lane)):])
		}
	}
	uniq := uniqueStrings(ids)
	sum := 0.0
	if s.Settings.weightedFlag() == 1 {
		for _, t := range uniq {
			sum += s.WeightFor(ctx, t)
		}
	}
	return activeView{count: len(uniq), sumWeight: sum}
}

func (s *Scheduler) bustWeightCache() {
	s.weightMu.Lock()
	s.weightsCache = nil
	s.weightMu.Unlock()
}

func tenantFromPayload(raw []byte) string {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	if t, ok := m["tenant_id"].(string); ok && t != "" {
		return t
	}
	if b, ok := m["batch_id"].(string); ok && b != "" {
		return b
	}
	if j, ok := m["job_id"].(string); ok {
		return j
	}
	return ""
}

func TenantFromMessage(m map[string]interface{}) string {
	if t, ok := m["tenant_id"].(string); ok && t != "" {
		return t
	}
	if b, ok := m["batch_id"].(string); ok && b != "" {
		return b
	}
	if j, ok := m["job_id"].(string); ok && j != "" {
		return j
	}
	return "default"
}

func markSlot(raw []byte, tenantID, slotID string, lane Lane) ([]byte, string, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, "", err
	}
	m["_fair_slot"] = true
	m["_fair_type"] = lane.String()
	m["_fair_slot_id"] = slotID
	if _, ok := m["tenant_id"]; !ok {
		m["tenant_id"] = tenantID
	}
	out, err := json.Marshal(m)
	key, _ := m["job_id"].(string)
	return out, key, err
}

func uniqueStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
