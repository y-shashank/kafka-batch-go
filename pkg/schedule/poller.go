package schedule

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// Producer dispatches due jobs back to execution topics.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// PartitionedProducer optionally sends to an explicit partition.
type PartitionedProducer interface {
	ProducePartition(ctx context.Context, topic, key string, payload []byte, partition int32) error
}

// Router resolves a scheduled payload to a destination.
type Router interface {
	Route(payload map[string]interface{}) (Route, error)
}

// BatchCancelled reports whether a batch should be dropped.
type BatchCancelled func(ctx context.Context, batchID string) (bool, error)

// Poller drains the delayed-job index (Ruby SchedulePoller).
type Poller struct {
	Cfg            config.Daemon
	Store          IndexStore
	Reader         PayloadReader
	Producer       Producer
	Router         Router
	Cancelled      BatchCancelled
	Now            func() time.Time
	RecordActivity func() // optional hook for liveness probes

	lastReclaim time.Time
}

const maxReadMisses = 10

func (p *Poller) Tick(ctx context.Context) (int, error) {
	now := p.now()
	if p.Cfg.ScheduleReclaimEvery > 0 && now.Sub(p.lastReclaim) >= p.Cfg.ScheduleReclaimEvery {
		if _, err := p.Store.Reclaim(ctx, now); err != nil {
			return 0, err
		}
		p.lastReclaim = now
	}

	members, err := p.Store.ClaimDue(ctx, now, p.Cfg.ScheduleLeaseSeconds, p.Cfg.ScheduleBatchSize)
	if err != nil || len(members) == 0 {
		return 0, err
	}

	byPartition := map[int32][]int64{}
	parsed := make([]struct {
		member string
		Member
	}, 0, len(members))
	for _, m := range members {
		pm, ok := ParseMember(m)
		if !ok {
			continue
		}
		parsed = append(parsed, struct {
			member string
			Member
		}{m, pm})
		byPartition[pm.Partition] = append(byPartition[pm.Partition], pm.Offset)
	}
	for part := range byPartition {
		sort.Slice(byPartition[part], func(i, j int) bool { return byPartition[part][i] < byPartition[part][j] })
	}

	read, err := p.Reader.Read(ctx, byPartition)
	if err != nil {
		return 0, err
	}
	lostSet := map[string]struct{}{}
	for _, loc := range read.Lost {
		lostSet[loc] = struct{}{}
	}

	acked := 0
	done := make([]string, 0, len(parsed))
	for _, item := range parsed {
		loc := BuildKey(item.Partition, item.Offset)
		if _, lost := lostSet[loc]; lost {
			log.Printf("[kbatch-schedule] payload missing at %s/%s (retention) — dropping job_id=%s",
				p.Cfg.ScheduledTopic, loc, item.JobID)
			done = append(done, item.member)
			continue
		}

		raw, ok := read.Found[loc]
		if !ok {
			misses, _ := p.Store.RecordReadMiss(ctx, item.member)
			if misses >= maxReadMisses {
				_ = p.Store.ClearReadMiss(ctx, item.member)
				done = append(done, item.member)
			}
			continue
		}
		_ = p.Store.ClearReadMiss(ctx, item.member)

		if p.produceDue(ctx, raw, item.JobID) {
			acked++
			done = append(done, item.member)
		}
	}
	if len(done) > 0 {
		if err := p.Store.Ack(ctx, done); err != nil {
			log.Printf("[kbatch-schedule] ack error (%d members): %v", len(done), err)
		}
	}
	return acked, nil
}

func (p *Poller) produceDue(ctx context.Context, raw []byte, jobID string) bool {
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		log.Printf("[kbatch-schedule] invalid payload job_id=%s: %v", jobID, err)
		return true
	}
	if p.Cfg.SkipCancelledJobs {
		if batchID, _ := data["batch_id"].(string); batchID != "" && p.Cancelled != nil {
			cancelled, err := p.Cancelled(ctx, batchID)
			if err == nil && cancelled {
				workerClass, _ := data["worker_class"].(string)
				instrument.JobCancelled(jobID, batchID, workerClass)
				return true
			}
		}
	}
	route, err := p.Router.Route(data)
	if err != nil {
		// Permanent misconfig (unknown job_type / fairness lane). Do not silent-drop:
		// park on DLT then ACK. If DLT is unset or produce fails, leave the lease so
		// reclaim retries after ops fix the route (or DLT recovers).
		log.Printf("[kbatch-schedule] route job_id=%s: %v", jobID, err)
		return p.parkRouteError(ctx, jobID, raw, data, err)
	}
	key := route.Key
	if key == "" {
		if k, ok := data["job_id"].(string); ok {
			key = k
		}
	}
	var produceErr error
	if route.Partition != nil {
		if pp, ok := p.Producer.(PartitionedProducer); ok {
			produceErr = pp.ProducePartition(ctx, route.Topic, key, raw, *route.Partition)
		} else {
			produceErr = p.Producer.Produce(ctx, route.Topic, key, raw)
		}
	} else {
		produceErr = p.Producer.Produce(ctx, route.Topic, key, raw)
	}
	if produceErr != nil {
		log.Printf("[kbatch-schedule] produce job_id=%s: %v", jobID, produceErr)
		return false
	}
	batchID, _ := data["batch_id"].(string)
	workerClass, _ := data["worker_class"].(string)
	if workerClass == "" {
		if jt, ok := data["job_type"].(string); ok {
			workerClass = jt
		}
	}
	instrument.ScheduledDispatched(jobID, batchID, workerClass, route.Topic)
	return true
}

// parkRouteError publishes an unroutable scheduled job to the dead-letter topic
// and returns true (ACK) on success. Returns false to keep the inflight lease
// when DLT is unavailable so the job is not silently deleted from the index.
func (p *Poller) parkRouteError(ctx context.Context, jobID string, raw []byte, data map[string]interface{}, routeErr error) bool {
	if p.Cfg.DeadLetterTopic == "" || p.Producer == nil {
		log.Printf("[kbatch-schedule] route error job_id=%s — leaving leased (no dead_letter_topic)", jobID)
		return false
	}
	batchID, _ := data["batch_id"].(string)
	workerClass, _ := data["worker_class"].(string)
	if workerClass == "" {
		if jt, ok := data["job_type"].(string); ok {
			workerClass = jt
		}
	}
	dlt := map[string]interface{}{
		"job_id":            jobID,
		"batch_id":          batchID,
		"worker_class":      workerClass,
		"dlt_type":          "schedule_route_error",
		"dlt_error_message": routeErr.Error(),
		"dlt_raw_payload":   string(raw),
	}
	rawDLT, err := json.Marshal(dlt)
	if err != nil {
		return false
	}
	if err := p.Producer.Produce(ctx, p.Cfg.DeadLetterTopic, jobID, rawDLT); err != nil {
		log.Printf("[kbatch-schedule] DLT produce failed job_id=%s: %v — leaving leased", jobID, err)
		return false
	}
	instrument.DLTPublished(jobID, batchID, "schedule_route_error", p.Cfg.ScheduledTopic)
	return true
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Run blocks until ctx is cancelled.
//
// When due jobs exist, Tick runs in a tight drain loop with no sleep — claim →
// produce → ack in batches until a tick returns 0. Only then does the poller
// resume the idle wait cycle (exponential backoff up to SchedulePollMaxInterval).
func (p *Poller) Run(ctx context.Context) {
	interval := p.Cfg.SchedulePollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	cap := p.Cfg.SchedulePollMaxInterval
	if cap <= 0 {
		cap = 60 * time.Second
	}
	if cap < interval {
		cap = interval
	}
	wait := interval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		drained, err := p.drainDue(ctx)
		if err != nil {
			log.Printf("[kbatch-schedule] tick error: %v", err)
			time.Sleep(p.jittered(wait))
			wait = min(wait*2, cap)
			continue
		}
		if drained {
			wait = interval
		}
		// No more due work (or just finished draining) — resume poll wait.
		time.Sleep(p.jittered(wait))
		if !drained {
			wait = min(wait*2, cap)
		}
	}
}

// drainDue repeatedly Ticks while due jobs remain. Returns whether any job was
// dispatched. Errors from an empty-progress Tick are returned to the caller.
func (p *Poller) drainDue(ctx context.Context) (drained bool, err error) {
	for {
		select {
		case <-ctx.Done():
			return drained, nil
		default:
		}
		n, tickErr := p.Tick(ctx)
		if p.RecordActivity != nil {
			p.RecordActivity()
		}
		if tickErr != nil {
			return drained, tickErr
		}
		if n == 0 {
			return drained, nil
		}
		drained = true
		// More due work may remain — keep claiming without idle sleep.
	}
}

func (p *Poller) jittered(d time.Duration) time.Duration {
	j := p.Cfg.SchedulePollJitter
	if j <= 0 {
		return d
	}
	f := 1 + ((rand.Float64()*2)-1)*j
	return time.Duration(float64(d) * f)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
