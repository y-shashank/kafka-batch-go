package daemon

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

// SuperFetchExecutor claims Redis ownership, Kafka-acks immediately, then runs
// #perform on a bounded goroutine pool without blocking the poll loop.
type SuperFetchExecutor struct {
	Work             *workset.Store
	ConsumerID       string
	LeaseTTL         time.Duration // working-set job key TTL (renewed during perform)
	HeartbeatTTL     time.Duration // live:consumer:* TTL used for pod-alive checks
	HeartbeatEvery   time.Duration // how often to refresh the heartbeat key
	OrphanGrace      time.Duration // steal grace aligned with daemon reclaim
	Sem              chan struct{}
	Process          func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error)
	Apply            func(ctx context.Context, out job.Outcome) error
	heartbeatStarted sync.Once

	// lifeCtx is the process/member lifetime (not the poll-scoped procCtx).
	// #perform must outlive DispatchClaimsAndAcks — endProc cancels procCtx.
	lifeMu  sync.Mutex
	lifeCtx context.Context

	inFlight sync.Map // job_id → struct{} while perform runs locally
}

func NewSuperFetchExecutor(cfg config.Daemon, work *workset.Store, consumerID string,
	process func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error),
	apply func(ctx context.Context, out job.Outcome) error,
) *SuperFetchExecutor {
	n := cfg.SuperFetchWorkers()
	lease := cfg.SuperFetchLeaseTTL
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	grace := cfg.SuperFetchOrphanGrace
	if grace <= 0 {
		grace = workset.DefaultOrphanGrace
	}
	return &SuperFetchExecutor{
		Work:           work,
		ConsumerID:     consumerID,
		LeaseTTL:       lease,
		HeartbeatTTL:   cfg.LivenessTTLDuration(),
		HeartbeatEvery: cfg.LivenessHeartbeatIntervalDuration(),
		OrphanGrace:    grace,
		Sem:            make(chan struct{}, n),
		Process:        process,
		Apply:          apply,
	}
}

// BindLife pins the member lifetime context used by #perform / renew / heartbeat.
// Must be called with the supervised consumer ctx (not a poll-scoped procCtx).
func (e *SuperFetchExecutor) BindLife(ctx context.Context) {
	if e == nil || ctx == nil {
		return
	}
	e.lifeMu.Lock()
	if e.lifeCtx == nil {
		e.lifeCtx = ctx
	}
	life := e.lifeCtx
	e.lifeMu.Unlock()
	e.StartHeartbeatLoop(life)
}

func (e *SuperFetchExecutor) life() context.Context {
	e.lifeMu.Lock()
	defer e.lifeMu.Unlock()
	if e.lifeCtx != nil {
		return e.lifeCtx
	}
	return context.Background()
}

// StartHeartbeatLoop keeps the SuperFetch member id alive even when idle or
// during long performs (independent of the Kafka poll path).
func (e *SuperFetchExecutor) StartHeartbeatLoop(ctx context.Context) {
	if e == nil || e.Work == nil {
		return
	}
	e.heartbeatStarted.Do(func() {
		interval := e.HeartbeatEvery
		if interval <= 0 {
			interval = 20 * time.Second
		}
		go func() {
			if err := e.Work.TouchConsumer(ctx, e.ConsumerID, e.HeartbeatTTL); err != nil {
				log.Printf("[kbatch-superfetch] heartbeat touch consumer=%s: %v", e.ConsumerID, err)
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := e.Work.TouchConsumer(ctx, e.ConsumerID, e.HeartbeatTTL); err != nil {
						log.Printf("[kbatch-superfetch] heartbeat touch consumer=%s: %v", e.ConsumerID, err)
					}
				}
			}
		}()
	})
}

// DispatchClaimsAndAcks claims each record, marks the Kafka offset, and starts
// perform in the background. Returns when all records in the batch are claimed
// (or skipped) — does not wait for #perform to finish.
//
// ctx may be the poll-scoped procCtx (canceled when this function returns).
// #perform uses BindLife's context so it is not canceled by endProc.
func (e *SuperFetchExecutor) DispatchClaimsAndAcks(ctx context.Context, cl *kgo.Client, recs []*kgo.Record, group string) {
	if e == nil || e.Work == nil {
		return
	}
	life := e.life()
	e.StartHeartbeatLoop(life)
	for _, rec := range recs {
		select {
		case <-ctx.Done():
			return
		case e.Sem <- struct{}{}:
		}
		jobID := extractJobID(rec.Value)
		if jobID == "" {
			// Malformed — process synchronously for DLT then ack (no Redis claim).
			e.processMissingJobID(ctx, cl, rec, group)
			<-e.Sem
			continue
		}
		if _, loaded := e.inFlight.LoadOrStore(jobID, struct{}{}); loaded {
			// Already performing in this process (kafka redelivery).
			cl.MarkCommitRecords(rec)
			<-e.Sem
			continue
		}
		claim, err := e.Work.Claim(life, workset.ClaimParams{
			JobID: jobID, Payload: rec.Value, Topic: rec.Topic,
			Partition: rec.Partition, Offset: rec.Offset,
			ConsumerID: e.ConsumerID, LeaseTTL: e.LeaseTTL,
			HeartbeatTTL: e.HeartbeatTTL, StealGrace: e.OrphanGrace,
		})
		if err != nil {
			log.Printf("[kbatch-superfetch] claim error group=%s job_id=%s: %v — leaving unacked",
				group, jobID, err)
			e.inFlight.Delete(jobID)
			<-e.Sem
			continue
		}
		if !claim.Won {
			log.Printf("[kbatch-superfetch] claim lost group=%s job_id=%s — acking duplicate",
				group, jobID)
			cl.MarkCommitRecords(rec)
			e.inFlight.Delete(jobID)
			<-e.Sem
			continue
		}
		// Durability: Redis owns the job before Kafka forgets it.
		cl.MarkCommitRecords(rec)
		go e.perform(life, rec, jobID, claim.Fence, group)
	}
}

func (e *SuperFetchExecutor) perform(ctx context.Context, rec *kgo.Record, jobID, fence, group string) {
	defer func() {
		e.inFlight.Delete(jobID)
		<-e.Sem
	}()
	stopRenew := e.startRenew(ctx, jobID, fence)
	defer stopRenew()

	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.Process(ctx, rec.Value, src)
	if err != nil {
		log.Printf("[kbatch-superfetch] process error group=%s job_id=%s: %v — leaving in workset",
			group, jobID, err)
		return
	}
	owned, err := e.Work.StillOwned(ctx, jobID, e.ConsumerID, fence)
	if err != nil || !owned {
		log.Printf("[kbatch-superfetch] lost fence group=%s job_id=%s owned=%v err=%v — skip apply",
			group, jobID, owned, err)
		return
	}
	if err := e.Apply(ctx, out); err != nil {
		log.Printf("[kbatch-superfetch] apply error group=%s job_id=%s: %v — leaving in workset",
			group, jobID, err)
		return
	}
	for i := 0; i < 5; i++ {
		if err := e.Work.Complete(ctx, jobID, e.ConsumerID, fence); err != nil {
			log.Printf("[kbatch-superfetch] complete error group=%s job_id=%s attempt=%d: %v",
				group, jobID, i+1, err)
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
			continue
		}
		return
	}
}

func (e *SuperFetchExecutor) startRenew(ctx context.Context, jobID, fence string) func() {
	stop := make(chan struct{})
	// Job-lease renew; member heartbeat is owned by StartHeartbeatLoop (every 20s).
	interval := e.LeaseTTL / 3
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				ok, err := e.Work.Renew(ctx, jobID, e.ConsumerID, fence, e.LeaseTTL)
				if err != nil {
					// Transient Redis errors must not stop renew — lease expiry
					// after Kafka ack would drop the job with no reclaim path.
					log.Printf("[kbatch-superfetch] renew error job_id=%s: %v — will retry", jobID, err)
					continue
				}
				if !ok {
					log.Printf("[kbatch-superfetch] renew lost fence job_id=%s — stop renew", jobID)
					return
				}
			}
		}
	}()
	return func() { close(stop) }
}

func (e *SuperFetchExecutor) processMissingJobID(ctx context.Context, cl *kgo.Client, rec *kgo.Record, group string) {
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.Process(ctx, rec.Value, src)
	if err != nil {
		log.Printf("[kbatch-superfetch] missing job_id process error group=%s: %v", group, err)
		return
	}
	if err := e.Apply(ctx, out); err != nil {
		log.Printf("[kbatch-superfetch] missing job_id apply error group=%s: %v", group, err)
		return
	}
	cl.MarkCommitRecords(rec)
}

func extractJobID(raw []byte) string {
	var m struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	return m.JobID
}
