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
	Work       *workset.Store
	ConsumerID string
	LeaseTTL   time.Duration
	Sem        chan struct{}
	Process    func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error)
	Apply      func(ctx context.Context, out job.Outcome) error

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
	return &SuperFetchExecutor{
		Work:       work,
		ConsumerID: consumerID,
		LeaseTTL:   lease,
		Sem:        make(chan struct{}, n),
		Process:    process,
		Apply:      apply,
	}
}

// DispatchClaimsAndAcks claims each record, marks the Kafka offset, and starts
// perform in the background. Returns when all records in the batch are claimed
// (or skipped) — does not wait for #perform to finish.
func (e *SuperFetchExecutor) DispatchClaimsAndAcks(ctx context.Context, cl *kgo.Client, recs []*kgo.Record, group string) {
	if e == nil || e.Work == nil {
		return
	}
	// Heartbeat the workset consumer id (may differ from the shared liveness reporter id).
	_ = e.Work.TouchConsumer(ctx, e.ConsumerID, e.LeaseTTL)
	for _, rec := range recs {
		select {
		case <-ctx.Done():
			return
		case e.Sem <- struct{}{}:
		}
		jobID := extractJobID(rec.Value)
		if jobID == "" {
			// Malformed — process synchronously for DLT then ack.
			e.runLegacy(ctx, cl, rec, group)
			<-e.Sem
			continue
		}
		if _, loaded := e.inFlight.LoadOrStore(jobID, struct{}{}); loaded {
			// Already performing in this process (kafka redelivery).
			cl.MarkCommitRecords(rec)
			<-e.Sem
			continue
		}
		claim, err := e.Work.Claim(ctx, workset.ClaimParams{
			JobID: jobID, Payload: rec.Value, Topic: rec.Topic,
			Partition: rec.Partition, Offset: rec.Offset,
			ConsumerID: e.ConsumerID, LeaseTTL: e.LeaseTTL,
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
		go e.perform(ctx, rec, jobID, claim.Fence, group)
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
	if err := e.Work.Complete(ctx, jobID, e.ConsumerID, fence); err != nil {
		log.Printf("[kbatch-superfetch] complete error group=%s job_id=%s: %v", group, jobID, err)
	}
}

func (e *SuperFetchExecutor) startRenew(ctx context.Context, jobID, fence string) func() {
	stop := make(chan struct{})
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
				_ = e.Work.TouchConsumer(ctx, e.ConsumerID, e.LeaseTTL)
				ok, err := e.Work.Renew(ctx, jobID, e.ConsumerID, fence, e.LeaseTTL)
				if err != nil || !ok {
					return
				}
			}
		}
	}()
	return func() { close(stop) }
}

func (e *SuperFetchExecutor) runLegacy(ctx context.Context, cl *kgo.Client, rec *kgo.Record, group string) {
	src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
	out, err := e.Process(ctx, rec.Value, src)
	if err != nil {
		log.Printf("[kbatch-superfetch] legacy process error group=%s: %v", group, err)
		return
	}
	if err := e.Apply(ctx, out); err != nil {
		log.Printf("[kbatch-superfetch] legacy apply error group=%s: %v", group, err)
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
