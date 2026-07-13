package fairness

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/jobexpiry"
)

// Producer publishes to Kafka.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Forwarder checks out fairly-selected jobs and produces to the ready topic.
type Forwarder struct {
	Lane              Lane
	Scheduler         *Scheduler
	ReadyTopic        string
	ResolveReadyTopic func(payload []byte) (string, error)
	Producer          Producer
	IdleSleep      time.Duration
	Burst          int
	Now            func() time.Time
	OnExpired      func(ctx context.Context, job *CheckoutResult, raw []byte) error
	RecordActivity func() // optional hook for liveness probes

	lastLeaseReclaim   time.Time
	lastForwardReclaim time.Time
}

const (
	defaultIdleSleep = 50 * time.Millisecond
	defaultBurst     = 50
	reclaimInterval  = 30 * time.Second
)

// ForwardOnce forwards one job when available.
func (f *Forwarder) ForwardOnce(ctx context.Context) bool {
	if f.Scheduler == nil {
		return false
	}
	job, err := f.Scheduler.Checkout(ctx)
	if err != nil || job == nil {
		return false
	}
	if f.handleExpired(ctx, job) {
		return true
	}
	if err := f.forwardJob(ctx, job); err != nil {
		log.Printf("[kbatch-fair-forwarder] forward error lane=%s: %v", f.Lane, err)
		return false
	}
	return true
}

func (f *Forwarder) handleExpired(ctx context.Context, job *CheckoutResult) bool {
	var m map[string]interface{}
	if err := json.Unmarshal(job.Payload, &m); err != nil {
		return false
	}
	validTill, _ := m["valid_till"].(string)
	if !jobexpiry.Expired(validTill, f.now()) {
		return false
	}
	// The payload was already LPOP'd from the ready list during Checkout, so the
	// forwarding entry (+ its lease) is now the ONLY durable record of this job.
	// Emit the completion/DLT drop first; only release the slot and confirm the
	// forward once it succeeds. If the drop fails we leave the forwarding entry
	// and lease intact so stale-forward recovery re-produces the job instead of
	// losing it (and stranding its batch). Mirrors forwardJob's produce→confirm.
	if f.OnExpired != nil {
		if err := f.OnExpired(ctx, job, job.Payload); err != nil {
			log.Printf("[kbatch-fair-forwarder] expired drop lane=%s: %v (retained for stale-forward recovery)", f.Lane, err)
			return true
		}
	}
	_ = f.Scheduler.Complete(ctx, job.TenantID, job.SlotID, 0)
	_, _ = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	return true
}

func (f *Forwarder) forwardJob(ctx context.Context, job *CheckoutResult) error {
	marked, key, err := markSlot(job.Payload, job.TenantID, job.SlotID, f.Lane)
	if err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	if key == "" {
		key = job.TenantID
	}
	readyTopic, err := f.readyTopicFor(job.Payload)
	if err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	if err := f.Producer.Produce(ctx, readyTopic, key, marked); err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	_, err = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	return err
}

// Run blocks until ctx is cancelled.
func (f *Forwarder) Run(ctx context.Context) {
	idle := f.IdleSleep
	if idle <= 0 {
		idle = defaultIdleSleep
	}
	burst := f.Burst
	if burst <= 0 {
		burst = defaultBurst
	}
	log.Printf("[kbatch-fair-forwarder] started lane=%s ready=%s", f.Lane, f.ReadyTopic)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[kbatch-fair-forwarder] stopped lane=%s", f.Lane)
			return
		default:
		}
		if f.RecordActivity != nil {
			f.RecordActivity()
		}
		forwarded := 0
		for forwarded < burst {
			if !f.ForwardOnce(ctx) {
				break
			}
			forwarded++
		}
		f.maybeReclaim(ctx)
		if forwarded == 0 {
			time.Sleep(idle)
		}
	}
}

func (f *Forwarder) maybeReclaim(ctx context.Context) {
	now := time.Now()
	if now.Sub(f.lastLeaseReclaim) >= reclaimInterval {
		f.lastLeaseReclaim = now
		if n, err := f.Scheduler.ReclaimExpiredLeases(ctx); err != nil {
			log.Printf("[kbatch-fair-forwarder] reclaim leases lane=%s: %v", f.Lane, err)
		} else if n > 0 {
			log.Printf("[kbatch-fair-forwarder] reclaimed %d expired lease(s) lane=%s", n, f.Lane)
		}
	}
	if now.Sub(f.lastForwardReclaim) >= reclaimInterval {
		f.lastForwardReclaim = now
		stale, err := f.Scheduler.ListStaleForwards(ctx)
		if err != nil {
			log.Printf("[kbatch-fair-forwarder] list stale forwards lane=%s: %v", f.Lane, err)
			return
		}
		for _, e := range stale {
			if err := f.Scheduler.ReclaimStaleForward(ctx, e, func(payload []byte, key string) error {
				readyTopic, err := f.readyTopicFor(payload)
				if err != nil {
					return err
				}
				return f.Producer.Produce(ctx, readyTopic, key, payload)
			}); err != nil {
				log.Printf("[kbatch-fair-forwarder] reclaim stale forward lane=%s slot=%s: %v", f.Lane, e.SlotID, err)
			}
		}
	}
}

func (f *Forwarder) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Forwarder) readyTopicFor(payload []byte) (string, error) {
	if f.ResolveReadyTopic != nil {
		return f.ResolveReadyTopic(payload)
	}
	if f.ReadyTopic != "" {
		return f.ReadyTopic, nil
	}
	return "", fmt.Errorf("forwarder has no ready topic router configured")
}
