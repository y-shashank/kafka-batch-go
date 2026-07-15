package workset

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

// Producer re-publishes reclaimed job payloads.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// ReclaimResult counts one reclaim sweep.
type ReclaimResult struct {
	Checked  int
	Reclaimed int
	Failed   int
	Skipped  int
}

// ReclaimOrphans finds jobs whose consumer heartbeat is gone, re-produces the
// stored payload to the original topic with _reclaim=true, then drops ownership.
func (s *Store) ReclaimOrphans(ctx context.Context, prod Producer, limit int, lockTTL time.Duration) (ReclaimResult, error) {
	var out ReclaimResult
	if s == nil || prod == nil {
		return out, nil
	}
	orphans, err := s.ListOrphans(ctx, limit)
	if err != nil {
		return out, err
	}
	out.Checked = len(orphans)
	for _, e := range orphans {
		won, err := s.BeginReclaim(ctx, e.JobID, lockTTL)
		if err != nil {
			out.Failed++
			continue
		}
		if !won {
			out.Skipped++
			continue
		}
		body, err := markReclaimPayload(e.Payload)
		if err != nil {
			log.Printf("[kbatch-workset] reclaim encode job_id=%s: %v", e.JobID, err)
			_ = s.AbortReclaim(ctx, e.JobID)
			out.Failed++
			continue
		}
		if e.Topic == "" {
			log.Printf("[kbatch-workset] reclaim missing topic job_id=%s — abort", e.JobID)
			_ = s.AbortReclaim(ctx, e.JobID)
			out.Failed++
			continue
		}
		if err := prod.Produce(ctx, e.Topic, e.JobID, body); err != nil {
			log.Printf("[kbatch-workset] reclaim produce job_id=%s topic=%s: %v", e.JobID, e.Topic, err)
			_ = s.AbortReclaim(ctx, e.JobID)
			out.Failed++
			continue
		}
		if err := s.FinishReclaim(ctx, e); err != nil {
			log.Printf("[kbatch-workset] reclaim finish job_id=%s: %v", e.JobID, err)
			out.Failed++
			continue
		}
		out.Reclaimed++
		log.Printf("[kbatch-workset] reclaimed job_id=%s → topic=%s (dead consumer=%s)",
			e.JobID, e.Topic, e.ConsumerID)
	}
	return out, nil
}

func markReclaimPayload(raw []byte) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["_reclaim"] = true
	return json.Marshal(m)
}
