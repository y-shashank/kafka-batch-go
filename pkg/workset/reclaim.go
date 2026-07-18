package workset

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

// Producer re-publishes reclaimed job payloads.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// ReclaimResult counts one reclaim sweep.
type ReclaimResult struct {
	Checked   int
	Reclaimed int
	Failed    int
	Skipped   int
}

// ReclaimOrphans finds jobs whose consumer heartbeat is gone past grace,
// re-produces the stored payload to the original topic with _reclaim=true, then
// drops ownership. Produce is idempotent: after a successful Kafka produce a
// Redis marker prevents a second produce if FinishReclaim fails.
//
// At-least-once #perform on the reclaimed message is expected (claim → mark
// offset → perform). The invariant here is: do not load the same orphan from
// Kafka twice due to a Finish failure.
func (s *Store) ReclaimOrphans(ctx context.Context, prod Producer, limit int, lockTTL, grace time.Duration) (ReclaimResult, error) {
	var out ReclaimResult
	if s == nil || prod == nil {
		return out, nil
	}
	orphans, err := s.ListOrphans(ctx, limit, grace)
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
		if err := s.reclaimOne(ctx, prod, e); err != nil {
			out.Failed++
			continue
		}
		out.Reclaimed++
		log.Printf("[kbatch-workset] reclaimed job_id=%s → topic=%s (dead consumer=%s)",
			e.JobID, e.Topic, e.ConsumerID)
	}
	return out, nil
}

func (s *Store) reclaimOne(ctx context.Context, prod Producer, e Entry) error {
	already, err := s.ProducedFence(ctx, e.JobID)
	if err != nil {
		log.Printf("[kbatch-workset] reclaim produced-check job_id=%s: %v", e.JobID, err)
		_ = s.AbortReclaim(ctx, e.JobID)
		return err
	}
	if already != "" && already != e.Fence {
		// Marker from a prior ownership generation — clear and produce for current fence.
		_ = s.client.Del(ctx, producedKey(e.JobID)).Err()
		already = ""
	}
	if already != "" {
		if err := s.finishReclaimChecked(ctx, e); err != nil {
			log.Printf("[kbatch-workset] reclaim finish-only job_id=%s: %v", e.JobID, err)
			return err
		}
		return nil
	}

	rawPayload, err := PayloadForReclaim(&e)
	if err != nil {
		// Deterministic failure — retrying every sweep will never succeed and the
		// entry just TTL-expires into a silent loss. Dead-letter it instead.
		log.Printf("[kbatch-workset] reclaim decode job_id=%s: %v", e.JobID, err)
		return s.deadLetterUnreclaimable(ctx, prod, e, "decode", err)
	}
	body, err := markReclaimPayload(rawPayload)
	if err != nil {
		log.Printf("[kbatch-workset] reclaim encode job_id=%s: %v", e.JobID, err)
		return s.deadLetterUnreclaimable(ctx, prod, e, "encode", err)
	}
	if e.Topic == "" {
		log.Printf("[kbatch-workset] reclaim missing topic job_id=%s", e.JobID)
		return s.deadLetterUnreclaimable(ctx, prod, e, "missing_topic", errMissingTopic)
	}
	if err := prod.Produce(ctx, e.Topic, e.JobID, body); err != nil {
		log.Printf("[kbatch-workset] reclaim produce job_id=%s topic=%s: %v", e.JobID, e.Topic, err)
		_ = s.AbortReclaim(ctx, e.JobID)
		return err
	}
	// Durable produce ack before Finish so a Finish failure cannot double-produce.
	if err := s.markProducedRetry(ctx, e.JobID, e.Fence); err != nil {
		log.Printf("[kbatch-workset] reclaim mark-produced job_id=%s: %v — trying finish anyway", e.JobID, err)
		// Produce already happened — never Abort. If Finish succeeds we are done;
		// if not, the next sweep may double-produce only when the marker is absent
		// (Redis was too sick to SET) — prefer that rare case over dropping the job.
		if ferr := s.finishReclaimChecked(ctx, e); ferr == nil {
			return nil
		}
		return err
	}
	if err := s.finishReclaimChecked(ctx, e); err != nil {
		log.Printf("[kbatch-workset] reclaim finish job_id=%s: %v (produced marker kept)", e.JobID, err)
		return err
	}
	return nil
}

// deadLetterUnreclaimable parks a payload that can never be reclaimed (undecodable,
// unencodable, or missing its source topic) onto the dead-letter topic and removes
// the workset entry, instead of retrying it every sweep until the lease TTL expires
// and the job is silently lost. When no DLT topic is configured it falls back to the
// prior behavior (keep the entry for retry). Returns nil once the job is safely
// parked (so the sweep counts it handled), or the original cause otherwise.
func (s *Store) deadLetterUnreclaimable(ctx context.Context, prod Producer, e Entry, reason string, cause error) error {
	if s.dltTopic == "" {
		_ = s.AbortReclaim(ctx, e.JobID)
		return cause
	}
	errMsg := ""
	if cause != nil {
		errMsg = cause.Error()
	}
	dlt := map[string]interface{}{
		"job_id":            e.JobID,
		"dlt_type":          "workset_unreclaimable",
		"dlt_reason":        reason,
		"dlt_error_message": errMsg,
		"topic":             e.Topic,
	}
	if decoded, derr := PayloadForReclaim(&e); derr == nil {
		dlt["dlt_raw_payload"] = string(decoded)
	} else {
		dlt["dlt_raw_payload"] = string(e.Payload)
	}
	raw, merr := json.Marshal(dlt)
	if merr != nil {
		_ = s.AbortReclaim(ctx, e.JobID)
		return cause
	}
	if perr := prod.Produce(ctx, s.dltTopic, e.JobID, raw); perr != nil {
		log.Printf("[kbatch-workset] reclaim dead-letter produce job_id=%s: %v — keeping entry for retry", e.JobID, perr)
		_ = s.AbortReclaim(ctx, e.JobID)
		return cause
	}
	instrument.WorksetUnreclaimable(reason)
	if ferr := s.finishReclaimChecked(ctx, e); ferr != nil {
		log.Printf("[kbatch-workset] reclaim dead-letter finish job_id=%s: %v (entry may retry once more)", e.JobID, ferr)
	}
	log.Printf("[kbatch-workset] reclaim dead-lettered job_id=%s reason=%s → topic=%s", e.JobID, reason, s.dltTopic)
	return nil
}

func (s *Store) markProducedRetry(ctx context.Context, jobID, fence string) error {
	var err error
	for i := 0; i < 5; i++ {
		err = s.MarkProduced(ctx, jobID, fence, producedMarkerTTL)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 20 * time.Millisecond)
	}
	return err
}

func (s *Store) finishReclaimChecked(ctx context.Context, e Entry) error {
	n, err := s.FinishReclaim(ctx, e)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("workset: finish reclaim noop job_id=%s (fence mismatch or gone)", e.JobID)
	}
	return nil
}

type missingTopicError struct{}

func (missingTopicError) Error() string { return "workset: reclaim missing topic" }

var errMissingTopic = missingTopicError{}

func markReclaimPayload(raw []byte) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["_reclaim"] = true
	return json.Marshal(m)
}
