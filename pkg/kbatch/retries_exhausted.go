package kbatch

import (
	"sync"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// RetriesExhaustedSummary mirrors Ruby Worker.retries_exhausted_job_summary.
type RetriesExhaustedSummary struct {
	JobID        string
	BatchID      string
	WorkerClass  string
	Payload      map[string]interface{}
	Attempt      int
	MaxRetries   int
	BatchCounted bool
	EnqueuedAt   string
	TenantID     string
	ErrorClass   string
	ErrorMessage string
}

// RetriesExhaustedFunc runs once when a job exhausts its retry budget (before DLT).
type RetriesExhaustedFunc func(summary RetriesExhaustedSummary, err error)

var (
	retriesExhaustedMu sync.RWMutex
	retriesExhausted   = map[string]RetriesExhaustedFunc{}
)

// OnRetriesExhausted registers a Sidekiq-compatible exhaustion hook for a job_type.
func OnRetriesExhausted(jobType string, fn RetriesExhaustedFunc) {
	retriesExhaustedMu.Lock()
	defer retriesExhaustedMu.Unlock()
	retriesExhausted[jobType] = fn
}

// RunRetriesExhausted invokes the registered hook, if any. Best-effort — panics are swallowed.
func RunRetriesExhausted(job protocol.JobMessage, execErr error, maxRetries int) bool {
	retriesExhaustedMu.RLock()
	fn, ok := retriesExhausted[job.JobType]
	retriesExhaustedMu.RUnlock()
	if !ok || fn == nil {
		return false
	}
	func() {
		defer func() { _ = recover() }()
		fn(retriesExhaustedSummary(job, execErr, maxRetries), execErr)
	}()
	return true
}

func retriesExhaustedSummary(job protocol.JobMessage, execErr error, maxRetries int) RetriesExhaustedSummary {
	s := RetriesExhaustedSummary{
		JobID:        job.JobID,
		WorkerClass:  job.WorkerClass,
		Payload:      job.Payload,
		Attempt:      job.Attempt,
		MaxRetries:   maxRetries,
		BatchCounted: job.BatchCounted,
		EnqueuedAt:   job.EnqueuedAt,
		ErrorClass:   exhaustionErrorClass(execErr),
		ErrorMessage: execErr.Error(),
	}
	if job.BatchID != nil {
		s.BatchID = *job.BatchID
	}
	if job.TenantID != nil {
		s.TenantID = *job.TenantID
	}
	if s.Payload == nil {
		s.Payload = map[string]interface{}{}
	}
	return s
}

func exhaustionErrorClass(err error) string {
	if he, ok := err.(*HandlerError); ok && he.Class != "" {
		return he.Class
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// ResetRetriesExhausted clears exhaustion hooks (tests only).
func ResetRetriesExhausted() {
	retriesExhaustedMu.Lock()
	defer retriesExhaustedMu.Unlock()
	retriesExhausted = map[string]RetriesExhaustedFunc{}
}
