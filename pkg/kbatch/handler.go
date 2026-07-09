package kbatch

import (
	"fmt"
	"sync"
)

// Context is passed to registered Go job handlers.
type Context struct {
	JobType    string
	JobID      string
	BatchID    string
	Attempt    int
	Payload    map[string]interface{}
	TenantID   string
	EnqueuedAt string
}

// HandlerFunc runs one job. Return nil on success or an error to fail the job.
type HandlerFunc func(ctx *Context) error

// HandlerError carries a stable error_class for the control plane.
type HandlerError struct {
	Class   string
	Message string
}

func (e *HandlerError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Class
}

var (
	registryMu sync.RWMutex
	registry   = map[string]HandlerFunc{}
)

// Register binds a job_type to a Go handler. Panics on duplicate registration.
func Register(jobType string, fn HandlerFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[jobType]; exists {
		panic(fmt.Sprintf("kbatch: job_type %q already registered", jobType))
	}
	registry[jobType] = fn
}

// Lookup returns the handler for job_type.
func Lookup(jobType string) (HandlerFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	fn, ok := registry[jobType]
	return fn, ok
}

// Reset clears all registrations (tests only).
func Reset() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]HandlerFunc{}
	ResetRetriesExhausted()
}
