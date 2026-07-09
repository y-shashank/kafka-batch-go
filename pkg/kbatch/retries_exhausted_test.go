package kbatch

import (
	"errors"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestRunRetriesExhaustedInvokesHook(t *testing.T) {
	Reset()
	var called bool
	var summary RetriesExhaustedSummary
	OnRetriesExhausted("test.hook", func(s RetriesExhaustedSummary, err error) {
		called = true
		summary = s
		if err.Error() != "boom" {
			t.Fatalf("err %v", err)
		}
	})

	batchID := "b1"
	job := protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.hook",
		WorkerClass: "go:test.hook", Payload: map[string]interface{}{"x": 1},
		Attempt: 3, MaxRetries: 3,
	}
	if !RunRetriesExhausted(job, &HandlerError{Class: "Boom", Message: "boom"}, 3) {
		t.Fatal("expected hook to run")
	}
	if !called {
		t.Fatal("hook not called")
	}
	if summary.JobID != "j1" || summary.BatchID != "b1" || summary.ErrorClass != "Boom" {
		t.Fatalf("summary %+v", summary)
	}
}

func TestRunRetriesExhaustedSwallowsHookPanic(t *testing.T) {
	Reset()
	OnRetriesExhausted("test.panic", func(RetriesExhaustedSummary, error) {
		panic("oops")
	})
	job := protocol.JobMessage{JobID: "j1", JobType: "test.panic", Attempt: 1}
	if !RunRetriesExhausted(job, errors.New("fail"), 1) {
		t.Fatal("expected true even when hook panics")
	}
}

func TestRunRetriesExhaustedNoHook(t *testing.T) {
	Reset()
	job := protocol.JobMessage{JobID: "j1", JobType: "missing", Attempt: 1}
	if RunRetriesExhausted(job, errors.New("fail"), 1) {
		t.Fatal("expected false")
	}
}
