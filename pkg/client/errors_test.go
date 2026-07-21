package client

import (
	"errors"
	"testing"
)

func TestErrorStrings(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "BatchClosedError",
			err:  BatchClosedError{BatchID: "b1", Reason: "closed"},
			want: "batch b1 is closed",
		},
		{
			name: "BatchNotFoundError",
			err:  BatchNotFoundError{BatchID: "missing"},
			want: "batch missing not found",
		},
		{
			name: "BatchExistsError",
			err:  BatchExistsError{BatchID: "dup"},
			want: "batch dup already exists",
		},
		{
			name: "UnknownWorkerClassError",
			err:  UnknownWorkerClassError{WorkerClass: "NoSuchWorker"},
			want: `unknown worker class "NoSuchWorker"`,
		},
		{
			name: "DuplicateJobError",
			err:  DuplicateJobError{WorkerClass: "ExportWorker"},
			want: "duplicate uniq job for ExportWorker",
		},
		{
			name: "UnknownHandlerError",
			err:  UnknownHandlerError{JobType: "ghost.go"},
			want: `unknown job_type "ghost.go"`,
		},
		{
			name: "PartialProduceError",
			err:  PartialProduceError{Message: "produced 3 of 5", ProducedCount: 3},
			want: "produced 3 of 5",
		},
		{
			name: "ConfigurationError",
			err:  ConfigurationError{Message: "redis url required"},
			want: "redis url required",
		},
		{
			name: "ErrJobSkipped",
			err:  ErrJobSkipped,
			want: "job skipped (uniq duplicate)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}

	if !errors.Is(ErrJobSkipped, ErrJobSkipped) {
		t.Fatal("ErrJobSkipped should match itself via errors.Is")
	}
}
