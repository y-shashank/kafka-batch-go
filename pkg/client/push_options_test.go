package client

import (
	"testing"
)

func TestPushOptionsJobID(t *testing.T) {
	explicit := PushOptions{JobID: "fixed-id"}
	if got := explicit.jobID(); got != "fixed-id" {
		t.Fatalf("jobID=%q", got)
	}
	generated := (PushOptions{}).jobID()
	if generated == "" {
		t.Fatal("expected generated uuid")
	}
	if (PushOptions{}).jobID() == generated {
		t.Fatal("expected unique uuids")
	}
}

func TestPushOptionsTenantID(t *testing.T) {
	tests := []struct {
		name         string
		opts         PushOptions
		batchDefault string
		want         string
	}{
		{name: "explicit wins", opts: PushOptions{TenantID: "acme"}, batchDefault: "batch", want: "acme"},
		{name: "falls back to batch", opts: PushOptions{}, batchDefault: "batch", want: "batch"},
		{name: "empty both", opts: PushOptions{}, batchDefault: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.tenantID(tt.batchDefault); got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}
