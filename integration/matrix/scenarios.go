//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// Scenario describes one cross-runtime integration scenario.
type Scenario struct {
	Name        string
	NeedsFair   bool
	NeedsSched  bool
	Run         func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error)
	Assert      func(t *testing.T, s *e2e.Stack, got ScenarioResult)
}

// ScenarioResult captures ids produced by a scenario act phase.
type ScenarioResult struct {
	BatchID string
	JobIDs  map[string]string // key: go | ruby | go_fair | ruby_fair
}

// Catalog returns Phase 1 matrix scenarios (shared across runtime combos).
func Catalog() []Scenario {
	return append(phase1CoreScenarios(), phase2Scenarios()...)
}

func phase1CoreScenarios() []Scenario {
	return []Scenario{
		{
			Name: "batch_completion_go",
			Run: func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix go"}, func(b *client.Batch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{BatchID: batch.ID(), JobIDs: map[string]string{"go": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				s.WaitBatch(ctx, got.BatchID, "success")
				if m := s.WaitMarkerAt(s.MarkerPath, 45*time.Second); m != got.JobIDs["go"] {
					t.Fatalf("go marker = %q want %q", m, got.JobIDs["go"])
				}
			},
		},
		{
			Name: "batch_completion_ruby",
			Run: func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix ruby"}, func(b *client.Batch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.ruby_plain", map[string]interface{}{"order_id": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{BatchID: batch.ID(), JobIDs: map[string]string{"ruby": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				s.WaitBatch(ctx, got.BatchID, "success")
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 45*time.Second); m != got.JobIDs["ruby"] {
					t.Fatalf("ruby marker = %q want %q", m, got.JobIDs["ruby"])
				}
			},
		},
		{
			Name: "mixed_batch_go_and_ruby",
			Run: func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error) {
				ids := map[string]string{}
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix mixed"}, func(b *client.Batch) error {
					var err error
					ids["go"], err = b.PushJob(ctx, "integration.go_multi", map[string]interface{}{"n": 1}, client.PushOptions{})
					if err != nil {
						return err
					}
					ids["ruby"], err = b.PushJob(ctx, "integration.ruby_plain", map[string]interface{}{"order_id": 2}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{BatchID: batch.ID(), JobIDs: ids}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				row := s.WaitBatch(ctx, got.BatchID, "success")
				if row.CompletedCount != 2 {
					t.Fatalf("completed_count = %d want 2", row.CompletedCount)
				}
				// go_multi handler does not write marker; verify ruby leg executed
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 45*time.Second); m != got.JobIDs["ruby"] {
					t.Fatalf("ruby marker = %q want %q", m, got.JobIDs["ruby"])
				}
			},
		},
	}
}

// phase2Scenarios need extra retry/fair wiring for Ruby execution (tracked separately).
func phase2Scenarios() []Scenario {
	return []Scenario{
		{
			Name:      "fair_routing_ruby_execution",
			NeedsFair: true,
			Run: func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error) {
				jobID, err := c.EnqueueJob(ctx, "integration.ruby_fair", map[string]interface{}{"tenant": "rb"}, client.PushOptions{TenantID: "tenant-rb"})
				if err != nil {
					return ScenarioResult{}, err
				}
				// Wait for Go control plane to forward ingest → ready.ruby before drain.
				s.PollTopic(ctx, s.TimeReadyRuby, func(m map[string]interface{}) bool {
					return m["job_id"] == jobID
				}, 60*time.Second)
				return ScenarioResult{JobIDs: map[string]string{"ruby_fair": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				msg := s.PollTopic(ctx, s.TimeReadyRuby, func(m map[string]interface{}) bool {
					return m["job_id"] == got.JobIDs["ruby_fair"]
				}, 60*time.Second)
				if msg["job_type"] != "integration.ruby_fair" {
					t.Fatalf("job_type = %v", msg["job_type"])
				}
				want := got.JobIDs["ruby_fair"] + ":rb"
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 60*time.Second); m != want {
					t.Fatalf("ruby marker = %q want %q", m, want)
				}
			},
		},
		{
			Name: "retry_then_success_ruby",
			Run: func(ctx context.Context, s *e2e.Stack, c *client.Client) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix ruby retry"}, func(b *client.Batch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.ruby_retry_once", map[string]interface{}{"n": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{BatchID: batch.ID(), JobIDs: map[string]string{"ruby": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				s.WaitBatch(ctx, got.BatchID, "success")
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 60*time.Second); m != got.JobIDs["ruby"] {
					t.Fatalf("ruby marker = %q want %q", m, got.JobIDs["ruby"])
				}
			},
		},
	}
}

// Combo identifies a client × control × execution matrix row.
type Combo struct {
	Name string
	Exec e2e.ExecMode
}

// Phase1Combos are the runtime combinations exercised in Phase 1.
func Phase1Combos() []Combo {
	return []Combo{
		{Name: "go_control_go_exec", Exec: e2e.ExecMode{Go: true}},
		{Name: "go_control_ruby_exec", Exec: e2e.ExecMode{Ruby: true}},
		{Name: "go_control_go_and_ruby_exec", Exec: e2e.ExecMode{Go: true, Ruby: true}},
	}
}

// Phase1Scenarios returns scenarios run in CI matrix Phase 1.
func Phase1Scenarios() []Scenario {
	return phase1CoreScenarios()
}

// ScenariosForCombo filters scenarios runnable under a combo.
func ScenariosForCombo(combo Combo, all []Scenario) []Scenario {
	out := make([]Scenario, 0, len(all))
	for _, sc := range all {
		if sc.Name == "batch_completion_go" && !combo.Exec.Go {
			continue
		}
		if sc.Name == "batch_completion_ruby" && !combo.Exec.Ruby {
			continue
		}
		if sc.Name == "mixed_batch_go_and_ruby" && !(combo.Exec.Go && combo.Exec.Ruby) {
			continue
		}
		if sc.Name == "fair_routing_ruby_execution" && !combo.Exec.Ruby {
			continue
		}
		if sc.Name == "retry_then_success_ruby" && !combo.Exec.Ruby {
			continue
		}
		out = append(out, sc)
	}
	return out
}
