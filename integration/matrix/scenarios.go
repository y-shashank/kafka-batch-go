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
	Name          string
	NeedsFair     bool
	NeedsSched    bool
	NeedsPriority bool
	NeedsGoExec   bool
	NeedsRubyExec bool
	GoClientOnly  bool
	Run           func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error)
	Assert        func(t *testing.T, s *e2e.Stack, got ScenarioResult)
}

// ScenarioResult captures ids produced by a scenario act phase.
type ScenarioResult struct {
	BatchID string
	JobIDs  map[string]string
}

// Combo identifies a client × control × execution matrix row.
type Combo struct {
	Name    string
	Client  ClientMode
	Control ControlMode
	Exec    e2e.ExecMode
}

func batchJobID(batch MatrixBatch, pushedID, key string) string {
	if pushedID != "" {
		return pushedID
	}
	if ids := BatchJobIDsAfterCreate(batch); ids != nil {
		return ids[key]
	}
	return ""
}

func phase1CoreScenarios() []Scenario {
	return []Scenario{
		{
			Name:        "batch_completion_go",
			NeedsGoExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix go"}, func(b MatrixBatch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.go_daemon", map[string]interface{}{"ping": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				jobID = batchJobID(batch, jobID, "go")
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
			Name:          "batch_completion_ruby",
			NeedsRubyExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix ruby"}, func(b MatrixBatch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.ruby_plain", map[string]interface{}{"order_id": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				jobID = batchJobID(batch, jobID, "ruby")
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
			Name:          "mixed_batch_go_and_ruby",
			NeedsGoExec:   true,
			NeedsRubyExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				ids := map[string]string{}
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix mixed"}, func(b MatrixBatch) error {
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
				if ids["go"] == "" {
					ids["go"] = batchJobID(batch, "", "go")
				}
				if ids["ruby"] == "" {
					ids["ruby"] = batchJobID(batch, "", "ruby")
				}
				return ScenarioResult{BatchID: batch.ID(), JobIDs: ids}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				row := s.WaitBatch(ctx, got.BatchID, "success")
				if row.CompletedCount != 2 {
					t.Fatalf("completed_count = %d want 2", row.CompletedCount)
				}
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 45*time.Second); m != got.JobIDs["ruby"] {
					t.Fatalf("ruby marker = %q want %q", m, got.JobIDs["ruby"])
				}
			},
		},
	}
}

func phase2Scenarios() []Scenario {
	return []Scenario{
		{
			Name:          "fair_routing_ruby_execution",
			NeedsFair:     true,
			NeedsRubyExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				jobID, err := c.EnqueueJob(ctx, "integration.ruby_fair", map[string]interface{}{"tenant": "rb"}, client.PushOptions{TenantID: "tenant-rb"})
				if err != nil {
					return ScenarioResult{}, err
				}
				s.PollTopic(ctx, s.TimeReadyRuby, func(m map[string]interface{}) bool {
					return m["job_id"] == jobID
				}, 60*time.Second)
				return ScenarioResult{JobIDs: map[string]string{"ruby_fair": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				want := got.JobIDs["ruby_fair"] + ":rb"
				if m := s.WaitMarkerAt(s.RubyMarkerPath, 60*time.Second); m != want {
					t.Fatalf("ruby marker = %q want %q", m, want)
				}
			},
		},
		{
			Name:          "retry_then_success_ruby",
			NeedsRubyExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix ruby retry"}, func(b MatrixBatch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.ruby_retry_once", map[string]interface{}{"n": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				jobID = batchJobID(batch, jobID, "ruby")
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

func expandedScenarios() []Scenario {
	return []Scenario{
		{
			Name:        "retry_then_success_go",
			NeedsGoExec: true,
			GoClientOnly: false,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix go retry"}, func(b MatrixBatch) error {
					var err error
					jobID, err = b.PushJob(ctx, "integration.go_retry_once", map[string]interface{}{"ping": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				jobID = batchJobID(batch, jobID, "go")
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
			Name:        "dlt_exhausted_go",
			NeedsGoExec: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix dlt"}, func(b MatrixBatch) error {
					_, err := b.PushJob(ctx, "integration.go_always_fail", map[string]interface{}{"x": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{BatchID: batch.ID()}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				row := s.WaitBatch(ctx, got.BatchID, "complete")
				if row.FailedCount != 1 {
					t.Fatalf("failed_count = %d want 1", row.FailedCount)
				}
				dlt := s.PollTopic(ctx, s.DLTTopic, func(m map[string]interface{}) bool {
					return m["batch_id"] == got.BatchID
				}, 30*time.Second)
				if dlt["dlt_error_class"] != "Permanent" {
					t.Fatalf("dlt_error_class = %v", dlt["dlt_error_class"])
				}
			},
		},
		{
			Name:         "scheduled_job_go",
			NeedsSched:   true,
			NeedsGoExec:  true,
			GoClientOnly: true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				var jobID string
				batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "matrix sched"}, func(b MatrixBatch) error {
					var err error
					jobID, err = b.PushJobIn(ctx, 2*time.Second, "integration.go_scheduled", map[string]interface{}{"n": 1}, client.PushOptions{})
					return err
				})
				if err != nil {
					return ScenarioResult{}, err
				}
				jobID = batchJobID(batch, jobID, "go")
				return ScenarioResult{BatchID: batch.ID(), JobIDs: map[string]string{"go": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				ctx := context.Background()
				s.WaitBatch(ctx, got.BatchID, "success")
				if m := s.WaitMarkerAt(s.MarkerPath, 60*time.Second); m != got.JobIDs["go"] {
					t.Fatalf("go marker = %q want %q", m, got.JobIDs["go"])
				}
			},
		},
		{
			Name:          "priority_p0_go",
			NeedsPriority: true,
			NeedsGoExec:   true,
			GoClientOnly:  true,
			Run: func(ctx context.Context, s *e2e.Stack, c MatrixClient) (ScenarioResult, error) {
				jobID, err := c.EnqueueJob(ctx, "integration.go_p0", map[string]interface{}{"rank": 0}, client.PushOptions{})
				if err != nil {
					return ScenarioResult{}, err
				}
				return ScenarioResult{JobIDs: map[string]string{"go": jobID}}, nil
			},
			Assert: func(t *testing.T, s *e2e.Stack, got ScenarioResult) {
				if m := s.WaitMarkerAt(s.P0MarkerPath, 45*time.Second); m != got.JobIDs["go"] {
					t.Fatalf("p0 marker = %q want %q", m, got.JobIDs["go"])
				}
			},
		},
	}
}

// Phase1Scenarios returns scenarios run in CI matrix Phase 1.
func Phase1Scenarios() []Scenario {
	return phase1CoreScenarios()
}

// Phase2Scenarios returns fair/retry Ruby execution scenarios.
func Phase2Scenarios() []Scenario {
	return phase2Scenarios()
}

// ExpandedScenarios returns schedule/priority/dlt scenarios under Go control.
func ExpandedScenarios() []Scenario {
	return expandedScenarios()
}

// PRScenarios returns the default PR matrix catalog.
func PRScenarios() []Scenario {
	out := make([]Scenario, 0, 16)
	out = append(out, phase1CoreScenarios()...)
	out = append(out, expandedScenarios()...)
	return out
}

// FullScenarios returns all matrix scenarios for nightly runs.
func FullScenarios() []Scenario {
	out := make([]Scenario, 0, 24)
	out = append(out, phase1CoreScenarios()...)
	out = append(out, phase2Scenarios()...)
	out = append(out, expandedScenarios()...)
	return out
}

// Phase1Combos are the runtime combinations exercised in Phase 1.
func Phase1Combos() []Combo {
	return []Combo{
		{Name: "go_client_go_control_go_exec", Client: ClientGo, Control: ControlGo, Exec: e2e.ExecMode{Go: true}},
		{Name: "go_client_go_control_ruby_exec", Client: ClientGo, Control: ControlGo, Exec: e2e.ExecMode{Ruby: true}},
		{Name: "go_client_go_control_mixed_exec", Client: ClientGo, Control: ControlGo, Exec: e2e.ExecMode{Go: true, Ruby: true}},
	}
}

// Phase3Combos exercise Ruby client against Go control.
func Phase3Combos() []Combo {
	return []Combo{
		{Name: "ruby_client_go_control_go_exec", Client: ClientRuby, Control: ControlGo, Exec: e2e.ExecMode{Go: true}},
		{Name: "ruby_client_go_control_ruby_exec", Client: ClientRuby, Control: ControlGo, Exec: e2e.ExecMode{Ruby: true}},
		{Name: "ruby_client_go_control_mixed_exec", Client: ClientRuby, Control: ControlGo, Exec: e2e.ExecMode{Go: true, Ruby: true}},
	}
}

// Phase4Combos exercise Ruby Karafka control plane.
func Phase4Combos() []Combo {
	return []Combo{
		{Name: "go_client_ruby_control_go_exec", Client: ClientGo, Control: ControlRuby, Exec: e2e.ExecMode{Go: true}},
		{Name: "ruby_client_ruby_control_ruby_exec", Client: ClientRuby, Control: ControlRuby, Exec: e2e.ExecMode{Ruby: true}},
	}
}

// ScenariosForCombo filters scenarios runnable under a combo.
func ScenariosForCombo(combo Combo, all []Scenario) []Scenario {
	out := make([]Scenario, 0, len(all))
	for _, sc := range all {
		if sc.GoClientOnly && combo.Client != ClientGo {
			continue
		}
		if sc.NeedsGoExec && !combo.Exec.Go {
			continue
		}
		if sc.NeedsRubyExec && !combo.Exec.Ruby {
			continue
		}
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
