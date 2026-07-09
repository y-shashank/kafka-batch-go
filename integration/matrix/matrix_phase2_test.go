//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
)

// Phase 2: fair + retry through Ruby execution (not yet in default CI matrix).
func TestMatrix_Phase2_RubyFairAndRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("phase 2 matrix scenarios are slow")
	}
	catalog := phase2Scenarios()
	combo := Combo{Name: "go_control_ruby_exec", Exec: e2e.ExecMode{Ruby: true}}
	for _, sc := range catalog {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			extra := func(s *e2e.Stack, cfg *e2e.DaemonYAML) {
				if sc.NeedsFair {
					e2e.ApplyFairConfig(s, cfg)
				}
			}
			s := e2e.NewStack(t, e2e.BaseHandlersStack, extra)
			s.StartWith(combo.Exec)
			defer s.Stop()

			c := s.NewClient()
			defer c.Close()

			ctx := context.Background()
			got, err := sc.Run(ctx, s, c)
			if err != nil {
				t.Fatal(err)
			}
			timeout := 60 * time.Second
			if sc.Name == "retry_then_success_ruby" {
				timeout = 120 * time.Second
			}
			if sc.NeedsFair {
				s.DrainRubyExecution(timeout, s.TimeReadyRuby)
			} else {
				s.DrainRubyExecution(timeout)
			}
			sc.Assert(t, s, got)
		})
	}
}
