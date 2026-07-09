//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
)

func TestMatrix_Phase1(t *testing.T) {
	catalog := Phase1Scenarios()
	for _, combo := range Phase1Combos() {
		combo := combo
		scenarios := ScenariosForCombo(combo, catalog)
		for _, sc := range scenarios {
			sc := sc
			t.Run(combo.Name+"/"+sc.Name, func(t *testing.T) {
				extra := func(s *e2e.Stack, cfg *e2e.DaemonYAML) {
					if sc.NeedsFair {
						e2e.ApplyFairConfig(s, cfg)
					}
					if sc.NeedsSched {
						e2e.ApplyScheduleConfig(s, cfg)
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
				if combo.Exec.Ruby {
					timeout := 60 * time.Second
					if sc.Name == "retry_then_success_ruby" {
						timeout = 120 * time.Second
					}
					s.DrainRubyExecution(timeout)
				}
				sc.Assert(t, s, got)
			})
		}
	}
}
