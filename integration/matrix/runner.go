//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
)

func runMatrixScenario(t *testing.T, combo Combo, sc Scenario) {
	t.Helper()
	handlersFn := e2e.BaseHandlersStack
	if sc.NeedsPriority {
		handlersFn = e2e.PriorityHandlersStack
	}
	extra := func(s *e2e.Stack, cfg *e2e.DaemonYAML) {
		if sc.NeedsFair {
			e2e.ApplyFairConfig(s, cfg)
		}
		if sc.NeedsSched {
			e2e.ApplyScheduleConfig(s, cfg)
		}
		if sc.NeedsPriority {
			e2e.ApplyPriorityConfig(s, cfg)
		}
	}
	s := e2e.NewStack(t, handlersFn, extra)
	s.StartWithOptions(e2e.StackStartOptions{
		Control: e2e.ControlMode(combo.Control),
		Exec:    combo.Exec,
	})
	defer s.Stop()

	c := NewClient(s, combo.Client)
	defer c.Close()

	ctx := context.Background()
	got, err := sc.Run(ctx, s, c)
	if err != nil {
		t.Fatal(err)
	}
	if sc.NeedsRubyExec {
		timeout := 60 * time.Second
		if sc.Name == "retry_then_success_ruby" {
			timeout = 120 * time.Second
		}
		if sc.NeedsFair && sc.Name == "fair_routing_ruby_execution" {
			s.DrainRubyExecution(timeout, s.TimeReadyRuby)
		} else {
			s.DrainRubyExecution(timeout)
		}
	}
	sc.Assert(t, s, got)
}

func runMatrixCatalog(t *testing.T, combos []Combo, catalog []Scenario) {
	for _, combo := range combos {
		combo := combo
		scenarios := ScenariosForCombo(combo, catalog)
		for _, sc := range scenarios {
			sc := sc
			t.Run(combo.Name+"/"+sc.Name, func(t *testing.T) {
				runMatrixScenario(t, combo, sc)
			})
		}
	}
}
