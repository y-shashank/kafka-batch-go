//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

func TestMatrix_Phase4_RubyControl(t *testing.T) {
	if testing.Short() {
		t.Skip("phase 4 matrix scenarios require Ruby control")
	}
	catalog := []Scenario{
		Phase1Scenarios()[0], // batch_completion_go
		Phase2Scenarios()[1], // retry_then_success_ruby — only for ruby exec combo
	}
	for _, combo := range Phase4Combos() {
		combo := combo
		for _, sc := range ScenariosForCombo(combo, catalog) {
			sc := sc
			t.Run(combo.Name+"/"+sc.Name, func(t *testing.T) {
				runMatrixScenario(t, combo, sc)
			})
		}
	}
}

func runClientEnvelopeParity(t *testing.T) {
	t.Helper()
	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Go: true}})
	defer s.Stop()

	ctx := context.Background()
	goC := s.NewClient()
	defer goC.Close()

	goID, err := goC.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"n": 1}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	goMsg := s.PollTopic(ctx, s.WorkerTopic, func(m map[string]interface{}) bool {
		return m["job_id"] == goID
	}, 30*time.Second)

	rubyC := NewClient(s, ClientRuby)
	defer rubyC.Close()
	rubyID, err := rubyC.EnqueueJob(ctx, "integration.go_daemon", map[string]interface{}{"n": 1}, client.PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rubyMsg := s.PollTopic(ctx, s.WorkerTopic, func(m map[string]interface{}) bool {
		return m["job_id"] == rubyID
	}, 30*time.Second)

	for _, key := range []string{"job_type", "worker_class", "runtime"} {
		if goMsg[key] != rubyMsg[key] {
			t.Fatalf("%s: go=%v ruby=%v", key, goMsg[key], rubyMsg[key])
		}
	}
	if goMsg["payload"] == nil || rubyMsg["payload"] == nil {
		t.Fatal("missing payload in produced message")
	}
}
