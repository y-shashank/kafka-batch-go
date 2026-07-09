//go:build integration

package matrix

import (
	"testing"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
)

func TestMatrix_Phase2_RubyFairAndRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("phase 2 matrix scenarios are slow")
	}
	combo := Combo{Name: "go_client_go_control_ruby_exec", Client: ClientGo, Control: ControlGo, Exec: e2e.ExecMode{Ruby: true}}
	for _, sc := range ScenariosForCombo(combo, Phase2Scenarios()) {
		sc := sc
		t.Run(sc.Name, func(t *testing.T) {
			runMatrixScenario(t, combo, sc)
		})
	}
}
