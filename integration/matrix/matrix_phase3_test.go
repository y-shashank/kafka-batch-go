//go:build integration

package matrix

import "testing"

func TestMatrix_Phase3_RubyClient(t *testing.T) {
	if testing.Short() {
		t.Skip("phase 3 matrix scenarios require Ruby client")
	}
	runMatrixCatalog(t, Phase3Combos(), PRScenarios())
}

func TestMatrix_Phase3_ClientEnvelopeParity(t *testing.T) {
	if testing.Short() {
		t.Skip("envelope parity requires live Kafka")
	}
	runClientEnvelopeParity(t)
}
