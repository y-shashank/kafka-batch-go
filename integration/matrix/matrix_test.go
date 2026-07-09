//go:build integration

package matrix

import "testing"

func TestMatrix_Phase1(t *testing.T) {
	runMatrixCatalog(t, Phase1Combos(), Phase1Scenarios())
}

func TestMatrix_PR(t *testing.T) {
	runMatrixCatalog(t, Phase1Combos(), PRScenarios())
}
