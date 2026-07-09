//go:build integration

package matrix

import "testing"

func TestMatrix_Full(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix is slow")
	}
	combos := append(append(Phase1Combos(), Phase3Combos()...), Phase4Combos()...)
	runMatrixCatalog(t, combos, FullScenarios())
}
