package version

import "testing"

func TestVersion(t *testing.T) {
	if Version != "0.0.11" {
		t.Fatalf("Version = %q, want 0.0.11", Version)
	}
}
