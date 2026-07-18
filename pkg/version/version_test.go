package version

import "testing"

func TestVersion(t *testing.T) {
	if Version != "0.0.9" {
		t.Fatalf("Version = %q, want 0.0.9", Version)
	}
}
