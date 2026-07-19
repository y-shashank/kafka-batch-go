package version

import "testing"

func TestVersion(t *testing.T) {
	if Version != "0.0.12" {
		t.Fatalf("Version = %q, want 0.0.12", Version)
	}
}
