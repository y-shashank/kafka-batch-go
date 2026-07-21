package consumption

import (
	"testing"
	"time"
)

func TestTopicAndPartitionKeys(t *testing.T) {
	if got := TopicKey("g1", "topic.a"); got != "g1"+sep+"topic.a" {
		t.Fatalf("TopicKey = %q", got)
	}
	if got := PartitionKey("g1", "topic.a", 7); got != "g1"+sep+"topic.a"+sep+"7" {
		t.Fatalf("PartitionKey = %q", got)
	}
}

func TestNewControlDefaultInterval(t *testing.T) {
	c := NewControl(nil, 0)
	if c.Interval != 30*time.Second {
		t.Fatalf("Interval = %s, want 30s", c.Interval)
	}
}

func TestToSetTrimsAndDropsEmpty(t *testing.T) {
	got := toSet([]string{" a ", "", "b", "  "})
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2: %v", len(got), got)
	}
	if _, ok := got["a"]; !ok {
		t.Fatalf("missing a: %v", got)
	}
	if _, ok := got["b"]; !ok {
		t.Fatalf("missing b: %v", got)
	}
}
