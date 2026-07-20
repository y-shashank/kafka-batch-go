package kafkaclient

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestExplicitOrHashPartitioner(t *testing.T) {
	tp := explicitOrHashPartitioner().ForTopic("jobs")

	// Explicit partition is honored (this is what fairness tenant routing needs).
	for _, p := range []int32{0, 3, 9} {
		if got := tp.Partition(&kgo.Record{Partition: p}, 10); got != int(p) {
			t.Fatalf("explicit partition %d routed to %d", p, got)
		}
		if !tp.RequiresConsistency(&kgo.Record{Partition: p}) {
			t.Fatalf("explicit partition %d should require consistency", p)
		}
	}

	// Explicit partition out of range falls back to key-hash (defensive).
	if got := tp.Partition(&kgo.Record{Partition: 99, Key: []byte("k")}, 10); got < 0 || got >= 10 {
		t.Fatalf("out-of-range explicit fell back to invalid partition %d", got)
	}

	// -1 (no explicit) → key-hash fallback: deterministic per key, in range.
	a := tp.Partition(&kgo.Record{Partition: -1, Key: []byte("lt-t1")}, 10)
	b := tp.Partition(&kgo.Record{Partition: -1, Key: []byte("lt-t1")}, 10)
	if a != b {
		t.Fatalf("key-hash not deterministic: %d vs %d", a, b)
	}
	if a < 0 || a >= 10 {
		t.Fatalf("key-hash partition out of range: %d", a)
	}
}
