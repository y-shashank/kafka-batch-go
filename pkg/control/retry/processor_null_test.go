package retry

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

// A literal JSON `null` (or null Kafka value) unmarshals without error into a nil
// map. It must be treated as malformed and routed to the DLT with the offset
// committed — NOT panic on a nil-map write and poison-pill the retry partition.
func TestProcessNullPayloadDoesNotPanic(t *testing.T) {
	p := &Processor{Producer: &memProducer{}}
	for _, raw := range [][]byte{[]byte("null"), []byte("  null  ")} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Process panicked on %q: %v", raw, r)
				}
			}()
			out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short", Partition: 0, Offset: 3})
			if err != nil {
				t.Fatalf("unexpected error on %q: %v", raw, err)
			}
			if !out.CommitOffset {
				t.Fatalf("null payload must commit (not redeliver) — poison pill risk: %+v", out)
			}
			if out.DLTPayload == nil {
				t.Fatalf("null payload must route to DLT: %+v", out)
			}
			var dlt map[string]interface{}
			if err := json.Unmarshal(out.DLTPayload, &dlt); err != nil {
				t.Fatalf("DLT payload not valid JSON: %v", err)
			}
			if dlt["dlt_raw_payload"] == nil {
				t.Fatalf("DLT should preserve raw payload: %v", dlt)
			}
		}()
	}
}

// dltMap must degrade rather than panic if ever handed a nil map directly.
func TestDLTMapNilSafe(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("dltMap panicked on nil map: %v", r)
		}
	}()
	b, key := dltMap(nil, []byte("null"), "retry.short")
	if b == nil || key == "" {
		t.Fatalf("dltMap(nil) = %q/%q", b, key)
	}
}
