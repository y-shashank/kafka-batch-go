package uniq

import (
	"testing"

	"github.com/cespare/xxhash/v2"
)

func TestFingerprintStableKeyOrder(t *testing.T) {
	a := DigestHex("Worker", map[string]interface{}{"a": 1, "b": 2})
	b := DigestHex("Worker", map[string]interface{}{"b": 2, "a": 1})
	if a != b {
		t.Fatalf("fingerprints differ: %s vs %s", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("expected 32 hex chars, got %d", len(a))
	}
}

func TestFingerprintDiffersByWorker(t *testing.T) {
	payload := map[string]interface{}{"id": 1}
	a := DigestHex("WorkerA", payload)
	b := DigestHex("WorkerB", payload)
	if a == b {
		t.Fatal("expected different fingerprints")
	}
}

// TestCanonicalPayloadNoHTMLEscape pins the canonical serialization to Ruby's
// Oj.dump(mode: :compat) output. encoding/json's default HTML escaping would
// turn '<', '>', '&' into </>/& — which Ruby never does — so a
// Ruby-enqueued and a Go-enqueued uniq job with the same payload would compute
// different fingerprints and both run instead of deduping. These payloads are
// realistic (URLs with query strings, names, HTML fragments).
func TestCanonicalPayloadNoHTMLEscape(t *testing.T) {
	cases := []struct {
		payload map[string]interface{}
		want    string
	}{
		{map[string]interface{}{"html": "<a>&</a>"}, `{"html":"<a>&</a>"}`},
		{map[string]interface{}{"url": "https://x/?a=1&b=2"}, `{"url":"https://x/?a=1&b=2"}`},
		{map[string]interface{}{"b": 2, "a": 1}, `{"a":1,"b":2}`},   // deep key sort
		{map[string]interface{}{"name": "café"}, `{"name":"café"}`}, // non-ASCII passthrough
	}
	for _, c := range cases {
		if got := canonicalPayload(c.payload); got != c.want {
			t.Fatalf("canonicalPayload(%v) = %q want %q", c.payload, got, c.want)
		}
	}
}

// TestFingerprintRubyAlgorithmParity independently reconstructs the documented
// Ruby fingerprint algorithm (material = "<worker>\0<canonical>", dual XXHash64
// with the uniq_salt_v1 salt, packed little-endian) and asserts DigestHex agrees
// on a special-character payload. This locks the on-wire _uniq_fp contract that
// the cross-runtime matrix test verifies against a live Ruby client.
func TestFingerprintRubyAlgorithmParity(t *testing.T) {
	worker := "go:integration.go_uniq"
	payload := map[string]interface{}{"html": "<a>&</a>", "n": 1}

	canonical := `{"html":"<a>&</a>","n":1}`
	if got := canonicalPayload(payload); got != canonical {
		t.Fatalf("canonical = %q want %q", got, canonical)
	}

	material := worker + "\x00" + canonical
	h1 := xxhash.Sum64String(material)
	h2 := xxhash.Sum64String(material + "\x00uniq_salt_v1")
	want := make([]byte, 16)
	for i, h := range []uint64{h1, h2} {
		for j := 0; j < 8; j++ {
			want[i*8+j] = byte(h >> (8 * j))
		}
	}

	if got := hexEncode(want); got != DigestHex(worker, payload) {
		t.Fatalf("DigestHex = %q want %q", DigestHex(worker, payload), got)
	}
}

func hexEncode(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = digits[c>>4]
		out[i*2+1] = digits[c&0x0f]
	}
	return string(out)
}
