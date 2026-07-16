package workset

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMarshalEntryGzipRoundTrip(t *testing.T) {
	body := bytes.Repeat([]byte(`{"job_id":"j1","payload":{"x":1}}`), 20) // >256
	e := &Entry{
		JobID: "j1", Payload: body, Topic: "jobs", Partition: 1, Offset: 9,
		ConsumerID: "c1", Fence: "f1", Runtime: "go",
	}
	raw, err := marshalEntryJSON(e)
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Encoding != encodingGzip {
		t.Fatalf("encoding=%q", got.Encoding)
	}
	if len(got.Payload) >= len(body) {
		t.Fatalf("expected compressed payload smaller: %d vs %d", len(got.Payload), len(body))
	}
	plain, err := PayloadForReclaim(&got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain, body) {
		t.Fatal("round-trip mismatch")
	}
}

func TestMarshalEntrySmallUncompressed(t *testing.T) {
	e := &Entry{JobID: "j", Payload: []byte(`{"job_id":"j"}`), Topic: "t", Runtime: "go"}
	raw, err := marshalEntryJSON(e)
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Encoding != "" {
		t.Fatalf("expected no encoding for small payload, got %q", got.Encoding)
	}
}
