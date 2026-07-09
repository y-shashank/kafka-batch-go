package protocol

import (
	"encoding/json"
	"testing"
)

func TestDecodeJSONMap(t *testing.T) {
	if got := DecodeJSONMap(""); got != nil {
		t.Fatalf("empty: got %v", got)
	}
	m := DecodeJSONMap(`{"run_id":"42"}`)
	if m["run_id"] != "42" {
		t.Fatalf("got %v", m)
	}
}

func TestCallbackMessageCallbackArgs(t *testing.T) {
	cb := CallbackMessage{
		BatchID:      "b1",
		Outcome:      "success",
		CallbackArgs: map[string]interface{}{"channel": "#ops"},
		FinishedAt:   NowISO(),
	}
	raw, err := json.Marshal(cb)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	args, ok := decoded["callback_args"].(map[string]interface{})
	if !ok || args["channel"] != "#ops" {
		t.Fatalf("callback_args missing: %v", decoded)
	}
	if _, hasMeta := decoded["meta"]; hasMeta {
		t.Fatal("meta should be omitted when unset")
	}
}
