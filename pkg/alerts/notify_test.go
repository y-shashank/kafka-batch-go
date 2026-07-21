package alerts

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestTruncate(t *testing.T) {
	if truncate("abc", 10) != "abc" {
		t.Fatal("short")
	}
	if truncate("abcdef", 3) != "abc" {
		t.Fatal("long")
	}
}

func TestPostJSONAndDeliver(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		b, _ := io.ReadAll(r.Body)
		var m map[string]interface{}
		_ = json.Unmarshal(b, &m)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type")
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer fail.Close()

	m := NewMulti(Config{
		ChannelSlack:     true,
		ChannelWebhook:   true,
		ChannelMetrics:   true,
		MetricsEnabled:   true,
		EncryptionSalt:   "salt",
		SlackWebhookURL:  srv.URL,
		WebhookURLs:      []string{srv.URL, fail.URL},
	})
	m.hc = srv.Client()

	var events []string
	remove := instrument.AddHandler(func(event string, _ map[string]interface{}, _ float64) {
		events = append(events, event)
	})
	defer remove()

	m.Deliver(Payload{
		Event: "fired", RuleID: "r1", Title: "Hello", Summary: "body",
		Severity: "warning", Fingerprint: "fp1", Link: "/lag",
	})
	if posts.Load() < 2 {
		t.Fatalf("expected slack+webhook posts, got %d", posts.Load())
	}
	if len(events) != 1 || events[0] != "alert.fired" {
		t.Fatalf("events=%v", events)
	}

	m2 := NewMulti(Config{})
	m2.hc = fail.Client()
	if err := m2.postJSON(fail.URL, map[string]string{"a": "b"}); err == nil {
		t.Fatal("expected http error")
	}

	m3 := NewMulti(Config{ChannelMetrics: true, MetricsEnabled: true})
	events = nil
	m3.Deliver(Payload{Event: "resolved", RuleID: "r", Title: "t", Fingerprint: "f"})
	if len(events) != 1 || events[0] != "alert.resolved" {
		t.Fatalf("resolved events=%v", events)
	}
}
