package retry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

type memProducer struct {
	msgs []struct {
		topic string
		key   string
		body  []byte
	}
}

func (m *memProducer) Produce(_ context.Context, topic, key string, payload []byte) error {
	m.msgs = append(m.msgs, struct {
		topic string
		key   string
		body  []byte
	}{topic, key, payload})
	return nil
}

func TestProcessRepublishesWhenReady(t *testing.T) {
	now := time.Unix(100, 0)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id":      "j1",
		"retry_after": now.Add(-time.Second).UTC().Format(time.RFC3339),
		"retry_to":    "jobs.worker",
		"attempt":     float64(1),
	})

	p := &Processor{Producer: &memProducer{}, Now: func() time.Time { return now }, MaxPause: 30 * time.Second}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short", Partition: 0, Offset: 9})
	if err != nil {
		t.Fatal(err)
	}
	if out.Pause {
		t.Fatal("expected no pause")
	}
	if out.ProduceTopic != "jobs.worker" || out.ProduceBody == nil {
		t.Fatalf("produce %+v", out)
	}
	var m map[string]interface{}
	_ = json.Unmarshal(out.ProduceBody, &m)
	if _, ok := m["retry_after"]; ok {
		t.Fatal("retry_after should be stripped")
	}
}

func TestProcessPausesWhenNotReady(t *testing.T) {
	now := time.Unix(100, 0)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id":      "j1",
		"retry_after": now.Add(2 * time.Minute).UTC().Format(time.RFC3339),
		"retry_to":    "jobs.worker",
	})

	p := &Processor{Producer: &memProducer{}, Now: func() time.Time { return now }, MaxPause: 30 * time.Second}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short"})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Pause || out.PauseFor <= 0 {
		t.Fatalf("pause %+v", out)
	}
	if out.CommitOffset {
		t.Fatal("should not commit while paused")
	}
}

func TestProcessParsesRubyISO8601Offset(t *testing.T) {
	now := time.Date(2026, 7, 13, 11, 8, 0, 0, time.FixedZone("IST", 5*3600+1800))
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id":      "j1",
		"retry_after": "2026-07-13T11:07:55+05:30",
		"retry_to":    "jobs.worker",
	})
	p := &Processor{Producer: &memProducer{}, Now: func() time.Time { return now }, MaxPause: 30 * time.Second}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Pause {
		t.Fatalf("expected due retry, got pause %+v", out)
	}
	if out.ProduceTopic != "jobs.worker" {
		t.Fatalf("produce %+v", out)
	}
}

func TestProcessMissingRetryToDLT(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id":    "j1",
		"batch_id":  "b1",
		"batch_seq": float64(1),
	})
	p := &Processor{Producer: &memProducer{}, Now: time.Now}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short"})
	if err != nil {
		t.Fatal(err)
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
	if out.Event == nil || out.Event.Status != "failed" {
		t.Fatalf("event %+v", out.Event)
	}
}
