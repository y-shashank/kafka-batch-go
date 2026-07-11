package kafkaclient

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRequiredAcksFromConfig(t *testing.T) {
	acks, err := RequiredAcksFromConfig("all_isr")
	if err != nil {
		t.Fatal(err)
	}
	if acks != kgo.AllISRAcks() {
		t.Fatalf("got %v", acks)
	}
	acks, err = RequiredAcksFromConfig("leader")
	if err != nil {
		t.Fatal(err)
	}
	if acks != kgo.LeaderAck() {
		t.Fatalf("got %v", acks)
	}
	if _, err := RequiredAcksFromConfig("bogus"); err == nil {
		t.Fatal("expected error")
	}
}

func TestProduceManyEmpty(t *testing.T) {
	cl, err := New([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceMany(t.Context()); err != nil {
		t.Fatal(err)
	}
}
