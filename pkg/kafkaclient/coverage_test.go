package kafkaclient

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWithRequiredAcksAndInnerPoll(t *testing.T) {
	opt := WithRequiredAcks(kgo.LeaderAck())
	cfg := clientOpts{requiredAcks: kgo.AllISRAcks()}
	opt(&cfg)
	if cfg.requiredAcks != kgo.LeaderAck() {
		t.Fatal(cfg.requiredAcks)
	}
	// franz-go requires all-ISR acks when idempotency is enabled (client default).
	cl, err := New([]string{"127.0.0.1:1"}, WithRequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if cl.Inner() == nil {
		t.Fatal("inner")
	}
	if err := cl.Poll(context.Background(), []string{"t"}, "g", nil); err == nil {
		t.Fatal("expected poll stub error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cl.Produce(ctx, "t", "k", []byte("v")); err == nil {
		t.Fatal("expected produce failure against dead broker")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if err := cl.ProducePartition(ctx2, "t", "k", []byte("v"), 0); err == nil {
		t.Fatal("expected produce partition failure")
	}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel3()
	if _, err := cl.ProduceSync(ctx3, "t", "k", []byte("v"), nil); err == nil {
		t.Fatal("expected produce sync failure")
	}
	p := int32(1)
	ctx4, cancel4 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel4()
	if _, err := cl.ProduceSync(ctx4, "t", "k", []byte("v"), &p); err == nil {
		t.Fatal("expected produce sync with partition failure")
	}
	if _, err := cl.ProduceManySync(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	ctx5, cancel5 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel5()
	if _, err := cl.ProduceManySync(ctx5, []ProduceRecord{{Topic: "t", Key: "k", Payload: []byte("v"), Partition: &p}}); err == nil {
		t.Fatal("expected many sync failure")
	}
	ctx6, cancel6 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel6()
	if _, err := cl.TopicPartitionCount(ctx6, "missing"); err == nil {
		t.Fatal("expected metadata failure")
	}
}

func TestPartitionerOnNewBatch(t *testing.T) {
	tp := explicitOrHashPartitioner().ForTopic("jobs").(*explicitTopicPartitioner)
	tp.OnNewBatch() // should not panic
	if !tp.RequiresConsistency(&kgo.Record{Partition: 1}) {
		t.Fatal("explicit consistency")
	}
	_ = tp.RequiresConsistency(&kgo.Record{Partition: -1, Key: []byte("k")})
}
