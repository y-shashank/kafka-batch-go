//go:build integration

package kafkaclient_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
)

func TestIntegration_ProduceSyncAndConsume(t *testing.T) {
	brokers := integrationBrokers(t)
	if !kafkaReachable(brokers) {
		t.Skip("Kafka broker not reachable")
	}

	topic := "kb.kafkaclient.itest." + strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	ctx := context.Background()

	prod, err := kafkaclient.New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := createTopic(ctx, cl, topic); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"hello":"kafka"}`)
	key := "job-1"
	delivery, err := prod.ProduceSync(ctx, topic, key, payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Partition < 0 {
		t.Fatalf("partition = %d", delivery.Partition)
	}

	cons, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		fetches := cons.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll: %v", errs[0].Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			if string(rec.Key) != key {
				continue
			}
			if string(rec.Value) != string(payload) {
				t.Fatalf("value = %q want %q", rec.Value, payload)
			}
			if rec.Partition != delivery.Partition {
				t.Fatalf("partition = %d want %d", rec.Partition, delivery.Partition)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("timeout waiting for consumed record")
}

func TestIntegration_ProduceManySync(t *testing.T) {
	brokers := integrationBrokers(t)
	if !kafkaReachable(brokers) {
		t.Skip("Kafka broker not reachable")
	}

	topic := "kb.kafkaclient.many." + strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	ctx := context.Background()

	prod, err := kafkaclient.New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := createTopic(ctx, cl, topic); err != nil {
		t.Fatal(err)
	}

	records := []kafkaclient.ProduceRecord{
		{Topic: topic, Key: "a", Payload: []byte("1")},
		{Topic: topic, Key: "b", Payload: []byte("2")},
	}
	deliveries, err := prod.ProduceManySync(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 2 {
		t.Fatalf("deliveries = %d want 2", len(deliveries))
	}

	count, err := prod.TopicPartitionCount(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if count < 1 {
		t.Fatalf("partition count = %d", count)
	}
}

func TestIntegration_ProducePartition(t *testing.T) {
	brokers := integrationBrokers(t)
	if !kafkaReachable(brokers) {
		t.Skip("Kafka broker not reachable")
	}

	topic := "kb.kafkaclient.part." + strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	ctx := context.Background()

	prod, err := kafkaclient.New(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := createTopic(ctx, cl, topic); err != nil {
		t.Fatal(err)
	}

	partition := int32(0)
	if err := prod.ProducePartition(ctx, topic, "k", []byte("p"), partition); err != nil {
		t.Fatal(err)
	}
}

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	if os.Getenv("KAFKA_BATCH_INTEGRATION") != "1" && os.Getenv("KAFKA_BATCH_TEST_BROKERS") == "" {
		t.Skip("set KAFKA_BATCH_INTEGRATION=1 to run Kafka integration tests")
	}
	if v := os.Getenv("KAFKA_BATCH_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

func kafkaReachable(brokers []string) bool {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return cl.Ping(ctx) == nil
}

func createTopic(ctx context.Context, cl *kgo.Client, name string) error {
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(ctx, 1, 1, nil, name)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "exist") {
		return err
	}
	return nil
}
