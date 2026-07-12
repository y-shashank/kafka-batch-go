//go:build integration

package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
)

// TestMatrix_PartitionParity verifies that the Go producer (franz-go) and the
// Ruby producer (WaterDrop/librdkafka with partitioner=murmur2_random) assign
// the SAME partition to the same key on a multi-partition topic.
//
// This is the fairness co-partitioning contract: the fair-ingest topics are
// keyed by tenant_id so every tenant lands on a fixed partition regardless of
// which runtime produced the job. If the two clients hashed keys differently
// (e.g. franz-go murmur2 vs librdkafka's default CRC32), a tenant's jobs would
// scatter across partitions and per-tenant ordering / dispatch fairness would
// break — invisibly, because every single-partition test would still pass.
func TestMatrix_PartitionParity(t *testing.T) {
	if testing.Short() {
		t.Skip("partition parity requires live Kafka + Ruby")
	}
	if !e2e.RubyItestAvailable() {
		t.Skip("Ruby client unavailable (compat/ruby bundle install && kafka-batch gem)")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	// No control/exec tiers needed — this is a pure producer-parity check.
	defer s.Stop()

	const (
		partitions = 6
		keyCount   = 24
	)
	topic := "kb.e2e.partparity." + s.Suffix
	s.CreateTopicPartitions(topic, partitions)

	ctx := context.Background()

	// Go side: produce via a default franz-go client — same partitioner config
	// as pkg/kafkaclient uses in production (no explicit RecordPartitioner).
	produceGoKeys(t, s.Brokers, topic, keyCount)

	// Ruby side: produce the identical keys through WaterDrop.
	runRubyProduceRaw(t, s, topic, keyCount)

	// Collect key -> {src: partition} from the topic and assert agreement.
	byKey := map[string]map[string]int32{}
	total := keyCount * 2
	consumeRecords(t, ctx, s.Brokers, topic, total, 45*time.Second, func(rec *kgo.Record) {
		var m map[string]interface{}
		if json.Unmarshal(rec.Value, &m) != nil {
			return
		}
		key, _ := m["key"].(string)
		src, _ := m["src"].(string)
		if key == "" || src == "" {
			return
		}
		if byKey[key] == nil {
			byKey[key] = map[string]int32{}
		}
		byKey[key][src] = rec.Partition
	})

	if len(byKey) != keyCount {
		t.Fatalf("saw %d distinct keys, want %d", len(byKey), keyCount)
	}
	for key, parts := range byKey {
		goP, okGo := parts["go"]
		rubyP, okRuby := parts["ruby"]
		if !okGo || !okRuby {
			t.Fatalf("key %q missing a side: %+v", key, parts)
		}
		if goP != rubyP {
			t.Fatalf("key %q: go->partition %d, ruby->partition %d (murmur2 mismatch)", key, goP, rubyP)
		}
	}
}

func produceGoKeys(t *testing.T, brokers []string, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("pk-%d", i)
		val, _ := json.Marshal(map[string]interface{}{"key": key, "src": "go", "i": i})
		if res := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: val}); res.FirstErr() != nil {
			t.Fatalf("go produce key %s: %v", key, res.FirstErr())
		}
	}
}

func runRubyProduceRaw(t *testing.T, s *e2e.Stack, topic string, keys int) {
	t.Helper()
	cmd := rubyScriptCommand("ruby_client_ittest.rb", s.ConfigPath, s.ManifestPath,
		"--mode", "produce-raw", "--topic", topic, "--keys", fmt.Sprintf("%d", keys))
	cmd.Env = append(os.Environ(),
		"REDIS_URL="+s.Redis,
		"KBATCH_RUBY_GEM_ROOT="+e2e.KafkaBatchGemRoot(),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ruby produce-raw: %v\n%s", err, string(out))
	}
}

func consumeRecords(t *testing.T, ctx context.Context, brokers []string, topic string, want int, timeout time.Duration, fn func(*kgo.Record)) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("kb-e2e-partparity-"+uuid.NewString()[:8]),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	seen := 0
	deadline := time.Now().Add(timeout)
	for seen < want && time.Now().Before(deadline) {
		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			// Timeout on an empty poll is expected while waiting; keep looping.
			continue
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			fn(iter.Next())
			seen++
		}
	}
	if seen < want {
		t.Fatalf("consumed %d records from %s, want %d (timeout)", seen, topic, want)
	}
}
