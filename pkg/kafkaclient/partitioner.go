package kafkaclient

import "github.com/twmb/franz-go/pkg/kgo"

// explicitOrHashPartitioner honors an explicitly-set Record.Partition (>= 0) and
// otherwise falls back to franz-go's default murmur2 key-hash (sticky) partitioner.
//
// Why this exists: franz-go's default partitioner IGNORES Record.Partition and
// always routes by key. Fairness ingest routing resolves a tenant -> exclusive
// ingest partition and sets Record.Partition with an EMPTY key, so under the
// default partitioner every fair record was sticky-routed to a single partition
// (all tenants collapsed onto one partition, destroying per-tenant isolation and
// ingest parallelism). Callers set Record.Partition = -1 to request key-hash
// routing (the non-fair default).
func explicitOrHashPartitioner() kgo.Partitioner {
	return explicitPartitioner{fallback: kgo.StickyKeyPartitioner(nil)}
}

type explicitPartitioner struct{ fallback kgo.Partitioner }

func (e explicitPartitioner) ForTopic(t string) kgo.TopicPartitioner {
	return &explicitTopicPartitioner{fallback: e.fallback.ForTopic(t)}
}

type explicitTopicPartitioner struct {
	fallback kgo.TopicPartitioner
}

func (e *explicitTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	if r.Partition >= 0 {
		return true
	}
	return e.fallback.RequiresConsistency(r)
}

func (e *explicitTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 && int(r.Partition) < n {
		return int(r.Partition)
	}
	return e.fallback.Partition(r, n)
}

// OnNewBatch forwards to the fallback so its sticky rotation still works for
// empty-key, non-explicit records.
func (e *explicitTopicPartitioner) OnNewBatch() {
	if b, ok := e.fallback.(interface{ OnNewBatch() }); ok {
		b.OnNewBatch()
	}
}
