package schedule

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Reader fetches payloads from the scheduled topic by partition/offset.
// Uses assign-based direct partition consuming (no consumer group), matching
// Ruby Schedule::ScheduledReader.
type Reader struct {
	brokers []string
	topic   string
	adm     *kadm.Client
	client  *kgo.Client // admin/metadata only
}

func NewReader(brokers []string, topic string) (*Reader, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &Reader{brokers: brokers, topic: topic, client: cl, adm: kadm.NewClient(cl)}, nil
}

type ReadResult struct {
	Found map[string][]byte
	Lost  []string
}

type partitionWant struct {
	partition int32
	want      map[int64]struct{}
	minOff    int64
	maxOff    int64
}

// Read loads payloads for partition→offsets map.
func (r *Reader) Read(ctx context.Context, byPartition map[int32][]int64) (ReadResult, error) {
	out := ReadResult{Found: map[string][]byte{}}
	if len(byPartition) == 0 {
		return out, nil
	}

	startOffs, err := r.adm.ListStartOffsets(ctx, r.topic)
	if err != nil {
		return out, fmt.Errorf("list start offsets: %w", err)
	}
	endOffs, err := r.adm.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return out, fmt.Errorf("list end offsets: %w", err)
	}

	var reads []partitionWant
	assign := map[string]map[int32]kgo.Offset{r.topic: {}}

	for partition, offsets := range byPartition {
		if len(offsets) == 0 {
			continue
		}
		sorted := append([]int64(nil), offsets...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		low, _ := startOffs.Lookup(r.topic, partition)
		high, _ := endOffs.Lookup(r.topic, partition)

		want := make(map[int64]struct{})
		minOff := int64(-1)
		maxOff := int64(-1)
		for _, off := range sorted {
			loc := BuildKey(partition, off)
			if off < low.Offset {
				out.Lost = append(out.Lost, loc)
				continue
			}
			if off >= high.Offset {
				continue
			}
			want[off] = struct{}{}
			if minOff < 0 || off < minOff {
				minOff = off
			}
			if off > maxOff {
				maxOff = off
			}
		}
		if len(want) == 0 {
			continue
		}
		reads = append(reads, partitionWant{partition: partition, want: want, minOff: minOff, maxOff: maxOff})
		assign[r.topic][partition] = kgo.NewOffset().At(minOff)
	}

	if len(reads) == 0 {
		return out, nil
	}

	readCl, err := kgo.NewClient(
		kgo.SeedBrokers(r.brokers...),
		kgo.ConsumePartitions(assign),
	)
	if err != nil {
		return out, fmt.Errorf("schedule read client: %w", err)
	}
	defer readCl.Close()

	remaining := 0
	for _, pr := range reads {
		remaining += len(pr.want)
	}

	// Bound the read by the deadline and per-partition offset span ONLY — never a
	// global record cap. A prior global `scanned < 1000` cap truncated wide-span
	// batches (offsets reflect produce time, not due time, so a claimed batch can
	// span >1000 physical offsets on a busy topic — and any schedule_batch_size >
	// 1000 always) leaving high-offset due jobs unread → read-missed → dead-
	// lettered. Ruby bounds per-partition span (max_offset + SCAN_SLACK) with no
	// global count cap; match that. The 5s deadline caps worst-case scan time.
	deadline := time.Now().Add(5 * time.Second)
	const scanSlack = 1000
	for time.Now().Before(deadline) && remaining > 0 {
		fetches := readCl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			return out, fmt.Errorf("poll scheduled topic: %v", errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if rec.Topic != r.topic {
				return
			}
			for i := range reads {
				pr := &reads[i]
				if pr.partition != rec.Partition {
					continue
				}
				if len(pr.want) == 0 {
					break // partition already satisfied or abandoned
				}
				if rec.Offset > pr.maxOff+scanSlack {
					// Read past this partition's wanted span (+slack): the still-
					// wanted offsets are unreachable (gap/compaction). Abandon them
					// so the loop can terminate; they surface upstream as read
					// misses instead of stalling the scan for the full deadline.
					remaining -= len(pr.want)
					pr.want = map[int64]struct{}{}
					break
				}
				if _, ok := pr.want[rec.Offset]; ok {
					key := BuildKey(pr.partition, rec.Offset)
					out.Found[key] = append([]byte(nil), rec.Value...)
					delete(pr.want, rec.Offset)
					remaining--
				}
				break
			}
		})
	}

	return out, nil
}

func (r *Reader) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// ParseOffsets converts string map keys from tests.
func ParseOffsets(m map[string][]int64) map[int32][]int64 {
	out := map[int32][]int64{}
	for k, offs := range m {
		p, _ := strconv.Atoi(k)
		out[int32(p)] = offs
	}
	return out
}
