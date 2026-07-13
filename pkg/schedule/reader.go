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

	deadline := time.Now().Add(5 * time.Second)
	scanned := 0
	const scanSlack = 1000
	for time.Now().Before(deadline) && remaining > 0 && scanned < scanSlack {
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
				if rec.Offset > pr.maxOff+scanSlack {
					continue
				}
				scanned++
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
