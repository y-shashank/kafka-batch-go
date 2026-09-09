package kafkaclient

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// franz-go's ProduceSync appends results in promise-completion order, not
// submission order: keyed records hash to different partitions/brokers and
// finish interleaved. These tests pin the identity-based correlation that
// replaced the positional zip (which silently paired job A with job B's
// partition/offset — schedule-index corruption).

func mkRecs(n int) ([]*kgo.Record, map[*kgo.Record]int) {
	recs := make([]*kgo.Record, n)
	idx := make(map[*kgo.Record]int, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "t", Partition: int32(i), Offset: int64(100 + i)}
		idx[recs[i]] = i
	}
	return recs, idx
}

func TestCorrelateOutcomes_ShuffledCompletionOrder(t *testing.T) {
	recs, idx := mkRecs(5)
	// Results arrive in an order unrelated to submission order.
	order := []int{3, 0, 4, 2, 1}
	results := make(kgo.ProduceResults, 0, len(recs))
	for _, i := range order {
		results = append(results, kgo.ProduceResult{Record: recs[i]})
	}
	out, err := correlateOutcomes(len(recs), idx, results)
	if err != nil {
		t.Fatal(err)
	}
	for i, o := range out {
		if o.Err != nil {
			t.Fatalf("record %d unexpected err %v", i, o.Err)
		}
		if o.Delivery.Partition != int32(i) || o.Delivery.Offset != int64(100+i) {
			t.Fatalf("record %d got delivery %+v — coordinates from another record", i, o.Delivery)
		}
	}
}

func TestCorrelateOutcomes_MidBatchFailure(t *testing.T) {
	recs, idx := mkRecs(3)
	boom := errors.New("broker unreachable")
	results := kgo.ProduceResults{
		{Record: recs[2]},             // completes first
		{Record: recs[0], Err: boom},  // fails
		{Record: recs[1]},             // completes after the failure
	}
	out, err := correlateOutcomes(len(recs), idx, results)
	if err == nil {
		t.Fatal("expected first error returned")
	}
	if out[0].Err == nil {
		t.Fatal("failed record must carry its error — it was previously counted as delivered")
	}
	if out[1].Err != nil || out[2].Err != nil {
		t.Fatalf("successful records must not inherit the failure: %v %v", out[1].Err, out[2].Err)
	}
	if out[1].Delivery.Offset != 101 || out[2].Delivery.Offset != 102 {
		t.Fatalf("deliveries misattributed: %+v %+v", out[1].Delivery, out[2].Delivery)
	}
}

func TestCorrelateOutcomes_MissingResultFailsRecord(t *testing.T) {
	recs, idx := mkRecs(2)
	results := kgo.ProduceResults{{Record: recs[0]}}
	out, err := correlateOutcomes(len(recs), idx, results)
	if err == nil {
		t.Fatal("expected error for missing result")
	}
	if out[0].Err != nil {
		t.Fatalf("record 0 delivered: %v", out[0].Err)
	}
	if out[1].Err == nil {
		t.Fatal("record 1 has no result — a zero Delivery must not pass as partition 0 / offset 0")
	}
}
