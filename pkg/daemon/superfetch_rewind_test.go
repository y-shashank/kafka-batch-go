package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

// fakeDispatchClient records marks and rewinds so tests can assert the dispatch
// loop never commits past a record it failed to claim.
type fakeDispatchClient struct {
	marked  []*kgo.Record
	rewinds []map[string]map[int32]kgo.EpochOffset
}

func (f *fakeDispatchClient) MarkCommitRecords(rs ...*kgo.Record) {
	f.marked = append(f.marked, rs...)
}

func (f *fakeDispatchClient) SetOffsets(m map[string]map[int32]kgo.EpochOffset) {
	f.rewinds = append(f.rewinds, m)
}

// A transient Redis failure at claim time previously logged "leaving unacked"
// and CONTINUED — later records on the same partition were marked, the commit
// passed the skipped offset, and the job was permanently lost (no workset
// entry existed to reclaim). The dispatch loop must instead rewind the
// un-dispatched tail and stop, so redelivery retries the claim.
func TestDispatchClaimErrorRewindsAndStops(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2

	exec := NewSuperFetchExecutor(cfg, work, "c-rewind",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			t.Error("no record may perform when its claim errored")
			return job.Outcome{}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return nil },
	)
	life := context.Background()
	exec.BindLife(life)

	r1 := &kgo.Record{Topic: "jobs", Partition: 3, Offset: 50, Value: []byte(`{"job_id":"a","job_type":"x"}`)}
	r2 := &kgo.Record{Topic: "jobs", Partition: 3, Offset: 51, Value: []byte(`{"job_id":"dup-b","job_type":"x"}`)}
	// r2 is an in-process duplicate: under the old skip-and-continue behavior it
	// would be reached and MARKED, committing past r1's failed claim.
	exec.inFlight.Store("dup-b", struct{}{})

	mr.SetError("redis brownout")
	fake := &fakeDispatchClient{}
	exec.DispatchClaimsAndAcks(life, fake, []*kgo.Record{r1, r2}, "g")

	if len(fake.marked) != 0 {
		t.Fatalf("marked %d records after a claim error — commit would pass the lost job", len(fake.marked))
	}
	if len(fake.rewinds) != 1 {
		t.Fatalf("rewinds=%d want 1", len(fake.rewinds))
	}
	eo, ok := fake.rewinds[0]["jobs"][3]
	if !ok || eo.Offset != 50 {
		t.Fatalf("rewind=%+v want jobs/3 offset 50", fake.rewinds[0])
	}
	if _, loaded := exec.inFlight.Load("a"); loaded {
		t.Fatal("failed claim must not stay in the in-process dedup map")
	}
	if n := len(exec.ClaimWindow); n != 0 {
		t.Fatalf("ClaimWindow holds %d leaked slots", n)
	}
}

// A malformed record (no job_id) whose DLT routing fails must also rewind
// rather than be skipped past by later marks.
func TestDispatchMissingJobIDApplyErrorRewinds(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2

	exec := NewSuperFetchExecutor(cfg, work, "c-dlt",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{}, nil
		},
		func(ctx context.Context, out job.Outcome) error { return errors.New("dlt produce down") },
	)
	life := context.Background()
	exec.BindLife(life)

	bad := &kgo.Record{Topic: "jobs", Partition: 0, Offset: 7, Value: []byte(`{"job_type":"x"}`)}
	next := &kgo.Record{Topic: "jobs", Partition: 0, Offset: 8, Value: []byte(`{"job_id":"dup-c","job_type":"x"}`)}
	exec.inFlight.Store("dup-c", struct{}{})

	fake := &fakeDispatchClient{}
	exec.DispatchClaimsAndAcks(life, fake, []*kgo.Record{bad, next}, "g")

	if len(fake.marked) != 0 {
		t.Fatalf("marked %d records after DLT failure", len(fake.marked))
	}
	if len(fake.rewinds) != 1 {
		t.Fatalf("rewinds=%d want 1", len(fake.rewinds))
	}
	if eo := fake.rewinds[0]["jobs"][0]; eo.Offset != 7 {
		t.Fatalf("rewind offset=%d want 7", eo.Offset)
	}
	if n := len(exec.ClaimWindow); n != 0 {
		t.Fatalf("ClaimWindow holds %d leaked slots", n)
	}
}

// Sanity: a healthy dispatch still marks records and never rewinds — the fix
// must not change the happy path.
func TestDispatchHealthyClaimStillMarks(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	work := workset.NewStore(rdb)
	cfg := config.DefaultDaemon()
	cfg.SuperFetchConcurrency = 2

	done := make(chan struct{}, 1)
	exec := NewSuperFetchExecutor(cfg, work, "c-ok",
		func(ctx context.Context, raw []byte, src protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{CommitOffset: true}, nil
		},
		func(ctx context.Context, out job.Outcome) error {
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		},
	)
	life := context.Background()
	exec.BindLife(life)

	rec := &kgo.Record{Topic: "jobs", Partition: 1, Offset: 9, Value: []byte(`{"job_id":"ok-1","job_type":"x"}`)}
	fake := &fakeDispatchClient{}
	exec.DispatchClaimsAndAcks(life, fake, []*kgo.Record{rec}, "g")

	if len(fake.marked) != 1 || fake.marked[0].Offset != 9 {
		t.Fatalf("marked=%v want offset 9", fake.marked)
	}
	if len(fake.rewinds) != 0 {
		t.Fatalf("unexpected rewind on healthy path: %v", fake.rewinds)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("perform did not run")
	}
}
