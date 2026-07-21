package alerts

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/instrument"
)

func TestEvaluateOnceDisabledAndLock(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	sum := EvaluateOnce(ctx, rdb, Config{Enabled: false})
	if sum["reason"] != "disabled" {
		t.Fatalf("%v", sum)
	}

	st := NewState(rdb)
	_ = st.TryLock(ctx, 60)
	sum = EvaluateOnce(ctx, rdb, Config{Enabled: true, Interval: 60})
	if sum["reason"] != "lock" {
		t.Fatalf("%v", sum)
	}
}

func TestEvaluateOnceOKNoBrokers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	sum := EvaluateOnce(ctx, rdb, Config{
		Enabled:  true,
		Interval: 30,
		ForTicks: 1,
		Rules:    defaultRules(),
	})
	if sum["ok"] != true {
		t.Fatalf("%v", sum)
	}
	if sum["runtime"] != "go" {
		t.Fatalf("%v", sum)
	}
}

func TestApplyHysteresisFireAndResolve(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewState(rdb)
	ctx := context.Background()
	cfg := Config{ForTicks: 2, ResolveTicks: 2}

	f := Finding{RuleID: "r", Fingerprint: "fp", Title: "T", Summary: "S", Severity: "warning", Link: "/x"}

	tr := applyHysteresis(ctx, st, cfg, []Finding{f})
	if len(tr) != 0 {
		t.Fatalf("need 2 ticks, got %+v", tr)
	}
	tr = applyHysteresis(ctx, st, cfg, []Finding{f})
	if len(tr) != 1 || tr[0].event != "fired" {
		t.Fatalf("fire %+v", tr)
	}
	tr = applyHysteresis(ctx, st, cfg, []Finding{f})
	if len(tr) != 0 {
		t.Fatalf("no re-fire %+v", tr)
	}
	tr = applyHysteresis(ctx, st, cfg, nil)
	if len(tr) != 0 {
		t.Fatalf("need 2 healthy ticks %+v", tr)
	}
	tr = applyHysteresis(ctx, st, cfg, nil)
	if len(tr) != 1 || tr[0].event != "resolved" {
		t.Fatalf("resolve %+v", tr)
	}
}

func TestNotifyTransitionsDedupe(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewState(rdb)
	ctx := context.Background()
	cfg := Config{ChannelMetrics: true, MetricsEnabled: true, Interval: 60}

	tr := []transition{{
		event: "fired",
		finding: Finding{
			RuleID: "r", Fingerprint: "fp", Title: "t", Summary: "s", Severity: "warning",
		},
		firedAt: time.Now().UTC().Format(time.RFC3339),
	}}
	notifyTransitions(ctx, st, cfg, tr)
	notifyTransitions(ctx, st, cfg, tr) // deduped
	if countEvent(tr, "fired") != 1 {
		t.Fatal("countEvent")
	}
	if str(nil) != "" || str("x") != "x" || str(1) != "" {
		t.Fatal("str")
	}
}

func TestInstallInstrumentHooks(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	remove := InstallInstrumentHooks(rdb)
	defer remove()

	instrument.Emit("dlt.published", nil, 0)
	instrument.Emit("cron.stale", map[string]interface{}{
		"schedule": "nightly", "job_type": "Job", "stale_seconds": 12,
	}, 0)
	instrument.Emit("cron.stale", map[string]interface{}{"job_type": "x"}, 0) // no schedule

	st := NewState(rdb)
	ctx := context.Background()
	if st.DLTCountLastMinute(ctx) != 1 {
		t.Fatalf("dlt=%d", st.DLTCountLastMinute(ctx))
	}
	entries := st.CronStaleEntries(ctx)
	if len(entries) != 1 {
		t.Fatalf("%v", entries)
	}
	if intFrom(12) != 12 || intFrom(int64(3)) != 3 || intFrom(4.0) != 4 || intFrom("5") != 5 || intFrom(true) != 0 {
		t.Fatal("intFrom")
	}
}

func TestRunSchedulerImmediateTick(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticks := 0
	done := make(chan struct{})
	RunScheduler(ctx, config.Daemon{
		AlertsIntervalSec: 60,
		AlertsEnabled:     false,
	}, rdb, func() {
		ticks++
		close(done)
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for scheduler tick")
	}
	cancel()
	if ticks < 1 {
		t.Fatalf("ticks=%d", ticks)
	}
}

func TestSamplerRedisHelpers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()
	st := NewState(rdb)

	_ = rdb.SAdd(ctx, "kafka_batch:consumption:topics", "g\x1ft").Err()
	_ = rdb.HSet(ctx, "kafka_batch:reconciler:last", "ran_at", time.Now().UTC().Format(time.RFC3339)).Err()
	_ = rdb.ZAdd(ctx, "kafka_batch:sched:pending", redis.Z{Score: 1, Member: "a"}).Err()
	_ = rdb.Set(ctx, "kafka_batch:live:consumer:c1", "1", 0).Err()
	bucket := (time.Now().UTC().Unix() / 60) * 60
	rttKey := "kafka_batch:perf:min:" + strconv.FormatInt(bucket, 10) + ":rtt"
	_ = rdb.HSet(ctx, rttKey, map[string]interface{}{
		"count": "2", "sum_us": "4000", "max_us": "3000", "errors": "0",
	}).Err()

	sample := collectSample(ctx, rdb, st, Config{})
	if len(sample.PausedKeys) != 1 || sample.LiveConsumers != 1 {
		t.Fatalf("%+v", sample)
	}
	if sample.SchedulePending != 1 || sample.Reconciler["ran_at"] == "" {
		t.Fatalf("%+v", sample)
	}
	if sample.RTT == nil {
		t.Fatal("rtt")
	}
	c, e := int64(1), int64(4)
	persistBaseline(ctx, st, Sample{LagTopics: []LagRow{{
		Group: "g", Topic: "t", Lag: 3, CommittedSum: &c, EndSum: &e,
	}}})
	if st.LoadBaseline(ctx)["g|t"]["lag"] != float64(3) {
		t.Fatalf("%v", st.LoadBaseline(ctx))
	}
	rows, pending := collectLag(ctx, Config{}) // no brokers
	if rows != nil || pending != 0 {
		t.Fatalf("%v %d", rows, pending)
	}
}
