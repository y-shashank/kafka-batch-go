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
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
	"github.com/y-shashank/kafka-batch-go/pkg/health"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
)

func TestPrefixOr(t *testing.T) {
	if got := prefixOr("", "jobs"); got != "jobs" {
		t.Fatalf("%q", got)
	}
	if got := prefixOr("kb", "kb.jobs"); got != "kb.jobs" {
		t.Fatalf("%q", got)
	}
	if got := prefixOr("kb", "jobs"); got != "kb.jobs" {
		t.Fatalf("%q", got)
	}
}

func TestCallbackDLT(t *testing.T) {
	var d *callbackDLT
	if err := d.ProduceDLT(context.Background(), "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	prod := &memProd{}
	d = &callbackDLT{prod: prod, topic: ""}
	if err := d.ProduceDLT(context.Background(), "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
	d.topic = "dlt"
	if err := d.ProduceDLT(context.Background(), "k", []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestAttachIngestLag(t *testing.T) {
	lag := lagStub{}
	s := attachIngestLag(fairness.Settings{ActiveCountSource: "ingest_lag"}, lag)
	if s.IngestLag == nil {
		t.Fatal("expected lag wired for ingest_lag")
	}
	s = attachIngestLag(fairness.Settings{ResetVtimeWhenIdle: true}, lag)
	if s.IngestLag == nil {
		t.Fatal("expected lag wired for idle reset")
	}
	s = attachIngestLag(fairness.Settings{}, lag)
	if s.IngestLag != nil {
		t.Fatal("should not wire lag when unused")
	}
	s = attachIngestLag(fairness.Settings{ActiveCountSource: "ingest_lag"}, nil)
	if s.IngestLag != nil {
		t.Fatal("nil lag stays nil")
	}
}

type lagStub struct{}

func (lagStub) IngestActiveCount(context.Context, string, string) (int, error) { return 2, nil }

func TestLoopHealthLifecycle(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.LivenessHeartbeatInterval = 10 * time.Second
	h := NewLoopHealth(cfg)
	if h == nil {
		t.Fatal("nil")
	}
	var nilH *LoopHealth
	nilH.Register("x")
	nilH.RecordTick("x")
	ok, detail := nilH.Healthy(context.Background())
	if !ok || detail == "" {
		t.Fatalf("nil healthy=%v %q", ok, detail)
	}

	h.Register("")
	h.RecordTick("")
	ok, _ = h.Healthy(context.Background())
	if !ok {
		t.Fatal("no loops registered yet")
	}
	h.Register("sched")
	ok, _ = h.Healthy(context.Background())
	if !ok {
		t.Fatal("boot grace")
	}
	h.RecordTick("sched")
	ok, detail = h.Healthy(context.Background())
	if !ok {
		t.Fatalf("%s", detail)
	}
	// Force stale tick
	h.mu.Lock()
	h.lastTick["sched"] = time.Now().Add(-h.maxStale - time.Second)
	h.mu.Unlock()
	ok, _ = h.Healthy(context.Background())
	if ok {
		t.Fatal("expected stale")
	}

	h2 := NewLoopHealth(cfg)
	h2.bootGrace = 5 * time.Millisecond
	h2.Register("never")
	time.Sleep(10 * time.Millisecond)
	ok, _ = h2.Healthy(context.Background())
	if ok {
		t.Fatal("never ticked past boot grace")
	}
}

type stubChecker struct {
	ok     bool
	detail string
}

func (s stubChecker) Healthy(context.Context) (bool, string) { return s.ok, s.detail }

func TestCompositeHealth(t *testing.T) {
	okChk := stubChecker{ok: true, detail: "a"}
	badChk := stubChecker{ok: false, detail: "bad"}
	c := compositeHealth{checkers: []health.Checker{okChk, badChk}}
	ok, detail := c.Healthy(context.Background())
	if ok || detail != "bad" {
		t.Fatalf("ok=%v detail=%q", ok, detail)
	}
	c = compositeHealth{checkers: []health.Checker{okChk}}
	ok, detail = c.Healthy(context.Background())
	if !ok || detail != "ok" {
		t.Fatalf("ok=%v detail=%q", ok, detail)
	}
}

func TestStallHelperBranches(t *testing.T) {
	prev := consumerStallTimeoutSetting
	t.Cleanup(func() { consumerStallTimeoutSetting = prev })
	SetConsumerStallTimeout(0) // no-op
	if consumerStallTimeoutSetting != prev {
		t.Fatal("zero should not change")
	}
	SetConsumerStallTimeout(2 * time.Second)
	if consumerStallTimeoutSetting != 2*time.Second {
		t.Fatal(consumerStallTimeoutSetting)
	}
	if effectiveStallTimeout(5*time.Second) != 5*time.Second {
		t.Fatal("override")
	}
	if stallHeartbeatInterval(300*time.Millisecond) != 100*time.Millisecond {
		t.Fatal("floor")
	}
	if stallHeartbeatInterval(12*time.Second) != 2*time.Second {
		t.Fatal("tick")
	}
	if err := runWithStallHeartbeat(nil, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	if err := consumerLoopDoneErr(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errConsumerStalled)
	if !errors.Is(consumerLoopDoneErr(ctx), errConsumerStalled) {
		t.Fatal("stalled")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := consumerLoopDoneErr(ctx2); err != nil {
		t.Fatalf("canceled should soft-exit: %v", err)
	}
	if stalledRestartErr("g").Error() == "" {
		t.Fatal("empty")
	}
	closeGroupConsumer(nil)
	releasePollGate(nil)
	fake := &fakeRebalance{}
	releasePollGate(fake)
	if !fake.allowed {
		t.Fatal("allow")
	}
	closeGroupConsumer(fake)
	if !fake.closed {
		t.Fatal("close")
	}
}

type fakeRebalance struct {
	allowed, closed bool
}

func (f *fakeRebalance) AllowRebalance()         { f.allowed = true }
func (f *fakeRebalance) CloseAllowingRebalance() { f.closed = true }

func TestAttachConsumerStallGuardClosesClient(t *testing.T) {
	fake := &fakeRebalance{}
	ctx, _, stop := attachConsumerStallGuardFor(context.Background(), fake, "lbl", 40*time.Millisecond)
	defer stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(context.Cause(ctx), errConsumerStalled) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(context.Cause(ctx), errConsumerStalled) {
		t.Fatalf("cause=%v err=%v", context.Cause(ctx), ctx.Err())
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !(fake.closed && fake.allowed) {
		time.Sleep(5 * time.Millisecond)
	}
	if !fake.closed || !fake.allowed {
		t.Fatalf("fake=%+v", fake)
	}
	// Default attach path
	_, _, stop2 := attachConsumerStallGuard(context.Background(), nil, "x")
	stop2()
}

func TestDerefStrAndMemberLabelEdges(t *testing.T) {
	if derefStr(nil) != "" {
		t.Fatal("nil")
	}
	s := "hi"
	if derefStr(&s) != "hi" {
		t.Fatal(s)
	}
	if memberLabel(0, 0) != "1/1" {
		t.Fatal(memberLabel(0, 0))
	}
	if healthMemberKey("", 1, 2) != "" {
		t.Fatal("empty group")
	}
}

func TestWiringBuilders(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.Store = "mysql"
	cfg.StoreMySQLDSN = ""
	if _, _, _, err := BuildPauseControl(cfg, nil); err == nil {
		t.Fatal("expected mysql dsn error")
	}
	if _, _, err := BuildFailureRecorder(cfg, nil); err == nil {
		t.Fatal("expected mysql dsn error")
	}
	cfg.Store = "redis"
	if fr, closeFn, err := BuildFailureRecorder(cfg, nil); err != nil || fr != nil {
		t.Fatalf("fr=%v err=%v", fr, err)
	} else {
		closeFn()
	}
	cfg.FairnessTenantPartitions = nil
	cfg.FairnessDynamicTenantPartitions = false
	if BuildTenantPartitions(cfg, nil, nil) != nil {
		t.Fatal("expected nil tenant partitions")
	}
	cfg.FairnessDynamicTenantPartitions = true
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if tp := BuildTenantPartitions(cfg, rdb, nil); tp == nil {
		t.Fatal("expected tenant partitions")
	}
	cfg.LivenessEnabled = false
	StartHealthServer(context.Background(), cfg, "daemon", nil)
	cfg.LivenessEnabled = true
	cfg.LivenessHTTPAddr = "127.0.0.1:0"
	ctx, cancel := context.WithCancel(context.Background())
	StartHealthServer(ctx, cfg, "daemon", stubChecker{ok: true, detail: "ok"})
	cancel()
	StartLivenessHeartbeatLoop(context.Background(), nil)
	live := NewLivenessReporter(func() config.Daemon {
		c := config.DefaultDaemon()
		c.LivenessEnabled = true
		return c
	}(), rdb)
	ctx2, cancel2 := context.WithCancel(context.Background())
	StartLivenessHeartbeatLoop(ctx2, live)
	cancel2()
}

func TestStartRecurringSchedulerRequiresDSN(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.RecurringMySQLDSN = ""
	cfg.ScheduleMySQLDSN = ""
	if _, err := StartRecurringScheduler(context.Background(), cfg, nil, NewLoopHealth(cfg)); err == nil {
		t.Fatal("expected dsn error")
	}
}

func TestNewExpiredPublisherAndApplyJobSideEffects(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.EventsTopic = "events"
	cfg.DeadLetterTopic = "dlt"
	cfg.EventEmitRetries = 0
	p := newExpiredPublisher(cfg, nil, nil, nil)
	if p.now == nil {
		t.Fatal("now")
	}
	out := job.Outcome{
		Event:        &protocol.EventMessage{BatchID: "b", JobID: "j", SrcTopic: "jobs", SrcPartition: 1},
		RetryPayload: []byte(`{"job_id":"j"}`),
		RetryTopic:   "retry.short",
		RetryKey:     "j",
		DLTPayload:   []byte(`{"job_id":"j","dlt_type":"x"}`),
		DLTKey:       "j",
	}
	if err := ApplyJobSideEffects(context.Background(), cfg, memProd{}, out); err != nil {
		t.Fatal(err)
	}
	if err := ApplyJobSideEffects(context.Background(), cfg, memProd{}, job.Outcome{}); err != nil {
		t.Fatal(err)
	}
}

func TestPauseForRetryAndFairBackpressureError(t *testing.T) {
	pauseForRetry(context.Background(), 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pauseForRetry(ctx, time.Hour)
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel2()
	}()
	pauseForRetry(ctx2, 5*time.Second)

	err := &fairBackpressureError{lane: "time", tenantID: "t", duration: time.Millisecond}
	if err.Error() == "" {
		t.Fatal("empty")
	}
	rp := &retryPausedError{duration: time.Millisecond}
	if rp.Error() != "retry paused" {
		t.Fatal(rp.Error())
	}
}

func TestEffectivePartitionPollAndClearDeliverPause(t *testing.T) {
	if effectivePartitionPoll(0) != defaultPartitionPollRecords {
		t.Fatal("default")
	}
	if effectivePartitionPoll(7) != 7 {
		t.Fatal("override")
	}
	e := &partitionEngine{cl: &consumerClient{enginePaused: map[string]map[int32]struct{}{
		"jobs": {1: {}, 2: {}},
	}}}
	e.clearDeliverPauseTracking("jobs", 1)
	if _, ok := e.cl.enginePaused["jobs"][1]; ok {
		t.Fatal("deleted")
	}
	e.clearDeliverPauseTracking("jobs", 2)
	if _, ok := e.cl.enginePaused["jobs"]; ok {
		t.Fatal("topic map should be gone")
	}
	(&partitionEngine{}).clearDeliverPauseTracking("jobs", 0)
}

func TestNewConsumerHealthTrackerDefaults(t *testing.T) {
	h := NewConsumerHealthTracker(0, 0)
	if h.maxStale != 90*time.Second || h.bootGrace != 45*time.Second {
		t.Fatalf("%+v", h)
	}
	var nilH *ConsumerHealth
	nilH.Register("g")
	nilH.RecordPoll("g")
	ok, _ := nilH.Healthy(context.Background())
	if !ok {
		t.Fatal("nil health ok")
	}
	h.Register("")
	h.RecordPoll("")
	ok, _ = h.Healthy(context.Background())
	if !ok {
		t.Fatal("no groups")
	}
}

func TestDeferPartitionPauseNilSafe(t *testing.T) {
	deferPartitionPause(nil, nil, time.Second)
	deferPartitionPause(&recordingFetchPauser{}, nil, time.Second)
	deferPartitionPause(&recordingFetchPauser{}, &kgo.Record{Topic: "t", Partition: 1}, 0)
	fp := &recordingFetchPauser{}
	deferPartitionPause(fp, &kgo.Record{Topic: "t", Partition: 1}, 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	if len(fp.partPaused["t"]) == 0 {
		t.Fatal("expected pause")
	}
}

func TestDropDeferredForRevoked(t *testing.T) {
	cc := &consumerClient{deferredPaused: map[string]map[int32]int64{
		"jobs": {0: 1, 1: 2},
	}}
	cc.dropDeferredForRevoked(map[string][]int32{"jobs": {0}})
	if _, ok := cc.deferredPaused["jobs"][0]; ok {
		t.Fatal("revoked partition should drop")
	}
	if _, ok := cc.deferredPaused["jobs"][1]; !ok {
		t.Fatal("other partition kept")
	}
	cc.dropDeferredForRevoked(map[string][]int32{"jobs": {1}})
	if _, ok := cc.deferredPaused["jobs"]; ok {
		t.Fatal("empty topic map removed")
	}
}
