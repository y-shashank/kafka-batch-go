package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/control/job"
	"github.com/y-shashank/kafka-batch-go/pkg/daemon"
	"github.com/y-shashank/kafka-batch-go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch-go/pkg/priority"
	"github.com/y-shashank/kafka-batch-go/pkg/protocol"
	"github.com/y-shashank/kafka-batch-go/pkg/workset"
)

func registerOnce(t *testing.T, jobType string) {
	t.Helper()
	if _, ok := kbatch.Lookup(jobType); ok {
		return
	}
	kbatch.Register(jobType, func(*kbatch.Context) error { return nil })
}

func TestDrainWorkerWatermark(t *testing.T) {
	var mu sync.Mutex
	var wms []*daemon.WatermarkExecutor
	drainWorkerWatermark(&mu, &wms, time.Millisecond) // empty

	wm := daemon.NewWatermarkExecutor(config.DefaultDaemon(), "c1",
		func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{}, nil
		},
		func(context.Context, job.Outcome) error { return nil },
	)
	wms = []*daemon.WatermarkExecutor{wm}
	drainWorkerWatermark(&mu, &wms, 50*time.Millisecond)
	if wm.InFlightCount() != 0 {
		t.Fatalf("inFlight=%d", wm.InFlightCount())
	}
}

func TestDrainWorkerSuperFetch(t *testing.T) {
	var mu sync.Mutex
	var sfs []*daemon.SuperFetchExecutor
	drainWorkerSuperFetch(nil, &mu, &sfs, time.Millisecond) // empty

	sf := daemon.NewSuperFetchExecutor(config.DefaultDaemon(), nil, "c1",
		func(context.Context, []byte, protocol.SourceCoords) (job.Outcome, error) {
			return job.Outcome{}, nil
		},
		func(context.Context, job.Outcome) error { return nil },
	)
	sfs = []*daemon.SuperFetchExecutor{sf}
	drainWorkerSuperFetch(workset.NewStore(nil), &mu, &sfs, 50*time.Millisecond)
}

func TestGoPriorityConfigsSkipsEmptyTopics(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.ConsumerGroup = "kb"
	reg := priority.Registry{Configs: []priority.Config{{
		Topics:              []string{"ruby.only"},
		ConsumerGroupSuffix: "jobs-fast",
	}}}
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"ruby.job": {Runtime: config.RuntimeRuby, Topic: "ruby.only"},
	}}
	out := goPriorityConfigs(cfg, reg, manifest, "default.jobs")
	if len(out) != 0 {
		t.Fatalf("expected empty, got %+v", out)
	}
}

func TestRunMissingConfig(t *testing.T) {
	err := Run(context.Background(), "/no/such/daemon.yaml", "")
	if err == nil {
		t.Fatal("expected load error")
	}
}

func TestRunNoGoHandlers(t *testing.T) {
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  only.ruby:
    runtime: ruby
    topic: ruby.jobs
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: redis://127.0.0.1:6379/0
handler_manifest: `+manifest+`
`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), cfgPath, "")
	if err == nil {
		t.Fatal("expected no go handlers error")
	}
}

func TestRunControlPlaneTopicRejected(t *testing.T) {
	registerOnce(t, "bad.go")
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  bad.go:
    runtime: go
    topic: kafka_batch.events
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: redis://127.0.0.1:6379/0
events_topic: kafka_batch.events
handler_manifest: `+manifest+`
`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), cfgPath, "")
	if err == nil || !strings.Contains(err.Error(), "control-plane") {
		t.Fatalf("expected control-plane rejection, got %v", err)
	}
}

func TestRunInvalidExecutionMode(t *testing.T) {
	registerOnce(t, "ok.go")
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  ok.go:
    runtime: go
    topic: jobs.ok
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: redis://127.0.0.1:6379/0
execution_mode: not-a-mode
handler_manifest: `+manifest+`
`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), cfgPath, "")
	if err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("expected invalid execution_mode, got %v", err)
	}
}

func TestRunBadManifestPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: redis://127.0.0.1:6379/0
handler_manifest: /no/such/handlers.yaml
`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), cfgPath, "")
	if err == nil {
		t.Fatal("expected manifest load error")
	}
}

func TestRunBadRedisURL(t *testing.T) {
	registerOnce(t, "ok.go")
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  ok.go:
    runtime: go
    topic: jobs.ok
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: not-a-redis-url
handler_manifest: `+manifest+`
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), cfgPath, ""); err == nil {
		t.Fatal("expected redis URL parse error")
	}
}

func TestRunRedisPingFails(t *testing.T) {
	registerOnce(t, "ok.go")
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  ok.go:
    runtime: go
    topic: jobs.ok
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - localhost:9092
consumer_group: test
redis_url: redis://127.0.0.1:1/0
handler_manifest: `+manifest+`
`), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := Run(ctx, cfgPath, ""); err == nil {
		t.Fatal("expected redis ping error")
	}
}

func TestRunMysqlStoreMissingDSN(t *testing.T) {
	registerOnce(t, "ok.go")
	mr := miniredis.RunT(t)
	dir := t.TempDir()
	manifest := filepath.Join(dir, "handlers.yaml")
	if err := os.WriteFile(manifest, []byte(`
handlers:
  ok.go:
    runtime: go
    topic: jobs.ok
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "daemon.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
brokers:
  - 127.0.0.1:1
consumer_group: test
redis_url: redis://`+mr.Addr()+`/0
store: mysql
handler_manifest: `+manifest+`
execution_mode: watermark
`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), cfgPath, "")
	if err == nil || !strings.Contains(err.Error(), "store_mysql_dsn") {
		t.Fatalf("got %v", err)
	}
}
