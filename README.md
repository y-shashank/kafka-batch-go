# kafka-batch-go

[![CI](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch-go/badges/coverage.json)](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml)

Go implementation of [KafkaBatch](https://github.com/y-shashank/kafka-batch) — Sidekiq Pro Batches on Kafka. Install as a library in your Go services or run the bundled `kbatch` CLI.

Wire-compatible with the Ruby gem: same Redis batch keys, job JSON envelope, handler manifest, schedule index, and uniq fingerprints.

## Three tiers

Each tier is an independently deployable process. Pick Go or Ruby per tier in a mixed deployment; tiers communicate only via **Kafka + Redis**.

| Tier | Package / binary | Role |
|------|------------------|------|
| **1 — Client** | `pkg/client` | Produce jobs & batches (`EnqueueJob`, `CreateBatch`, `PerformIn`, cancel) |
| **2 — Control** | `pkg/daemon` / `kbatch daemon` | Fair ingest→forward, events, retry, schedule poller (no job handlers) |
| **3 — Execution** | `pkg/worker` + `pkg/kbatch` / `kbatch worker` | Consume **go** job topics + `fair_*_ready.go`; run registered handlers |

Ruby equivalents live in the [kafka-batch](https://github.com/y-shashank/kafka-batch) gem (Karafka consumers).

## Install

```bash
go get github.com/y-shashank/kafka-batch-go/pkg/client
go get github.com/y-shashank/kafka-batch-go/pkg/daemon
go get github.com/y-shashank/kafka-batch-go/pkg/worker
go get github.com/y-shashank/kafka-batch-go/pkg/kbatch
```

## Tier 1 — Client library

```go
import "github.com/y-shashank/kafka-batch-go/pkg/client"

cfg := client.DefaultConfig()
cfg.Brokers = []string{"localhost:9092"}
cfg.RedisURL = "redis://localhost:6379/0"
cfg.ManifestPath = "config/kafka_batch_handlers.yml"

c, err := client.New(cfg)
defer c.Close()

// Standalone job (routes ruby or go runtime via manifest)
_, _ = c.EnqueueJob(ctx, "orders.process", map[string]interface{}{"id": 1}, client.PushOptions{})

// Batch — callback_args are passed only to on_success / on_complete handlers (not work jobs)
_, _ = c.CreateBatch(ctx, client.BatchOptions{
    OnComplete:   "MyCallback",
    Meta:         map[string]interface{}{"source": "api"},              // batch metadata only
    CallbackArgs: map[string]interface{}{"run_id": "42", "channel": "#ops"},
}, func(b *client.Batch) error {
    _, err := b.PushJob(ctx, "orders.process", map[string]interface{}{"id": 1}, client.PushOptions{})
    return err
})
```

`meta` is stored on the batch hash for dashboards and APIs. `callback_args` is stored separately and included in callback job payloads / legacy callback messages — work jobs never see it.

## Batches & callbacks

When a batch finalizes, kafka-batch enqueues callback jobs (or legacy Ruby class callbacks) with a batch summary payload. Use `BatchOptions.CallbackArgs` for custom data your callback handler needs:

```go
kbatch.Register("import.on_complete", func(ctx *kbatch.Context) error {
    runID := ctx.Payload["callback_args"].(map[string]interface{})["run_id"]
    return notify(runID, ctx.Payload["failed_count"])
})
```

Ruby Karafka `CallbackConsumer` handles legacy `on_success` / `on_complete` class strings; job-style callbacks run on your chosen Go or Ruby execution topic (same as the Ruby gem).

## Tier 2 — Control plane

```go
import "github.com/y-shashank/kafka-batch-go/pkg/daemon"

// Blocks until SIGINT/SIGTERM
daemon.Run(ctx, "config/kbatch_daemon.yml", "config/kafka_batch_handlers.yml")
```

Or CLI:

```bash
go build -o kbatch ./cmd/kbatch
kbatch daemon --config config/kbatch_daemon.yml --manifest config/kafka_batch_handlers.yml
```

Consumes: fair **ingest** (dispatch + forwarder), **events**, **retry**, schedule poller. Does **not** run job handlers or batch callbacks.

When batches use Ruby `on_success` / `on_complete` classes, deploy Ruby Karafka `CallbackConsumer` from the kafka-batch gem (Go daemon does not consume the callbacks topic).

## Tier 3 — Job execution

Register handlers in your `main` package, then run the worker:

```go
import (
    "github.com/y-shashank/kafka-batch-go/pkg/kbatch"
    "github.com/y-shashank/kafka-batch-go/pkg/worker"
)

func init() {
    kbatch.Register("segment.export", func(ctx *kbatch.Context) error {
        return exportSegment(ctx.Payload)
    })
}

func main() {
    worker.Run(context.Background(), "config/kbatch_daemon.yml", "config/kafka_batch_handlers.yml")
}
```

Or CLI:

```bash
kbatch worker --config config/kbatch_daemon.yml --manifest config/kafka_batch_handlers.yml
```

Consumes: **go** plain topics, go priority topics, `fair_*_ready.go` only.

## Handler manifest

Shared YAML with the Ruby gem (`config/kafka_batch_handlers.yml`):

```yaml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: kafka_batch.jobs.ruby
```

One execution topic = one runtime. Fair jobs use shared **ingest** topics; control forwards to `.go` / `.ruby` **ready** topics.

## Go E2E integration tests

Full three-tier tests (client → daemon → worker) against live Kafka + Redis:

```bash
go build -o bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest
go build -o bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest

export KAFKA_BATCH_INTEGRATION=1
export KAFKA_BATCH_TEST_REDIS_URL=redis://127.0.0.1:6379/15
go test -tags=integration ./integration/e2e/ -v -count=1
```

## CLI

```bash
kbatch daemon --config PATH [--manifest PATH]   # tier 2
kbatch worker --config PATH [--manifest PATH]   # tier 3
kbatch reconcile --config PATH
kbatch topics create|validate [--manifest PATH]
```

## Local development

```bash
export KAFKA_PREFIX=dev
export REDIS_URL=redis://localhost:6379/0

# Terminal A — control
kbatch daemon --config config/daemon.example.yml --manifest config/kafka_batch_handlers.yml

# Terminal B — execution (link your handlers via kbatch.Register in worker main)
kbatch worker --config config/daemon.example.yml --manifest config/kafka_batch_handlers.yml
```

## Config

Daemon and worker load `config/daemon.example.yml` (or your app copy), then **environment variables override YAML** when set:

| Variable | Used by | Purpose |
|----------|---------|---------|
| `KAFKA_BROKERS` | client, daemon, worker | Comma-separated broker list |
| `REDIS_URL` | client, daemon, worker | Redis URL |
| `KAFKA_PREFIX` | all | Topic + consumer_group prefix |
| `KAFKA_BATCH_HANDLER_MANIFEST` | all | Path to `kafka_batch_handlers.yml` |
| `KAFKA_BATCH_SCHEDULE_MYSQL_DSN` | client, daemon | MySQL schedule index |
| `KAFKA_BATCH_PRIORITY_CONFIG(S)` | daemon, worker | Priority YAML path(s) |
| `KAFKA_BATCH_STORE_MYSQL_DSN` | daemon, worker | MySQL failures / pause store |
| `KAFKA_BATCH_METRICS_*` | daemon, worker | StatsD metrics |
| `KAFKA_BATCH_LIVENESS_*` | daemon, worker | HTTP health probes |

Client library: pass `client.DefaultConfig()` and call `client.New(cfg)` — `ApplyEnv` runs automatically inside `New`.

Daemon/worker CLI: `kbatch daemon --config path/to/daemon.yml` — `config.LoadDaemon` applies the same env overrides after YAML parse.

See `config/daemon.example.yml` for the full YAML surface.

## Wire protocol

JSON job/event fixtures and legacy notes: `protocol/`.

## Ruby compatibility tests (mixed tier — deferred)

Optional cross-language integration specs live under `compat/ruby/`. See `compat/ruby/README.md`.

## Related

- [kafka-batch](https://github.com/y-shashank/kafka-batch) — Ruby gem (client, Karafka control, Karafka `JobConsumer`)
