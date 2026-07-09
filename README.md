# kafka-batch-go

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

// Batch
_, _ = c.CreateBatch(ctx, client.BatchOptions{OnComplete: "MyCallback"}, func(b *client.Batch) error {
    _, err := b.PushJob(ctx, "orders.process", map[string]interface{}{"id": 1}, client.PushOptions{})
    return err
})
```

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
    topic: kafka_batch.jobs
```

One execution topic = one runtime. Fair jobs use shared **ingest** topics; forwarder routes to `.go` / `.ruby` **ready** topics.

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
kbatch daemon --config config/kafka_daemon.example.yml --manifest config/kafka_batch_handlers.yml

# Terminal B — execution (link your handlers via kbatch.Register in worker main)
kbatch worker --config config/kafka_daemon.example.yml --manifest config/kafka_batch_handlers.yml
```

## Config

See `config/daemon.example.yml` and `config/priority.example.yml`.

## Wire protocol

JSON job/event fixtures and legacy notes: `protocol/`.

## Ruby compatibility tests

Optional cross-language integration specs (Kafka + Redis + Ruby gem) live under `compat/ruby/`. See `compat/ruby/README.md`.

## Related

- [kafka-batch](https://github.com/y-shashank/kafka-batch) — Ruby gem (client, Karafka control, Karafka `JobConsumer`)
