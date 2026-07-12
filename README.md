# kafka-batch-go

[![CI](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch-go/badges/coverage.json)](https://github.com/y-shashank/kafka-batch-go/actions/workflows/ci.yml)

Go implementation of [KafkaBatch](https://github.com/y-shashank/kafka-batch) — Sidekiq Pro Batches on Kafka. Install as a library in your Go services or run the bundled `kbatch` CLI.

Wire-compatible with the Ruby gem: same Redis batch keys, job JSON envelope, handler manifest, schedule index, and uniq fingerprints.

## Three tiers

Each tier is an independently deployable process. Pick **Go or Ruby per tier** in a mixed deployment; tiers communicate only via **Kafka + Redis** (same batch ledger, job envelope, and handler manifest).

| Tier | Go | Ruby |
|------|-----|------|
| **1 — Client** | `pkg/client` | `KafkaBatch::Batch` in the [kafka-batch](https://github.com/y-shashank/kafka-batch) gem |
| **2 — Control** | `kbatch daemon` / `pkg/daemon` | Karafka control groups (`EventConsumer`, `RetryConsumer`, fair dispatch/forward, schedule poller) |
| **3 — Execution** | `kbatch worker` / `pkg/worker` | Karafka `JobConsumer` on ruby job topics + `fair_*_ready.ruby` |

**Rules for mixing:**

- **One control plane per cluster** — run either Go daemon *or* Ruby Karafka control, not both on the same topics (they would double-consume).
- **Client is per app** — a Go API and a Rails app can both enqueue jobs; routing is driven by the shared handler manifest.
- **Execution can be both** — run Go workers and Ruby JobConsumers side by side; each handler's `runtime` in the manifest decides which topic and worker fleet receives the job.
- **Same batch, mixed runtimes** — one batch can contain both `runtime: go` and `runtime: ruby` jobs; control finalizes the batch when all legs complete.

```mermaid
flowchart TB
  subgraph clients [Tier 1 — pick one or more apps]
    GC[Go client]
    RC[Ruby client]
  end
  subgraph control [Tier 2 — exactly one per cluster]
    GD[Go daemon]
    RK[Ruby Karafka control]
  end
  subgraph exec [Tier 3 — one or both]
    GW[Go worker]
    RJ[Ruby JobConsumer]
  end
  Redis[(Redis batch ledger)]
  Kafka[(Kafka topics)]
  GC --> Kafka
  RC --> Kafka
  Kafka --> GD
  Kafka --> RK
  GD --> Redis
  RK --> Redis
  GD --> Kafka
  RK --> Kafka
  Kafka --> GW
  Kafka --> RJ
  GW --> Kafka
  RJ --> Kafka
  Kafka --> GD
  Kafka --> RK
```

## Mixed-runtime deployment

The handler manifest is the routing contract. Every producer (Go or Ruby) and every control/execution process loads the same `kafka_batch_handlers.yml`:

```yaml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports          # Go worker consumes this
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: kafka_batch.jobs.ruby    # Ruby JobConsumer consumes this
  campaigns.send:
    runtime: go
    fairness_type: time             # fair ingest → control forwards to ready.go / ready.ruby
```

Plain jobs go straight to the handler topic. Fair jobs go to shared **ingest** topics; control forwards to `fair_*_ready.go` or `fair_*_ready.ruby` based on `runtime`.

### Deployment patterns

| Pattern | Client | Control | Execution | Typical use |
|---------|--------|---------|-----------|-------------|
| **All Go** | Go | Go daemon | Go worker | New Go services, lowest ops surface |
| **Go control + mixed exec** | Go or Ruby | Go daemon | Go worker **+** Ruby JobConsumer | Migrate handlers one at a time; most common hybrid |
| **Ruby control + Go exec** | Go or Ruby | Ruby Karafka control | Go worker | Keep Ruby control plane; move hot handlers to Go |
| **All Ruby** | Ruby | Ruby Karafka control | Ruby JobConsumer | Legacy Rails-only stack |

### Example: Go control + mixed execution (recommended hybrid)

Deploy three process types plus optional Ruby callback consumer:

```bash
# 1 — Control (single replica set; scale horizontally with same consumer group)
kbatch daemon --config config/daemon.yml --manifest config/kafka_batch_handlers.yml

# 2 — Go execution (handlers registered via kbatch.Register in your worker main)
kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml

# 3 — Ruby execution (Karafka JobConsumer on ruby topics from the gem)
bundle exec karafka server --include-consumer-groups "${CG}-jobs,${CG}-jobs-fair-time"
```

Go and Ruby APIs both enqueue via their respective clients using the **same manifest and Redis URL**. A single batch can push Go and Ruby jobs; the batch completes when every job emits a success/failure event and control updates the ledger.

### Example: Ruby control + Go worker

Use Ruby Karafka for tier 2 only; keep Go for execution on `runtime: go` handlers:

```bash
# Ruby control — events, retry, fair dispatch/forward, schedule poller
bundle exec karafka server --include-consumer-groups \
  "${CG}-control,${CG}-dispatch-time,${CG}-dispatch-throughput"

# Go worker — plain + priority + fair ready.go topics
kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml
```

Go `pkg/client` (or Ruby `KafkaBatch::Batch`) produces jobs identically; Ruby `EventConsumer` drives batch completion.

### What must stay aligned across tiers

| Shared resource | Why |
|-----------------|-----|
| `kafka_batch_handlers.yml` | Routes `job_type` → runtime, topic, retries |
| Redis URL | Batch ledger, uniq locks, fair scheduler state |
| Topic names / `KAFKA_PREFIX` | Producers and consumers must agree |
| Events / retry / fair ingest topics | Control plane wiring |

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

One execution topic = one runtime. Fair jobs use shared **ingest** topics; control forwards to `.go` / `.ruby` **ready** topics. See [Mixed-runtime deployment](#mixed-runtime-deployment) for how to run both execution tiers together.

## Cross-runtime matrix tests

Integration tests exercise the [mixed-runtime combinations](#mixed-runtime-deployment) above against live Kafka + Redis. See also `compat/ruby/README.md`.

```bash
cd compat/ruby && bundle install

export KAFKA_BATCH_INTEGRATION=1
go test -tags=integration -p 1 ./integration/matrix/ -count=1 -timeout 45m -v
```

### CI-validated combinations

| Combo | Client | Control | Execution | Scenarios |
|-------|--------|---------|-----------|-----------|
| Phase 1 / PR | Go | Go | Go / Ruby / **both** | Batch completion, mixed batch, retry, DLT, schedule, priority |
| Phase 2 | Go | Go | Ruby | Fair routing, retry through Ruby JobConsumer |
| Phase 3 | **Ruby** | Go | Go / Ruby / **both** | Same PR scenarios + Ruby/Go envelope parity |
| Phase 4 | Go / Ruby | **Ruby** | Go / Ruby | Batch completion via Ruby control; Ruby full-stack retry |
| Nightly | All above | All above | All above | Full catalog (`.github/workflows/nightly-matrix.yml`) |

Every PR runs Phase 1–4 plus the Go E2E suite (`-p 1` so packages do not race on shared Redis).

| Phase | Test |
|-------|------|
| 1 / PR | `TestMatrix_Phase1`, `TestMatrix_PR` |
| 2 | `TestMatrix_Phase2_RubyFairAndRetry` |
| 3 | `TestMatrix_Phase3_RubyClient`, `TestMatrix_Phase3_ClientEnvelopeParity` |
| 4 | `TestMatrix_Phase4_RubyControl` |
| Nightly | `TestMatrix_Full` |

Set `KAFKA_BATCH_GEM_PATH` or clone [kafka-batch](https://github.com/y-shashank/kafka-batch) as `kafka-batch/` in this repo (or sibling `../kafka-batch` locally).

## Go E2E integration tests

Full three-tier tests (client → daemon → worker) against live Kafka + Redis:

```bash
export KAFKA_BATCH_INTEGRATION=1
export KAFKA_BATCH_TEST_REDIS_URL=redis://127.0.0.1:6379/15
go test -tags=integration ./integration/e2e/ ./pkg/kafkaclient/ -v -count=1
```

Itest daemon/worker binaries are built automatically on first run (or pre-build with `go build -o bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest` and `go build -o bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest`).

## CLI

```bash
kbatch daemon --config PATH [--manifest PATH]   # tier 2
kbatch worker --config PATH [--manifest PATH]   # tier 3
kbatch reconcile --config PATH
kbatch topics create|validate [--manifest PATH]
```

## Local development

**All-Go stack** (simplest):

```bash
export KAFKA_PREFIX=dev
export REDIS_URL=redis://localhost:6379/0

# Terminal A — control
kbatch daemon --config config/daemon.example.yml --manifest config/kafka_batch_handlers.yml

# Terminal B — execution (link your handlers via kbatch.Register in worker main)
kbatch worker --config config/daemon.example.yml --manifest config/kafka_batch_handlers.yml
```

**Mixed Go control + Ruby execution** — add a Ruby Karafka worker pod/process using the same config + manifest as the Go daemon. Jobs with `runtime: ruby` in the manifest are consumed by Ruby; `runtime: go` jobs stay on the Go worker.

**Mixed clients** — a Go service uses `pkg/client`; a Rails app uses `KafkaBatch::Batch`. Both point at the same brokers, Redis, and manifest; batch IDs and job envelopes are wire-compatible.

## Config

Daemon and worker load `config/daemon.example.yml` (or your app copy), then **environment variables override YAML** when set.

### Shared (client, daemon, worker)

| Variable | Purpose |
|----------|---------|
| `KAFKA_BROKERS` | Comma-separated broker list |
| `REDIS_URL` | Redis URL (batch ledger, uniq, fair scheduler) |
| `KAFKA_PREFIX` | Topic + `consumer_group` prefix |
| `KAFKA_BATCH_HANDLER_MANIFEST` | Path to `kafka_batch_handlers.yml` |
| `KAFKA_BATCH_SCHEDULE_MYSQL_DSN` | MySQL schedule index (client + daemon) |
| `KAFKA_BATCH_PRIORITY_CONFIG` / `KAFKA_BATCH_PRIORITY_CONFIGS` | Priority YAML path(s) (daemon + worker) |
| `KAFKA_BATCH_STORE_MYSQL_DSN` | MySQL failures / pause store (daemon + worker) |
| `KAFKA_BATCH_METRICS_ENABLED` / `KAFKA_BATCH_METRICS_PREFIX` / `KAFKA_BATCH_METRICS_STATSD_ADDR` | StatsD metrics export |
| `KAFKA_BATCH_LIVENESS_ENABLED` / `KAFKA_BATCH_LIVENESS_HTTP_ADDR` | HTTP `/health` probes |

Client: `client.DefaultConfig()` + `client.New(cfg)` applies env automatically.  
CLI: `config.LoadDaemon` applies env after YAML parse.

See `config/daemon.example.yml` for the full YAML surface (fairness, schedule, retry tiers, etc.).

### Throughput tuning — single Go pod

Go daemon and worker scale **inside one pod** with two mechanisms:

1. **In-process consumer members** — N franz-go clients join the same Kafka consumer group; the broker assigns partitions across them (same as N pods, one OS process).
2. **Per-poll parallelism (worker only)** — `job_process_concurrency` runs up to N jobs in parallel per poll per member (Karafka `config.concurrency` equivalent).
3. **Consumer fetch limits** — `consumer_fetch_*` caps how much data each poll prefetches from the broker. Defaults are tighter than franz-go/librdkafka (50 MiB / 1 MiB per partition) so multi-partition assignments get fairer turns per poll.

**Max concurrent job executions** (worker):

```text
jobs_consumer_members × job_process_concurrency
  (+ same formula per fair-ready lane and priority group)
```

Set **member count ≈ topic partition count** for partition-bound throughput. Raise **`job_process_concurrency`** when handlers are slow but partitions are scarce.

#### Control plane (`kbatch daemon`)

| Env variable | YAML key | Default | Used by | What it does |
|--------------|----------|---------|---------|--------------|
| `KAFKA_BATCH_EVENTS_CONSUMER_CONCURRENCY` | `events_consumer_concurrency` | `8` | daemon | In-process members for `{group}-events`. Each poll is batched through `ProcessBatch` (one pipelined Redis round trip per poll). Set to **events topic partition count** (e.g. `32`) when event lag grows. |
| `KAFKA_BATCH_RETRY_CONSUMER_CONCURRENCY` | `retry_consumer_concurrency` | `4` | daemon | In-process members for `{group}-retry` (non-transactional path). Set up to **retry topic partition count** if retry-tier lag is high. |
| `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS` | `producer_required_acks` | `all_isr` | daemon, worker | `all_isr` (safest, default) or `leader` (lower produce latency; small loss risk on unclean leader failover). Affects callbacks, events, retry reroutes, schedule dispatch. |

Events consumer batching and batched callback produce are always on — no extra knob.

#### Consumer fetch (daemon + worker)

Applied to all daemon/worker Kafka consumers (events, retry, fair dispatch, jobs, fair-ready, priority).

| Env variable | YAML key | Default | What it does |
|--------------|----------|---------|--------------|
| `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES` | `consumer_fetch_max_bytes` | `1048576` (1 MiB) | Max total bytes per broker fetch response. Lower values reduce head-of-line blocking when one member holds many partitions. |
| `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES` | `consumer_fetch_max_partition_bytes` | `131072` (128 KiB) | Max bytes per partition in a fetch. Prevents one hot partition from filling the entire fetch budget. |
| `KAFKA_BATCH_CONSUMER_FETCH_MAX_WAIT_MS` | `consumer_fetch_max_wait_ms` | `200` | Max time the broker waits to accumulate data before returning a partial fetch. |

Raise these when messages are large or brokers are far away and polls return too little data; lower them when lag is uneven across partitions on the same consumer member.

#### Execution plane (`kbatch worker`)

| Env variable | YAML key | Default | What it does |
|--------------|----------|---------|--------------|
| `KAFKA_BATCH_JOBS_CONSUMER_CONCURRENCY` | `jobs_consumer_concurrency` | `8` | In-process members for `{group}-go-worker-jobs` (plain go topics). |
| `KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY` | `fair_ready_consumer_concurrency` | `8` | In-process members **per** fair-ready lane (`time`, `throughput`). |
| `KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY` | `priority_consumer_concurrency` | `4` | In-process members **per** priority YAML group. |
| `KAFKA_BATCH_JOB_PROCESS_CONCURRENCY` | `job_process_concurrency` | `1` | Parallel handler executions per poll **per member**. `1` = serial (safest). `4`–`8` for CPU-bound handlers. |
| `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS` | `producer_required_acks` | `all_isr` | Same as daemon — event emission after job completion. |

Fairness admission is capped by control-plane `fairness_global_concurrency` (YAML). Raising worker concurrency above what control admits will backlog **ready** topics, not speed up end-to-end.

#### Tuning profiles

**32 partitions per topic** (typical production):

```yaml
# Control — match partition count
events_consumer_concurrency: 32
retry_consumer_concurrency: 8
producer_required_acks: all_isr

# Optional — raise for large messages or high-latency brokers
# consumer_fetch_max_bytes: 2097152
# consumer_fetch_max_partition_bytes: 262144
# consumer_fetch_max_wait_ms: 500

# Worker — see job type below
jobs_consumer_concurrency: 32
fair_ready_consumer_concurrency: 32
priority_consumer_concurrency: 8
producer_required_acks: all_isr
```

**I/O-heavy jobs** (HTTP calls, DB queries, object storage — handler waits on external systems):

- Bottleneck is **waiting**, not CPU. Prefer **more partition members**, keep **`job_process_concurrency: 1`** per member.
- Example: `jobs_consumer_concurrency: 32`, `job_process_concurrency: 1` → **32 concurrent jobs** on a 32-partition topic.
- Size the pod for goroutine/memory overhead (many blocked I/O waits are cheap in Go).
- If lag persists with members = partitions, add partitions (Kafka only allows increasing count).

**CPU-heavy jobs** (encoding, aggregation, image/PDF work — handler saturates CPU):

- Prefer **`job_process_concurrency`** up to **logical CPU count per member**, with fewer members if partition count is low.
- Example on 8-core pod, 32 partitions: `jobs_consumer_concurrency: 8`, `job_process_concurrency: 4` → **32 concurrent jobs**.
- Example on 16-core pod, 32 partitions: `jobs_consumer_concurrency: 16`, `job_process_concurrency: 2` → **32 concurrent jobs**.
- Watch CPU throttling and Redis/Kafka produce latency; back off if event emission or fair slot release lags.

**Mixed I/O + CPU fleet** — use separate handler topics (or fair lanes) with different worker deployments and tuning per fleet rather than one global `job_process_concurrency`.

#### Quick reference

| Symptom | Likely fix |
|---------|------------|
| Events topic lag (control) | Raise `KAFKA_BATCH_EVENTS_CONSUMER_CONCURRENCY` toward partition count |
| Retry topic lag (control) | Raise `KAFKA_BATCH_RETRY_CONSUMER_CONCURRENCY` |
| Job topic lag, I/O-bound handlers | Raise `KAFKA_BATCH_JOBS_CONSUMER_CONCURRENCY` toward partition count |
| Job topic lag, CPU-bound handlers | Raise `KAFKA_BATCH_JOB_PROCESS_CONCURRENCY` (and/or members) |
| Fair ready lag, admission OK | Raise `KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY` |
| Priority tier lag | Raise `KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY` |
| Produce latency dominates | Try `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS=leader` (trade durability) |
| Ready topic backlog, ingest fine | Lower worker concurrency or raise `fairness_global_concurrency` on control |
| One partition lags while others idle on same member | Lower `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES` or `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES` |
| Polls return too few records (large messages / WAN) | Raise `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES` and/or `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES` |

## Operations (daemon / worker)

**Startup:** Redis is pinged before consumers start; unreachable Redis fails fast at boot.

**Consumer resilience:** Each Kafka consumer runs in a supervised loop — broker blips restart that consumer with exponential backoff (1s → 30s) instead of killing the whole process. Handler errors and panics log and skip commit (offset redelivered); panics no longer crash the pod.

**Health probes:** Enable `liveness_enabled: true` (or `KAFKA_BATCH_LIVENESS_ENABLED=true`). `GET /health` and `GET /live` return **503** when any registered consumer group has not polled Kafka within `2 × liveness_ttl` (min 60s). Wire Kubernetes liveness/readiness probes to `/health` with `restartPolicy: Always` so stale consumers trigger a pod restart.

```yaml
liveness_enabled: true
liveness_http_addr: ":8080"
liveness_ttl: 30s
```

## Wire protocol

JSON job/event fixtures and legacy notes: `protocol/`.

## Ruby compatibility

Cross-runtime integration specs and Ruby itest drivers live under `compat/ruby/`. The matrix harness in `integration/matrix/` is the primary CI gate for mixed deployments — see [Cross-runtime matrix tests](#cross-runtime-matrix-tests) above.

## Related

- [kafka-batch](https://github.com/y-shashank/kafka-batch) — Ruby gem (client, Karafka control, Karafka `JobConsumer`)
