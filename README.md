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

# 3 — Ruby execution only (no kafka-batch-control / dispatch-* — Go daemon owns tier 2)
bundle exec karafka server --include-consumer-groups \
  "${CG}-jobs,${CG}-jobs-fast,${CG}-jobs-fair-time,${CG}-jobs-fair-throughput"
```

Enable `fairness_enabled` and `schedule_poller_enabled` in `config/daemon.yml` — see [Setup: `config/daemon.yml`](#setup-configdaemonyml-go-control-plane).

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

## Build

The Go side ships **one binary — `kbatch`** — that runs the two server tiers as
subcommands. **Tier 1 (client) is a library you import, not a binary.**

```bash
# Build the kbatch binary (covers Tier 2 control plane + Tier 3 execution):
go build -o kbatch ./cmd/kbatch

# ...or install it onto $GOPATH/bin:
go install github.com/y-shashank/kafka-batch-go/cmd/kbatch@latest
```

How to build/run each tier:

| Tier | Build / import | Run |
|------|----------------|-----|
| 1 — Client (producer) | `go get .../pkg/client` (library — no binary) | import `pkg/client` in your app; see [Tier 1](#tier-1--client-library) |
| 2 — Control plane | `go build -o kbatch ./cmd/kbatch` | `kbatch daemon --config config/daemon.yml --manifest config/kafka_batch_handlers.yml` |
| 3 — Job execution | `go build -o kbatch ./cmd/kbatch` | `kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml` |

Both server tiers come from the **same** `kbatch` build — you deploy the one
binary and pick the tier at runtime via the subcommand. Run `kbatch help` for all
subcommands (`daemon`, `worker`, `topics`, `reconcile`, `version`).

## Tier 1 — Client library

```go
import (
    "time"

    "github.com/y-shashank/kafka-batch-go/pkg/client"
)

cfg := client.DefaultConfig()
cfg.Brokers = []string{"localhost:9092"}
cfg.RedisURL = "redis://localhost:6379/0"
cfg.ManifestPath = "config/kafka_batch_handlers.yml"

c, err := client.New(cfg)
defer c.Close()

// Standalone job (routes ruby or go runtime via manifest)
_, _ = c.EnqueueJob(ctx, "orders.process", map[string]interface{}{"id": 1}, client.PushOptions{})

// Many standalone jobs (no batch ledger / no completion events) — chunked produce.
// Same idea as Ruby KafkaBatch::Batch.enqueue_many / enqueue_job loop.
payloads := make([]map[string]interface{}, 50)
for i := range payloads {
    payloads[i] = map[string]interface{}{"id": i}
}
// By manifest job_type (Go or Ruby runtime from handlers.yml):
_, _ = c.EnqueueManyJobs(ctx, "orders.process", payloads, client.PushOptions{TenantID: "acme"})
// Fair handlers: TenantID drives WFQ ingest partitioning.
_, _ = c.EnqueueManyJobs(ctx, "fair.time.go", payloads, client.PushOptions{TenantID: "acme"})

// By Ruby worker class name (when registered in manifest / Workers config):
_, _ = c.EnqueueMany(ctx, "Orders::ProcessWorker", payloads, client.PushOptions{TenantID: "acme"})

// Delayed many
_, _ = c.EnqueueManyJobsIn(ctx, 30*time.Minute, "orders.process", payloads, client.PushOptions{TenantID: "acme"})
_, _ = c.EnqueueManyJobsAt(ctx, time.Now().Add(time.Hour), "orders.process", payloads, client.PushOptions{})

// Batch — callback_args are passed only to on_success / on_complete handlers (not work jobs)
_, _ = c.CreateBatch(ctx, client.BatchOptions{
    OnComplete:   "MyCallback",
    TenantID:     "acme",
    Meta:         map[string]interface{}{"source": "api"},              // batch metadata only
    CallbackArgs: map[string]interface{}{"run_id": "42", "channel": "#ops"},
}, func(b *client.Batch) error {
    // Bulk inside a batch (tracks completion; emits batch events)
    _, err := b.PushManyJobs(ctx, "orders.process", payloads, client.PushOptions{})
    return err
})
```

| Go API | Ruby equivalent |
|--------|-----------------|
| `EnqueueJob` | `Batch.enqueue_job` |
| `EnqueueManyJobs` | `Batch.enqueue_many` by job_type / loop `enqueue_job` (standalone, no ledger) |
| `Enqueue` / `EnqueueMany` | `Batch.enqueue` / `Batch.enqueue_many` (worker class) |
| `PushManyJobs` / `PushMany` | `batch.push_many` (with batch ledger + completion events) |
| `EnqueueManyJobsIn` / `At` | `Batch.enqueue_many_in` / `enqueue_many_at` |

`EnqueueMany*` / `EnqueueManyJobs*` do **not** create a Redis batch — fire-and-forget Kafka delivery only. Use `CreateBatch` + `PushMany*` when you need completion counting / callbacks.

`PushOptions.TenantID` is required for fair handlers (same role as Ruby `tenant_id:`).

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
kbatch daemon --config config/daemon.yml --manifest config/kafka_batch_handlers.yml
```

### Setup: `config/daemon.yml` (Go control plane)

Copy `config/daemon.example.yml` to your app as `config/daemon.yml`. The daemon **only** starts consumers for settings you enable — unlike Ruby Karafka's single `kafka-batch-control` group, Go splits control into separate Kafka groups:

| YAML / behavior | Kafka consumer group(s) | What it does |
|-----------------|-------------------------|--------------|
| always | `{consumer_group}-events` | Batch completion events, ledger updates |
| `retry_tiers` present | `{consumer_group}-retry` | Tiered retry consumption |
| `fairness_enabled: true` | `{consumer_group}-dispatch-time`, `{consumer_group}-dispatch-throughput` | Fair ingest → Redis WFQ → ready topics |
| `schedule_poller_enabled: true` | *(no group — in-process poller)* | Dispatches due `perform_in` / `perform_at` jobs from the schedule index |

With `consumer_group: kafka-batch` (no `topic_prefix`), a fully wired control plane looks like:

```yaml
# config/daemon.yml — minimal Go control plane for hybrid Go control + Ruby execution
brokers:
  - localhost:9092

consumer_group: kafka-batch
handler_manifest: config/kafka_batch_handlers.yml

events_topic: kafka_batch.events
callbacks_topic: kafka_batch.callbacks
dead_letter_topic: kafka_batch.dead_letter
retry_topic: kafka_batch.jobs.retry

redis_url: ${REDIS_URL:-redis://localhost:6379/0}

max_retries: 7
retry_tiers:
  short: 30
  medium: 420
  large: 1200

# Fairness dispatch (tier 2) — REQUIRED for fair_time_ingest / fair_throughput_ingest handlers.
# Without this, jobs sit on ingest topics and never reach fair_*_ready.* execution topics.
fairness_enabled: true
fairness_global_concurrency: 500   # in-flight window per lane (time + throughput)
fairness_lease_ttl: 1800           # seconds; must exceed longest job runtime

# Delayed jobs (tier 2) — REQUIRED when clients use perform_in / perform_at.
# Index store: redis (default) or mysql (pairs with schedule_mysql_dsn).
schedule_poller_enabled: true
schedule_store: mysql                # redis | mysql
scheduled_topic: kafka_batch.scheduled
schedule_mysql_dsn: ${KAFKA_BATCH_SCHEDULE_MYSQL_DSN:-mysql2://user:pass@127.0.0.1:3306/kafka_batch_development}

# Priority YAML paths — daemon uses these for schedule routing defaults; worker
# loads the same files for kafka-batch-jobs-fast / kafka-batch-jobs-slow groups.
priority_config_paths:
  - config/priority/jobs-fast.yml
  - config/priority/jobs-slow.yml

producer_required_acks: all_isr

liveness_enabled: true
liveness_http_addr: ":8080"
```

**Hybrid local dev (Go control + Ruby execution)** — three terminals:

```bash
# 1 — Go control (events, retry, fair dispatch, schedule poller)
kbatch daemon --config config/daemon.yml --manifest config/kafka_batch_handlers.yml

# 2 — Go worker (runtime: go handlers)
kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml

# 3 — Ruby execution ONLY — do NOT include kafka-batch-control or dispatch-* groups
bundle exec karafka server --include-consumer-groups \
  "kafka-batch-jobs,kafka-batch-jobs-fast,kafka-batch-jobs-slow,kafka-batch-jobs-fair-time,kafka-batch-jobs-fair-throughput"
```

Do **not** run Go `kbatch daemon` and Ruby `kafka-batch-control` on the same events/retry topics (double consumption). Pick one control runtime.

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

## Priority queues

Run several job topics as one **ordered group** so a worker always drains the higher-priority topics before touching lower ones. Ordering is by topic rank, defined in a small YAML file per group (Sidekiq-`config/sidekiq.yml`-style). Priority is a **tier-3 (worker) feature** — the `kbatch worker` loads the priority YAML(s) and runs one lag-gated consumer group per file; the daemon is not involved. It's wire-compatible with the Ruby gem's priority YAML.

**Priority is selection, not preemption.** In-flight jobs are never killed when higher-priority work arrives; the gate only decides which topic the worker *starts* the next job from.

### 1. Route handlers onto priority topics (manifest)

A job reaches a priority topic the normal way — its handler's `topic` in the manifest points there:

```yaml
# config/kafka_batch_handlers.yml
handlers:
  orders.settle:        # critical — highest rank
    runtime: go
    topic: kafka_batch.jobs.p0
  orders.email:         # normal
    runtime: go
    topic: kafka_batch.jobs.p1
  orders.cleanup:       # background
    runtime: go
    topic: kafka_batch.jobs.p2
```

### 2. Define the priority group (YAML)

One file per consumer group. Topics are listed **highest priority first**:

```yaml
# config/priority/jobs-fast.yml
consumer_group_suffix: jobs-fast     # → Kafka group "<consumer_group>-jobs-fast"
mode: weighted                       # weighted (default) | strict
weighted_interleave: 4               # weighted only: run 1-in-N lower-rank jobs while a higher topic has lag
topics:                              # rank 0, 1, 2 … (highest first)
  - kafka_batch.jobs.p0
  - kafka_batch.jobs.p1
  - kafka_batch.jobs.p2
```

Strict group (no interleave — lower ranks wait entirely):

```yaml
# config/priority/jobs-slow.yml
consumer_group_suffix: jobs-slow
mode: strict
topics:
  - kafka_batch.jobs.slow_p0
  - kafka_batch.jobs.slow_p1
```

### 3. Wire the YAML into the worker

Via daemon/worker config:

```yaml
# config/daemon.yml
priority_config_paths:
  - config/priority/jobs-fast.yml
  - config/priority/jobs-slow.yml
priority_lag_check_interval: 2      # seconds between Kafka lag checks (default 2)
priority_weighted_interleave: 4     # default interleave when a group omits weighted_interleave
```

or via environment (comma-separated for multiple):

```bash
export KAFKA_BATCH_PRIORITY_CONFIGS="config/priority/jobs-fast.yml,config/priority/jobs-slow.yml"
# single file:
export KAFKA_BATCH_PRIORITY_CONFIG="config/priority/jobs-fast.yml"
```

### Modes

| Mode | Behavior while a higher-rank topic still has lag |
|------|--------------------------------------------------|
| **`strict`** | Lower-rank topics start **no** new jobs until every higher topic is fully drained. |
| **`weighted`** | Lower-rank topics interleave — `1` in every `weighted_interleave` polls proceeds (default 4), so low-priority work still trickles through instead of starving. |

- Lag is read via the Kafka Admin API, rate-limited to one check per `priority_lag_check_interval`. If the cluster is unreachable the gate **fails open** (processes anyway) rather than stalling.
- A higher topic that is **paused** via consumption control (the shared `kafka_batch:consumption:topics` Redis set) is treated as inactive for gating, so lower ranks keep flowing.
- `topic_prefix` / `KAFKA_PREFIX` is applied to priority topic names automatically (list base names).

### Boot rules

- Each topic belongs to **exactly one** priority group (duplicates across files are rejected at load).
- The default flat jobs topic (`kafka_batch.jobs`, or your `jobs_topics`) **cannot** appear in a priority group — that would double-process.
- `mode` must be `strict` or `weighted`; omitted → `weighted`.

### Deployment

Priority runs inside the normal Go worker — the same `kbatch worker` that consumes plain and fair-ready topics also runs every priority group. Just point it at the priority YAML(s):

```bash
kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml
# daemon.yml lists priority_config_paths (or set KAFKA_BATCH_PRIORITY_CONFIGS)
```

To isolate a hot priority group on its own pods, run a second worker deployment whose config lists **only** that group's YAML (and drop those topics from the other deployment's config). Scale in-process members per group with `priority_consumer_concurrency` / `KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY` (default 4). Because each group is its own Kafka consumer group (`<consumer_group>-<suffix>`), you can also scale it horizontally by running more replicas of that deployment.

Mixed runtime: a priority topic, like any execution topic, is **one runtime only**. Ruby `PriorityJobConsumer` and the Go worker must not share a priority topic — split by `runtime` in the manifest.

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

### Cross-runtime contract tests

These guard shared-state contracts that single-runtime tests can't catch (run in PR CI and nightly):

| Test | What it proves |
|------|----------------|
| `TestMatrix_UniqDedupCrossRuntime` | A uniq job enqueued from one runtime dedupes against the other via the shared Redis lock — fingerprints match byte-for-byte, including payloads with `<`, `>`, `&`, and non-ASCII. |
| `TestMatrix_PartitionParity` | Go (franz-go) and Ruby (WaterDrop `murmur2_random`) assign the **same partition** to the same key on a multi-partition topic — the fairness co-partitioning contract. |
| `TestMatrix_ScheduledJobRubyClientGoExec` | A Ruby client's delayed job is picked up by the Go daemon's schedule poller and run by a Go worker — schedule-index parity. |
| `TestMatrix_CancellationCrossRuntime` | A batch cancelled by the Go client is skipped by a Ruby JobConsumer via the shared `kafka_batch:index:cancelled` set. |
| `TestMatrix_DLTExhaustedRubyExec` | A Ruby job retried through the Go control plane and exhausted lands in the dead-letter topic — shared retry-tier + DLT envelope. |
| `TestMatrix_CallbackMessageCrossRuntime` | A batch with a legacy class-string `on_complete` finalized under Go control emits a callback message (with `callback_args`) for the Ruby `CallbackConsumer`. |
| `TestMatrix_ConsumptionPauseCrossRuntime` | A pause written to the shared `kafka_batch:consumption:topics` set is honored by the Go worker; resume drains — the cross-runtime killswitch. |

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

# Terminal A — control (see Setup: config/daemon.yml above for required YAML keys)
kbatch daemon --config config/daemon.yml --manifest config/kafka_batch_handlers.yml

# Terminal B — execution (link your handlers via kbatch.Register in worker main)
kbatch worker --config config/daemon.yml --manifest config/kafka_batch_handlers.yml
```

**Mixed Go control + Ruby execution** — Go daemon owns tier 2 (`-events`, `-retry`, `-dispatch-*`, schedule poller). Ruby Karafka runs **execution groups only** (`-jobs`, `-jobs-fast`, `-jobs-fair-*`) with the same brokers, Redis URL, and manifest. See [Setup: `config/daemon.yml`](#setup-configdaemonyml-go-control-plane).

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
| `KAFKA_BATCH_LIVENESS_TTL` / `liveness_ttl` | Redis EX TTL (seconds) on `kafka_batch:live:consumer:*` heartbeats — default **180**. Pod is considered dead when the key expires. Also used by SuperFetch reclaim. |
| `KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL` / `liveness_heartbeat_interval` | How often (seconds) processes refresh the heartbeat key — default **20** (≈9 misses before TTL expiry). |

Client: `client.DefaultConfig()` + `client.New(cfg)` applies env automatically.  
CLI: `config.LoadDaemon` applies env after YAML parse.

See [`config/daemon.example.yml`](config/daemon.example.yml) and the [Appendix](#appendix-go-only-bootstrap-reference) for the full YAML surface (fairness, schedule, recurring, retry tiers, etc.).

#### MySQL connection strings

`KAFKA_BATCH_SCHEDULE_MYSQL_DSN` and `KAFKA_BATCH_STORE_MYSQL_DSN` (and their YAML
keys `schedule_mysql_dsn` / `store_mysql_dsn`) accept **either** form:

```bash
# 1. Native go-sql-driver DSN
export KAFKA_BATCH_STORE_MYSQL_DSN='dbuser:secret@tcp(mysql:3306)/kafka_batch?parseTime=true&loc=UTC'

# 2. Rails-style URL (as in DATABASE_URL / database.yml)
export KAFKA_BATCH_STORE_MYSQL_DSN='mysql2://dbuser:secret@mysql:3306/kafka_batch?parseTime=true&loc=UTC'
```

The `mysql2://` and `mysql://` URL forms are converted to the driver DSN at connect
time (port defaults to `3306`; query params such as `parseTime`, `loc`, `tls`, and
`charset` are preserved). Use `parseTime=true&loc=UTC` so MySQL timestamps scan into
Go `time.Time` in UTC — the schedule index depends on it.

**Reference an env var from the config (one value, all three roles).** Any value in
`daemon.yml` may contain `${VAR}` or `${VAR:-default}` — expanded from the environment
at load time. Set the connection string once and point every DSN key at it:

```yaml
# daemon.yml
schedule_mysql_dsn: ${KB_MYSQL_URL}
store_mysql_dsn:    ${KB_MYSQL_URL}
```

```bash
export KB_MYSQL_URL='mysql2://dbuser:secret@mysql:3306/kafka_batch?parseTime=true&loc=UTC'
```

The daemon (control) and worker (execution) pick this up when they load `daemon.yml`.
The client — which builds its `Config` in code rather than reading the YAML — expands the
same `${VAR}` refs in `ScheduleMySQLDSN` via `client.ApplyEnv` (called by `client.New`),
so setting `cfg.ScheduleMySQLDSN = "${KB_MYSQL_URL}"` works there too. A bare `$VAR`
(no braces) is intentionally left untouched so a literal `$` in a password survives.

Interpolation is not limited to DSNs — **any** value in `daemon.yml` accepts it. A typical
`redis_url` with a local fallback:

```yaml
# daemon.yml — use REDIS_URL when set, else localhost
redis_url: ${REDIS_URL:-redis://localhost:6379/0}
```

### Schedule poller (delayed jobs — `perform_in` / `perform_at`)

Runs on the **control** tier only (gated by `schedule_poller_enabled`). Each tick claims up
to `schedule_batch_size` due jobs in one query and dispatches them; when a tick finds nothing
due it sleeps and backs off exponentially (with jitter) up to `schedule_poll_max_interval`,
resetting to `schedule_poll_interval` the moment work reappears. **Same keys and defaults as
the Ruby gem**, so the two runtimes are interchangeable.

| YAML key | Env-equivalent* | Default | What it does |
|----------|-----------------|---------|--------------|
| `schedule_poller_enabled` | — | `false` | Enable the delayed-job poller (control tier). |
| `scheduled_topic` | — | `kafka_batch.scheduled` | Durable payload topic for `perform_in`/`perform_at`. |
| `schedule_store` | — | `redis` | Schedule-index backend: `redis` or `mysql`. |
| `schedule_mysql_dsn` | `KAFKA_BATCH_SCHEDULE_MYSQL_DSN` | — | Required when `schedule_store: mysql` (DSN or `mysql2://` URL). |
| `schedule_poll_interval` | — | `5` (sec) | **How often it checks** for due jobs while work is flowing. |
| `schedule_poll_max_interval` | — | `60` (sec) | Cap for exponential backoff while idle. |
| `schedule_poll_jitter` | — | `0.1` | ± fraction on the sleep so pods de-sync (`0` disables). |
| `schedule_batch_size` | — | `100` | **How many jobs fetched per query** (the claim `LIMIT`). |
| `schedule_lease_seconds` | — | `60` (sec) | Lease TTL on a claimed job pointer. |
| `schedule_reclaim_interval` | — | `30` (sec) | How often to reclaim expired leases. |

<sub>*Only the MySQL DSN has a dedicated env override; the rest are set via YAML (use `${VAR}` interpolation if you need them env-driven).</sub>

```yaml
# daemon.yml — control tier
schedule_poller_enabled: true
schedule_store: mysql
schedule_mysql_dsn: ${KB_MYSQL_URL}
schedule_poll_interval: 5      # poll cadence when jobs are flowing
schedule_poll_max_interval: 60 # idle backoff cap
schedule_batch_size: 100       # due jobs claimed per poll
```

> **Runtime parity note:** the Go `DefaultDaemon()` leaves `schedule_poll_jitter` at `0` unless
> set, whereas the Ruby gem defaults it to `0.1`. Set `schedule_poll_jitter: 0.1` explicitly in
> the Go config if you want matching pod de-sync behavior across runtimes.

### Throughput tuning — single Go pod

Go daemon and worker scale **inside one pod** with:

1. **In-process consumer members** — N franz-go clients join the same Kafka consumer group; the broker assigns partitions across them (same as N pods, one OS process).
2. **SuperFetch (worker, always on)** — Redis working-set claim → Kafka offset ack → `#perform` on a goroutine pool. Offsets advance at delivery rate, not perform latency, so **one partition can feed many long jobs** (seconds–30m). On pod death (heartbeat missing past `super_fetch_orphan_grace`) the daemon reclaim loop re-produces orphaned payloads once (`_reclaim: true`); a Redis `work:produced:` marker makes reclaim produce idempotent if Finish fails. Reclaimed messages still claim → mark offset → `#perform` (at-least-once perform is OK).
3. **Consumer fetch limits** — `consumer_fetch_*` caps how much data each poll prefetches from the broker.

**Max concurrent job executions** (worker):

```text
jobs_consumer_members × super_fetch_concurrency
  (+ same formula per fair-ready lane and priority group)
```

Set **member count ≈ topic partition count** for balanced lag. Default **`super_fetch_concurrency` is `10`**; raise it for IO-bound work (Go has true parallelism — see [tuning profiles](#tuning-profiles) below).

#### Control plane (`kbatch daemon`)

| Env variable | YAML key | Default | Used by | What it does |
|--------------|----------|---------|---------|--------------|
| `KAFKA_BATCH_RETRY_MAX_PAUSE` | `retry_max_pause` | `30` (sec) | daemon | Max sleep before re-checking a not-yet-due retry message (Ruby: `retry_max_pause_seconds`). Lower = faster dispatch after `retry_after`. |
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
| `KAFKA_BATCH_SUPER_FETCH_CONCURRENCY` | `super_fetch_concurrency` | `10` | Goroutine pool size **per member** for in-flight performs. Try higher for IO — see [tuning profiles](#tuning-profiles). |
| `KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW` | `super_fetch_claim_window` | `0` → `2×` concurrency | Max Claimed∨Queued∨Performing per member. Claim+ack is gated here (not the perform pool) so rebalance is not held for long `#perform`. Renew starts at Claim. |
| — | `super_fetch_orphan_grace` | `40` | Seconds after claim before a missing heartbeat counts as death (reclaim/steal). |
| `KAFKA_BATCH_CANCELLATION_CACHE_TTL` | `cancellation_cache_ttl` | `120` | Process-local cancelled-batch index TTL (Ruby parity). Workers refresh the Redis ZSET at most once per window — **not** `ZSCORE` per job. Cancellation is eventually consistent until refresh; same-process `CancelBatch` updates the cache immediately. |
| `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS` | `producer_required_acks` | `all_isr` | Same as daemon — event emission after job completion. |

Fairness admission is capped by control-plane `fairness_global_concurrency` (YAML). Raising worker concurrency above what control admits will backlog **ready** topics, not speed up end-to-end.

**Hot tenants (fair ingest):** `fairness_dynamic_tenant_partitions` defaults to **true** so each tenant gets an exclusive ingest partition (avoids hash collisions under partition-serial dispatch). Override whales with `fairness_tenant_partitions`, or set dynamic to `false` for murmur2 key-hash only. Weighted checkout always passes a positive weight-sum hint (`shint`) so Redis does not full-scan the tenant ring.

**Virtual-time (vtime) fairness:** checkout serves the ready tenant with the smallest vtime; a returning idle tenant is re-admitted at the current frontier (`max(its vtime, min ring vtime)`) so it cannot burst ahead of busy tenants. `fairness_reset_vtime_when_idle` (default **true**) clears the vtime ledger (weights preserved) once a lane is fully quiescent — empty ring, no live leases, no pending forwards, zero ingest lag — for `fairness_vtime_idle_reset_debounce` seconds (default 15). This yields fresh fairness per active period and bounds vtime growth; the reset is atomic under a ring-empty guard and never fires mid-run. Mirrors the Ruby gem's behavior.

#### Tuning profiles

**32 partitions per topic** (typical production):

```yaml
# Control — one client per group per pod; the events consumer fans out one
# goroutine per assigned partition. Scale by adding daemon pods and/or partitions.
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

**`super_fetch_concurrency` guidance (Go has true parallelism)**

Defaults start at **`10`**. Because goroutines run in parallel across cores, scale the pool with **CPU cores × per-core target**, then validate under load.

Per **1 CPU core** (multiply by `N` on an `N`-core pod → `N × per-core value`):

| Workload | Per-core `super_fetch_concurrency` | Notes |
|----------|-------------------------------------|--------|
| I/O-bound (HTTP, DB, object storage, seconds–30m) | try **~50** | SuperFetch shines here; Redis owns completion while Kafka offsets advance |
| Light CPU + some waiting | **5–10** (default band) | Good starting point for mixed handlers |
| Very CPU-intensive | **1–2** | Avoid oversubscription; prefer more members/pods if lag grows |

Examples:

- 1-core pod, IO jobs: `super_fetch_concurrency: 50`
- 4-core pod, IO jobs: `super_fetch_concurrency: 200` (`4 × 50`)
- 8-core pod, light CPU: `jobs_consumer_concurrency: 8`, `super_fetch_concurrency: 10` → up to **80** in-flight across members
- 8-core pod, heavy CPU: `super_fetch_concurrency: 8`–`16` (`1`–`2` × cores), not hundreds

Handlers should stay idempotent — reclaim after death can re-run a job.

**Mixed fleets** — separate handler topics / worker deployments with different `super_fetch_concurrency` rather than one global pool size.

#### Quick reference

| Symptom | Likely fix |
|---------|------------|
| Events topic lag (control) | Add events-topic partitions and/or daemon pods (one client fans out per-partition goroutines; more pods = more owned partitions) |
| Retry topic lag (control) | Add retry-topic partitions and/or daemon pods |
| Job topic lag, long/I/O handlers | Raise `KAFKA_BATCH_SUPER_FETCH_CONCURRENCY` and/or members toward partition count |
| Job topic lag, CPU-bound handlers | Raise `KAFKA_BATCH_SUPER_FETCH_CONCURRENCY` (near CPUs) and/or members |
| Fair ready lag, admission OK | Raise `KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY` |
| Priority tier lag | Raise `KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY` |
| Produce latency dominates | Try `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS=leader` (trade durability) |
| Ready topic backlog, ingest fine | Lower worker concurrency or raise `fairness_global_concurrency` on control |
| One partition lags while others idle on same member | Lower `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES` or `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES` |
| Polls return too few records (large messages / WAN) | Raise `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES` and/or `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES` |

## Execution mode: `watermark` (advanced, opt-in)

By default, Go workers execute jobs with **SuperFetch**: Redis tracks every in-flight job in a working set, the Kafka offset is committed *ahead* of `#perform`, and a control-plane reclaim loop re-produces a dead pod's in-flight jobs. That costs ~3 Redis round-trips per job (`Claim` + `StillOwned` + `Complete`) but lets one partition feed many long jobs and survive a crash without re-running the window.

`execution_mode: watermark` is a **Redis-free** alternative for the execution path. Instead of a working set, the worker runs jobs concurrently out of order and commits only the **contiguous completed-offset prefix** per partition (the "watermark"). On crash or rebalance, everything after the last committed watermark is redelivered and re-run. This removes the per-job working-set Redis entirely (no `Claim`/`StillOwned`/`Complete`, no reclaim loop, no `live:consumer` heartbeat), raising throughput — but it is **off by default** and correct only under all of the following:

1. **Idempotent handlers are mandatory.** Watermark re-runs completed-but-uncommitted jobs on any crash or rebalance. A non-idempotent handler on a watermark topic *will* double-apply. (SuperFetch is also at-least-once, but watermark re-runs more — see #2.)
2. **Jobs on a topic must have similar runtimes.** The watermark cannot advance past a still-running job, so one long job holds back every faster job that finished behind it; on crash, that whole finished-but-uncommitted tail re-runs. Uniform durations keep the tail — and duplicate runs — small. Do **not** mix 1-second and 30-minute jobs on a watermark topic; keep those on SuperFetch.
3. **One mode per topic — never mix.** Every worker consuming a topic must use the same `execution_mode`. A topic consumed by both a SuperFetch and a watermark worker has undefined behavior (raced offsets → lost + duplicated jobs). Run watermark topics on their own dedicated worker deployment and ensure no SuperFetch worker subscribes to them. **The operator is responsible for enforcing this** — there is no cross-pod runtime check.

Everything downstream (events, batch counting, retries, DLT, callbacks) is identical to SuperFetch; watermark only changes the durability/offset mechanism. Standalone (non-batch) jobs are fully Redis-free in watermark mode; batched jobs still emit one completion event (the batch ledger is shared state regardless of execution mode).

```yaml
# daemon.yml — applies to every topic this worker consumes
execution_mode: watermark        # superfetch (default) | watermark
super_fetch_concurrency: 50      # reused as the watermark #perform pool size
super_fetch_claim_window: 200    # reused as max dispatched-but-uncommitted per member
                                 # (bounds the pending-completion map and crash re-run count)
```

Env override: `KAFKA_BATCH_EXECUTION_MODE=watermark`. Invalid values are rejected at boot.

**Boot guardrails.** A watermark worker logs a loud banner at startup listing the mode, the topics, and the three requirements above. When the daemon (control plane) config has `execution_mode: watermark`, the workset **reclaim loop is disabled** (it has nothing to reclaim — Kafka offsets own durability) and logs that it did so. Neither guardrail can detect a *sibling* SuperFetch deployment on the same topic, so rule #3 remains the operator's responsibility.

## Operations (daemon / worker)

**Startup:** Redis is pinged before consumers start; unreachable Redis fails fast at boot.

**Consumer resilience:** Each Kafka consumer runs in a supervised loop — broker blips restart that consumer with exponential backoff (1s → 30s) instead of killing the whole process. Handler errors and panics log and skip commit (offset redelivered); panics no longer crash the pod.

**Health probes:** Enable `liveness_enabled: true` (or `KAFKA_BATCH_LIVENESS_ENABLED=true`). `liveness_http_addr` (default `:8080`, or `KAFKA_BATCH_LIVENESS_HTTP_ADDR`) is the address the probe HTTP server binds to; point your Kubernetes probe port at it. `GET /health` and `GET /live` return **503** when any registered consumer group has not polled Kafka within `3 × liveness_heartbeat_interval` (default 60s). A background loop refreshes Redis heartbeats every `liveness_heartbeat_interval` (default 20s) with TTL `liveness_ttl` (default 180s) so CPU-heavy jobs can miss several cycles before SuperFetch reclaim treats the pod as dead. Wire Kubernetes liveness/readiness probes to `/health` with `restartPolicy: Always` so stale consumers trigger a pod restart.

```yaml
liveness_enabled: true
liveness_http_addr: ":8080"
liveness_ttl: 180                    # seconds; Redis TTL on live:consumer:* (env: KAFKA_BATCH_LIVENESS_TTL)
liveness_heartbeat_interval: 20      # seconds between heartbeat refreshes
```

## Wire protocol

JSON job/event fixtures and legacy notes: `protocol/`.

## Ruby compatibility

Cross-runtime integration specs and Ruby itest drivers live under `compat/ruby/`. The matrix harness in `integration/matrix/` is the primary CI gate for mixed deployments — see [Cross-runtime matrix tests](#cross-runtime-matrix-tests) above.

## Appendix: Go-only bootstrap reference

For Go-only (or Go-control) projects: copy the SQL blocks you need into MySQL, then copy the full `daemon.yml` / `kafka_batch_handlers.yml` templates below into your app and trim what you do not use. All YAML keys below match `config.LoadDaemon` / `config.LoadManifest`; values shown are library defaults from `DefaultDaemon()` unless noted.

### MySQL migrations (pick & run)

Run against the DB used by `schedule_mysql_dsn` / `store_mysql_dsn` / `recurring_mysql_dsn` (they may be the same database). Statements are idempotent (`IF NOT EXISTS`).

**A — Delayed-job index** (`schedule_store: mysql`)

```sql
-- kafka_batch_scheduled_jobs — pointer index for perform_in / perform_at
-- (payload lives on kafka_batch.scheduled; this row is partition + offset)
CREATE TABLE IF NOT EXISTS kafka_batch_scheduled_jobs (
  job_id       VARCHAR(36)  NOT NULL,
  run_at       DATETIME(6)  NOT NULL,
  partition_id INT          NOT NULL,
  kafka_offset BIGINT       NOT NULL,
  batch_id     VARCHAR(36)  NULL,
  lease_until  DATETIME(6)  NULL,
  created_at   DATETIME(6)  NOT NULL,
  PRIMARY KEY (job_id),
  KEY idx_kb_scheduled_due (run_at, lease_until),
  KEY idx_kb_scheduled_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**B — Failures + consumption pauses** (`store: mysql`)

```sql
-- kafka_batch_failures — per-job failure log for the dashboard
CREATE TABLE IF NOT EXISTS kafka_batch_failures (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  batch_id      VARCHAR(36)  NOT NULL,
  job_id        VARCHAR(36)  NOT NULL,
  worker_class  VARCHAR(255) NULL,
  error_class   VARCHAR(255) NULL,
  error_message TEXT         NULL,
  attempt       INT          NOT NULL DEFAULT 0,
  status        VARCHAR(20)  NOT NULL DEFAULT 'failed',
  next_retry_at DATETIME(6)  NULL,
  failed_at     DATETIME(6)  NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_kb_failures (batch_id, job_id),
  KEY idx_kb_failures_batch_failed_at (batch_id, failed_at),
  KEY idx_kb_failures_failed_at (failed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- kafka_batch_consumption_pauses — /lag pause state (partition_id = -1 = whole topic)
CREATE TABLE IF NOT EXISTS kafka_batch_consumption_pauses (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  consumer_group VARCHAR(255) NOT NULL,
  topic_name     VARCHAR(255) NOT NULL,
  partition_id   INT          NOT NULL,
  created_at     DATETIME     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_kb_consumption_pauses (consumer_group, topic_name, partition_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**C — Recurring (cron) scheduler** (`recurring_scheduler_enabled: true`)

Also auto-created by `Store.EnsureSchema` when the daemon starts with recurring enabled; prefer running this explicitly in production.

```sql
-- kafka_batch_recurring_schedules — cron definitions
CREATE TABLE IF NOT EXISTS kafka_batch_recurring_schedules (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  name           VARCHAR(191) NOT NULL,
  cron_expr      VARCHAR(120) NOT NULL,
  timezone       VARCHAR(64)  NOT NULL DEFAULT 'UTC',
  job_type       VARCHAR(120) NOT NULL,
  args_json      JSON NULL,
  tenant_id      VARCHAR(120) NULL,
  enabled        TINYINT(1) NOT NULL DEFAULT 1,
  misfire_policy VARCHAR(16) NOT NULL DEFAULT 'fire_once',
  next_run_at    DATETIME NOT NULL,
  last_fire_at   DATETIME NULL,
  created_at     DATETIME NOT NULL,
  updated_at     DATETIME NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_name (name),
  KEY idx_due (enabled, next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- kafka_batch_recurring_fires — fire idempotency ledger
-- PRIMARY KEY (schedule_id, fire_at) makes INSERT IGNORE exactly-once per instant
CREATE TABLE IF NOT EXISTS kafka_batch_recurring_fires (
  schedule_id   BIGINT UNSIGNED NOT NULL,
  fire_at       DATETIME NOT NULL,
  status        VARCHAR(16) NOT NULL DEFAULT 'pending',
  job_id        VARCHAR(191) NULL,
  created_at    DATETIME NOT NULL,
  dispatched_at DATETIME NULL,
  PRIMARY KEY (schedule_id, fire_at),
  KEY idx_pending (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

| Feature | Tables | Config knobs |
|---------|--------|--------------|
| Delayed jobs on MySQL | A | `schedule_store: mysql`, `schedule_mysql_dsn` |
| Durable failures / pauses | B | `store: mysql`, `store_mysql_dsn` |
| Recurring cron | C | `recurring_scheduler_enabled: true`, `recurring_mysql_dsn` (falls back to `schedule_mysql_dsn`) |

Batch ledger / uniq / fair weights stay in **Redis** regardless of MySQL options.

### Full `daemon.yml` (all keys + defaults)

Copy to `config/daemon.yml`. Commented lines show optional keys; uncomment and change when needed. Env vars listed in [Config](#config) still override YAML when set.

```yaml
# ── Kafka & identity ──────────────────────────────────────────────────────────
brokers:
  - localhost:9092
# topic_prefix: ""                       # optional namespace for topics + consumer_group
consumer_group: kafka-batch
# node_id: ""                            # default: hostname#pid — set per replica for fencing
handler_manifest: config/kafka_batch_handlers.yml
# jobs_topics:                           # optional explicit list; usually derived from manifest
#   - kafka_batch.jobs
redis_url: ${REDIS_URL:-redis://localhost:6379/0}

# ── Core topics ───────────────────────────────────────────────────────────────
events_topic: kafka_batch.events
callbacks_topic: kafka_batch.callbacks
dead_letter_topic: kafka_batch.dead_letter
retry_topic: kafka_batch.jobs.retry      # base; tiers become retry.short / .medium / .large

# ── Job retry policy ──────────────────────────────────────────────────────────
max_retries: 7
retry_tiers:
  short: 30                              # seconds
  medium: 420
  large: 1200
retry_max_pause: 30                      # sec — max sleep before re-checking not-yet-due retry
# producer_required_acks: all_isr        # all_isr (default) | leader
# consumer_stall_timeout: 90             # sec — force reconnect when poll loop stalls

# Consumer fetch (daemon + worker)
# consumer_fetch_max_bytes: 1048576                # 1 MiB
# consumer_fetch_max_partition_bytes: 131072       # 128 KiB
# consumer_fetch_max_wait_ms: 200

# ── Worker throughput (kbatch worker) ─────────────────────────────────────────
jobs_consumer_concurrency: 8
fair_ready_consumer_concurrency: 8
priority_consumer_concurrency: 4
super_fetch_concurrency: 10
# super_fetch_claim_window: 0            # 0 → 2× super_fetch_concurrency
# super_fetch_lease_ttl: 120             # sec
# super_fetch_reclaim_interval: 30       # sec
# super_fetch_reclaim_limit: 100
# super_fetch_orphan_grace: 40           # sec
# super_fetch_drain_timeout: 30          # sec
execution_mode: superfetch               # superfetch | watermark (see README)

# ── Delayed jobs (perform_in / perform_at) ────────────────────────────────────
schedule_poller_enabled: false           # enable on scheduler / daemon pods only
scheduled_topic: kafka_batch.scheduled
schedule_store: redis                    # redis | mysql  (run SQL A when mysql)
# schedule_mysql_dsn: ${KB_MYSQL_URL}
schedule_poll_interval: 5                # sec
schedule_poll_max_interval: 60           # sec — idle backoff ceiling
schedule_poll_jitter: 0                  # 0 = off; 0.1 ≈ ±10% de-sync
schedule_batch_size: 100
schedule_lease_seconds: 60
schedule_reclaim_interval: 30            # sec

# ── Recurring cron scheduler ──────────────────────────────────────────────────
recurring_scheduler_enabled: false       # enable on scheduler pods; run SQL C
# recurring_mysql_dsn: ${KB_MYSQL_URL}   # defaults to schedule_mysql_dsn when empty
recurring_window: 30                     # sec — poll / resolution
recurring_lock_ttl: 60                   # sec — Redis leader lease
recurring_batch_size: 100
recurring_misfire_grace: 60              # sec
recurring_max_backfill: 1000
recurring_recover_every: 300             # sec
recurring_recover_grace: 120             # sec
recurring_prune_every: 3600              # sec
recurring_prune_retention: 604800        # sec — 7 days
recurring_heartbeat_every: 60            # sec
recurring_stale_factor: 2.0

# ── Priority YAML groups ──────────────────────────────────────────────────────
# priority_config_paths:
#   - config/priority/jobs-fast.yml
#   - config/priority/jobs-slow.yml
priority_lag_check_interval: 2           # sec
priority_weighted_interleave: 4

# ── Fairness dispatch ─────────────────────────────────────────────────────────
fairness_enabled: false
# Ready topics are always runtime-split (.go / .ruby); no combined fair_*_ready topic.
fairness_time_ingest: kafka_batch.fair_time_ingest
fairness_time_ready_go: kafka_batch.fair_time_ready.go
fairness_time_ready_ruby: kafka_batch.fair_time_ready.ruby
fairness_throughput_ingest: kafka_batch.fair_throughput_ingest
fairness_throughput_ready_go: kafka_batch.fair_throughput_ready.go
fairness_throughput_ready_ruby: kafka_batch.fair_throughput_ready.ruby
fairness_ready_window: 100
fairness_global_concurrency: 50
# fairness_max_inflight_per_tenant: 0    # 0 = dynamic share only
fairness_lease_ttl: 1800                 # sec
fairness_default_weight: 1.0
fairness_weighted_concurrency: true
fairness_active_count_ttl: 5             # sec
fairness_active_count_source: inflight_plus_ready  # | inflight
fairness_reset_vtime_when_idle: true     # clear vtime ledger (weights kept) once a lane is idle
fairness_vtime_idle_reset_debounce: 15   # sec a lane must stay idle before the reset fires
fairness_dynamic_tenant_partitions: true
fairness_tenant_partition_cache_ttl: 30  # sec
# fairness_tenant_partitions:
#   acme: 0
#   globex: 1

# ── Consumption pause/resume (/lag) ───────────────────────────────────────────
consumption_control_refresh_interval: 30 # sec

# ── Batch store / failures ────────────────────────────────────────────────────
store: redis                             # redis | mysql  (run SQL B when mysql)
# store_mysql_dsn: ${KB_MYSQL_URL}
skip_cancelled_jobs: true
cancellation_cache_ttl: 120              # sec

# ── Reconciler ────────────────────────────────────────────────────────────────
reconciliation_interval: 300             # sec
reconciler_lock_ttl: 600                 # sec
max_reconcile_per_run: 100

# ── Liveness HTTP ─────────────────────────────────────────────────────────────
liveness_enabled: false
liveness_http_addr: ":8080"
liveness_ttl: 180                        # sec
liveness_heartbeat_interval: 20          # sec
track_running_jobs: true

# ── Metrics (StatsD) ──────────────────────────────────────────────────────────
metrics_enabled: false
metrics_prefix: kafka_batch
# metrics_statsd_addr: localhost:8125

# ── Performance dashboard (Redis time-series) ─────────────────────────────────
performance_metrics_enabled: false
performance_metrics_retention: 86400     # sec — 24h
performance_metrics_max_job_types: 50
performance_metrics_bucket_seconds: 60
performance_metrics_sample_rate: 1.0
redis_rtt_probe_interval: 15             # sec
redis_rtt_probe_timeout: 0.2             # sec (200ms)

# ── Health alerts (Ruby /alerts UI; shared Redis) ─────────────────────────────
# Daemon hosts the evaluator; worker does not. Redis UI settings win after save.
alerts_enabled: false
# ai_encryption_salt: ""                 # decrypt Slack/webhook/email secrets
# alerts_interval: 60
```

Code-only defaults (not YAML today): `batch_ttl` = 7d, `event_emit_retries` = 3, `event_emit_backoff` = 1s, `retry_jitter` = 0.1.

**Health alerts:** `kbatch daemon` starts `pkg/alerts` (same Redis keys / NX lock as Ruby). Enable via UI `/alerts` or `alerts_enabled` / `KAFKA_BATCH_ALERTS_ENABLED`. Set `ai_encryption_salt` (or `KAFKA_BATCH_AI_ENCRYPTION_SALT`) to use channel secrets saved from the Ruby dashboard. Slack/webhook fire **once per open** and **once per resolve** (no reminder spam; shared with Ruby).

### Full `kafka_batch_handlers.yml` (all handler fields)

Shared with the Ruby gem. One Kafka topic = one `runtime`. Fair handlers omit a plain `topic` (they use lane ingest → ready).

```yaml
# config/kafka_batch_handlers.yml
#
# Per-handler fields:
#   runtime            go | ruby                          (required)
#   worker_class       Ruby constant                      (required when runtime: ruby)
#   topic              Kafka topic for plain jobs         (default: kafka_batch.jobs /
#                                                         topic_prefix.kafka_batch.jobs)
#   apply_topic_prefix true → prepend topic_prefix        (default: false)
#   max_retries        override daemon max_retries        (omit → use daemon default)
#   retry_tier         short | medium | large             (optional pin)
#   fairness_type      time | throughput                  (fair lane; no plain topic)
#   uniq               true → fingerprint dedupe          (default: false)
#
handlers:
  # Plain Go job
  segment.export:
    runtime: go
    topic: segment.exports
    max_retries: 5
    uniq: false

  # Plain Ruby job (consumed by Ruby JobConsumer)
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: kafka_batch.jobs.ruby
    max_retries: 7

  # Fair time-lane Go job (ingest → fair_time_ready.go)
  campaigns.dispatch:
    runtime: go
    fairness_type: time
    max_retries: 3
    uniq: true

  # Fair time-lane Ruby job (ingest → fair_time_ready.ruby)
  segments.recompute:
    runtime: ruby
    worker_class: Segments::RecomputeWorker
    fairness_type: time

  # Fair throughput-lane Go job
  bulk.export:
    runtime: go
    fairness_type: throughput

  # Priority-topic routing (pair with priority_config_paths YAML)
  orders.settle:
    runtime: go
    topic: kafka_batch.jobs.p0
    retry_tier: short

  orders.cleanup:
    runtime: go
    topic: kafka_batch.jobs.p1
    apply_topic_prefix: true             # becomes <topic_prefix>.kafka_batch.jobs.p1
```

After config + tables: create topics (`kbatch topics create --include-control` and/or your harness script), then run `kbatch daemon` + `kbatch worker`.

## Related

- [kafka-batch](https://github.com/y-shashank/kafka-batch) — Ruby gem (client, Karafka control, Karafka `JobConsumer`)
