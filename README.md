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

events_consumer_concurrency: 8
retry_consumer_concurrency: 4
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

Client: `client.DefaultConfig()` + `client.New(cfg)` applies env automatically.  
CLI: `config.LoadDaemon` applies env after YAML parse.

See `config/daemon.example.yml` for the full YAML surface (fairness, schedule, retry tiers, etc.).

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

**Health probes:** Enable `liveness_enabled: true` (or `KAFKA_BATCH_LIVENESS_ENABLED=true`). `liveness_http_addr` (default `:8080`, or `KAFKA_BATCH_LIVENESS_HTTP_ADDR`) is the address the probe HTTP server binds to; point your Kubernetes probe port at it. `GET /health` and `GET /live` return **503** when any registered consumer group has not polled Kafka within `2 × liveness_ttl` (min 60s). Wire Kubernetes liveness/readiness probes to `/health` with `restartPolicy: Always` so stale consumers trigger a pod restart.

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
